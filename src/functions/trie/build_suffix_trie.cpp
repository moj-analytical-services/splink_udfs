#include "duckdb.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "trie/address_trie_functions.hpp"
#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {
// Output is a raw BLOB with a small header; no base64

// ─────────────────────────────────────────────
// Shared reversed-suffix trie (counts on path)
// ─────────────────────────────────────────────
struct TrieNode {
	idx_t cnt = 0;
	unordered_map<string, unique_ptr<TrieNode>> next;
};

static void InsertReversed(TrieNode &root, const vector<string> &toks) {
	root.cnt++; // count root as well
	auto *n = &root;
	for (idx_t i = toks.size(); i > 0; --i) {
		const string &tok = toks[i - 1]; // right→left
		auto &child = n->next[tok];
		if (!child)
			child = make_uniq<TrieNode>();
		n = child.get();
		n->cnt++;
	}
}

// ─────────────────────────────────────────────
// Aggregate state
// ─────────────────────────────────────────────
struct BuildTrieState {
	TrieNode *root;
};

static idx_t StateSize(const AggregateFunction &) {
	return sizeof(BuildTrieState);
}

static void StateInit(const AggregateFunction &, data_ptr_t state) {
	auto *st = reinterpret_cast<BuildTrieState *>(state);
	st->root = new TrieNode();
}

// Per-state destructor: called once per state object
static void TrieStateDestructor(Vector &state, AggregateInputData &, idx_t count) {
	auto state_ptrs = FlatVector::GetData<data_ptr_t>(state);
	for (idx_t i = 0; i < count; i++) {
		auto *st = reinterpret_cast<BuildTrieState *>(state_ptrs[i]);
		if (st && st->root) {
			delete st->root;
			st->root = nullptr;
		}
	}
}

static void StateUpdate(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state, idx_t count) {
	D_ASSERT(input_count == 1);
	auto &list_vec = inputs[0];

	UnifiedVectorFormat list_data;
	list_vec.ToUnifiedFormat(count, list_data);
	auto state_ptrs = FlatVector::GetData<data_ptr_t>(state);

	auto &child_vec = ListVector::GetEntry(list_vec);
	UnifiedVectorFormat child_data;
	child_vec.ToUnifiedFormat(ListVector::GetListSize(list_vec), child_data);
	auto child_vals = UnifiedVectorFormat::GetData<string_t>(child_data);
	auto list_entries = ListVector::GetData(list_vec);

	for (idx_t i = 0; i < count; i++) {
		const auto rid = list_data.sel->get_index(i);
		if (!list_data.validity.RowIsValid(rid))
			continue;

		auto *st = reinterpret_cast<BuildTrieState *>(state_ptrs[i]);
		auto le = list_entries[rid];

		vector<string> toks;
		toks.reserve(le.length);
		for (idx_t k = 0; k < le.length; k++) {
			const auto cidx = child_data.sel->get_index(le.offset + k);
			if (!child_data.validity.RowIsValid(cidx))
				continue;
			toks.emplace_back(child_vals[cidx].GetString());
		}
		if (!toks.empty()) {
			InsertReversed(*st->root, toks);
		}
	}
}

static void MergeTrie(TrieNode &dst, const TrieNode &src) {
	dst.cnt += src.cnt;
	for (auto const &kv : src.next) {
		auto &dchild = dst.next[kv.first];
		if (!dchild)
			dchild = make_uniq<TrieNode>();
		MergeTrie(*dchild, *kv.second);
	}
}

static void StateCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src_ptrs = FlatVector::GetData<data_ptr_t>(source);
	auto dst_ptrs = FlatVector::GetData<data_ptr_t>(target);
	for (idx_t i = 0; i < count; i++) {
		auto *src = reinterpret_cast<BuildTrieState *>(src_ptrs[i]);
		auto *dst = reinterpret_cast<BuildTrieState *>(dst_ptrs[i]);
		if (!src || !src->root || !dst || !dst->root)
			continue;
		MergeTrie(*dst->root, *src->root);
		delete src->root;
		src->root = nullptr;
	}
}

// ─────────────────────────────────────────────
// Binary serialisation (raw BLOB)
// Versioned format to preserve backward compatibility.
// Header:
//   u32 magic 'QCK1' (0x314B4351), u8 flags (always 0)
//
// Node layout (little-endian):
//   u32 cnt, u32 num_children, children...
// ─────────────────────────────────────────────
static inline void W32(vector<uint8_t> &buf, uint32_t v) {
	buf.push_back((uint8_t)(v & 0xFF));
	buf.push_back((uint8_t)((v >> 8) & 0xFF));
	buf.push_back((uint8_t)((v >> 16) & 0xFF));
	buf.push_back((uint8_t)((v >> 24) & 0xFF));
}
static inline void WStr(vector<uint8_t> &buf, const string &s) {
	W32(buf, (uint32_t)s.size());
	buf.insert(buf.end(), s.begin(), s.end());
}
static void SerializeNodeV1(const TrieNode &n, vector<uint8_t> &buf) {
	W32(buf, (uint32_t)n.cnt);
	W32(buf, (uint32_t)n.next.size());

	// collect & sort keys
	vector<pair<string, const TrieNode *>> items;
	items.reserve(n.next.size());
	for (auto &kv : n.next)
		items.emplace_back(kv.first, kv.second.get());
	sort(items.begin(), items.end(),
	     [](const pair<string, const TrieNode *> &a, const pair<string, const TrieNode *> &b) {
		     return a.first < b.first;
	     });

	for (auto &it : items) {
		WStr(buf, it.first);
		SerializeNodeV1(*it.second, buf);
	}
}

static void StateFinalize(Vector &state, AggregateInputData &, Vector &result, idx_t count, idx_t) {
	auto st_ptrs = FlatVector::GetData<data_ptr_t>(state);
	auto out = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto *st = reinterpret_cast<BuildTrieState *>(st_ptrs[i]);
		if (!st || !st->root) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		vector<uint8_t> bin;
		bin.reserve(1024);
		// header magic 'QCK1' and flags = 0x00 (no legacy fields)
		W32(bin, 0x314B4351u);
		bin.push_back(0x00);
		SerializeNodeV1(*st->root, bin);

		out[i] = StringVector::AddString(result, reinterpret_cast<const char *>(bin.data()), bin.size());

		delete st->root;
		st->root = nullptr;
	}
}

AggregateFunctionSet GetBuildSuffixTrieAggregateSet() {
	AggregateFunctionSet set("build_suffix_trie");

	AggregateFunction fn({LogicalType::LIST(LogicalType::VARCHAR)}, LogicalType::BLOB, StateSize, StateInit,
	                     StateUpdate, StateCombine, StateFinalize, FunctionNullHandling::DEFAULT_NULL_HANDLING);
	fn.destructor = TrieStateDestructor;

	set.AddFunction(fn);

	return set;
}

} // namespace duckdb
