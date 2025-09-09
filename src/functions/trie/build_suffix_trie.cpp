#include "duckdb.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "trie/address_trie_functions.hpp"
#include "trie/suffix_trie.hpp"
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
    uint32_t term = 0;
    uint64_t uprn = 0; // valid iff term == 1
    unordered_map<string, unique_ptr<TrieNode>> next;
};

static void InsertReversed(TrieNode &root, const vector<string> &toks, uint64_t uprn_val) {
    root.cnt++; // count root as well
    auto *n = &root;
    for (idx_t i = toks.size(); i > 0; --i) {
        const string &tok = toks[i - 1]; // right→left
        auto &child = n->next[tok];
        if (!child) {
            child = make_uniq<TrieNode>();
        }
        n = child.get();
        n->cnt++;
    }
    n->term++;
    if (n->term == 1) {
        n->uprn = uprn_val;
    } else {
        n->uprn = 0; // ambiguous terminal
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
    D_ASSERT(input_count == 2);
    auto &uprn_vec = inputs[0];
    auto &list_vec = inputs[1];

    UnifiedVectorFormat uprn_data;
    uprn_vec.ToUnifiedFormat(count, uprn_data);
    auto uprn_vals = UnifiedVectorFormat::GetData<int64_t>(uprn_data);

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
        if (!list_data.validity.RowIsValid(rid)) {
            continue;
        }

        const auto uprn_rid = uprn_data.sel->get_index(i);
        if (!uprn_data.validity.RowIsValid(uprn_rid)) {
            continue;
        }

        auto *st = reinterpret_cast<BuildTrieState *>(state_ptrs[i]);
        auto le = list_entries[rid];

        vector<string> toks;
        toks.reserve(le.length);
        for (idx_t k = 0; k < le.length; k++) {
            const auto cidx = child_data.sel->get_index(le.offset + k);
            if (!child_data.validity.RowIsValid(cidx)) {
                continue;
            }
            toks.emplace_back(child_vals[cidx].GetString());
        }
        if (!toks.empty()) {
            const auto uprn_val = static_cast<uint64_t>(uprn_vals[uprn_rid]);
            InsertReversed(*st->root, toks, uprn_val);
        }
    }
}

// Variant: only token list provided; use uprn=0 for all entries
static void StateUpdateListOnly(Vector inputs[], AggregateInputData &aid, idx_t input_count, Vector &state,
                                idx_t count) {
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
        if (!list_data.validity.RowIsValid(rid)) {
            continue;
        }

        auto *st = reinterpret_cast<BuildTrieState *>(state_ptrs[i]);
        auto le = list_entries[rid];

        vector<string> toks;
        toks.reserve(le.length);
        for (idx_t k = 0; k < le.length; k++) {
            const auto cidx = child_data.sel->get_index(le.offset + k);
            if (!child_data.validity.RowIsValid(cidx)) {
                continue;
            }
            toks.emplace_back(child_vals[cidx].GetString());
        }
        if (!toks.empty()) {
            InsertReversed(*st->root, toks, 0 /*uprn*/);
        }
    }
}

static void MergeTrie(TrieNode &dst, const TrieNode &src) {
    dst.cnt += src.cnt;
    // merge terminal metadata
    const uint64_t dst_uprn_before = dst.uprn;
    dst.term += src.term;
    if (dst.term == 0) {
        dst.uprn = 0;
    } else if (dst.term == 1) {
        // exactly one terminal across both sides; pick the non-zero one
        dst.uprn = dst_uprn_before != 0 ? dst_uprn_before : src.uprn;
    } else {
        dst.uprn = 0; // ambiguous terminal
    }

    for (auto const &kv : src.next) {
        auto &dchild = dst.next[kv.first];
        if (!dchild) {
            dchild = make_uniq<TrieNode>();
        }
        MergeTrie(*dchild, *kv.second);
    }
}

static void StateCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto src_ptrs = FlatVector::GetData<data_ptr_t>(source);
	auto dst_ptrs = FlatVector::GetData<data_ptr_t>(target);
	for (idx_t i = 0; i < count; i++) {
		auto *src = reinterpret_cast<BuildTrieState *>(src_ptrs[i]);
		auto *dst = reinterpret_cast<BuildTrieState *>(dst_ptrs[i]);
		if (!src || !src->root || !dst || !dst->root) {
			continue;
		}
		MergeTrie(*dst->root, *src->root);
		delete src->root;
		src->root = nullptr;
	}
}

// ─────────────────────────────────────────────
// Binary serialisation (raw BLOB)
// QCK2 format
// Header:
//   u32 magic 'QCK2' (0x324B4351), u8 flags (always 0)
//
// Node layout (little-endian):
//   u32 cnt, u32 term, u64 uprn, u32 num_children, children...
// ─────────────────────────────────────────────
static inline void W32(vector<uint8_t> &buf, uint32_t v) {
	buf.push_back((uint8_t)(v & 0xFF));
	buf.push_back((uint8_t)((v >> 8) & 0xFF));
	buf.push_back((uint8_t)((v >> 16) & 0xFF));
	buf.push_back((uint8_t)((v >> 24) & 0xFF));
}
static inline void W64(vector<uint8_t> &buf, uint64_t v) {
    W32(buf, static_cast<uint32_t>(v & 0xFFFFFFFFULL));
    W32(buf, static_cast<uint32_t>(v >> 32));
}
static inline void WStr(vector<uint8_t> &buf, const string &s) {
	W32(buf, (uint32_t)s.size());
	buf.insert(buf.end(), s.begin(), s.end());
}
static void SerializeNodeQCK2(const TrieNode &n, vector<uint8_t> &buf) {
    W32(buf, static_cast<uint32_t>(n.cnt));
    W32(buf, static_cast<uint32_t>(n.term));
    W64(buf, static_cast<uint64_t>(n.uprn));

    // collect & sort keys
    vector<pair<string, const TrieNode *>> items;
    items.reserve(n.next.size());
    for (auto &kv : n.next) {
        items.emplace_back(kv.first, kv.second.get());
    }

    sort(items.begin(), items.end(),
         [](const pair<string, const TrieNode *> &a, const pair<string, const TrieNode *> &b) {
             return a.first < b.first;
         });

    W32(buf, static_cast<uint32_t>(items.size()));
    for (auto &it : items) {
        WStr(buf, it.first);
        SerializeNodeQCK2(*it.second, buf);
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
		// header magic 'QCK2' and flags = 0x00 (no legacy fields)
		W32(bin, QCK2_MAGIC);
		bin.push_back(0x00);
		SerializeNodeQCK2(*st->root, bin);

		out[i] = StringVector::AddString(result, reinterpret_cast<const char *>(bin.data()), bin.size());

		delete st->root;
		st->root = nullptr;
	}
}

AggregateFunctionSet GetBuildSuffixTrieAggregateSet() {
    AggregateFunctionSet set("build_suffix_trie");

    AggregateFunction fn({LogicalType::BIGINT, LogicalType::LIST(LogicalType::VARCHAR)}, LogicalType::BLOB, StateSize,
                         StateInit, StateUpdate, StateCombine, StateFinalize,
                         FunctionNullHandling::DEFAULT_NULL_HANDLING);
    fn.destructor = TrieStateDestructor;

    set.AddFunction(fn);

    // Overload: list-only variant
    AggregateFunction fn_list_only({LogicalType::LIST(LogicalType::VARCHAR)}, LogicalType::BLOB, StateSize, StateInit,
                                   StateUpdateListOnly, StateCombine, StateFinalize,
                                   FunctionNullHandling::DEFAULT_NULL_HANDLING);
    fn_list_only.destructor = TrieStateDestructor;
    set.AddFunction(fn_list_only);

    return set;
}

} // namespace duckdb
