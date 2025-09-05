#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/blob.hpp"

#include "trie/address_trie_functions.hpp"

// shared components
#include "trie/suffix_trie.hpp"
#include "trie/suffix_trie_cache.hpp"

#include <string>
#include <utility>
#include <vector>
#include <memory>

namespace duckdb {

// Per-connection local state (holds parsed-trie cache)
struct PeelLocalState : public FunctionLocalState {
	TrieCache cache;
	size_t parse_count = 0; // optional debug counter
};

// Bind data (kept for future steps; not used to alter behavior here)
struct PeelBindData : public FunctionData {
	int32_t steps = 4;
	int32_t max_k = 2;
	unique_ptr<FunctionData> Copy() const override {
		auto res = make_uniq<PeelBindData>();
		res->steps = steps;
		res->max_k = max_k;
		return std::move(res);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &o = (const PeelBindData &)other_p;
		return steps == o.steps && max_k == o.max_k;
	}
};

static unique_ptr<FunctionData> PeelBind(ClientContext &, ScalarFunction &, vector<unique_ptr<Expression>> &) {
	return make_uniq<PeelBindData>();
}

static unique_ptr<FunctionLocalState> PeelInitLocalState(ExpressionState &, const BoundFunctionExpression &,
                                                         FunctionData *) {
	return make_uniq<PeelLocalState>();
}

// Helper: resolve (and cache) a parsed trie for a given blob
static inline std::shared_ptr<const ParsedTrie> ResolveTrieFromBlob(PeelLocalState &local, const string_t &blob) {
	auto data_ptr = reinterpret_cast<const uint8_t *>(blob.GetDataUnsafe());
	size_t data_len = static_cast<size_t>(blob.GetSize());
	uint64_t key = FNV1aHash64(data_ptr, data_len);
	auto cached = local.cache.Get(key);
	if (!cached) {
		auto parsed = ParseQCK1(blob);
		if (!parsed) {
			return nullptr;
		}
		cached = std::shared_ptr<const ParsedTrie>(parsed.release());
		local.cache.Put(key, cached);
		local.parse_count++;
	}
	return cached;
}

// One-step peel, max_k = 1
// One-step or multi-step peel with max_k and steps
static void PeelEndTokensExec(DataChunk &args, ExpressionState &state, Vector &result) {
	// args: [0]=LIST<VARCHAR> tokens, [1]=BLOB trie, [2]=steps (opt), [3]=max_k (opt)
	auto &local_state = ExecuteFunctionState::GetFunctionState(state)->Cast<PeelLocalState>();

	// ----- Inputs -----
	Vector &list_vec = args.data[0];
	UnifiedVectorFormat list_uvf;
	list_vec.ToUnifiedFormat(args.size(), list_uvf);
	auto list_entries = ListVector::GetData(list_vec);

	// Child column holding the actual VARCHAR tokens
	auto &in_child = ListVector::GetEntry(list_vec);
	UnifiedVectorFormat child_uvf;
	in_child.ToUnifiedFormat(ListVector::GetListSize(list_vec), child_uvf);
	auto child_vals = UnifiedVectorFormat::GetData<string_t>(child_uvf);

	// Trie blob
	Vector &trie_vec = args.data[1];
	UnifiedVectorFormat trie_uvf;
	trie_vec.ToUnifiedFormat(args.size(), trie_uvf);
	auto trie_vals = UnifiedVectorFormat::GetData<string_t>(trie_uvf);

	// Optional: steps / max_k columns
	const bool has_steps = args.ColumnCount() >= 3;
	const bool has_maxk = args.ColumnCount() >= 4;

	UnifiedVectorFormat steps_uvf;
	UnifiedVectorFormat maxk_uvf;
	const int32_t *steps_data = nullptr;
	const int32_t *maxk_data = nullptr;

	if (has_steps) {
		Vector &steps_vec = args.data[2];
		steps_vec.ToUnifiedFormat(args.size(), steps_uvf);
		steps_data = UnifiedVectorFormat::GetData<int32_t>(steps_uvf);
	}
	if (has_maxk) {
		Vector &maxk_vec = args.data[3];
		maxk_vec.ToUnifiedFormat(args.size(), maxk_uvf);
		maxk_data = UnifiedVectorFormat::GetData<int32_t>(maxk_uvf);
	}

	// Defaults if columns not present or NULL
	static constexpr int32_t DEFAULT_STEPS = 4;
	static constexpr int32_t DEFAULT_MAXK = 2;

	// First pass: compute final kept length per row
	std::vector<idx_t> out_len(args.size(), 0);
	idx_t total_elems = 0;

	// Scratch buffers reused across rows
	std::vector<std::string> toks;
	std::vector<std::string> tail_rev;   // reversed tail_k
	std::vector<std::string> anchor_vec; // size 1 for CountTail({anchor})
	anchor_vec.reserve(1);

	for (idx_t i = 0; i < args.size(); ++i) {
		const auto rid = list_uvf.sel->get_index(i);

		// NULL tokens => NULL out
		if (!list_uvf.validity.RowIsValid(rid)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// Per-row steps / max_k
		int32_t steps_val = DEFAULT_STEPS;
		int32_t maxk_val = DEFAULT_MAXK;
		if (has_steps) {
			const auto sid = steps_uvf.sel->get_index(i);
			if (steps_uvf.validity.RowIsValid(sid)) {
				steps_val = steps_data[sid];
			}
		}
		if (has_maxk) {
			const auto mid = maxk_uvf.sel->get_index(i);
			if (maxk_uvf.validity.RowIsValid(mid)) {
				maxk_val = maxk_data[mid];
			}
		}
		if (steps_val < 0)
			steps_val = 0;
		if (maxk_val < 1)
			maxk_val = 1;

		// Require a trie; if NULL → return NULL (spec)
		const auto trid = trie_uvf.sel->get_index(i);
		if (!trie_uvf.validity.RowIsValid(trid)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		std::shared_ptr<const ParsedTrie> trie_ptr = ResolveTrieFromBlob(local_state, trie_vals[trid]);
		// Treat parse failures as NULL too (garbled BLOB == unusable)
		if (!trie_ptr || !trie_ptr->root) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// Gather valid tokens into a contiguous vector
		auto le = list_entries[rid];
		toks.clear();
		toks.reserve(le.length);
		for (idx_t k = 0; k < le.length; ++k) {
			const auto cidx = child_uvf.sel->get_index(le.offset + k);
			if (!child_uvf.validity.RowIsValid(cidx))
				continue;
			toks.emplace_back(child_vals[cidx].GetString());
		}

		// If no trie or too few tokens, nothing to peel
		if (!trie_ptr || !trie_ptr->root || toks.size() < 2 || steps_val == 0) {
			out_len[i] = static_cast<idx_t>(toks.size());
			total_elems += out_len[i];
			continue;
		}

		// ---- Iterative peel (up to steps) ----
		// At each step, try k from min(max_k, n-1) .. 1; peel at first gain.
		for (int s = 0; s < steps_val; ++s) {
			if (toks.size() < 2)
				break;

			const idx_t n = static_cast<idx_t>(toks.size());
			idx_t try_maxk = std::min<idx_t>(static_cast<idx_t>(maxk_val), n - 1);

			bool peeled_this_step = false;

			for (idx_t k = try_maxk; k >= 1; --k) {
				// anchor at n-k-1, tail_k = toks[n-k..n-1]
				const idx_t anchor_idx = n - k - 1;
				// (anchor_idx >= 0 guaranteed by k <= n-1)
				const std::string &anchor = toks[anchor_idx];

				// CountTail({anchor})
				anchor_vec.clear();
				anchor_vec.emplace_back(anchor);
				const uint32_t c_anchor = CountTail(*trie_ptr, anchor_vec);

				// reversed tail_k : toks[n-1]..toks[n-k]
				tail_rev.clear();
				tail_rev.reserve(k);
				for (idx_t t = 0; t < k; ++t) {
					tail_rev.emplace_back(toks[n - 1 - t]);
				}

				// Build combo_rev = reversed(tail_k) + [anchor]
				// Reuse tail_rev by pushing anchor at the end
				tail_rev.emplace_back(anchor);
				const uint32_t c_combo = CountTail(*trie_ptr, tail_rev);
				tail_rev.pop_back(); // restore tail_rev (not strictly needed after this)

				if (c_anchor > c_combo) {
					// Peel k tokens from the end
					toks.resize(n - k);
					peeled_this_step = true;
					break;
				}
			}

			if (!peeled_this_step) {
				break; // stable
			}
		}

		out_len[i] = static_cast<idx_t>(toks.size());
		total_elems += out_len[i];
	}

	// Second pass: write LIST(VARCHAR) with first out_len[i] tokens of each row
	auto &out_child = ListVector::GetEntry(result);
	ListVector::Reserve(result, total_elems);
	auto res_entries = ListVector::GetData(result);
	auto child_out = FlatVector::GetData<string_t>(out_child);

	idx_t cur = 0;
	for (idx_t i = 0; i < args.size(); ++i) {
		if (FlatVector::IsNull(result, i))
			continue;

		const auto rid = list_uvf.sel->get_index(i);
		auto le = list_entries[rid];

		res_entries[i].offset = cur;
		res_entries[i].length = out_len[i];

		// Emit first out_len[i] valid tokens
		idx_t written = 0;
		for (idx_t k = 0; k < le.length && written < out_len[i]; ++k) {
			const auto cidx = child_uvf.sel->get_index(le.offset + k);
			if (!child_uvf.validity.RowIsValid(cidx))
				continue;
			const auto s = child_vals[cidx].GetString();
			child_out[cur + written] = StringVector::AddString(out_child, s);
			written++;
		}
		cur += out_len[i];
	}
	ListVector::SetListSize(result, cur);
}

ScalarFunctionSet GetPeelEndTokensFunctionSet() {
	ScalarFunctionSet set("peel_end_tokens");

	const LogicalType tokens_type = LogicalType::LIST(LogicalType::VARCHAR);

	ScalarFunction f1({tokens_type, LogicalType::BLOB}, tokens_type, PeelEndTokensExec, PeelBind);
	f1.init_local_state = PeelInitLocalState;
	set.AddFunction(f1);

	ScalarFunction f2({tokens_type, LogicalType::BLOB, LogicalType::INTEGER}, tokens_type, PeelEndTokensExec, PeelBind);
	f2.init_local_state = PeelInitLocalState;
	set.AddFunction(f2);

	ScalarFunction f3({tokens_type, LogicalType::BLOB, LogicalType::INTEGER, LogicalType::INTEGER}, tokens_type,
	                  PeelEndTokensExec, PeelBind);
	f3.init_local_state = PeelInitLocalState;
	set.AddFunction(f3);

	return set;
}

} // namespace duckdb
