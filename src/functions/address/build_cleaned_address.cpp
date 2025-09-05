#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

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
struct CleanAddrLocalState : public FunctionLocalState {
	TrieCache cache;
	size_t parse_count = 0; // optional debug counter
};

static unique_ptr<FunctionLocalState> CleanAddrInitLocalState(ExpressionState &, const BoundFunctionExpression &,
                                                              FunctionData *) {
	return make_uniq<CleanAddrLocalState>();
}

// Helper: resolve (and cache) a parsed trie for a given blob
static inline std::shared_ptr<const ParsedTrie> ResolveTrieFromBlob(CleanAddrLocalState &local, const string_t &blob) {
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

// build_cleaned_address(tokens, trie, drop_above_count, joiner)
// Updated semantics:
//   - Always keep at least the first 3 tokens (from the leaf side). If there are fewer than 3 tokens, keep all.
//   - For k = 1..n, let cnt_k = CountTail(tail_k) where tail_k = last k tokens (i.e., suffix of length k).
//   - Find the FIRST k such that cnt_k >= drop_above_count (the first time the suffix meets/exceeds the threshold).
//       * Normally: keep tokens up to and including the boundary token at index (n - k), and DROP all tokens after it.
//       * Exception: if cnt_k >= 4 * drop_above_count (a "very high" count), DO NOT include the boundary token
//         (i.e., keep only tokens strictly before index (n - k)).
//   - In all cases, enforce the "keep at least 3 tokens" rule on the final result.
//   - If no such k exists (no suffix meets/exceeds threshold), keep all tokens.
// Notes:
//   - If drop_above_count <= 0, the threshold condition is trivially true at k=1; the "very high" check is only
//     applied when drop_above_count > 0 to avoid degenerate behavior.
// Null policy (unchanged):
//   - NULL tokens/trie/threshold -> NULL; empty list -> "".
static void BuildCleanedAddressExec(DataChunk &args, ExpressionState &state, Vector &result) {
	// args: [0]=LIST<VARCHAR> tokens, [1]=BLOB trie, [2]=INTEGER drop_above_count, [3]=VARCHAR joiner
	auto &local_state = ExecuteFunctionState::GetFunctionState(state)->Cast<CleanAddrLocalState>();

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

	// Threshold
	Vector &thr_vec = args.data[2];
	UnifiedVectorFormat thr_uvf;
	thr_vec.ToUnifiedFormat(args.size(), thr_uvf);
	auto thr_vals = UnifiedVectorFormat::GetData<int32_t>(thr_uvf);

	// Joiner
	Vector &join_vec = args.data[3];
	UnifiedVectorFormat join_uvf;
	join_vec.ToUnifiedFormat(args.size(), join_uvf);
	auto join_vals = UnifiedVectorFormat::GetData<string_t>(join_uvf);

	auto out = FlatVector::GetData<string_t>(result);

	// Scratch buffers reused across rows
	std::vector<std::string> toks;     // valid tokens in order
	std::vector<std::string> tail_rev; // reversed suffix for CountTail

	for (idx_t i = 0; i < args.size(); ++i) {
		const auto rid = list_uvf.sel->get_index(i);

		// NULL tokens => NULL out
		if (!list_uvf.validity.RowIsValid(rid)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// Require a trie; if NULL → return NULL
		const auto trid = trie_uvf.sel->get_index(i);
		if (!trie_uvf.validity.RowIsValid(trid)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// Require threshold; if NULL → return NULL
		const auto tid = thr_uvf.sel->get_index(i);
		if (!thr_uvf.validity.RowIsValid(tid)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		int32_t threshold = thr_vals[tid];
		if (threshold < 0) {
			threshold = 0; // clamp
		}

		// Joiner: if NULL, treat as single space (typical)
		std::string joiner_str;
		const auto jid = join_uvf.sel->get_index(i);
		if (join_uvf.validity.RowIsValid(jid)) {
			joiner_str = join_vals[jid].GetString();
		} else {
			joiner_str = " ";
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
			if (!child_uvf.validity.RowIsValid(cidx)) {
				continue;
			}
			toks.emplace_back(child_vals[cidx].GetString());
		}

		// Empty list -> empty string
		const idx_t n = static_cast<idx_t>(toks.size());
		if (n == 0) {
			out[i] = StringVector::AddString(result, "");
			continue;
		}

		// Determine how many tokens to keep using the new rules
		// Minimum tokens to keep regardless of threshold logic
		const idx_t min_keep = n < 3 ? n : static_cast<idx_t>(3);

		idx_t keep_end = n; // default: keep all tokens
		bool found = false;
		bool hit = false;

		// Walk from LEAF outward: check prefix tails toks[0..k-1]

		for (idx_t start = 0; start < n; ++start) {
			// tail = toks[start..n-1]  (build reversed order for CountTail)
			tail_rev.clear();
			tail_rev.reserve(n - start);
			for (idx_t t = n; t-- > start;) { // t = n-1, n-2, ..., start
				tail_rev.emplace_back(toks[t]);
			}

			const uint32_t cnt = CountTail(*trie_ptr, tail_rev);
			if (static_cast<int64_t>(cnt) >= static_cast<int64_t>(threshold)) {
				const idx_t boundary_idx = start; // the FIRST token whose suffix meets threshold
				const bool very_high =
				    (threshold > 0) && (static_cast<uint64_t>(cnt) >= static_cast<uint64_t>(threshold) * 4ULL);

				// Normally include boundary; if very high, exclude it
				idx_t candidate_keep_end = very_high ? boundary_idx : (boundary_idx + 1);

				// Enforce "always keep at least 3 tokens (from the leaf)"
				if (candidate_keep_end < min_keep) {
					candidate_keep_end = min_keep;
				}

				keep_end = candidate_keep_end;
				hit = true;
				break; // stop at the FIRST token above threshold
			}
		}

		if (!hit && keep_end < min_keep) {
			keep_end = min_keep;
		}

		size_t total_len = 0;
		if (keep_end > 0) {
			for (idx_t j = 0; j < keep_end; ++j) {
				total_len += toks[j].size();
			}
			if (keep_end >= 2) {
				total_len += (keep_end - 1) * joiner_str.size();
			}
		}

		std::string out_str;
		out_str.reserve(total_len);
		for (idx_t j = 0; j < keep_end; ++j) {
			if (j > 0) {
				out_str += joiner_str;
			}
			out_str += toks[j];
		}

		out[i] = StringVector::AddString(result, out_str);
	} // end for rows i
} // end BuildCleanedAddressExec

ScalarFunctionSet GetBuildCleanedAddressFunctionSet() {
	ScalarFunctionSet set("build_cleaned_address");

	const LogicalType tokens_type = LogicalType::LIST(LogicalType::VARCHAR);
	// Keep the 4-arg form you’re using: (tokens, trie, drop_above_count, joiner)
	ScalarFunction f({tokens_type, LogicalType::BLOB, LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                 BuildCleanedAddressExec);
	f.init_local_state = CleanAddrInitLocalState;
	set.AddFunction(f);

	// Optional convenience: 3-arg form with default joiner = ' '
	{
		ScalarFunction f3({tokens_type, LogicalType::BLOB, LogicalType::INTEGER}, LogicalType::VARCHAR,
		                  BuildCleanedAddressExec);
		f3.init_local_state = CleanAddrInitLocalState;
		set.AddFunction(f3);
	}

	return set;
}

} // namespace duckdb
