// src/functions/address/build_cleaned_address.cpp
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
#include <algorithm>

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

/*
build_cleaned_address(tokens, trie, drop_above_count[, strip_redundant_count_one_tokens])

Changes vs previous version:
  - The joiner argument has been removed; output is always joined with a single space " ".
  - New optional boolean argument strip_redundant_count_one_tokens (default FALSE):
      * Compute suffix counts for each token position j as CountTail(tokens[j..end]).
      * If the leading run (from the leaf side: tokens[0], tokens[1], ...) has count==1,
        drop ALL of those except the one closest to the root (i.e., keep only the last in that run).
        - Example counts: [1,1,1,1,1,10,10,...]  -> drop first 4, keep from index 4 onward.
        - If all tokens are count==1, keep only the last (root-nearest) token.
      * After this pre-strip, apply the existing threshold rules and then enforce "keep at least 3 tokens"
        on the shortened list.

Existing threshold rules (unchanged):
  - Let cnt_k = CountTail(tail_k) where tail_k is the suffix starting at index k.
  - Find the FIRST boundary index 'start' such that cnt_start >= drop_above_count.
      • Normally keep tokens up to and including the boundary token (keep_end = start + 1).
      • If cnt_start >= 4 * drop_above_count (and drop_above_count > 0), exclude the boundary token
        (keep_end = start).
  - Enforce "keep at least 3 tokens (from the leaf side)" after all logic.

Null/edge policy (unchanged from prior):
  - NULL tokens/trie/threshold -> NULL
  - Empty token list -> "" (empty string)
*/
static void BuildCleanedAddressExec(DataChunk &args, ExpressionState &state, Vector &result) {
	// args:
	//   [0] = LIST<VARCHAR> tokens
	//   [1] = BLOB trie
	//   [2] = INTEGER drop_above_count
	//   [3] = (optional) BOOLEAN strip_redundant_count_one_tokens
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

	// Optional strip flag
	const bool has_strip_flag = args.ColumnCount() >= 4;
	UnifiedVectorFormat flag_uvf;
	const bool *flag_vals = nullptr;
	if (has_strip_flag) {
		Vector &flag_vec = args.data[3];
		flag_vec.ToUnifiedFormat(args.size(), flag_uvf);
		flag_vals = UnifiedVectorFormat::GetData<bool>(flag_uvf);
	}

	auto out = FlatVector::GetData<string_t>(result);

	// Scratch buffers reused across rows
	std::vector<std::string> toks;     // valid tokens in order
	std::vector<std::string> rev_toks; // reversed tokens (rightmost first)
	std::vector<uint32_t> counts;      // counts per token position (for suffix at that index)
	std::vector<std::string> tail_rev; // reversed suffix for CountTail during threshold step

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

		// Optional flag: default FALSE
		bool strip_redundant = false;
		if (has_strip_flag) {
			const auto fid = flag_uvf.sel->get_index(i);
			if (flag_uvf.validity.RowIsValid(fid)) {
				strip_redundant = flag_vals[fid];
			}
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
		if (toks.empty()) {
			out[i] = StringVector::AddString(result, "");
			continue;
		}

		// ---------------------------------------------------------------------
		// Precompute suffix counts per token position (single pass along path)
		// counts[j] = CountTail(tokens[j..end])
		// ---------------------------------------------------------------------
		const idx_t n = static_cast<idx_t>(toks.size());
		rev_toks.clear();
		rev_toks.reserve(n);
		for (idx_t k = 0; k < n; ++k) {
			rev_toks.emplace_back(toks[n - 1 - k]);
		}

		counts.assign(n, 0);
		{
			const PNode *node = trie_ptr->root;
			bool path_broken = false;
			for (idx_t t = 0; t < n; ++t) {
				if (path_broken || !node) {
					break;
				}
				const auto &kids = node->kids;
				const std::string &tok = rev_toks[t];

				// binary search in sorted children
				size_t lo = 0, hi = kids.size();
				bool found = false;
				while (lo < hi) {
					size_t mid = (lo + hi) / 2;
					int cmp = tok.compare(kids[mid].first);
					if (cmp == 0) {
						node = kids[mid].second;
						found = true;
						break;
					}
					if (cmp < 0) {
						hi = mid;
					} else {
						lo = mid + 1;
					}
				}
				if (!found) {
					path_broken = true;
					break;
				}
				const idx_t idx = n - 1 - t; // forward index
				counts[idx] = node->cnt;
			}
		}

		// ---------------------------------------------------------------------
		// (NEW) Optional pre-strip of leading count==1 tokens
		// Keep only the last token in the leading run of 1's (closest to root).
		// If no leading 1's or only one, do nothing. If all are 1, keep last.
		// ---------------------------------------------------------------------
		idx_t start_idx = 0;
		if (strip_redundant) {
			idx_t lead_ones = 0;
			while (lead_ones < n && counts[lead_ones] == 1) {
				lead_ones++;
			}
			if (lead_ones >= 2) {
				start_idx = lead_ones - 1; // keep the last '1' in the leading run
			} else if (lead_ones == n) {
				// all ones -> keep only the last
				start_idx = n - 1;
			}
		}

		// Build working token view after pre-strip
		std::vector<std::string> work_toks;
		work_toks.assign(toks.begin() + start_idx, toks.end());
		const idx_t wn = static_cast<idx_t>(work_toks.size());

		// If nothing left (paranoia), produce empty string
		if (wn == 0) {
			out[i] = StringVector::AddString(result, "");
			continue;
		}

		// ---------------------------------------------------------------------
		// Threshold logic (unchanged), now applied to work_toks
		// ---------------------------------------------------------------------
		// Minimum tokens to keep regardless of threshold logic
		const idx_t min_keep = wn < 3 ? wn : static_cast<idx_t>(3);

		idx_t keep_end = wn; // default: keep all tokens

		// Walk from LEAF outward: check suffixes work_toks[start..wn-1]
		for (idx_t start = 0; start < wn; ++start) {
			// Build reversed tail for CountTail once per candidate start
			tail_rev.clear();
			tail_rev.reserve(wn - start);
			for (idx_t t = wn; t-- > start;) { // t = wn-1, wn-2, ..., start
				tail_rev.emplace_back(work_toks[t]);
			}

			const uint32_t cnt = CountTail(*trie_ptr, tail_rev);
			if (static_cast<int64_t>(cnt) >= static_cast<int64_t>(threshold)) {
				const bool very_high =
				    (threshold > 0) && (static_cast<uint64_t>(cnt) >= static_cast<uint64_t>(threshold) * 4ULL);

				// Normally include boundary; if very high, exclude it
				idx_t candidate_keep_end = very_high ? start : (start + 1);

				// Enforce "always keep at least 3 tokens (from the leaf)"
				if (candidate_keep_end < min_keep) {
					candidate_keep_end = min_keep;
				}

				keep_end = candidate_keep_end;
				break; // stop at the FIRST token above threshold
			}
		}

		// If no boundary hit and keep_end < min_keep (shouldn't happen), enforce min_keep
		if (keep_end < min_keep) {
			keep_end = min_keep;
		}

		// ---------------------------------------------------------------------
		// Join the first keep_end tokens with a single space
		// ---------------------------------------------------------------------
		size_t total_len = 0;
		for (idx_t j = 0; j < keep_end; ++j) {
			total_len += work_toks[j].size();
		}
		if (keep_end >= 2) {
			total_len += (keep_end - 1); // one space between tokens
		}

		std::string out_str;
		out_str.reserve(total_len);
		for (idx_t j = 0; j < keep_end; ++j) {
			if (j > 0) {
				out_str.push_back(' ');
			}
			out_str += work_toks[j];
		}

		out[i] = StringVector::AddString(result, out_str);
	} // end for rows i
} // end BuildCleanedAddressExec

ScalarFunctionSet GetBuildCleanedAddressFunctionSet() {
	ScalarFunctionSet set("build_cleaned_address");

	const LogicalType tokens_type = LogicalType::LIST(LogicalType::VARCHAR);

	// 3-arg form: (tokens, trie, drop_above_count)
	{
		ScalarFunction f({tokens_type, LogicalType::BLOB, LogicalType::INTEGER}, LogicalType::VARCHAR,
		                 BuildCleanedAddressExec);
		f.init_local_state = CleanAddrInitLocalState;
		set.AddFunction(f);
	}

	// 4-arg form: (tokens, trie, drop_above_count, strip_redundant_count_one_tokens)
	{
		ScalarFunction f4({tokens_type, LogicalType::BLOB, LogicalType::INTEGER, LogicalType::BOOLEAN},
		                  LogicalType::VARCHAR, BuildCleanedAddressExec);
		f4.init_local_state = CleanAddrInitLocalState;
		set.AddFunction(f4);
	}

	return set;
}

} // namespace duckdb
