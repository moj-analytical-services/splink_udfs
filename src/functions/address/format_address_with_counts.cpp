#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

#include "trie/address_trie_functions.hpp"

// shared components
#include "trie/suffix_trie.hpp"
#include "trie/suffix_trie_cache.hpp"
#include "trie/trie_cache_utils.hpp"

#include <string>
#include <utility>
#include <vector>
#include <memory>

namespace duckdb {

// Per-connection local state (holds parsed-trie cache)
struct FormatCountsLocalState : public FunctionLocalState {
	TrieCache cache;
	size_t parse_count = 0; // optional debug counter
};

static unique_ptr<FunctionLocalState> FormatCountsInitLocalState(ExpressionState &, const BoundFunctionExpression &,
                                                                 FunctionData *) {
	return make_uniq<FormatCountsLocalState>();
}

// Parsing now centralized in trie_cache_utils

// format_address_with_counts(tokens, trie, joiner=' -> ')
// Example: ['40','AVERILL','STREET','LONDON']
//  -> "40 (1) -> AVERILL (20) -> STREET (20) -> LONDON (100)"
// NULL tokens/trie -> NULL; empty list -> ""
static void FormatAddressWithCountsExec(DataChunk &args, ExpressionState &state, Vector &result) {
	// args: [0]=LIST<VARCHAR> tokens, [1]=BLOB trie, [2]=VARCHAR joiner (optional)
	auto &local = ExecuteFunctionState::GetFunctionState(state)->Cast<FormatCountsLocalState>();

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

	// Optional joiner
	const bool has_joiner = args.ColumnCount() >= 3;
	UnifiedVectorFormat join_uvf;
	const string_t *join_vals = nullptr;
	if (has_joiner) {
		Vector &join_vec = args.data[2];
		join_vec.ToUnifiedFormat(args.size(), join_uvf);
		join_vals = UnifiedVectorFormat::GetData<string_t>(join_uvf);
	}

	auto out = FlatVector::GetData<string_t>(result);

	// Scratch buffers reused across rows
	std::vector<std::string> toks;     // valid tokens in order
	std::vector<std::string> rev_toks; // reversed tokens (rightmost first)
	std::vector<uint32_t> counts;      // counts per token position

	for (idx_t i = 0; i < args.size(); ++i) {
		// NULL tokens -> NULL
		const auto rid = list_uvf.sel->get_index(i);
		if (!list_uvf.validity.RowIsValid(rid)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// NULL trie -> NULL
		const auto trid = trie_uvf.sel->get_index(i);
		if (!trie_uvf.validity.RowIsValid(trid)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// Joiner (default ' -> ')
		std::string joiner = " -> ";
		if (has_joiner) {
			const auto jid = join_uvf.sel->get_index(i);
			if (join_uvf.validity.RowIsValid(jid)) {
				joiner = join_vals[jid].GetString();
			}
		}

		auto trie_ptr = GetOrParseTrie(local.cache, trie_vals[trid]);
		if (!trie_ptr || !trie_ptr->root) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// Gather valid tokens into contiguous vector
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

		const idx_t n = static_cast<idx_t>(toks.size());
		if (n == 0) {
			out[i] = StringVector::AddString(result, "");
			continue;
		}

		// Build reversed tokens once
		rev_toks.clear();
		rev_toks.reserve(n);
		for (idx_t k = 0; k < n; ++k) {
			rev_toks.emplace_back(toks[n - 1 - k]);
		}

		// Walk the trie once along rev_toks to get counts for each suffix:
		// For step t (0..n-1) on rev_toks: we advance one token deeper; the node's cnt is the suffix count
		// Map that to the forward index: idx = n - 1 - t.
		counts.assign(n, 0);
		const PNode *node = trie_ptr->root;
		bool path_broken = false;
		for (idx_t t = 0; t < n; ++t) {
			if (path_broken || !node) {
				// Once the path is broken, all remaining suffixes are 0
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
			// suffix starting at index idx has count = node->cnt
			const idx_t idx = n - 1 - t;
			counts[idx] = node->cnt;
		}
		// Any earlier indices not reached remain 0 (unknown path => 0)

		// Compose "TOKEN (count)" joined by joiner
		// Pre-size output buffer for fewer reallocations
		size_t total_len = 0;
		for (idx_t j = 0; j < n; ++j) {
			total_len += toks[j].size();
			// rough space for " (digits)"
			uint32_t c = counts[j];
			size_t digits = (c == 0) ? 1 : 0;
			while (c > 0) {
				digits++;
				c /= 10;
			}
			total_len += 3 + digits; // " (" + digits + ")"
			if (j + 1 < n) {
				total_len += joiner.size();
			}
		}

		std::string out_str;
		out_str.reserve(total_len);
		for (idx_t j = 0; j < n; ++j) {
			if (j > 0) {
				out_str += joiner;
			}
			out_str += toks[j];
			out_str += " (";
			out_str += std::to_string(counts[j]);
			out_str += ")";
		}

		out[i] = StringVector::AddString(result, out_str);
	}
}

ScalarFunctionSet GetFormatAddressWithCountsFunctionSet() {
	ScalarFunctionSet set("format_address_with_counts");

	const LogicalType tokens_type = LogicalType::LIST(LogicalType::VARCHAR);

	// 2-arg: (tokens, trie)
	{
		ScalarFunction f({tokens_type, LogicalType::BLOB}, LogicalType::VARCHAR, FormatAddressWithCountsExec);
		f.init_local_state = FormatCountsInitLocalState;
		set.AddFunction(f);
	}

	// 3-arg: (tokens, trie, joiner)
	{
		ScalarFunction f({tokens_type, LogicalType::BLOB, LogicalType::VARCHAR}, LogicalType::VARCHAR,
		                 FormatAddressWithCountsExec);
		f.init_local_state = FormatCountsInitLocalState;
		set.AddFunction(f);
	}

	return set;
}

} // namespace duckdb
