#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"

#include "trie/address_trie_functions.hpp"
#include "trie/suffix_trie.hpp"
#include "trie/suffix_trie_cache.hpp"
#include "trie/trie_cache_utils.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

// -----------------------------------------------------------------------------
// Local state (separate from find_address on purpose)
// -----------------------------------------------------------------------------
struct FindCandidatesLocalState : public FunctionLocalState {
	TrieCache cache;
};

static unique_ptr<FunctionLocalState> FindCandidatesInitLocal(ExpressionState & /*state*/,
                                                              const BoundFunctionExpression & /*expr*/,
                                                              FunctionData * /*bind_data*/) {
	return make_uniq<FindCandidatesLocalState>();
}

// Centralize local state access for portability across DuckDB versions.
static inline FindCandidatesLocalState &GetFindCandidatesLocal(ExpressionState &state) {
	auto ptr = ExecuteFunctionState::GetFunctionState(state);
	D_ASSERT(ptr);
	return ptr->Cast<FindCandidatesLocalState>();
}

// -----------------------------------------------------------------------------
// Re-implementation of the walk logic (no shared helpers with find_address)
// -----------------------------------------------------------------------------

// Hardening & tuning (same constants as find_address)
static constexpr uint32_t SKIP_MIN_LOCAL_COUNT = 10;
static constexpr uint32_t SKIP_MAX_IN_WALK = 2;

static constexpr size_t MIN_MATCHED_TOKENS = 2;
static constexpr uint32_t ENTRY_MIN_LOCAL_COUNT = 10;

static constexpr uint32_t MAX_TRIE_ENTRY_DEPTH = 2;

// Binary search for a child by token (kids are sorted by token)
static inline PNode *FindChild(PNode *node, const std::string &tok) {
	if (node == nullptr) {
		return nullptr;
	}
	auto &kids = node->kids;
	auto it =
	    std::lower_bound(kids.begin(), kids.end(), tok,
	                     [](const std::pair<std::string, PNode *> &kv, const std::string &s) { return kv.first < s; });
	if (it == kids.end()) {
		return nullptr;
	}
	if (it->first != tok) {
		return nullptr;
	}
	return it->second;
}

// Descend deterministically from a node whose subtree represents exactly one address.
// Returns the sole terminal node with term == 1, or nullptr if the subtree is malformed.
static inline PNode *ResolveUniqueTerminal(PNode *node) {
	if (node == nullptr) {
		return nullptr;
	}
	PNode *curr = node;
	while (curr != nullptr) {
		if (curr->term == 1) {
			return curr;
		}
		PNode *next = nullptr;
		for (const auto &kv : curr->kids) {
			PNode *child = kv.second;
			if (child == nullptr || child->cnt == 0) {
				continue;
			}
			if (next != nullptr) {
				return nullptr; // more than one viable child -> not unique
			}
			next = child;
		}
		curr = next;
	}
	return nullptr;
}

// Try to accept as "exact" under the same rules as find_address
static inline bool TryAcceptExact(PNode *node, size_t start_index, size_t tokens_consumed, size_t total_tokens,
                                  uint64_t &uprn_out) {
	if (node == nullptr) {
		return false;
	}
	const size_t matched = tokens_consumed > start_index ? (tokens_consumed - start_index) : 0;
	if (matched < MIN_MATCHED_TOKENS) {
		return false;
	}
	if (node->cnt == 1) {
		PNode *terminal = ResolveUniqueTerminal(node);
		if (terminal != nullptr && terminal->term == 1) {
			uprn_out = terminal->uprn;
			return true;
		}
	}
	if (node->term == 1) {
		const bool is_leaf = node->kids.empty();
		if (tokens_consumed == total_tokens || is_leaf) {
			uprn_out = node->uprn;
			return true;
		}
	}
	return false;
}

// Collect all UPRNs reachable from this node (terminal nodes only)
static void CollectUPRNs(const PNode *node, std::vector<uint64_t> &out) {
	if (node == nullptr) {
		return;
	}
	if (node->term == 1) {
		out.push_back(node->uprn);
	}
	for (const auto &kv : node->kids) {
		const PNode *child = kv.second;
		if (child != nullptr) {
			CollectUPRNs(child, out);
		}
	}
}

// A small record for the trace: which token we matched, and the node->cnt after stepping
struct TraceItem {
	std::string token;
	uint64_t cnt;
};

// Run the greedy walk (with entry-node seeding and bounded lookahead skips)
// Return either:
//  - early "exact" (uprn is set), or
//  - the "best attempt" node + trace we reached (most tokens consumed)
struct WalkResult {
	bool exact = false;
	uint64_t exact_uprn = 0;

	PNode *final_node = nullptr;       // best node reached
	size_t best_consumed = 0;          // number of matched tokens on best path
	bool tokens_exhausted = false;     // whether that path consumed all tokens
	std::vector<TraceItem> best_trace; // token/count pairs for the best path
};

static WalkResult WalkBest(PNode *root, const std::vector<std::string> &tokens) {
	WalkResult wr;
	if (root == nullptr || tokens.empty()) {
		return wr;
	}
	const size_t N = tokens.size();

	// Precompute entry nodes up to MAX_TRIE_ENTRY_DEPTH edges below the root.
	std::vector<PNode *> entry_nodes;
	entry_nodes.reserve(8);
	entry_nodes.push_back(root);
	if (MAX_TRIE_ENTRY_DEPTH > 0) {
		struct StackItem {
			PNode *node;
			uint32_t depth;
		};
		std::vector<StackItem> stack;
		stack.push_back(StackItem {root, 0});
		while (!stack.empty()) {
			StackItem it = stack.back();
			stack.pop_back();
			if (it.depth == MAX_TRIE_ENTRY_DEPTH) {
				continue;
			}
			for (const auto &kv : it.node->kids) {
				PNode *child = kv.second;
				if (child != nullptr) {
					if (child->cnt >= ENTRY_MIN_LOCAL_COUNT) {
						entry_nodes.push_back(child);
					}
					stack.push_back(StackItem {child, it.depth + 1});
				}
			}
		}
	}

	// Try starts s in [0, N)
	for (size_t s = 0; s < N; ++s) {
		for (PNode *entry : entry_nodes) {
			PNode *node = entry;
			size_t i = s;
			uint32_t skips_used = 0;

			std::vector<TraceItem> trace;
			trace.reserve(8);

			// Early check in case entry node itself already "accepts" (rare)
			uint64_t early_uprn = 0;
			if (TryAcceptExact(node, s, i, N, early_uprn)) {
				wr.exact = true;
				wr.exact_uprn = early_uprn;
				wr.final_node = node;
				wr.best_consumed = i - s;
				wr.tokens_exhausted = (i == N);
				wr.best_trace = std::move(trace);
				return wr;
			}

			while (i < N) {
				const std::string &tok = tokens[N - 1 - i];
				PNode *child = FindChild(node, tok);
				if (child != nullptr) {
					node = child;
					++i;
					trace.push_back(TraceItem {tok, static_cast<uint64_t>(node->cnt)});

					uint64_t uprn = 0;
					if (TryAcceptExact(node, s, i, N, uprn)) {
						wr.exact = true;
						wr.exact_uprn = uprn;
						wr.final_node = node;
						wr.best_consumed = i - s;
						wr.tokens_exhausted = (i == N);
						wr.best_trace = std::move(trace);
						return wr;
					}
					continue;
				}

				// No direct child. Try a lookahead skip for up to (SKIP_MAX_IN_WALK - skips_used) tokens.
				if (skips_used < SKIP_MAX_IN_WALK) {
					const size_t max_lookahead = std::min<size_t>(SKIP_MAX_IN_WALK - skips_used, (N - 1) - i);
					size_t delta = 0;
					PNode *next_child = nullptr;
					for (size_t d = 1; d <= max_lookahead; ++d) {
						const std::string &la = tokens[N - 1 - (i + d)];
						PNode *cand = FindChild(node, la);
						if (cand != nullptr && cand->cnt > SKIP_MIN_LOCAL_COUNT) {
							delta = d;
							next_child = cand;
							break;
						}
					}
					if (next_child != nullptr) {
						skips_used += static_cast<uint32_t>(delta);
						node = next_child;
						i += delta + 1;
						const std::string &matched = tokens[N - 1 - (i - 1)];
						trace.push_back(TraceItem {matched, static_cast<uint64_t>(node->cnt)});

						uint64_t uprn2 = 0;
						if (TryAcceptExact(node, s, i, N, uprn2)) {
							wr.exact = true;
							wr.exact_uprn = uprn2;
							wr.final_node = node;
							wr.best_consumed = i - s;
							wr.tokens_exhausted = (i == N);
							wr.best_trace = std::move(trace);
							return wr;
						}
						continue;
					}
				}

				// Mismatch and no permissible skip -> stop this walk
				break;
			}

			// Record "best attempt" so far (prefer more tokens consumed; tie-break by smaller cnt i.e. more specific)
			const size_t consumed = i > s ? (i - s) : 0;
			if (consumed > wr.best_consumed) {
				wr.best_consumed = consumed;
				wr.final_node = node;
				wr.tokens_exhausted = i >= N;
				wr.best_trace = std::move(trace);
			} else if (consumed == wr.best_consumed) {
				const uint64_t curr_cnt = node != nullptr ? static_cast<uint64_t>(node->cnt) : UINT64_MAX;
				const uint64_t best_cnt =
				    wr.final_node != nullptr ? static_cast<uint64_t>(wr.final_node->cnt) : UINT64_MAX;
				if (curr_cnt < best_cnt) {
					wr.final_node = node;
					wr.tokens_exhausted = i >= N;
					wr.best_trace = std::move(trace);
				}
			}
		}
	}

	return wr;
}

// -----------------------------------------------------------------------------
// DuckDB scalar: (LIST<VARCHAR> tokens, BLOB trie_blob) -> STRUCT(
//    uprns:  LIST<BIGINT>,
//    status: VARCHAR,                          -- 'exact' | 'impossible' | 'ambiguous'
//    tokens: LIST(STRUCT(token VARCHAR, cnt BIGINT))
// )
// -----------------------------------------------------------------------------
static void FindCandidatesScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto &list_vec = args.data[0];
	auto &blob_vec = args.data[1];

	const idx_t count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &entries = StructVector::GetEntries(result);
	Vector &uprns_vec = *entries[0];
	Vector &status_vec = *entries[1];
	Vector &trace_vec = *entries[2];

	Vector &uprns_child = ListVector::GetEntry(uprns_vec);
	Vector &trace_child = ListVector::GetEntry(trace_vec);
	auto &trace_struct_children = StructVector::GetEntries(trace_child);
	Vector &trace_tok_vec = *trace_struct_children[0];
	Vector &trace_cnt_vec = *trace_struct_children[1];

	auto uprns_entries = ListVector::GetData(uprns_vec);
	auto trace_entries = ListVector::GetData(trace_vec);
	auto status_out = FlatVector::GetData<string_t>(status_vec);
	auto uprns_out_child = FlatVector::GetData<int64_t>(uprns_child);
	auto trace_tok_out = FlatVector::GetData<string_t>(trace_tok_vec);
	auto trace_cnt_out = FlatVector::GetData<int64_t>(trace_cnt_vec);

	idx_t uprns_offset = 0;
	idx_t trace_offset = 0;

	UnifiedVectorFormat list_data;
	list_vec.ToUnifiedFormat(count, list_data);
	auto list_entries_in = ListVector::GetData(list_vec);
	auto &child_vec = ListVector::GetEntry(list_vec);
	UnifiedVectorFormat child_data;
	child_vec.ToUnifiedFormat(ListVector::GetListSize(list_vec), child_data);
	auto child_vals = UnifiedVectorFormat::GetData<string_t>(child_data);

	UnifiedVectorFormat blob_data;
	blob_vec.ToUnifiedFormat(count, blob_data);
	auto blob_vals = UnifiedVectorFormat::GetData<string_t>(blob_data);

	auto &lstate = GetFindCandidatesLocal(state);

	for (idx_t row = 0; row < count; ++row) {
		const auto list_rid = list_data.sel->get_index(row);
		const auto blob_rid = blob_data.sel->get_index(row);

		if (!list_data.validity.RowIsValid(list_rid) || !blob_data.validity.RowIsValid(blob_rid)) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		const auto le = list_entries_in[list_rid];
		if (le.length == 0) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		const auto blob = blob_vals[blob_rid];
		auto parsed = GetOrParseTrie(lstate.cache, blob);
		if (!parsed || !parsed->root) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		std::vector<std::string> toks;
		toks.reserve(le.length);
		for (idx_t k = 0; k < le.length; ++k) {
			const idx_t cidx = child_data.sel->get_index(le.offset + k);
			if (!child_data.validity.RowIsValid(cidx)) {
				continue;
			}
			toks.emplace_back(child_vals[cidx].GetString());
		}
		if (toks.empty()) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		WalkResult wr = WalkBest(parsed->root, toks);

		std::string status_str;
		std::vector<uint64_t> uprns;
		uprns.reserve(8);

		if (wr.exact) {
			status_str = "exact";
			uprns.push_back(wr.exact_uprn);
		} else {
			if (wr.final_node != nullptr) {
				CollectUPRNs(wr.final_node, uprns);
			}
			if (wr.best_consumed == 0) {
				status_str = "ambiguous";
			} else {
				status_str = "impossible";
			}
		}

		FlatVector::SetNull(result, row, false);
		status_out[row] = StringVector::AddString(status_vec, status_str);

		const idx_t num_uprns = static_cast<idx_t>(uprns.size());
		if (num_uprns > 0) {
			ListVector::Reserve(uprns_vec, uprns_offset + num_uprns);
			for (idx_t i = 0; i < num_uprns; ++i) {
				uprns_out_child[uprns_offset + i] = static_cast<int64_t>(uprns[static_cast<size_t>(i)]);
			}
			uprns_entries[row].offset = uprns_offset;
			uprns_entries[row].length = num_uprns;
			uprns_offset += num_uprns;
		} else {
			uprns_entries[row].offset = uprns_offset;
			uprns_entries[row].length = 0;
		}

		const idx_t num_steps = static_cast<idx_t>(wr.best_trace.size());
		if (num_steps > 0) {
			ListVector::Reserve(trace_vec, trace_offset + num_steps);
			for (idx_t i = 0; i < num_steps; ++i) {
				const auto &ti = wr.best_trace[static_cast<size_t>(i)];
				trace_tok_out[trace_offset + i] = StringVector::AddString(trace_tok_vec, ti.token);
				trace_cnt_out[trace_offset + i] = static_cast<int64_t>(ti.cnt);
			}
			trace_entries[row].offset = trace_offset;
			trace_entries[row].length = num_steps;
			trace_offset += num_steps;
		} else {
			trace_entries[row].offset = trace_offset;
			trace_entries[row].length = 0;
		}
	}

	ListVector::SetListSize(uprns_vec, uprns_offset);
	ListVector::SetListSize(trace_vec, trace_offset);
}

ScalarFunction GetFindCandidatesFunction() {
	child_list_t<LogicalType> trace_struct_children;
	trace_struct_children.emplace_back("token", LogicalType::VARCHAR);
	trace_struct_children.emplace_back("cnt", LogicalType::BIGINT);
	LogicalType trace_elem = LogicalType::STRUCT(std::move(trace_struct_children));

	child_list_t<LogicalType> out_children;
	out_children.emplace_back("uprns", LogicalType::LIST(LogicalType::BIGINT));
	out_children.emplace_back("status", LogicalType::VARCHAR);
	out_children.emplace_back("tokens", LogicalType::LIST(std::move(trace_elem)));
	LogicalType out_struct = LogicalType::STRUCT(std::move(out_children));

	ScalarFunction fn("find_candidates", {LogicalType::LIST(LogicalType::VARCHAR), LogicalType::BLOB}, out_struct,
	                  FindCandidatesScalar);
	fn.init_local_state = FindCandidatesInitLocal;
	return fn;
}

} // namespace duckdb
