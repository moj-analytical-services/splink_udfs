#include "trie/address_lookup.hpp"
#include "trie/address_match_params.hpp"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

// Note the data structure for the trie
// (7,  '5 LOVE LANE KINGS LANGLEY'),
// (8,  '6 LOVE LANE KINGS LANGLEY'),
// (9,  '7 LOVE LANE KINGS LANGLEY'),
// (10,  'ANNEX 7 LOVE LANE KINGS LANGLEY'),
// (11, 'BUSINESS NAME 9 LOVE LANE KINGS LANGLEY');
//
// when deserialised from the QCK2 blob using Python is:
///
// {'cnt': 5,
//  'term': 0,
//  'uprn': 0,
//  'kids': [('LANGLEY',
//    {'cnt': 5,
//     'term': 0,
//     'uprn': 0,
//     'kids': [('KINGS',
//       {'cnt': 5,
//        'term': 0,
//        'uprn': 0,
//        'kids': [('LANE',
//          {'cnt': 5,
//           'term': 0,
//           'uprn': 0,
//           'kids': [('LOVE',
//             {'cnt': 5,
//              'term': 0,
//              'uprn': 0,
//              'kids': [('5', {'cnt': 1, 'term': 1, 'uprn': 7, 'kids': []}),
//               ('6', {'cnt': 1, 'term': 1, 'uprn': 8, 'kids': []}),
//               ('7',
//                {'cnt': 2,
//                 'term': 1,
//                 'uprn': 9,
//                 'kids': [('ANNEX',
//                   {'cnt': 1, 'term': 1, 'uprn': 10, 'kids': []})]}),
//               ('9',
//                {'cnt': 1,
//                 'term': 0,
//                 'uprn': 0,
//                 'kids': [('NAME',
//                   {'cnt': 1,
//                    'term': 0,
//                    'uprn': 0,
//                    'kids': [('BUSINESS',
//                      {'cnt': 1,
//                       'term': 1,
//                       'uprn': 11,
//                       'kids': []})]})]})]})]})]})]})]}

namespace duckdb {

// Hardening & tuning of skip behavior during a walk is controlled by AddressMatchParams.
// - Allow at most params.skip_max_in_walk in-walk skips using a one-token lookahead.
// - Only allow a skip at nodes with sufficient branching (high local cardinality),
//   controlled by params.skip_min_local_count, to avoid skipping near specific parts.
// - Seed entry nodes up to params.max_trie_entry_depth edges below the root when
//   their local count exceeds params.entry_min_local_count.
// - Ignore up to params.max_trailing_tokens_ignored trailing input tokens before
//   starting the walk.

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
				return nullptr;
			}
			next = child;
		}
		curr = next;
	}
	return nullptr;
}

static inline bool TryAcceptCurrentNode(const AddressMatchParams &params, PNode *node, size_t start_index,
                                        size_t tokens_consumed, size_t total_tokens, uint64_t &uprn_out) {
	if (node == nullptr) {
		return false;
	}
	const size_t matched = tokens_consumed > start_index ? (tokens_consumed - start_index) : 0;
	if (matched < static_cast<size_t>(params.min_matched_tokens)) {
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

bool FindAddressExact(const ParsedTrie &trie, const std::vector<std::string> &tokens, const AddressMatchParams &params,
                      uint64_t &uprn_out) {
	if (trie.root == nullptr) {
		return false;
	}
	const size_t N = tokens.size();
	if (N == 0) {
		return false;
	}

	// Precompute entry nodes up to params.max_trie_entry_depth edges below the root.
	// We seed walks from each such node before consuming any tokens (to allow
	// missing tail tokens in the messy input).
	std::vector<PNode *> entry_nodes;
	entry_nodes.reserve(8); // small default; expands as needed
	entry_nodes.push_back(trie.root);
	if (params.max_trie_entry_depth > 0) {
		// Simple depth-limited DFS from root
		struct StackItem {
			PNode *node;
			uint32_t depth;
		};
		std::vector<StackItem> stack;
		stack.push_back(StackItem {trie.root, 0});
		while (!stack.empty()) {
			StackItem it = stack.back();
			stack.pop_back();
			if (it.depth == params.max_trie_entry_depth) {
				continue;
			}
			for (const auto &kv : it.node->kids) {
				PNode *child = kv.second;
				if (child != nullptr) {
					if (child->cnt >= params.entry_min_local_count) {
						entry_nodes.push_back(child);
					}
					stack.push_back(StackItem {child, it.depth + 1});
				}
			}
		}
	}

	// Define a reversed view: R[i] = tokens[N - 1 - i]
	// Try starts s in [0, min(N - 1, params.max_trailing_tokens_ignored)].
	// For each s, try each entry-node seed; for each attempt, greedily walk
	// with up to params.skip_max_in_walk in-walk skips using one-token lookahead.

	const size_t max_start =
	    std::min<size_t>(static_cast<size_t>(params.max_trailing_tokens_ignored), N > 0 ? N - 1 : 0);
	for (size_t s = 0; s <= max_start; ++s) {
		for (PNode *entry : entry_nodes) {
			PNode *node = entry;
			size_t i = s;
			uint32_t skips_used = 0; // track how many in-walk skips were used
			bool anchored = false;   // has the first real token matched yet?
			if (TryAcceptCurrentNode(params, node, s, i, N, uprn_out)) {
				return true;
			}
			while (i < N) {
				const std::string &tok = tokens[N - 1 - i];
				PNode *child = FindChild(node, tok);
				if (child != nullptr) {
					node = child;
					i++;
					anchored = true; // <-- first real token matched; allow skips later
					if (TryAcceptCurrentNode(params, node, s, i, N, uprn_out)) {
						return true;
					}
					continue;
				}

				// No direct child. Try a lookahead skip for up to (params.skip_max_in_walk - skips_used) tokens.
				// Skip is allowed only if the LANDING child's count is sufficiently large,
				// to avoid skipping into very specific parts (e.g., house/flat numbers).
				if (skips_used < params.skip_max_in_walk) {
					const uint32_t remaining_skips = params.skip_max_in_walk - skips_used;
					size_t max_lookahead = std::min<size_t>(static_cast<size_t>(remaining_skips), (N - 1) - i);
					// If trailing tokens were ignored (s > 0), do NOT allow a skip
					// for the very first token we try to match: force a direct anchor.
					if (!anchored && s > 0) {
						max_lookahead = 0;
					}
					size_t delta = 0;
					PNode *next_child = nullptr;
					for (size_t d = 1; d <= max_lookahead; ++d) {
						const std::string &la = tokens[N - 1 - (i + d)];
						PNode *cand = FindChild(node, la);
						if (cand != nullptr && cand->cnt > params.skip_min_local_count) {
							delta = d; // number of tokens to skip
							next_child = cand;
							break;
						}
					}
					if (next_child != nullptr) {
						skips_used += static_cast<uint32_t>(delta);
						node = next_child;
						i += delta + 1;  // skip 'delta' tokens and consume the matched lookahead
						anchored = true; // we're anchored after this hop
						if (TryAcceptCurrentNode(params, node, s, i, N, uprn_out)) {
							return true;
						}
						continue;
					}
				}

				// Mismatch and no permissible skip -> stop this walk
				break;
			}
		}
	}
	return false;
}

} // namespace duckdb
