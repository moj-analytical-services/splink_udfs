#include "trie/address_lookup.hpp"

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

// Hardening & tuning of skip behavior during a walk:
// - Allow at most a small, fixed number of in-walk skips (token deletions) using
//   a one-token lookahead. This tolerates spurious tokens inside the span.
// - Only allow a skip at nodes with sufficient branching (high local cardinality),
//   to avoid skipping near very specific address parts (house/flat number).
static constexpr uint32_t SKIP_MIN_LOCAL_COUNT = 10; // allow skip only if current node->cnt > 10
static constexpr uint32_t SKIP_MAX_IN_WALK = 2;      // allow up to 2 skips per walk

static constexpr size_t MIN_MATCHED_TOKENS = 2;       // require at least two matched messy tokens
static constexpr uint32_t ENTRY_MIN_LOCAL_COUNT = 10; // seed only from high-fan-out nodes

// Allow seeding the walk from nodes up to K edges below the root (depth-limited entry nodes).
// This permits skipping missing tail tokens from the canonical (trie) side when they are
// absent in the messy input. Example: canonical "1 LOVE LANE KINGS LANGLEY" vs messy
// "1 LOVE LANE KINGS" can match by starting from the child at depth 1 (LANGLEY) before
// consuming any tokens.
static constexpr uint32_t MAX_TRIE_ENTRY_DEPTH = 2; // allows skipping end tokens present only in the trie

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

static inline bool TryAcceptCurrentNode(PNode *node, size_t start_index, size_t tokens_consumed, size_t total_tokens,
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

bool FindAddressExact(const ParsedTrie &trie, const std::vector<std::string> &tokens, uint64_t &uprn_out) {
	if (trie.root == nullptr) {
		return false;
	}
	const size_t N = tokens.size();
	if (N == 0) {
		return false;
	}

	// Precompute entry nodes up to MAX_TRIE_ENTRY_DEPTH edges below the root.
	// We seed walks from each such node before consuming any tokens (to allow
	// missing tail tokens in the messy input).
	std::vector<PNode *> entry_nodes;
	entry_nodes.reserve(8); // small default; expands as needed
	entry_nodes.push_back(trie.root);
	if (MAX_TRIE_ENTRY_DEPTH > 0) {
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

	// Define a reversed view: R[i] = tokens[N - 1 - i]
	// Try starts s in [0, N). For each s, try each entry node seed; for each attempt,
	// greedily walk with up to SKIP_MAX_IN_WALK in-walk skips using one-token lookahead.
	for (size_t s = 0; s < N; ++s) {
		for (PNode *entry : entry_nodes) {
			PNode *node = entry;
			size_t i = s;
			uint32_t skips_used = 0; // allow up to SKIP_MAX_IN_WALK in-walk skips
			if (TryAcceptCurrentNode(node, s, i, N, uprn_out)) {
				return true;
			}
			while (i < N) {
				const std::string &tok = tokens[N - 1 - i];
				PNode *child = FindChild(node, tok);
				if (child != nullptr) {
					node = child;
					i++;
					if (TryAcceptCurrentNode(node, s, i, N, uprn_out)) {
						return true;
					}
					continue;
				}

				// No direct child. Try a lookahead skip for up to (SKIP_MAX_IN_WALK - skips_used) tokens.
				// Skip is allowed only if the LANDING child's count is sufficiently large,
				// to avoid skipping into very specific parts (e.g., house/flat numbers).
				if (skips_used < SKIP_MAX_IN_WALK) {
					const size_t max_lookahead = std::min<size_t>(SKIP_MAX_IN_WALK - skips_used, (N - 1) - i);
					size_t delta = 0;
					PNode *next_child = nullptr;
					for (size_t d = 1; d <= max_lookahead; ++d) {
						const std::string &la = tokens[N - 1 - (i + d)];
						PNode *cand = FindChild(node, la);
						if (cand != nullptr && cand->cnt > SKIP_MIN_LOCAL_COUNT) {
							delta = d; // number of tokens to skip
							next_child = cand;
							break;
						}
					}
					if (next_child != nullptr) {
						skips_used += static_cast<uint32_t>(delta);
						node = next_child;
						i += delta + 1; // skip 'delta' tokens and consume the matched lookahead
						if (TryAcceptCurrentNode(node, s, i, N, uprn_out)) {
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
