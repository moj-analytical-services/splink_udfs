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

// Hardening: only allow an in-walk skip when we are at a node with
// sufficient branching (i.e., many addresses share this context). This avoids
// skipping in the most specific parts near the start of an address (house/flat).
static constexpr uint32_t SKIP_MIN_LOCAL_COUNT = 15; // allow skip only if current node->cnt > 15

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

bool FindAddressExact(const ParsedTrie &trie, const std::vector<std::string> &tokens, uint64_t &uprn_out) {
	if (trie.root == nullptr) {
		return false;
	}
	const size_t N = tokens.size();
	if (N == 0) {
		return false;
	}

	// Define a reversed view: R[i] = tokens[N - 1 - i]
	// Try starts s in [0, N). For each s, greedily walk with at most one
	// in-walk skip using a one-token lookahead on mismatch.
	for (size_t s = 0; s < N; ++s) {
		PNode *node = trie.root;
		size_t i = s;
		bool skipped = false; // allow at most one in-walk skip
		while (i < N) {
			const std::string &tok = tokens[N - 1 - i];
			PNode *child = FindChild(node, tok);
			if (child != nullptr) {
				node = child;
				i++;
				continue;
			}

			// No direct child. Try a single skip iff the next token matches a child
			if (!skipped && (i + 1) < N && node->cnt > SKIP_MIN_LOCAL_COUNT) {
				const std::string &lookahead = tokens[N - 1 - (i + 1)];
				PNode *child2 = FindChild(node, lookahead);
				if (child2 != nullptr) {
					skipped = true; // consume lookahead, effectively skipping R[i]
					node = child2;
					i += 2;
					continue;
				}
			}

			// Mismatch and no permissible skip -> stop this walk
			break;
		}

		// Acceptance:
		// Succeed only if the final node is a single exact terminal.
		//   - term == 1  → uprn is meaningful (may legitimately be 0)
		//   - term == 0  → non-terminal; uprn is 0 and ignored
		//   - term > 1   → ambiguous terminal; uprn is 0 and ignored
		// Additionally, allow early acceptance at terminal leaves:
		//   - Terminal leaf (no children): accept even if tokens remain (i < N)
		//   - Terminal non-leaf: accept only if we consumed all tokens (i == N)
		if (node->term == 1) {
			const bool is_leaf = node->kids.empty();
			if (is_leaf || i == N) {
				uprn_out = node->uprn;
				return true;
			}
		}
		// else: no terminal or ambiguous; try next start
	}
	return false;
}

} // namespace duckdb
