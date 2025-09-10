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

// Binary search for a child by token (kids are sorted by token)
static inline PNode *FindChild(PNode *node, const std::string &tok) {
	if (DUCKDB_UNLIKELY(node == nullptr)) {
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
	if (tokens.empty()) {
		return false;
	}
	PNode *node = trie.root;
	// Walk tokens from right to left to match reversed-suffix trie
	for (size_t i = tokens.size(); i > 0; --i) {
		const auto &tok = tokens[i - 1];
		node = FindChild(node, tok);
		if (node == nullptr) {
			return false;
		}
	}

	// Succeed only if the final node is a single exact terminal.
	//   - term == 1  → uprn is meaningful (may legitimately be 0)
	//   - term == 0  → non-terminal; uprn is 0 and ignored
	//   - term > 1   → ambiguous terminal; uprn is 0 and ignored
	if (node->term != 1) {
		return false; // caller should treat as no exact match
	}
	uprn_out = node->uprn;
	return true;
}

} // namespace duckdb
