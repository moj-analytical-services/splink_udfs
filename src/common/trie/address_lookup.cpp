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
	// Try starts s in [0, N). For each s, greedily walk with up to
	// SKIP_MAX_IN_WALK in-walk skips using a one-token lookahead on mismatch.
	for (size_t s = 0; s < N; ++s) {
		PNode *node = trie.root;
		size_t i = s;
		uint32_t skips_used = 0; // allow up to SKIP_MAX_IN_WALK in-walk skips
        while (i < N) {
            const std::string &tok = tokens[N - 1 - i];
            PNode *child = FindChild(node, tok);
            if (child != nullptr) {
                node = child;
                i++;
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
