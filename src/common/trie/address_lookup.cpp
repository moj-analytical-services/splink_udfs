#include "trie/address_lookup.hpp"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {

// Binary search for a child by token (kids are sorted by token)
static inline PNode *FindChild(PNode *node, const std::string &tok) {
    if (DUCKDB_UNLIKELY(node == nullptr)) {
        return nullptr;
    }
    auto &kids = node->kids;
    auto it = std::lower_bound(kids.begin(), kids.end(), tok,
                               [](const std::pair<std::string, PNode *> &kv, const std::string &s) {
                                   return kv.first < s;
                               });
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
    if (node->term == 0) {
        return false;
    }
    uprn_out = node->uprn; // may be 0 if ambiguous, by design
    return true;
}

} // namespace duckdb
