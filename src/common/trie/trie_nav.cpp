#include "trie/trie_nav.hpp"

namespace duckdb {

const PNode *FindChild(const PNode *node, const std::string &token) {
    if (node == nullptr) {
        return nullptr;
    }
    const auto &kids = node->kids;
    size_t lo = 0;
    size_t hi = kids.size();
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        int cmp = token.compare(kids[mid].first);
        if (cmp == 0) {
            return kids[mid].second;
        }
        if (cmp < 0) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return nullptr;
}

const PNode *FindChild(const PNode &node, const std::string &token) {
    return FindChild(&node, token);
}

bool HasChild(const PNode *node, const std::string &token) {
    return FindChild(node, token) != nullptr;
}

void PrecomputeSuffixCounts(const ParsedTrie &pt, const std::vector<std::string> &tokens,
                            std::vector<uint32_t> &counts_out) {
    const idx_t n = static_cast<idx_t>(tokens.size());
    counts_out.assign(n, 0);

    const PNode *node = pt.root;
    if (node == nullptr || n == 0) {
        return;
    }

    bool path_broken = false;
    for (idx_t t = 0; t < n; ++t) {
        if (path_broken || node == nullptr) {
            break;
        }
        const std::string &tok = tokens[n - 1 - t]; // consume from right
        node = FindChild(node, tok);
        if (node == nullptr) {
            path_broken = true;
            break;
        }
        const idx_t idx = n - 1 - t; // map back to forward index
        counts_out[idx] = node->cnt;
    }
}

const PNode *WalkExact(const ParsedTrie &pt, const std::vector<std::string> &toks) {
    const PNode *n = pt.root;
    if (n == nullptr) {
        return nullptr;
    }
    for (const auto &t : toks) {
        n = FindChild(n, t);
        if (n == nullptr) {
            return nullptr;
        }
    }
    return n;
}

} // namespace duckdb
