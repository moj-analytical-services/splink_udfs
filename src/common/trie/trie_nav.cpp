#include "trie/trie_nav.hpp"
#include <algorithm>

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
    // Our trie is a reversed-suffix trie: children from rightmost token first
    for (idx_t i = toks.size(); i > 0; --i) {
        n = FindChild(n, toks[i - 1]);
        if (n == nullptr) {
            return nullptr;
        }
    }
    return n;
}

GreedySkipMatchResult GreedyWalkWithSkips(const ParsedTrie &pt, const std::vector<std::string> &toks,
                                          bool allow_prefix, int32_t max_skips) {
    GreedySkipMatchResult res;
    const PNode *root = pt.root;
    if (root == nullptr) {
        return res;
    }
    if (max_skips < 0) {
        max_skips = 0;
    }

    if (!allow_prefix) {
        const PNode *node = root;
        int32_t skips_left = max_skips;
        for (idx_t ti = toks.size(); ti > 0; --ti) {
            const std::string &tok = toks[ti - 1];
            const PNode *next = FindChild(node, tok);
            if (next) {
                node = next;
                res.last_node = node;
                res.matched_len++;
                if (node->term == 1 && node->uprn != 0) {
                    res.deepest_unique = node;
                }
                continue;
            }
            if (skips_left > 0 && ti > 1) {
                const std::string &tok_after_skip = toks[ti - 2];
                const PNode *after = FindChild(node, tok_after_skip);
                if (after) {
                    res.skipped++;
                    skips_left--;
                    node = after;
                    res.last_node = node;
                    res.matched_len++;
                    if (node->term == 1 && node->uprn != 0) {
                        res.deepest_unique = node;
                    }
                    // we consumed one extra token (ti-2), so advance the loop an extra step
                    --ti;
                    continue;
                }
            }
            break;
        }
        return res;
    }

    const PNode *node = root;
    const PNode *best_last = nullptr;
    int32_t best_len = 0;
    int32_t curr_len = 0;
    int32_t skips_left = max_skips;

    for (idx_t ti = toks.size(); ti > 0; --ti) {
        const std::string &tok = toks[ti - 1];
        const PNode *next = FindChild(node, tok);
        if (next) {
            node = next;
            curr_len++;
            res.last_node = node;
            if (curr_len > best_len) {
                best_len = curr_len;
                best_last = node;
            }
            if (node->term == 1 && node->uprn != 0) {
                res.deepest_unique = node;
            }
            continue;
        }

        if (skips_left > 0 && ti > 1) {
            const std::string &tok_after_skip = toks[ti - 2];
            const PNode *after = FindChild(node, tok_after_skip);
            if (after) {
                res.skipped++;
                skips_left--;
                node = after;
                curr_len++;
                res.last_node = node;
                if (curr_len > best_len) {
                    best_len = curr_len;
                    best_last = node;
                }
                if (node->term == 1 && node->uprn != 0) {
                    res.deepest_unique = node;
                }
                --ti;
                continue;
            }
        }

        // miss and cannot skip: reset to root and retry this token from root
        node = root;
        skips_left = max_skips;
        curr_len = 0;
        next = FindChild(node, tok);
        if (next) {
            node = next;
            curr_len = 1;
            res.last_node = node;
            if (curr_len > best_len) {
                best_len = curr_len;
                best_last = node;
            }
            if (node->term == 1 && node->uprn != 0) {
                res.deepest_unique = node;
            }
            continue;
        }
        // if even root fails, continue to the next earlier token (shorter suffix)
    }

    if (best_last != nullptr) {
        res.last_node = best_last;
        res.matched_len = best_len;
    }
    return res;
}

} // namespace duckdb
