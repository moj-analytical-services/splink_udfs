#include "trie/peel_utils.hpp"

namespace duckdb {

void PeelEndTokensInPlace(std::vector<std::string> &tokens, const ParsedTrie &pt, int32_t steps, int32_t max_k) {
    if (steps < 0) {
        steps = 0;
    }
    if (max_k < 1) {
        max_k = 1;
    }
    if (!pt.root || tokens.size() < 2 || steps == 0) {
        return;
    }

    std::vector<std::string> tail_rev;
    std::vector<std::string> anchor_vec;
    anchor_vec.reserve(1);

    for (int s = 0; s < steps; ++s) {
        if (tokens.size() < 2) {
            break;
        }

        const idx_t n = static_cast<idx_t>(tokens.size());
        idx_t try_maxk = std::min<idx_t>(static_cast<idx_t>(max_k), n - 1);
        bool peeled_this_step = false;

        for (idx_t k = try_maxk; k >= 1; --k) {
            const idx_t anchor_idx = n - k - 1; // safe since k <= n-1
            const std::string &anchor = tokens[anchor_idx];

            // CountTail({anchor})
            anchor_vec.clear();
            anchor_vec.emplace_back(anchor);
            const uint32_t c_anchor = CountTail(pt, anchor_vec);

            // reversed tail_k : tokens[n-1]..tokens[n-k]
            tail_rev.clear();
            tail_rev.reserve(k + 1);
            for (idx_t t = 0; t < k; ++t) {
                tail_rev.emplace_back(tokens[n - 1 - t]);
            }
            // combo = reversed(tail_k) + [anchor]
            tail_rev.emplace_back(anchor);
            const uint32_t c_combo = CountTail(pt, tail_rev);

            if (c_anchor > c_combo) {
                // Peel k tokens from the end
                tokens.resize(n - k);
                peeled_this_step = true;
                break;
            }
        }

        if (!peeled_this_step) {
            break; // stable
        }
    }
}

} // namespace duckdb

