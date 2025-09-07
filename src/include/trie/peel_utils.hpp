#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "trie/suffix_trie.hpp"

namespace duckdb {

// In-place peel of end tokens using the same heuristic as peel_end_tokens.cpp
// Steps: number of peel iterations; max_k: max tail length to consider at each step.
void PeelEndTokensInPlace(std::vector<std::string> &tokens, const ParsedTrie &pt, int32_t steps = 4,
                          int32_t max_k = 2);

} // namespace duckdb

