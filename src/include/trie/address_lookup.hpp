#pragma once

#include "trie/suffix_trie.hpp"
#include "trie/address_match_params.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {

// Core, engine-agnostic trie lookup.
// Given left-to-right tokens, walks the reversed-suffix trie and allows skipping
// tokens at the start of the reversed sequence (i.e., ignores up to s trailing
// tokens from the left-to-right input). Finds the longest suffix that fully
// matches a path from the root and succeeds only if the final node is a single
// exact terminal (term == 1). Returns true and sets uprn_out on success; returns
// false otherwise.
bool FindAddressExact(const ParsedTrie &trie,
                      const std::vector<std::string> &tokens,
                      const AddressMatchParams &params,
                      uint64_t &uprn_out);

inline bool FindAddressExact(const ParsedTrie &trie, const std::vector<std::string> &tokens, uint64_t &uprn_out) {
    return FindAddressExact(trie, tokens, DefaultMatchParams(), uprn_out);
}

} // namespace duckdb
