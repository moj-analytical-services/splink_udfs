#pragma once

#include "trie/suffix_trie.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {

// Core, engine-agnostic trie lookup.
// Given left-to-right tokens, walks the reversed-suffix trie.
// Returns true and sets uprn_out if found and terminal (uprn_out may be 0 for ambiguous terminal by design).
// Returns false if path not found or not terminal.
bool FindAddressExact(const ParsedTrie &trie, const std::vector<std::string> &tokens, uint64_t &uprn_out);

} // namespace duckdb
