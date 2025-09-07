#pragma once

#include <string>
#include <vector>

#include "trie/suffix_trie.hpp"

namespace duckdb {

// Binary-search for a child token in a node's sorted children.
// Returns nullptr if not found.
const PNode *FindChild(const PNode *node, const std::string &token);
const PNode *FindChild(const PNode &node, const std::string &token);

// True when the node has a child with the given token.
bool HasChild(const PNode *node, const std::string &token);

// Precompute counts for every suffix of tokens (left→right tokens).
// Counts[i] = count of suffix tokens[i..end]. Fills with zeros if path breaks.
void PrecomputeSuffixCounts(const ParsedTrie &pt, const std::vector<std::string> &tokens,
                            std::vector<uint32_t> &counts_out);

// Walk an exact token path from root; returns the final node or nullptr if any token missing.
const PNode *WalkExact(const ParsedTrie &pt, const std::vector<std::string> &toks);

} // namespace duckdb
