#pragma once
#include "trie/suffix_trie.hpp"
#include "trie/suffix_trie_cache.hpp"

namespace duckdb {

// Parse a trie blob and cache it by content hash. Returns nullptr on parse failure.
std::shared_ptr<const ParsedTrie> GetOrParseTrie(TrieCache &cache, const string_t &blob);

} // namespace duckdb

