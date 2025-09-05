#pragma once
#include "duckdb.hpp"
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

// ---- On-disk format constants ----
static constexpr uint32_t QCK1_MAGIC = 0x314B4351u; // 'QCK1'
static constexpr uint8_t QCK1_FLAGS_EXPECTED = 0x00;

// ---- Parsed trie structures (immutable) ----
struct PNode {
	uint32_t cnt = 0;
	// Children are kept sorted lexicographically by token
	std::vector<std::pair<std::string, PNode *>> kids;
};

struct ParsedTrie {
	PNode *root = nullptr;
	// Arena owns nodes; kids point into this arena.
	std::vector<std::unique_ptr<PNode>> arena;
};

// ---- Parser ----
// Returns nullptr if blob is not a valid QCK1 trie.
std::unique_ptr<ParsedTrie> ParseQCK1(const string_t &blob);

// ---- Lookup ----
// Walk a reversed tail (rightmost token first). Returns 0 if path missing.
uint32_t CountTail(const ParsedTrie &pt, const std::vector<std::string> &tail_reversed);

} // namespace duckdb
