#pragma once
#include "duckdb.hpp"
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

// ---- On-disk format constants ----
static constexpr uint32_t QCK2_MAGIC = 0x324B4351u; // 'QCK2'

// ---- Parsed trie structures (immutable) ----
struct PNode {
	uint32_t cnt = 0;
	// Children are kept sorted lexicographically by token
	std::vector<std::pair<std::string, PNode *>> kids;

	// Terminal metadata for QCK2
	// - term: number of addresses that end at this node
	// - uprn: VALID ONLY if term == 1; otherwise MUST be 0 and ignored
	uint32_t term = 0; // number of addresses that end here
	uint64_t uprn = 0; // valid when term == 1
};

struct ParsedTrie {
	PNode *root = nullptr;
	// Arena owns nodes; kids point into this arena.
	std::vector<std::unique_ptr<PNode>> arena;
};

// ---- Parser ----
// Returns nullptr if blob is not a valid QCK2 trie.
std::unique_ptr<ParsedTrie> ParseQCK2(const string_t &blob);

} // namespace duckdb
