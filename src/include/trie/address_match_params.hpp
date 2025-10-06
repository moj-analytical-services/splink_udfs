#pragma once

#include <cstdint>

namespace duckdb {

struct AddressMatchParams {
	uint32_t skip_min_local_count = 10;       // corresponds to SKIP_MIN_LOCAL_COUNT
	uint32_t skip_max_in_walk = 2;            // corresponds to SKIP_MAX_IN_WALK
	uint32_t min_matched_tokens = 2;          // corresponds to MIN_MATCHED_TOKENS
	uint32_t entry_min_local_count = 10;      // corresponds to ENTRY_MIN_LOCAL_COUNT
	uint32_t max_trailing_tokens_ignored = 2; // corresponds to MAX_TRAILING_TOKENS_IGNORED
	uint32_t max_trie_entry_depth = 2;        // corresponds to MAX_TRIE_ENTRY_DEPTH
};

inline const AddressMatchParams &DefaultMatchParams() {
	static const AddressMatchParams defaults;
	return defaults;
}

} // namespace duckdb
