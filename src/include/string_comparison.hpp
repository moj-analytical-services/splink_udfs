#pragma once

#include <rapidfuzz/distance/Levenshtein.hpp>
#include <rapidfuzz/distance/DamerauLevenshtein.hpp>
#include <string_view>

namespace duckdb {

// --- Two-argument versions (no threshold) ---
inline int64_t LevenshteinDistance(const std::string_view a, const std::string_view b) {
	return static_cast<int64_t>(rapidfuzz::levenshtein_distance(a, b));
}

// --- Three-argument versions (with threshold) ---
inline int64_t LevenshteinDistance(const std::string_view a, const std::string_view b, int64_t max_dist) {
	// The rapidfuzz `max` parameter is size_t (unsigned)
	if (max_dist < 0) {
		return LevenshteinDistance(a, b); // Fallback for negative threshold
	}
	// Note: The {1, 1, 1} represents the weights for (insertion, deletion, substitution)
	return static_cast<int64_t>(rapidfuzz::levenshtein_distance(a, b, {1, 1, 1}, static_cast<size_t>(max_dist)));
}

} // namespace duckdb