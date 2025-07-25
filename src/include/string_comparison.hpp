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

// --- Damerau-Levenshtein (Two-argument version) ---
inline int64_t DamerauLevenshteinDistance(const std::string_view a, const std::string_view b) {
	// Note: The function is in the 'experimental' namespace in this version of rapidfuzz
	return static_cast<int64_t>(rapidfuzz::experimental::damerau_levenshtein_distance(a, b));
}

// --- Damerau-Levenshtein (Three-argument version with threshold) ---
inline int64_t DamerauLevenshteinDistance(const std::string_view a, const std::string_view b, int64_t max_dist) {
	if (max_dist < 0) {
		return DamerauLevenshteinDistance(a, b); // Fallback for negative threshold
	}
	return static_cast<int64_t>(
	    rapidfuzz::experimental::damerau_levenshtein_distance(a, b, static_cast<size_t>(max_dist)));
}

} // namespace duckdb