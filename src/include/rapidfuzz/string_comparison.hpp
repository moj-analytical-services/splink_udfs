#pragma once

#include <rapidfuzz/distance/Levenshtein.hpp>
#include <rapidfuzz/distance/DamerauLevenshtein.hpp>
#include <string_view>
#include <cstdint>
#include <cstdlib>
#include <array>
#include <utf8proc.h>
#include <string>

/* ------------------------------------------------------------------------- */
/*  UTF-8 to UTF-32 conversion for proper Unicode code-point handling       */
/* ------------------------------------------------------------------------- */
static std::u32string Utf8ToU32(std::string_view in) {
	std::u32string out;
	out.reserve(in.size()); // upper bound
	const utf8proc_uint8_t *p = reinterpret_cast<const utf8proc_uint8_t *>(in.data());
	const utf8proc_uint8_t *end = p + in.size();

	while (p < end) {
		utf8proc_int32_t cp;
		auto n = utf8proc_iterate(p, end - p, &cp);
		if (n <= 0) { // invalid byte → skip 1
			++p;
			continue;
		}
		out.push_back(static_cast<char32_t>(cp));
		p += n;
	}
	return out;
}

/* ------------------------------------------------------------------------- */
/*  Cheap "obviously‑different" guard                                        */
/* ------------------------------------------------------------------------- */
inline bool DefinitelyAboveK(std::string_view a, std::string_view b, int k) {
	if (k < 0)
		return false; // guard disabled → fall through

	if (std::abs(static_cast<int>(a.size()) - static_cast<int>(b.size())) > k)
		return true;

	std::array<int, 256> hist {};

	for (unsigned char ch : a)
		++hist[ch];
	for (unsigned char ch : b)
		--hist[ch];

	int imbalance = 0;
	for (int v : hist)
		imbalance += std::abs(v);

	/*  Each edit can fix at most two histogram mismatches          */
	return (imbalance >> 1) > k; // divide by 2 without fp
}

namespace duckdb {

// --- Two-argument versions (no threshold) ---
inline int64_t LevenshteinDistance(const std::string_view a, const std::string_view b) {
	auto ua = Utf8ToU32(a);
	auto ub = Utf8ToU32(b);
	return static_cast<int64_t>(rapidfuzz::levenshtein_distance(ua, ub));
}

// --- Three-argument versions (with threshold) ---
inline int64_t LevenshteinDistance(const std::string_view a, const std::string_view b, int64_t max_dist) {
	// The rapidfuzz `max` parameter is size_t (unsigned)
	if (max_dist < 0) {
		return LevenshteinDistance(a, b); // Fallback for negative threshold
	}
	auto ua = Utf8ToU32(a);
	auto ub = Utf8ToU32(b);
	// Note: The {1, 1, 1} represents the weights for (insertion, deletion, substitution)
	return static_cast<int64_t>(rapidfuzz::levenshtein_distance(ua, ub, {1, 1, 1}, static_cast<size_t>(max_dist)));
}

// --- Damerau-Levenshtein (Two-argument version) ---
inline int64_t DamerauLevenshteinDistance(const std::string_view a, const std::string_view b) {
	auto ua = Utf8ToU32(a);
	auto ub = Utf8ToU32(b);
	// Note: The function is in the 'experimental' namespace in this version of rapidfuzz
	return static_cast<int64_t>(rapidfuzz::experimental::damerau_levenshtein_distance(ua, ub));
}

// --- Damerau-Levenshtein (Three-argument version with threshold) ---
inline int64_t DamerauLevenshteinDistance(std::string_view a, std::string_view b, int64_t max_dist) {
	if (max_dist < 0) {
		return DamerauLevenshteinDistance(a, b); // Fallback for negative threshold
	}

	// Early exit if the character histogram proves the distance is > max_dist
	if (DefinitelyAboveK(a, b, static_cast<int>(max_dist))) {
		return max_dist + 1;
	}

	auto ua = Utf8ToU32(a);
	auto ub = Utf8ToU32(b);
	return static_cast<int64_t>(
	    rapidfuzz::experimental::damerau_levenshtein_distance(ua, ub, static_cast<size_t>(max_dist)));
}

} // namespace duckdb
