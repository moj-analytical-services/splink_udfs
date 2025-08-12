#pragma once

#include <memory>
#include <string>
#include <vector>
#include <utility>
#include "utf8proc_compat.hpp"
#include "duckdb/common/exception.hpp"

namespace phonetic {

struct Utf8procDeleter {
	void operator()(utf8proc_uint8_t *p) const {
		free(p);
	}
};
using Utf8Buf = std::unique_ptr<utf8proc_uint8_t, Utf8procDeleter>;

inline std::string StripDiacritics(const std::string &utf8) {
	utf8proc_uint8_t *out_raw = nullptr;
	constexpr utf8proc_option_t FLAGS = static_cast<utf8proc_option_t>(UTF8PROC_NULLTERM | // NUL-terminated input
	                                                                   UTF8PROC_COMPAT |   // expand ligatures (Æ→AE)
	                                                                   UTF8PROC_DECOMPOSE | // NFKD decomposition
	                                                                   UTF8PROC_STRIPMARK | // drop combining accents
	                                                                   UTF8PROC_LUMP        // fold punctuation variants
	);

	// Note: utf8proc_map returns utf8proc_ssize_t
	utf8proc_ssize_t rc = utf8proc_map(reinterpret_cast<const utf8proc_uint8_t *>(utf8.c_str()),
	                                   0, // length = NUL-terminated
	                                   &out_raw, FLAGS);
	if (rc < 0) {
		throw duckdb::InternalException("utf8proc error: %s", utf8proc_errmsg(rc));
	}
	Utf8Buf holder(out_raw); // RAII: free() when going out of scope
	return std::string(reinterpret_cast<char *>(holder.get()));
}

inline std::string Unaccent(const std::string &utf8) {

	std::string result = StripDiacritics(utf8);

	static const std::vector<std::pair<std::string, std::string>> REPLACEMENTS = {// Latin Extended-A
	                                                                              {"Ø", "O"},
	                                                                              {"ø", "o"},
	                                                                              {"Þ", "Th"},
	                                                                              {"þ", "th"},
	                                                                              {"Ð", "D"},
	                                                                              {"ð", "d"},
	                                                                              {"ß", "ss"},
	                                                                              {"Æ", "AE"},
	                                                                              {"æ", "ae"},
	                                                                              {"Œ", "OE"},
	                                                                              {"œ", "oe"},
	                                                                              // Other common ones
	                                                                              {"Ł", "L"},
	                                                                              {"ł", "l"},
	                                                                              {"Đ", "D"},
	                                                                              {"đ", "d"}};

	for (const auto &rule : REPLACEMENTS) {
		std::string::size_type pos = 0;
		while ((pos = result.find(rule.first, pos)) != std::string::npos) {
			result.replace(pos, rule.first.length(), rule.second);
			pos += rule.second.length();
		}
	}

	return result;
}

} // namespace phonetic
