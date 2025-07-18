#pragma once

#include <memory>
#include <string>
#include <utf8proc.h>
#include "duckdb/common/exception.hpp"

namespace phonetic { // keep alongside your Soundex class

struct Utf8procDeleter {
	void operator()(utf8proc_uint8_t *p) const {
		free(p);
	}
};
using Utf8Buf = std::unique_ptr<utf8proc_uint8_t, Utf8procDeleter>;

inline std::string StripDiacritics(const std::string &utf8) {
	utf8proc_uint8_t *out_raw = nullptr;
	constexpr utf8proc_option_t FLAGS = static_cast<utf8proc_option_t>(UTF8PROC_NULLTERM |  // NUL-terminated input
	                                                                   UTF8PROC_COMPAT |    // expand ligatures (Æ→AE)
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

} // namespace phonetic