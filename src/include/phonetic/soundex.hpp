// -------------------------------------------------------------------------
// We are grateful to Rob Tillaart's https://github.com/RobTillaart/Soundex
// MIT-licensed Arduino Soundex library, which served as inspiration for
// this C++ implementation.
// -------------------------------------------------------------------------

#pragma once

#include <cctype>
#include <cstdint>
#include <cstring>

// Use fast_mem.hpp for DuckDB memory utilities
#include "duckdb/common/fast_mem.hpp"

namespace phonetic {

// Soundex constants – kept in the header because callers may need them.
constexpr uint8_t kSoundexMinLen = 4;
constexpr uint8_t kSoundexMaxLen = 12;

class Soundex {
public:
	explicit Soundex(uint8_t length = kSoundexMinLen) {
		SetLength(length);
	}

	void SetLength(uint8_t length);
	uint8_t Length() const {
		return length_;
	}

	// Returns a NUL‑terminated buffer owned by *this*.
	const char *Encode(const char *str);

private:
	char buffer_[kSoundexMaxLen] = {};
	uint8_t length_ = kSoundexMinLen;

	static uint8_t ClassCode(char ch);
};

inline void Soundex::SetLength(uint8_t length) {
	length_ = (length < kSoundexMinLen)       ? kSoundexMinLen
	          : (length > kSoundexMaxLen - 1) ? (kSoundexMaxLen - 1)
	                                          : length;
}

inline uint8_t Soundex::ClassCode(char ch) {
	// Add bounds check for safety
	if (ch < 'A' || ch > 'Z') {
		return 0;
	}
	// clang‑format off
	static constexpr uint8_t lut[26] = {0, 1, 2, 3, 0, 1, 2, 0, 0, 2, 2, 4, 5, 5, 0, 1, 2, 6, 2, 3, 0, 1, 0, 2, 0, 2};
	// clang‑format on
	return lut[ch - 'A'];
}

inline const char *Soundex::Encode(const char *str) {
	duckdb::FastMemset(buffer_, '0', length_);
	buffer_[length_] = '\0';

	// Skip non‑letters
	while (*str && !std::isalpha(static_cast<unsigned char>(*str))) {
		++str;
	}
	if (*str == '\0') {
		return buffer_;
	}

	buffer_[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(*str)));
	uint8_t last = ClassCode(buffer_[0]);
	++str;

	uint8_t out_idx = 1;
	for (; *str && out_idx < length_; ++str) {
		if (!std::isalpha(static_cast<unsigned char>(*str))) {
			continue;
		}
		uint8_t code = ClassCode(static_cast<char>(std::toupper(static_cast<unsigned char>(*str))));
		if (code != 0 && code != last) {
			buffer_[out_idx++] = static_cast<char>('0' + code);
		}
		last = code;
	}
	return buffer_;
}

} // namespace phonetic
