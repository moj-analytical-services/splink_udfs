// -------------------------------------------------------------------------
// We are grateful to Rob Tillaart's https://github.com/RobTillaart/Soundex
// MIT-licensed Arduino Soundex library, which served as inspiration for
// this C++ implementation.
// -------------------------------------------------------------------------

#pragma once

#include <cctype>
#include <cstdint>
#include <cstring>
#include <string_view>

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
	const char *Encode(std::string_view input);

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

inline const char *Soundex::Encode(std::string_view input) {
	std::memset(buffer_, '0', length_);
	buffer_[length_] = '\0';

	// Safely find the first ASCII letter, skipping all other bytes.
	size_t input_idx = 0;
	for (; input_idx < input.size(); ++input_idx) {
		unsigned char current_byte = static_cast<unsigned char>(input[input_idx]);
		// Only process ASCII characters. This makes the function UTF-8 safe.
		// This is consistent with the postgres implementation
		if (current_byte <= 127 && std::isalpha(current_byte)) {
			break;
		}
	}

	// If no ASCII letters were found in the string, return the default code.
	if (input_idx == input.size()) {
		return buffer_;
	}

	// Store the first letter and its code.
	buffer_[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(input[input_idx])));
	uint8_t last = ClassCode(buffer_[0]);

	// Start processing from the character AFTER the first letter found.
	++input_idx;

	uint8_t out_idx = 1;
	for (; input_idx < input.size() && out_idx < length_; ++input_idx) {
		unsigned char current_byte = static_cast<unsigned char>(input[input_idx]);

		// MODIFICATION: Explicitly ignore non-ASCII bytes and non-letters.
		if (current_byte > 127 || !std::isalpha(current_byte)) {
			continue;
		}

		uint8_t code = ClassCode(static_cast<char>(std::toupper(current_byte)));
		if (code != 0 && code != last) {
			buffer_[out_idx++] = static_cast<char>('0' + code);
		}
		last = code;
	}
	return buffer_;
}

} // namespace phonetic
