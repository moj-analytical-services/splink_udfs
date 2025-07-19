// double_metaphone.hpp
// -----------------------------------------------------------------------------
// A C++11 port of Apache Commons Codec's DoubleMetaphone (original Java source
// licensed under the Apache License 2.0).
//
// Style and layout comply with DuckDB community‑extension guidelines:
//   * #pragma once
//   * CamelCase for classes / functions, snake_case for variables
//   * 120‑column limit, brace‑every‑block, clang‑format friendly
//   * No raw new/delete, no global state, no RTTI hacks
//   * Namespace duckdb::ext_phonetic (single nesting)
// -----------------------------------------------------------------------------
#pragma once

#include <array>
#include <cctype>
#include <initializer_list>
#include <stdexcept>
#include <string>

namespace duckdb {
namespace ext_phonetic {

// Forward declaration ---------------------------------------------------------
class DoubleMetaphone;

// -----------------------------------------------------------------------------
// Helper (nested) struct that stores the parallel primary/alternate encodings.
// -----------------------------------------------------------------------------
class DoubleMetaphoneResult {
public:
	explicit DoubleMetaphoneResult(int32_t max_length) : max_length_(max_length) {
		primary_.reserve(max_length_);
		alternate_.reserve(max_length_);
	}

	// --- append helpers ------------------------------------------------------
	void Append(char value) {
		AppendPrimary(value);
		AppendAlternate(value);
	}
	void Append(char primary, char alternate) {
		AppendPrimary(primary);
		AppendAlternate(alternate);
	}
	void Append(const std::string &value) {
		AppendPrimary(value);
		AppendAlternate(value);
	}
	void Append(const std::string &primary, const std::string &alternate) {
		AppendPrimary(primary);
		AppendAlternate(alternate);
	}

	void AppendPrimary(char value) {
		if (static_cast<int32_t>(primary_.size()) < max_length_) {
			primary_.push_back(value);
		}
	}
	void AppendPrimary(const std::string &value) {
		auto add = std::min<int32_t>(Remaining(primary_), static_cast<int32_t>(value.size()));
		primary_.append(value, 0, static_cast<size_t>(add));
	}

	void AppendAlternate(char value) {
		if (static_cast<int32_t>(alternate_.size()) < max_length_) {
			alternate_.push_back(value);
		}
	}
	void AppendAlternate(const std::string &value) {
		auto add = std::min<int32_t>(Remaining(alternate_), static_cast<int32_t>(value.size()));
		alternate_.append(value, 0, static_cast<size_t>(add));
	}

	const std::string &Primary() const {
		return primary_;
	}
	const std::string &Alternate() const {
		return alternate_;
	}

	bool IsComplete() const {
		return static_cast<int32_t>(primary_.size()) >= max_length_ &&
		       static_cast<int32_t>(alternate_.size()) >= max_length_;
	}

private:
	int32_t Remaining(const std::string &s) const {
		return max_length_ - static_cast<int32_t>(s.size());
	}

	std::string primary_;
	std::string alternate_;
	int32_t max_length_;
};

// -----------------------------------------------------------------------------
// Main class – a faithful C++ port of the Java reference implementation.
// -----------------------------------------------------------------------------
class DoubleMetaphone {
public:
	// Port preserves Java API names (camelCase) for algorithmic methods -------

	std::string DoubleMetaphoneEncode(const std::string &value) {
		return DoubleMetaphoneEncode(value, false);
	}

	std::string DoubleMetaphoneEncode(const std::string &value, bool use_alternate) {
		auto cleaned = CleanInput(value);
		if (cleaned.empty()) {
			return {};
		}

		bool slavo_germanic = IsSlavoGermanic(cleaned);
		int32_t index = IsSilentStart(cleaned) ? 1 : 0;

		DoubleMetaphoneResult res(max_code_len_);

		while (!res.IsComplete() && index < static_cast<int32_t>(cleaned.size())) {
			char ch = cleaned[static_cast<size_t>(index)];
			switch (ch) {
			case 'A':
			case 'E':
			case 'I':
			case 'O':
			case 'U':
			case 'Y':
				index = HandleAEIOUY(res, index);
				break;
			case 'B':
				res.Append('P');
				index += (CharAt(cleaned, index + 1) == 'B') ? 2 : 1;
				break;
			case '\xC7': // Ç
				res.Append('S');
				++index;
				break;
			case 'C':
				index = HandleC(cleaned, res, index);
				break;
			case 'D':
				index = HandleD(cleaned, res, index);
				break;
			case 'F':
				res.Append('F');
				index += (CharAt(cleaned, index + 1) == 'F') ? 2 : 1;
				break;
			case 'G':
				index = HandleG(cleaned, res, index, slavo_germanic);
				break;
			case 'H':
				index = HandleH(cleaned, res, index);
				break;
			case 'J':
				index = HandleJ(cleaned, res, index, slavo_germanic);
				break;
			case 'K':
				res.Append('K');
				index += (CharAt(cleaned, index + 1) == 'K') ? 2 : 1;
				break;
			case 'L':
				index = HandleL(cleaned, res, index);
				break;
			case 'M':
				res.Append('M');
				index += ConditionM0(cleaned, index) ? 2 : 1;
				break;
			case 'N':
				res.Append('N');
				index += (CharAt(cleaned, index + 1) == 'N') ? 2 : 1;
				break;
			case '\xD1': // Ñ
				res.Append('N');
				++index;
				break;
			case 'P':
				index = HandleP(cleaned, res, index);
				break;
			case 'Q':
				res.Append('K');
				index += (CharAt(cleaned, index + 1) == 'Q') ? 2 : 1;
				break;
			case 'R':
				index = HandleR(cleaned, res, index, slavo_germanic);
				break;
			case 'S':
				index = HandleS(cleaned, res, index, slavo_germanic);
				break;
			case 'T':
				index = HandleT(cleaned, res, index);
				break;
			case 'V':
				res.Append('F');
				index += (CharAt(cleaned, index + 1) == 'V') ? 2 : 1;
				break;
			case 'W':
				index = HandleW(cleaned, res, index);
				break;
			case 'X':
				index = HandleX(cleaned, res, index);
				break;
			case 'Z':
				index = HandleZ(cleaned, res, index, slavo_germanic);
				break;
			default:
				++index;
				break;
			}
		}
		return use_alternate ? res.Alternate() : res.Primary();
	}

	// Utility – compare two strings’ metaphones ----------------------------------
	bool IsEqual(const std::string &lhs, const std::string &rhs, bool use_alternate = false) {
		return DoubleMetaphoneEncode(lhs, use_alternate) == DoubleMetaphoneEncode(rhs, use_alternate);
	}

	// Configuration -------------------------------------------------------------
	int32_t MaxCodeLen() const {
		return max_code_len_;
	}
	void SetMaxCodeLen(int32_t len) {
		max_code_len_ = len;
	}

private:
	// --- Constants -------------------------------------------------------------
	static constexpr const char *VOWELS = "AEIOUY";
	static constexpr std::array<const char *, 5> SILENT_START = {"GN", "KN", "PN", "WR", "PS"};
	static constexpr std::array<const char *, 10> L_R_N_M_B_H_F_V_W_SPACE = {"L", "R", "N", "M", "B",
	                                                                         "H", "F", "V", "W", " "};
	static constexpr std::array<const char *, 11> ES_EP_EB_EL_EY_IB_IL_IN_IE_EI_ER = {
	    "ES", "EP", "EB", "EL", "EY", "IB", "IL", "IN", "IE", "EI", "ER"};
	static constexpr std::array<const char *, 8> L_T_K_S_N_M_B_Z = {"L", "T", "K", "S", "N", "M", "B", "Z"};

	// --- Member data -----------------------------------------------------------
	int32_t max_code_len_ = 4;

	// --- Small helpers ---------------------------------------------------------
	static std::string CleanInput(const std::string &input) {
		std::string out;
		out.reserve(input.size());
		for (char ch : input) {
			if (!std::isspace(static_cast<unsigned char>(ch))) {
				out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));
			}
		}
		return out;
	}

	static char CharAt(const std::string &s, int32_t index) {
		if (index < 0 || index >= static_cast<int32_t>(s.size())) {
			return '\0';
		}
		return s[static_cast<size_t>(index)];
	}

	static bool IsVowel(char ch) {
		return std::strchr(VOWELS, ch) != nullptr;
	}

	// NEW – handles brace-initialiser lists like {"CK","CG","CQ"}
	static bool Contains(const std::string &value, int32_t start, int32_t length,
	                     std::initializer_list<const char *> criteria) {
		if (start < 0 || start + length > static_cast<int32_t>(value.size())) {
			return false;
		}
		std::string_view target(value.data() + start, static_cast<size_t>(length));
		for (auto pat : criteria) {
			if (target == pat) {
				return true;
			}
		}
		return false;
	}

	static bool Contains(const std::string &value, int32_t start, int32_t length,
	                     const std::array<const char *, 1> &criteria) {
		return Contains(value, start, length, criteria.data(), criteria.size());
	}
	template <size_t N>
	static bool Contains(const std::string &value, int32_t start, int32_t length,
	                     const std::array<const char *, N> &criteria) {
		return Contains(value, start, length, criteria.data(), N);
	}
	static bool Contains(const std::string &value, int32_t start, int32_t length, const char *const *criteria,
	                     size_t criterion_count) {
		if (start < 0 || start + length > static_cast<int32_t>(value.size())) {
			return false;
		}
		std::string_view target(value.data() + start, static_cast<size_t>(length));
		for (size_t i = 0; i < criterion_count; ++i) {
			if (target == criteria[i]) {
				return true;
			}
		}
		return false;
	}

	// --- Origin checks ---------------------------------------------------------
	static bool IsSilentStart(const std::string &value) {
		for (const auto *prefix : SILENT_START) {
			if (value.rfind(prefix, 0) == 0) { // starts_with
				return true;
			}
		}
		return false;
	}

	static bool IsSlavoGermanic(const std::string &value) {
		return value.find('W') != std::string::npos || value.find('K') != std::string::npos ||
		       value.find("CZ") != std::string::npos || value.find("WITZ") != std::string::npos;
	}

	// --------------------------------------------------------------------------
	// Individual letter handlers (direct C++ translations of the Java methods)
	// For brevity, only the more complex ones are shown in full; the remaining
	// handlers follow the same structure and are included below verbatim.
	// --------------------------------------------------------------------------

	// Vowel handler ------------------------------------------------------------
	int32_t HandleAEIOUY(DoubleMetaphoneResult &res, int32_t index) {
		if (index == 0) {
			res.Append('A');
		}
		return index + 1;
	}

	// --- C --------------------------------------------------------------------
	bool ConditionC0(const std::string &val, int32_t idx) {
		if (Contains(val, idx, 4, {"CHIA"})) {
			return true;
		}
		if (idx <= 1) {
			return false;
		}
		if (IsVowel(CharAt(val, idx - 2))) {
			return false;
		}
		if (!Contains(val, idx - 1, 3, {"ACH"})) {
			return false;
		}
		char c = CharAt(val, idx + 2);
		return (c != 'I' && c != 'E') || Contains(val, idx - 2, 6, {"BACHER", "MACHER"});
	}

	int32_t HandleC(const std::string &val, DoubleMetaphoneResult &res, int32_t idx) {
		if (ConditionC0(val, idx)) {
			res.Append('K');
			return idx + 2;
		}
		if (idx == 0 && Contains(val, idx, 6, {"CAESAR"})) {
			res.Append('S');
			return idx + 2;
		}
		if (Contains(val, idx, 2, {"CH"})) {
			return HandleCH(val, res, idx);
		}
		if (Contains(val, idx, 2, {"CZ"}) && !Contains(val, idx - 2, 4, {"WICZ"})) {
			res.Append('S', 'X');
			return idx + 2;
		}
		if (Contains(val, idx + 1, 3, {"CIA"})) {
			res.Append('X');
			return idx + 3;
		}
		if (Contains(val, idx, 2, {"CC"}) && !(idx == 1 && CharAt(val, 0) == 'M')) {
			return HandleCC(val, res, idx);
		}
		if (Contains(val, idx, 2, {"CK", "CG", "CQ"})) {
			res.Append('K');
			return idx + 2;
		}
		if (Contains(val, idx, 2, {"CI", "CE", "CY"})) {
			if (Contains(val, idx, 3, {"CIO", "CIE", "CIA"})) {
				res.Append('S', 'X');
			} else {
				res.Append('S');
			}
			return idx + 2;
		}
		res.Append('K');
		if (Contains(val, idx + 1, 2, {" C", " Q", " G"})) {
			return idx + 3;
		}
		if (Contains(val, idx + 1, 1, {"C", "K", "Q"}) && !Contains(val, idx + 1, 2, {"CE", "CI"})) {
			return idx + 2;
		}
		return idx + 1;
	}

	int32_t HandleCC(const std::string &val, DoubleMetaphoneResult &res, int32_t idx) {
		if (Contains(val, idx + 2, 1, {"I", "E", "H"}) && !Contains(val, idx + 2, 2, {"HU"})) {
			if ((idx == 1 && CharAt(val, idx - 1) == 'A') || Contains(val, idx - 1, 5, {"UCCEE", "UCCES"})) {
				res.Append("KS");
			} else {
				res.Append('X');
			}
			return idx + 3;
		}
		res.Append('K');
		return idx + 2;
	}

	// --- CH -------------------------------------------------------------------
	bool ConditionCH0(const std::string &val, int32_t idx) {
		if (idx != 0) {
			return false;
		}
		if (!Contains(val, idx + 1, 5, {"HARAC", "HARIS"}) &&
		    !Contains(val, idx + 1, 3, {"HOR", "HYM", "HIA", "HEM"})) {
			return false;
		}
		return !Contains(val, 0, 5, {"CHORE"});
	}
	bool ConditionCH1(const std::string &val, int32_t idx) {
		return Contains(val, 0, 4, {"VAN ", "VON "}) || Contains(val, 0, 3, {"SCH"}) ||
		       Contains(val, idx - 2, 6, {"ORCHES", "ARCHIT", "ORCHID"}) || Contains(val, idx + 2, 1, {"T", "S"}) ||
		       ((Contains(val, idx - 1, 1, {"A", "O", "U", "E"}) || idx == 0) &&
		        (Contains(val, idx + 2, 1, L_R_N_M_B_H_F_V_W_SPACE) ||
		         idx + 1 == static_cast<int32_t>(val.size()) - 1));
	}

	int32_t HandleCH(const std::string &val, DoubleMetaphoneResult &res, int32_t idx) {
		if (idx > 0 && Contains(val, idx, 4, {"CHAE"})) {
			res.Append('K', 'X');
			return idx + 2;
		}
		if (ConditionCH0(val, idx)) {
			res.Append('K');
			return idx + 2;
		}
		if (ConditionCH1(val, idx)) {
			res.Append('K');
			return idx + 2;
		}
		if (idx > 0) {
			if (Contains(val, 0, 2, {"MC"})) {
				res.Append('K');
			} else {
				res.Append('X', 'K');
			}
		} else {
			res.Append('X');
		}
		return idx + 2;
	}

	// --- D --------------------------------------------------------------------
	int32_t HandleD(const std::string &val, DoubleMetaphoneResult &res, int32_t idx) {
		if (Contains(val, idx, 2, {"DG"})) {
			if (Contains(val, idx + 2, 1, {"I", "E", "Y"})) {
				res.Append('J');
				return idx + 3;
			}
			res.Append("TK");
			return idx + 2;
		}
		if (Contains(val, idx, 2, {"DT", "DD"})) {
			res.Append('T');
			return idx + 2;
		}
		res.Append('T');
		return idx + 1;
	}

	// --- G --------------------------------------------------------------------
	int32_t HandleG(const std::string &val, DoubleMetaphoneResult &res, int32_t idx, bool slavo_germanic) {
		if (CharAt(val, idx + 1) == 'H') {
			return HandleGH(val, res, idx);
		}
		if (CharAt(val, idx + 1) == 'N') {
			if (idx == 1 && IsVowel(CharAt(val, 0)) && !slavo_germanic) {
				res.Append("KN", "N");
			} else if (!Contains(val, idx + 2, 2, {"EY"}) && CharAt(val, idx + 1) != 'Y' && !slavo_germanic) {
				res.Append("N", "KN");
			} else {
				res.Append("KN");
			}
			return idx + 2;
		}
		if (Contains(val, idx + 1, 2, {"LI"}) && !slavo_germanic) {
			res.Append("KL", "L");
			return idx + 2;
		}
		if (idx == 0 && (CharAt(val, idx + 1) == 'Y' || Contains(val, idx + 1, 2, ES_EP_EB_EL_EY_IB_IL_IN_IE_EI_ER))) {
			res.Append('K', 'J');
			return idx + 2;
		}
		if ((Contains(val, idx + 1, 2, {"ER"}) || CharAt(val, idx + 1) == 'Y') &&
		    !Contains(val, 0, 6, {"DANGER", "RANGER", "MANGER"}) && !Contains(val, idx - 1, 1, {"E", "I"}) &&
		    !Contains(val, idx - 1, 3, {"RGY", "OGY"})) {
			res.Append('K', 'J');
			return idx + 2;
		}
		if (Contains(val, idx + 1, 1, {"E", "I", "Y"}) || Contains(val, idx - 1, 4, {"AGGI", "OGGI"})) {
			if (Contains(val, 0, 4, {"VAN ", "VON "}) || Contains(val, 0, 3, {"SCH"}) ||
			    Contains(val, idx + 1, 2, {"ET"})) {
				res.Append('K');
			} else if (Contains(val, idx + 1, 3, {"IER"})) {
				res.Append('J');
			} else {
				res.Append('J', 'K');
			}
			return idx + 2;
		}
		res.Append('K');
		return (CharAt(val, idx + 1) == 'G') ? idx + 2 : idx + 1;
	}

	int32_t HandleGH(const std::string &val, DoubleMetaphoneResult &res, int32_t idx) {
		if (idx > 0 && !IsVowel(CharAt(val, idx - 1))) {
			res.Append('K');
			return idx + 2;
		}
		if (idx == 0) {
			res.Append(CharAt(val, idx + 2) == 'I' ? 'J' : 'K');
			return idx + 2;
		}
		if ((idx > 1 && Contains(val, idx - 2, 1, {"B", "H", "D"})) ||
		    (idx > 2 && Contains(val, idx - 3, 1, {"B", "H", "D"})) ||
		    (idx > 3 && Contains(val, idx - 4, 1, {"B", "H"}))) {
			return idx + 2; // silent
		}
		if (idx > 2 && CharAt(val, idx - 1) == 'U' && Contains(val, idx - 3, 1, {"C", "G", "L", "R", "T"})) {
			res.Append('F');
		} else if (idx > 0 && CharAt(val, idx - 1) != 'I') {
			res.Append('K');
		}
		return idx + 2;
	}

	// --- H --------------------------------------------------------------------
	int32_t HandleH(const std::string &val, DoubleMetaphoneResult &res, int32_t idx) {
		if ((idx == 0 || IsVowel(CharAt(val, idx - 1))) && IsVowel(CharAt(val, idx + 1))) {
			res.Append('H');
			return idx + 2;
		}
		return idx + 1;
	}

	// --- J --------------------------------------------------------------------
	int32_t HandleJ(const std::string &val, DoubleMetaphoneResult &res, int32_t idx, bool slavo_germanic) {
		if (Contains(val, idx, 4, {"JOSE"}) || Contains(val, 0, 4, {"SAN "})) {
			if (idx == 0 && CharAt(val, idx + 4) == ' ') {
				res.Append('H');
			} else {
				res.Append('J', 'H');
			}
			return idx + 1;
		}
		if (idx == 0 && !Contains(val, idx, 4, {"JOSE"})) {
			res.Append('J', 'A');
		} else if (IsVowel(CharAt(val, idx - 1)) && !slavo_germanic &&
		           (CharAt(val, idx + 1) == 'A' || CharAt(val, idx + 1) == 'O')) {
			res.Append('J', 'H');
		} else if (idx == static_cast<int32_t>(val.size()) - 1) {
			res.Append('J', ' ');
		} else if (!Contains(val, idx + 1, 1, L_T_K_S_N_M_B_Z) && !Contains(val, idx - 1, 1, {"S", "K", "L"})) {
			res.Append('J');
		}
		return (CharAt(val, idx + 1) == 'J') ? idx + 2 : idx + 1;
	}

	// --- L --------------------------------------------------------------------
	bool ConditionL0(const std::string &val, int32_t idx) {
		if (idx == static_cast<int32_t>(val.size()) - 3 && Contains(val, idx - 1, 4, {"ILLO", "ILLA", "ALLE"})) {
			return true;
		}
		return (Contains(val, val.size() - 2, 2, {"AS", "OS"}) || Contains(val, val.size() - 1, 1, {"A", "O"})) &&
		       Contains(val, idx - 1, 4, {"ALLE"});
	}

	int32_t HandleL(const std::string &val, DoubleMetaphoneResult &res, int32_t idx) {
		if (CharAt(val, idx + 1) == 'L') {
			if (ConditionL0(val, idx)) {
				res.AppendPrimary('L');
			} else {
				res.Append('L');
			}
			return idx + 2;
		}
		res.Append('L');
		return idx + 1;
	}

	// --- M --------------------------------------------------------------------
	bool ConditionM0(const std::string &val, int32_t idx) {
		if (CharAt(val, idx + 1) == 'M') {
			return true;
		}
		return Contains(val, idx - 1, 3, {"UMB"}) &&
		       (idx + 1 == static_cast<int32_t>(val.size()) - 1 || Contains(val, idx + 2, 2, {"ER"}));
	}

	int32_t HandleP(const std::string &val, DoubleMetaphoneResult &res, int32_t idx) {
		if (CharAt(val, idx + 1) == 'H') {
			res.Append('F');
			return idx + 2;
		}
		res.Append('P');
		return Contains(val, idx + 1, 1, {"P", "B"}) ? idx + 2 : idx + 1;
	}

	// --- R --------------------------------------------------------------------
	int32_t HandleR(const std::string &val, DoubleMetaphoneResult &res, int32_t idx, bool slavo_germanic) {
		if (idx == static_cast<int32_t>(val.size()) - 1 && !slavo_germanic && Contains(val, idx - 2, 2, {"IE"}) &&
		    !Contains(val, idx - 4, 2, {"ME", "MA"})) {
			res.AppendAlternate('R');
		} else {
			res.Append('R');
		}
		return (CharAt(val, idx + 1) == 'R') ? idx + 2 : idx + 1;
	}

	// --- S --------------------------------------------------------------------
	int32_t HandleSC(const std::string &val, DoubleMetaphoneResult &res, int32_t idx) {
		if (CharAt(val, idx + 2) == 'H') {
			if (Contains(val, idx + 3, 2, {"OO", "ER", "EN", "UY", "ED", "EM"})) {
				if (Contains(val, idx + 3, 2, {"ER", "EN"})) {
					res.Append("X", "SK");
				} else {
					res.Append("SK");
				}
			} else if (idx == 0 && !IsVowel(CharAt(val, 3)) && CharAt(val, 3) != 'W') {
				res.Append('X', 'S');
			} else {
				res.Append('X');
			}
		} else if (Contains(val, idx + 2, 1, {"I", "E", "Y"})) {
			res.Append('S');
		} else {
			res.Append("SK");
		}
		return idx + 3;
	}

	int32_t HandleS(const std::string &val, DoubleMetaphoneResult &res, int32_t idx, bool slavo_germanic) {
		if (Contains(val, idx - 1, 3, {"ISL", "YSL"})) {
			return idx + 1;
		}
		if (idx == 0 && Contains(val, idx, 5, {"SUGAR"})) {
			res.Append('X', 'S');
			return idx + 1;
		}
		if (Contains(val, idx, 2, {"SH"})) {
			if (Contains(val, idx + 1, 4, {"HEIM", "HOEK", "HOLM", "HOLZ"})) {
				res.Append('S');
			} else {
				res.Append('X');
			}
			return idx + 2;
		}
		if (Contains(val, idx, 3, {"SIO", "SIA"}) || Contains(val, idx, 4, {"SIAN"})) {
			if (slavo_germanic) {
				res.Append('S');
			} else {
				res.Append('S', 'X');
			}
			return idx + 3;
		}
		if ((idx == 0 && Contains(val, idx + 1, 1, {"M", "N", "L", "W"})) || Contains(val, idx + 1, 1, {"Z"})) {
			res.Append('S', 'X');
			return Contains(val, idx + 1, 1, {"Z"}) ? idx + 2 : idx + 1;
		}
		if (Contains(val, idx, 2, {"SC"})) {
			return HandleSC(val, res, idx);
		}
		if (idx == static_cast<int32_t>(val.size()) - 1 && Contains(val, idx - 2, 2, {"AI", "OI"})) {
			res.AppendAlternate('S');
		} else {
			res.Append('S');
		}
		return Contains(val, idx + 1, 1, {"S", "Z"}) ? idx + 2 : idx + 1;
	}

	// --- T --------------------------------------------------------------------
	int32_t HandleT(const std::string &val, DoubleMetaphoneResult &res, int32_t idx) {
		if (Contains(val, idx, 4, {"TION"}) || Contains(val, idx, 3, {"TIA", "TCH"})) {
			res.Append('X');
			return idx + 3;
		}
		if (Contains(val, idx, 2, {"TH"}) || Contains(val, idx, 3, {"TTH"})) {
			if (Contains(val, idx + 2, 2, {"OM", "AM"}) || Contains(val, 0, 4, {"VAN ", "VON "}) ||
			    Contains(val, 0, 3, {"SCH"})) {
				res.Append('T');
			} else {
				res.Append('0', 'T');
			}
			return idx + 2;
		}
		res.Append('T');
		return Contains(val, idx + 1, 1, {"T", "D"}) ? idx + 2 : idx + 1;
	}

	// --- W --------------------------------------------------------------------
	int32_t HandleW(const std::string &val, DoubleMetaphoneResult &res, int32_t idx) {
		if (Contains(val, idx, 2, {"WR"})) {
			res.Append('R');
			return idx + 2;
		}
		if (idx == 0 && (IsVowel(CharAt(val, idx + 1)) || Contains(val, idx, 2, {"WH"}))) {
			if (IsVowel(CharAt(val, idx + 1))) {
				res.Append('A', 'F');
			} else {
				res.Append('A');
			}
			return idx + 1;
		}
		if ((idx == static_cast<int32_t>(val.size()) - 1 && IsVowel(CharAt(val, idx - 1))) ||
		    Contains(val, idx - 1, 5, {"EWSKI", "EWSKY", "OWSKI", "OWSKY"}) || Contains(val, 0, 3, {"SCH"})) {
			res.AppendAlternate('F');
			return idx + 1;
		}
		if (Contains(val, idx, 4, {"WICZ", "WITZ"})) {
			res.Append("TS", "FX");
			return idx + 4;
		}
		return idx + 1;
	}

	// --- X --------------------------------------------------------------------
	int32_t HandleX(const std::string &val, DoubleMetaphoneResult &res, int32_t idx) {
		if (idx == 0) {
			res.Append('S');
			return idx + 1;
		}
		if (!(idx == static_cast<int32_t>(val.size()) - 1 &&
		      (Contains(val, idx - 3, 3, {"IAU", "EAU"}) || Contains(val, idx - 2, 2, {"AU", "OU"})))) {
			res.Append("KS");
		}
		return Contains(val, idx + 1, 1, {"C", "X"}) ? idx + 2 : idx + 1;
	}

	// --- Z --------------------------------------------------------------------
	int32_t HandleZ(const std::string &val, DoubleMetaphoneResult &res, int32_t idx, bool slavo_germanic) {
		if (CharAt(val, idx + 1) == 'H') {
			res.Append('J');
			return idx + 2;
		}
		if (Contains(val, idx + 1, 2, {"ZO", "ZI", "ZA"}) ||
		    (slavo_germanic && idx > 0 && CharAt(val, idx - 1) != 'T')) {
			res.Append("S", "TS");
		} else {
			res.Append('S');
		}
		return (CharAt(val, idx + 1) == 'Z') ? idx + 2 : idx + 1;
	}
}; // class DoubleMetaphone

} // namespace ext_phonetic
} // namespace duckdb
