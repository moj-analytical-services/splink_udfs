#define DUCKDB_EXTENSION_MAIN // must precede DuckDB headers
#include "splink_udfs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include "phonetic/soundex.hpp"
#include "phonetic/strip_diacritics.hpp"
#include "phonetic/double_metaphone.hpp"

namespace duckdb {

using duckdb::ext_phonetic::DoubleMetaphone; // convenience alias

// Define static constexpr members from DoubleMetaphone class
constexpr std::array<const char *, 5> DoubleMetaphone::SILENT_START;
constexpr std::array<const char *, 10> DoubleMetaphone::L_R_N_M_B_H_F_V_W_SPACE;
constexpr std::array<const char *, 11> DoubleMetaphone::ES_EP_EB_EL_EY_IB_IL_IN_IE_EI_ER;
constexpr std::array<const char *, 8> DoubleMetaphone::L_T_K_S_N_M_B_Z;

static string_t MakeStringResult(Vector &result, const char *cstr) {
	return StringVector::AddString(result, cstr);
}

static void SoundexScalar(DataChunk &data_chunk, ExpressionState & /*state*/, Vector &result) {
	// Use idx_t for row counts
	const idx_t count = data_chunk.size();
	auto &input = data_chunk.data[0];

	// Reusable encoder instance per chunk
	phonetic::Soundex encoder;

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](const string_t &val) -> string_t {
		// Handle empty string edge case explicitly
		if (val.GetSize() == 0) {
			// Return "0000" or the desired empty-string result
			return StringVector::AddString(result, "0000");
		}
		const char *code = encoder.Encode(val.GetDataUnsafe());
		return MakeStringResult(result, code);
	});
}

static void StripDiacriticsScalar(DataChunk &data_chunk, ExpressionState & /*state*/, Vector &result) {
	const idx_t count = data_chunk.size();
	auto &input = data_chunk.data[0];

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](const string_t &val) -> string_t {
		if (val.GetSize() == 0) {
			return StringVector::AddString(result, "");
		}
		// utf8 bytes → std::string
		std::string in(val.GetDataUnsafe(), val.GetSize());

		std::string folded = phonetic::StripDiacritics(in);
		return StringVector::AddString(result, folded);
	});
}

static void UnaccentScalar(DataChunk &data_chunk, ExpressionState & /*state*/, Vector &result) {
	const idx_t count = data_chunk.size();
	auto &input = data_chunk.data[0];

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](const string_t &val) -> string_t {
		if (val.GetSize() == 0) {
			return StringVector::AddString(result, "");
		}
		// utf8 bytes → std::string
		std::string in(val.GetDataUnsafe(), val.GetSize());

		// Call the new, wider Unaccent function
		std::string unaccented = phonetic::Unaccent(in);
		return StringVector::AddString(result, unaccented);
	});
}

// -----------------------------------------------------------------------------
// Double Metaphone
// -----------------------------------------------------------------------------
static void DoubleMetaphoneScalarList(DataChunk &data_chunk, ExpressionState & /*state*/, Vector &result) {
	const idx_t count = data_chunk.size();
	auto &input = data_chunk.data[0];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	// Create/obtain the child vector that will store individual strings
	auto &child = ListVector::GetEntry(result);
	ListVector::Reserve(result, count * 2); // heuristic upper bound (primary+alt per row)

	// Track where we are writing inside the child vector
	idx_t child_offset = 0;

	DoubleMetaphone encoder;
	string_t *child_strings = FlatVector::GetData<string_t>(child);
	auto *list_entries = FlatVector::GetData<list_entry_t>(result);

	UnifiedVectorFormat input_format;
	input.ToUnifiedFormat(count, input_format);

	for (idx_t row = 0; row < count; ++row) {
		auto input_idx = input_format.sel->get_index(row);

		if (!input_format.validity.RowIsValid(input_idx)) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		// Read input
		string_t in = ((string_t *)input_format.data)[input_idx];
		if (in.GetSize() == 0) {
			// Empty string case - return empty list
			list_entry_t &entry = list_entries[row];
			entry.offset = child_offset;
			entry.length = 0;
			continue;
		}

		std::string_view sv(in.GetDataUnsafe(), in.GetSize());

		// ---- generate codes -------------------------------------------------
		std::string primary = encoder.DoubleMetaphoneEncode(std::string(sv), false);
		std::string alternate = encoder.DoubleMetaphoneEncode(std::string(sv), true);

		// ---- write into child vector ----------------------------------------
		list_entry_t &entry = list_entries[row];
		entry.offset = child_offset;

		// 1. primary (always present if not empty)
		if (!primary.empty()) {
			child_strings[child_offset++] = StringVector::AddString(child, primary);
		}

		// 2. alternate (only if non-empty and different)
		if (!alternate.empty() && alternate != primary) {
			child_strings[child_offset++] = StringVector::AddString(child, alternate);
		}

		entry.length = child_offset - entry.offset; // number of elements in *this* list
	}

	// Tell DuckDB how many rows we really appended to the child
	ListVector::SetListSize(result, child_offset);
}

static void LoadInternal(DatabaseInstance &instance) {
	ExtensionUtil::RegisterFunction(
	    instance, ScalarFunction("soundex", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SoundexScalar));

	ExtensionUtil::RegisterFunction(instance, ScalarFunction("strip_diacritics", {LogicalType::VARCHAR},
	                                                         LogicalType::VARCHAR, StripDiacriticsScalar));

	ExtensionUtil::RegisterFunction(
	    instance, ScalarFunction("unaccent", {LogicalType::VARCHAR}, LogicalType::VARCHAR, UnaccentScalar));

	// Register double_metaphone with new list return type
	ExtensionUtil::RegisterFunction(instance,
	                                ScalarFunction("double_metaphone", {LogicalType::VARCHAR},
	                                               LogicalType::LIST(LogicalType::VARCHAR), DoubleMetaphoneScalarList));
}

void SplinkUdfsExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string SplinkUdfsExtension::Name() {
	return "splink_udfs";
}

std::string SplinkUdfsExtension::Version() const {
	return duckdb::DuckDB::LibraryVersion();
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void splink_udfs_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB(db).LoadExtension<duckdb::SplinkUdfsExtension>();
}

DUCKDB_EXTENSION_API const char *splink_udfs_version() {
	return duckdb::DuckDB::LibraryVersion();
}

} // extern "C"
