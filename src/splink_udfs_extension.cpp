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
static void DoubleMetaphoneScalar(DataChunk &data_chunk, ExpressionState & /*state*/, Vector &result) {
	const idx_t count = data_chunk.size();
	auto &input = data_chunk.data[0];

	// Optional second argument: TRUE = alternate code, FALSE/NULL = primary
	bool has_alt_arg = data_chunk.ColumnCount() == 2;
	Vector *alt_vec = has_alt_arg ? &data_chunk.data[1] : nullptr;

	// Re-use one encoder per chunk
	DoubleMetaphone encoder;

	if (has_alt_arg) {
		// Binary executor for handling both arguments
		BinaryExecutor::Execute<string_t, bool, string_t>(
		    input, *alt_vec, result, count, [&](const string_t &val, bool use_alternate) -> string_t {
			    if (val.GetSize() == 0) {
				    return StringVector::AddString(result, "");
			    }
			    std::string_view sv(val.GetDataUnsafe(), val.GetSize());
			    std::string code = encoder.DoubleMetaphoneEncode(std::string(sv), use_alternate);
			    return StringVector::AddString(result, code);
		    });
	} else {
		// Unary executor for single argument (primary code only)
		UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](const string_t &val) -> string_t {
			if (val.GetSize() == 0) {
				return StringVector::AddString(result, "");
			}
			std::string_view sv(val.GetDataUnsafe(), val.GetSize());
			std::string code = encoder.DoubleMetaphoneEncode(std::string(sv), false); // primary code
			return StringVector::AddString(result, code);
		});
	}
}

static void LoadInternal(DatabaseInstance &instance) {
	ExtensionUtil::RegisterFunction(
	    instance, ScalarFunction("soundex", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SoundexScalar));

	ExtensionUtil::RegisterFunction(instance, ScalarFunction("strip_diacritics", {LogicalType::VARCHAR},
	                                                         LogicalType::VARCHAR, StripDiacriticsScalar));

	ExtensionUtil::RegisterFunction(
	    instance, ScalarFunction("unaccent", {LogicalType::VARCHAR}, LogicalType::VARCHAR, UnaccentScalar));

	// Register double_metaphone with optional second argument for alternate code
	ExtensionUtil::RegisterFunction(instance, ScalarFunction("double_metaphone", {LogicalType::VARCHAR},
	                                                         LogicalType::VARCHAR, DoubleMetaphoneScalar));
	ExtensionUtil::RegisterFunction(instance,
	                                ScalarFunction("double_metaphone", {LogicalType::VARCHAR, LogicalType::BOOLEAN},
	                                               LogicalType::VARCHAR, DoubleMetaphoneScalar));
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
