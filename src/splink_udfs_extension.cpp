#define DUCKDB_EXTENSION_MAIN // must precede DuckDB headers
#include "splink_udfs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include "phonetic/soundex.hpp"

namespace duckdb {

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

static void LoadInternal(DatabaseInstance &instance) {
	ExtensionUtil::RegisterFunction(
	    instance, ScalarFunction("soundex", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SoundexScalar));
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
