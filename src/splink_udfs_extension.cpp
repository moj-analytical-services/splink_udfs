#define DUCKDB_EXTENSION_MAIN

#include "splink_udfs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void SplinkUdfsScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "SplinkUdfs " + name.GetString() + " 🐥");
	});
}

inline void SplinkUdfsOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "SplinkUdfs " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto splink_udfs_scalar_function = ScalarFunction("splink_udfs", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SplinkUdfsScalarFun);
	ExtensionUtil::RegisterFunction(instance, splink_udfs_scalar_function);

	// Register another scalar function
	auto splink_udfs_openssl_version_scalar_function = ScalarFunction("splink_udfs_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, SplinkUdfsOpenSSLVersionScalarFun);
	ExtensionUtil::RegisterFunction(instance, splink_udfs_openssl_version_scalar_function);
}

void SplinkUdfsExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string SplinkUdfsExtension::Name() {
	return "splink_udfs";
}

std::string SplinkUdfsExtension::Version() const {
#ifdef EXT_VERSION_SPLINK_UDFS
	return EXT_VERSION_SPLINK_UDFS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void splink_udfs_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SplinkUdfsExtension>();
}

DUCKDB_EXTENSION_API const char *splink_udfs_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
