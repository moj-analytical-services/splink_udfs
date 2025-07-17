#pragma once

#include "duckdb.hpp"

namespace duckdb {

class SplinkUdfsExtension : public Extension {
public:
	// Called when the extension is loaded into a DuckDB instance
	void Load(DuckDB &db) override;

	// The SQL-visible name of the extension
	std::string Name() override;

	// The version string (can be empty)
	std::string Version() const override;
};

} // namespace duckdb

extern "C" {

// Entry-point for DuckDB’s load_extension() call
DUCKDB_EXTENSION_API void splink_udfs_init(duckdb::DatabaseInstance &db);

// Returns the DuckDB library version
DUCKDB_EXTENSION_API const char *splink_udfs_version();
}
