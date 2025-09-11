#pragma once

#include "duckdb.hpp"

namespace duckdb {

class SplinkUdfsExtension : public Extension {
public:
	// Called when the extension is loaded into a DuckDB instance
	void Load(ExtensionLoader &loader) override;

	// The SQL-visible name of the extension
	std::string Name() override;

	// The version string (can be empty)
	std::string Version() const override;
};

} // namespace duckdb
// no extern "C" declarations here; DUCKDB_CPP_EXTENSION_ENTRY is defined in the .cpp for v1.4+
