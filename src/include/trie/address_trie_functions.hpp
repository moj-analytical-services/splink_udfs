#pragma once

#include "duckdb.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

// Keep ONLY the builder aggregate declaration
AggregateFunctionSet GetBuildSuffixTrieAggregateSet();

} // namespace duckdb
