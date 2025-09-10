#pragma once

#include "duckdb.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

AggregateFunctionSet GetBuildSuffixTrieAggregateSet();

// Simple exact lookup: follows reversed tokens through the trie and returns UPRN if terminal, else NULL
ScalarFunction GetFindAddressFunction();

} // namespace duckdb
