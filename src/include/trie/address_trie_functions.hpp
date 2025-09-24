#pragma once

#include "duckdb.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

AggregateFunctionSet GetBuildSuffixTrieAggregateSet();

// Simple exact lookup: follows reversed tokens through the trie and returns UPRN if terminal, else NULL
ScalarFunction GetFindAddressFunction();

// Diagnostics-oriented lookup: same walk rules as find_address, but returns candidates + trace
ScalarFunction GetFindCandidatesFunction();

} // namespace duckdb
