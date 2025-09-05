#pragma once

#include "duckdb.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

// Aggregate/Scalar pair for serialized suffix tries
AggregateFunctionSet GetBuildSuffixTrieAggregateSet();
// Scalar function to peel end tokens from lists
ScalarFunctionSet GetPeelEndTokensFunctionSet();
// Scalar function build_cleaned_address
ScalarFunctionSet GetBuildCleanedAddressFunctionSet();
// Scalar: pretty-print tokens with suffix counts from the trie
ScalarFunctionSet GetFormatAddressWithCountsFunctionSet();

} // namespace duckdb
