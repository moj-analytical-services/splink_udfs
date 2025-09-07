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
// Scalar: debug formatter along path with term/uprn metadata
ScalarFunctionSet GetFormatAddressWithTermFunctionSet();
// Scalar: exact unique match -> BIGINT UPRN or NULL
ScalarFunctionSet GetFindAddressFromTrieFunctionSet();
// Debug struct variant: uprn/matched_len/is_terminal/ambiguous
ScalarFunctionSet GetFindAddressFromTrieDbgFunctionSet();

} // namespace duckdb
