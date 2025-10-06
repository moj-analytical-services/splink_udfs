#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/exception.hpp"

#include "trie/address_trie_functions.hpp"
#include "trie/address_match_params.hpp"
#include "trie/suffix_trie.hpp"
#include "trie/address_lookup.hpp"
#include "trie/suffix_trie_cache.hpp"
#include "trie/trie_cache_utils.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

struct FindAddressLocalState : public FunctionLocalState {
	TrieCache cache;
};

static unique_ptr<FunctionLocalState> FindAddressInitLocal(ExpressionState & /*state*/,
                                                           const BoundFunctionExpression & /*expr*/,
                                                           FunctionData * /*bind_data*/) {
	return make_uniq<FindAddressLocalState>();
}

// Centralize local state access for portability across DuckDB versions.
static inline FindAddressLocalState &GetFindAddressLocal(ExpressionState &state) {
	auto ptr = ExecuteFunctionState::GetFunctionState(state);
	D_ASSERT(ptr);
	return ptr->Cast<FindAddressLocalState>();
}

static uint32_t ClampAddressParam(int64_t value) {
	if (value < 0) {
		return 0;
	}
	const auto max_u32 = static_cast<int64_t>(std::numeric_limits<uint32_t>::max());
	if (value > max_u32) {
		return std::numeric_limits<uint32_t>::max();
	}
	return static_cast<uint32_t>(value);
}

static void AssignParamIfValid(uint32_t &field, const UnifiedVectorFormat &data, const int64_t *values, idx_t row) {
	const idx_t idx = data.sel->get_index(row);
	if (!data.validity.RowIsValid(idx)) {
		return;
	}
	field = ClampAddressParam(values[idx]);
}

static void ExecuteFindAddress(DataChunk &args, ExpressionState &state, Vector &result, bool has_params) {
	const idx_t expected_cols = has_params ? 8 : 2;
	D_ASSERT(args.ColumnCount() == expected_cols);

	auto &list_vec = args.data[0];
	auto &blob_vec = args.data[1];

	const idx_t count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetData<int64_t>(result);

	UnifiedVectorFormat list_data;
	list_vec.ToUnifiedFormat(count, list_data);
	auto list_entries = ListVector::GetData(list_vec);
	auto &child_vec = ListVector::GetEntry(list_vec);
	UnifiedVectorFormat child_data;
	child_vec.ToUnifiedFormat(ListVector::GetListSize(list_vec), child_data);
	auto child_vals = UnifiedVectorFormat::GetData<string_t>(child_data);

	UnifiedVectorFormat blob_data;
	blob_vec.ToUnifiedFormat(count, blob_data);
	auto blob_vals = UnifiedVectorFormat::GetData<string_t>(blob_data);

	UnifiedVectorFormat param_data[6];
	const int64_t *param_values[6] = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr};
	if (has_params) {
		for (idx_t col = 0; col < 6; ++col) {
			auto &vec = args.data[2 + col];
			vec.ToUnifiedFormat(count, param_data[col]);
			param_values[col] = UnifiedVectorFormat::GetData<int64_t>(param_data[col]);
		}
	}

	auto &lstate = GetFindAddressLocal(state);

	for (idx_t row = 0; row < count; ++row) {
		const auto list_rid = list_data.sel->get_index(row);
		const auto blob_rid = blob_data.sel->get_index(row);

		if (!list_data.validity.RowIsValid(list_rid) || !blob_data.validity.RowIsValid(blob_rid)) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		const auto le = list_entries[list_rid];
		if (le.length == 0) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		const auto blob = blob_vals[blob_rid];
		auto parsed = GetOrParseTrie(lstate.cache, blob);
		if (!parsed || !parsed->root) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		std::vector<std::string> toks;
		toks.reserve(le.length);
		for (idx_t k = 0; k < le.length; ++k) {
			const idx_t cidx = child_data.sel->get_index(le.offset + k);
			if (!child_data.validity.RowIsValid(cidx)) {
				continue;
			}
			toks.emplace_back(child_vals[cidx].GetString());
		}

		if (toks.empty()) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		AddressMatchParams params = DefaultMatchParams();
		if (has_params) {
			AssignParamIfValid(params.skip_min_local_count, param_data[0], param_values[0], row);
			AssignParamIfValid(params.skip_max_in_walk, param_data[1], param_values[1], row);
			AssignParamIfValid(params.min_matched_tokens, param_data[2], param_values[2], row);
			AssignParamIfValid(params.entry_min_local_count, param_data[3], param_values[3], row);
			AssignParamIfValid(params.max_trailing_tokens_ignored, param_data[4], param_values[4], row);
			AssignParamIfValid(params.max_trie_entry_depth, param_data[5], param_values[5], row);
		}

		uint64_t uprn = 0;
		const bool ok = FindAddressExact(*parsed, toks, params, uprn);
		if (!ok) {
			FlatVector::SetNull(result, row, true);
			continue;
		}
		out[row] = static_cast<int64_t>(uprn);
	}
}

static void FindAddressScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	ExecuteFindAddress(args, state, result, false);
}

static void FindAddressScalarParam(DataChunk &args, ExpressionState &state, Vector &result) {
	ExecuteFindAddress(args, state, result, true);
}

ScalarFunctionSet GetFindAddressFunctionSet() {
	ScalarFunctionSet set("find_address");

	ScalarFunction base_fn({LogicalType::LIST(LogicalType::VARCHAR), LogicalType::BLOB}, LogicalType::BIGINT,
	                       FindAddressScalar);
	base_fn.init_local_state = FindAddressInitLocal;
	set.AddFunction(base_fn);

	ScalarFunction param_fn({LogicalType::LIST(LogicalType::VARCHAR), LogicalType::BLOB, LogicalType::BIGINT,
	                         LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                         LogicalType::BIGINT},
	                        LogicalType::BIGINT, FindAddressScalarParam);
	param_fn.init_local_state = FindAddressInitLocal;
	set.AddFunction(param_fn);

	return set;
}

} // namespace duckdb
