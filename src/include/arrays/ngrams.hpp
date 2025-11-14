#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

// -------------------- Bind data --------------------
struct NgramsBindData : public FunctionData {
	idx_t n;
	LogicalType child_type;

	NgramsBindData(idx_t n_p, LogicalType child) : n(n_p), child_type(std::move(child)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<NgramsBindData>(n, child_type);
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<NgramsBindData>();
		return n == o.n && child_type == o.child_type;
	}
};

// -------------------- Binder --------------------
static unique_ptr<FunctionData> NgramsBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &args) {
	if (args.size() != 2) {
		throw BinderException("ngrams(list, n): expected exactly two arguments");
	}

	// First arg must be LIST(T) (or NULL, which we cast to LIST(VARCHAR))
	LogicalType list_type;
	LogicalType child_type;
	if (args[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		list_type = LogicalType::LIST(LogicalType::VARCHAR);
		child_type = LogicalType::VARCHAR;
		args[0] = BoundCastExpression::AddCastToType(context, std::move(args[0]), list_type);
	} else if (args[0]->return_type.id() == LogicalTypeId::LIST) {
		list_type = args[0]->return_type;
		child_type = ListType::GetChildType(list_type);
	} else {
		throw BinderException("ngrams(list, n): first argument must be a LIST");
	}

	// n must be a constant > 0
	if (!args[1]->IsFoldable()) {
		throw BinderException("ngrams(list, n): n must be a constant");
	}
	auto n_val = ExpressionExecutor::EvaluateScalar(context, *args[1]).CastAs(context, LogicalType::BIGINT);
	if (n_val.IsNull()) {
		throw BinderException("ngrams(list, n): n cannot be NULL");
	}
	auto n = n_val.GetValue<int64_t>();
	if (n <= 0) {
		throw BinderException("ngrams(list, n): n must be positive");
	}

	// Concrete argument/return types
	bound_function.arguments[0] = list_type;
	bound_function.arguments[1] = LogicalType::BIGINT;
	bound_function.return_type = LogicalType::LIST(LogicalType::ARRAY(child_type, idx_t(n)));

	return make_uniq<NgramsBindData>(idx_t(n), child_type);
}

// -------------------- Executor --------------------
static void NgramsExec(DataChunk &args, ExpressionState &state, Vector &result) {
	// Bind data
	auto &fexpr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind = fexpr.bind_info->Cast<NgramsBindData>();
	const idx_t n = bind.n;

	const idx_t row_count = args.size();
	auto &in_list = args.data[0];

	// Flatten list entries
	UnifiedVectorFormat list_uvf;
	in_list.ToUnifiedFormat(row_count, list_uvf);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_uvf);

	// First pass: total number of n-grams across the batch
	idx_t total_ngrams = 0;
	bool all_rows_null = true;
	for (idx_t row = 0; row < row_count; ++row) {
		auto i = list_uvf.sel->get_index(row);
		if (!list_uvf.validity.RowIsValid(i)) {
			continue;
		}
		all_rows_null = false;
		const auto len = list_entries[i].length;
		if (len >= n) {
			total_ngrams += (len - n + 1);
		}
	}

	// If all inputs are NULL → NULL result
	if (all_rows_null) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	// Prepare result list-of-arrays
	result.SetVectorType(VectorType::FLAT_VECTOR);
	ListVector::Reserve(result, total_ngrams);
	ListVector::SetListSize(result, total_ngrams);
	auto res_entries = FlatVector::GetData<list_entry_t>(result);
	auto &res_validity = FlatVector::Validity(result);

	// Child vectors: LIST -> ARRAY(T, n) -> T
	auto &array_vec = ListVector::GetEntry(result);
	auto &array_child = ArrayVector::GetEntry(array_vec);

	// Make sure the array child is a flat, writable vector of length total_ngrams * n
	array_child.Flatten(total_ngrams * n);

	// Access input child once
	auto &input_child = ListVector::GetEntry(in_list);

	// We'll copy n elements at a time using a selection vector
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	idx_t next_array_idx = 0; // number of arrays emitted so far
	idx_t next_child_idx = 0; // number of scalar elements written into array_child

	for (idx_t row = 0; row < row_count; ++row) {
		auto i = list_uvf.sel->get_index(row);

		// Handle NULL rows
		if (!list_uvf.validity.RowIsValid(i)) {
			res_validity.SetInvalid(row);
			res_entries[row] = {next_array_idx, 0};
			continue;
		}

		const auto start = list_entries[i].offset;
		const auto len = list_entries[i].length;

		const idx_t row_out_len = (len < n) ? 0 : (len - n + 1);
		res_entries[row] = {next_array_idx, row_out_len};

		for (idx_t g = 0; g < row_out_len; ++g) {
			// select contiguous [start+g, start+g+n)
			for (idx_t k = 0; k < n; ++k) {
				sel.set_index(k, start + g + k);
			}
			// Copy n elements into the array child, starting at next_child_idx
			VectorOperations::Copy(input_child, array_child, sel, n, /*source_offset*/ 0,
			                       /*target_offset*/ next_child_idx);
			next_child_idx += n;
			++next_array_idx;
		}
	}

	D_ASSERT(next_array_idx == total_ngrams);
	D_ASSERT(next_child_idx == total_ngrams * n);
}

//===--------------------------------------------------------------------===//
// Factory helpers
//===--------------------------------------------------------------------===//
static ScalarFunction MakeFunc() {
	auto list_any = LogicalType::LIST(LogicalType::ANY);
	ScalarFunction fun("ngrams", {list_any, LogicalType::BIGINT}, list_any, NgramsExec, NgramsBind);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

//===--------------------------------------------------------------------===//
// Registrar – call from the extension's LoadInternal()
//===--------------------------------------------------------------------===//
static inline void RegisterNgrams(ExtensionLoader &loader) {
	ScalarFunctionSet set("ngrams");
	set.AddFunction(MakeFunc());
	loader.RegisterFunction(set);
}

} // namespace duckdb
