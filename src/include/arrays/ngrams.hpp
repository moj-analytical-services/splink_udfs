#pragma once
// SPDX‑License‑Identifier: MIT
// ---------------------------------------------------------------------------
//  ngrams(list<any> [, n BIGINT]) → LIST(ARRAY(any,n))
// ---------------------------------------------------------------------------

#include "duckdb.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Bind data
//===--------------------------------------------------------------------===//
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
		return n == o.n && child_type == o.child_type; // Fix: Use == operator instead of AllEqual
	}
};

//===--------------------------------------------------------------------===//
// Binder
//===--------------------------------------------------------------------===//
static unique_ptr<FunctionData> NgramsBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &args) {
	if (args.size() != 2) {
		throw BinderException("ngrams: expected exactly two arguments (list, n)");
	}
	// First argument must be LIST
	auto &list_type = args[0]->return_type;
	if (list_type.id() != LogicalTypeId::LIST) {
		throw BinderException("ngrams: first argument must be a LIST");
	}
	auto child_type = ListType::GetChildType(list_type);

	// Parse n (constant BIGINT, 1‑based)
	if (!args[1]->IsFoldable()) {
		throw BinderException("ngrams: n must be a constant");
	}
	Value v = ExpressionExecutor::EvaluateScalar(context, *args[1]).CastAs(context, LogicalType::BIGINT);
	if (v.IsNull()) {
		throw BinderException("ngrams: n cannot be NULL");
	}
	auto n = v.GetValue<idx_t>();
	if (n <= 0) {
		throw BinderException("ngrams: n must be a positive integer");
	}

	// Fix the argument types and return type
	bound_function.arguments[0] = list_type; // concrete LIST type
	bound_function.arguments[1] = LogicalType::BIGINT;
	LogicalType array_type = LogicalType::ARRAY(child_type, n); // ARRAY(any,n)
	bound_function.return_type = LogicalType::LIST(array_type); // LIST(ARRAY(...))

	return make_uniq<NgramsBindData>(n, child_type);
}

//===--------------------------------------------------------------------===//
// Executor
//===--------------------------------------------------------------------===//
static void NgramsExec(DataChunk &args, ExpressionState &state, Vector &result) {
	// Fix: Access bind data through the function expression
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind = func_expr.bind_info->Cast<NgramsBindData>();
	const idx_t n = bind.n;
	idx_t row_count = args.size();

	auto &in_list = args.data[0];
	result.SetVectorType(VectorType::FLAT_VECTOR);
	ListVector::Reserve(result, STANDARD_VECTOR_SIZE);

	// Input list: flatten metadata
	UnifiedVectorFormat list_data;
	in_list.ToUnifiedFormat(row_count, list_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	auto &input_child = ListVector::GetEntry(in_list);
	idx_t input_child_size = ListVector::GetListSize(in_list);

	// We copy from a flattened view of the child once
	Vector input_child_flat(input_child);
	input_child_flat.Flatten(input_child_size);

	// First pass – count total n‑grams
	idx_t total_ngrams = 0;
	for (idx_t row = 0; row < row_count; ++row) {
		auto idx = list_data.sel->get_index(row);
		if (!list_data.validity.RowIsValid(idx)) {
			continue; // NULL row
		}
		auto len = list_entries[idx].length;
		if (len >= n) {
			total_ngrams += len - n + 1;
		}
	}

	// Reserve outer/inner space
	ListVector::Reserve(result, total_ngrams);
	ListVector::SetListSize(result, total_ngrams);

	auto &array_vec = ListVector::GetEntry(result);       // ARRAY(...)
	auto &array_child = ArrayVector::GetEntry(array_vec); // inner scalar values
	array_child.Resize(0, total_ngrams * n);
	array_child.Flatten(total_ngrams * n);

	auto res_entries = FlatVector::GetData<list_entry_t>(result);
	auto &res_validity = FlatVector::Validity(result);

	SelectionVector sel(n);
	idx_t next_array_idx = 0; // counts arrays (outer list)
	idx_t next_child_idx = 0; // counts scalar elements in array_child

	for (idx_t row = 0; row < row_count; ++row) {
		auto iidx = list_data.sel->get_index(row);
		if (!list_data.validity.RowIsValid(iidx)) {
			res_validity.SetInvalid(row);
			res_entries[row] = {next_array_idx, 0};
			continue;
		}

		const auto list_len = list_entries[iidx].length;
		const auto list_start = list_entries[iidx].offset;
		const idx_t out_len = (list_len < n) ? 0 : (list_len - n + 1);

		res_entries[row] = {next_array_idx, out_len};

		for (idx_t g = 0; g < out_len; ++g) {
			// Build selection for this n‑gram
			for (idx_t k = 0; k < n; ++k) {
				sel.set_index(k, list_start + g + k);
			}
			// Copy n elements into the correct slice of array_child
			VectorOperations::Copy(input_child_flat, array_child, sel, n, 0, next_child_idx);
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
static inline void RegisterNgrams(DatabaseInstance &db) {
	ScalarFunctionSet set("ngrams");
	set.AddFunction(MakeFunc());
	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace duckdb