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
#include "duckdb/planner/expression/bound_cast_expression.hpp"
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
	// First argument must be LIST or NULL
	// --- determine list element type ------------------------------------------
	auto &arg0_type = args[0]->return_type;

	LogicalType list_type;
	LogicalType child_type;

	if (arg0_type.id() == LogicalTypeId::SQLNULL) {
		// Special case: user wrote ngrams(NULL, n) → use VARCHAR as fallback
		// This avoids issues with ANY types in the internal type system
		list_type = LogicalType::LIST(LogicalType::VARCHAR);
		child_type = LogicalType::VARCHAR;

		// Inject an implicit cast so the rest of the pipeline sees the right type
		args[0] = BoundCastExpression::AddCastToType(context, std::move(args[0]), list_type);
	} else if (arg0_type.id() == LogicalTypeId::LIST) {
		list_type = arg0_type;
		child_type = ListType::GetChildType(list_type);
	} else {
		throw BinderException("ngrams: first argument must be a LIST or NULL");
	}

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

	bool fast_string_path = bind.child_type.InternalType() == PhysicalType::VARCHAR;

	auto &in_list = args.data[0];
	result.SetVectorType(VectorType::FLAT_VECTOR);

	// Create one reusable SelectionVector after row_count is known
	SelectionVector scratch_sel(STANDARD_VECTOR_SIZE);

	// Input list: flatten metadata
	UnifiedVectorFormat list_data;
	in_list.ToUnifiedFormat(row_count, list_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	auto &input_child = ListVector::GetEntry(in_list);
	idx_t input_child_size = ListVector::GetListSize(in_list);

	// Use UnifiedVectorFormat instead of full flatten
	UnifiedVectorFormat child_view;
	input_child.ToUnifiedFormat(input_child_size, child_view);
	auto child_data = UnifiedVectorFormat::GetData<data_t>(child_view);

	// First pass – count total n‑grams
	idx_t total_ngrams = 0;
	bool all_rows_null = true;
	for (idx_t row = 0; row < row_count; ++row) {
		auto idx = list_data.sel->get_index(row);
		if (!list_data.validity.RowIsValid(idx)) {
			continue; // NULL row
		}
		all_rows_null = false;
		auto len = list_entries[idx].length;
		if (len >= n) {
			total_ngrams += len - n + 1;
		}
	}

	// ───── fast bail‑out: nothing to emit ────────────────
	if (total_ngrams == 0) {
		if (all_rows_null) {
			// All input rows are NULL, so result should be NULL
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			// --- produce an empty list for every row in a flat vector ---
			result.SetVectorType(VectorType::FLAT_VECTOR);
			ListVector::Reserve(result, 0); // no child elements
			ListVector::SetListSize(result, 0);

			auto res_entries = FlatVector::GetData<list_entry_t>(result);
			auto &res_validity = FlatVector::Validity(result);

			for (idx_t row = 0; row < row_count; ++row) {
				res_entries[row] = {0, 0}; // offset 0, length 0
				                           // validity already true (not NULL)
			}
		}
		return;
	}

	// --- allocate selection buffer & build dictionary indices (fast string path) ---
	buffer_ptr<SelectionData> sel_buffer; // will also hold the SelectionVector
	SelectionVector sel_dict;             // defined only if we take the fast path

	if (fast_string_path && total_ngrams > 0) {
		sel_buffer = make_buffer<SelectionData>(total_ngrams * n); // owns data
		sel_dict = SelectionVector(sel_buffer);                    // views data

		idx_t pos = 0;
		for (idx_t row = 0; row < row_count; ++row) {
			auto idx = list_data.sel->get_index(row);
			if (!list_data.validity.RowIsValid(idx))
				continue;

			const auto len = list_entries[idx].length;
			const auto start = list_entries[idx].offset;
			if (len < n)
				continue;

			for (idx_t g = 0; g < len - n + 1; ++g)
				for (idx_t k = 0; k < n; ++k)
					sel_dict.set_index(pos++, start + g + k);
		}
		D_ASSERT(pos == total_ngrams * n);
	}

	// Reserve outer/inner space
	ListVector::Reserve(result, total_ngrams);
	ListVector::SetListSize(result, total_ngrams);

	auto &array_vec = ListVector::GetEntry(result);       // ARRAY(...)
	auto &array_child = ArrayVector::GetEntry(array_vec); // inner scalar values
	if (!fast_string_path) {
		array_child.Resize(0, total_ngrams * n);
		array_child.Flatten(total_ngrams * n);
	}

	if (fast_string_path && total_ngrams > 0) {
		Vector dict_child(input_child.GetType());
		dict_child.Reference(input_child); // share payload

		// zero‑copy slice + attach buffer for lifetime safety
		dict_child.Slice(sel_dict, total_ngrams * n);

		array_child.Reference(dict_child); // ARRAY now points to safe dictionary
	}

	auto res_entries = FlatVector::GetData<list_entry_t>(result);
	auto &res_validity = FlatVector::Validity(result);

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

		if (fast_string_path) {
			// only adjust `next_array_idx` / `next_child_idx`, no copying
			next_child_idx += out_len * n;
			next_array_idx += out_len;
			continue; // jump to next row
		}

		for (idx_t g = 0; g < out_len; ++g) {
			// Build selection for this n‑gram
			for (idx_t k = 0; k < n; ++k) {
				scratch_sel.set_index(k, list_start + g + k);
			}
			// Copy n elements into the correct slice of array_child using original vector
			VectorOperations::Copy(input_child, array_child, scratch_sel, n, 0, next_child_idx);
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
