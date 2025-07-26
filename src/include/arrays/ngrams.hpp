//===----------------------------------------------------------------------===//
// ngrams(list [, n])                                            SPLINK‑UDFS
// Header‑only implementation – uses only DuckDB public headers (C++11)
// Produces LIST(ARRAY(child_type, n)) of every contiguous n‑gram in the input
// list.  Requires that `n` is a positive scalar constant at bind‑time.
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/fixed_list_vector.hpp"
#include "duckdb/common/types/list_vector.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace splink_udfs {
using namespace duckdb;

// -------------------------------------------------------------------------
// Bind data
// -------------------------------------------------------------------------
struct NgramsBindData : public FunctionData {
	idx_t n;                 // fixed length of every ARRAY element
	LogicalType child_type;  // element type of the incoming LIST
	LogicalType return_type; // LIST(ARRAY(child_type,n))

public:
	//! Copy / equals boiler‑plate
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<NgramsBindData>(*this);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = const_cast<FunctionData &>(other_p).Cast<NgramsBindData>();
		return other.n == n && other.child_type == child_type;
	}
};

// -------------------------------------------------------------------------
// Binding
// -------------------------------------------------------------------------
static unique_ptr<FunctionData> NgramsBind(ClientContext &context, ScalarFunction &func,
                                           vector<unique_ptr<Expression>> &arguments) {
	// ---------- validate first argument ----------
	if (arguments[0]->return_type.id() != LogicalTypeId::LIST) {
		throw BinderException("ngrams: first argument must be a LIST<T>");
	}
	auto child_type = ListType::GetChildType(arguments[0]->return_type);

	// ---------- obtain & validate n (constant!) ----------
	Value n_val = Value::BIGINT(2); // default
	if (arguments.size() == 2) {
		if (!arguments[1]->IsFoldable()) {
			throw BinderException("ngrams: n must be a constant scalar");
		}
		n_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	}
	if (n_val.IsNull()) {
		throw BinderException("ngrams: n cannot be NULL");
	}
	if (!n_val.DefaultCastAs(LogicalType::BIGINT)) {
		throw BinderException("ngrams: n must be BIGINT");
	}
	auto n = n_val.GetValueUnsafe<int64_t>();
	if (n <= 0) {
		throw BinderException("ngrams: n must be positive");
	}

	// ---------- construct return type ----------
	LogicalType array_type = LogicalType::ARRAY(child_type, n);
	LogicalType return_type = LogicalType::LIST(array_type);
	func.return_type = return_type;

	auto bind = make_uniq<NgramsBindData>();
	bind->n = static_cast<idx_t>(n);
	bind->child_type = child_type;
	bind->return_type = return_type;
	return std::move(bind);
}

// -------------------------------------------------------------------------
// Execution
// -------------------------------------------------------------------------
static void NgramsExec(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.bind_data->Cast<NgramsBindData>();
	const idx_t n = bind_data.n;
	const idx_t count = args.size();

	// ----- read input LIST -----
	auto &in_list_vec = args.data[0];
	UnifiedVectorFormat list_uf;
	in_list_vec.ToUnifiedFormat(count, list_uf);
	auto in_entries = reinterpret_cast<list_entry_t *>(list_uf.data);
	auto &in_child = ListVector::GetEntry(in_list_vec);

	// ---- first pass: determine #arrays per row & total capacity ----
	vector<idx_t> per_row(count, 0);
	idx_t total_arrays = 0;

	for (idx_t i = 0; i < count; ++i) {
		const idx_t idx = list_uf.sel->get_index(i);
		if (!list_uf.validity.RowIsValid(idx)) {
			continue; // NULL row – leave per_row[i]==0; will mark NULL later
		}
		auto len = in_entries[idx].length;
		if (len >= n) {
			per_row[i] = len - n + 1;
			total_arrays += per_row[i];
		}
	}

	// ---- prepare result LIST(ARRAY(child,n)) ----
	ListVector::SetListSize(result, 0); // start empty
	ListVector::Reserve(result, total_arrays);
	auto &arrays_vec = ListVector::GetEntry(result); // VECTOR<ARRAY>
	FixedListVector::Reserve(arrays_vec, total_arrays);
	FixedListVector::SetListSize(arrays_vec, total_arrays);

	auto &array_values = FixedListVector::GetData(arrays_vec); // child vector holding real values

	// convenience pointers
	auto res_entries = FlatVector::GetData<list_entry_t>(result);
	auto &res_validity = FlatVector::Validity(result);

	// ---- second pass: populate ----
	idx_t global_array_idx = 0; // running offset into arrays_vec
	for (idx_t i = 0; i < count; ++i) {
		const idx_t sel_idx = list_uf.sel->get_index(i);

		// NULL row?
		if (!list_uf.validity.RowIsValid(sel_idx)) {
			res_validity.SetInvalid(i);
			continue;
		}

		// empty n‑gram list
		if (per_row[i] == 0) {
			res_entries[i].offset = global_array_idx;
			res_entries[i].length = 0;
			continue;
		}

		// offset/length for this outer row
		res_entries[i].offset = global_array_idx;
		res_entries[i].length = per_row[i];

		const auto src_off = in_entries[sel_idx].offset;

		for (idx_t j = 0; j < per_row[i]; ++j, ++global_array_idx) {
			// copy 'n' contiguous elements starting at src_off + j into destination
			const idx_t dest_off = global_array_idx * n;
			VectorOperations::Copy(in_child, array_values, src_off + j, dest_off, n);
		}
	}

	// ensure outer validity mask written for all rows without NULLs
	res_validity.EnsureWritable();
}

// -------------------------------------------------------------------------
// Registration helper
// -------------------------------------------------------------------------
static inline void RegisterNgrams(DatabaseInstance &inst) {
	ScalarFunctionSet set("ngrams");

	// (1) one‑argument form  → defaults n=2
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalTypeId::ANY /* filled in binder */,
	                               NgramsExec, NgramsBind));

	// (2) two‑argument form
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::BIGINT},
	                               LogicalTypeId::ANY /* filled in binder */, NgramsExec, NgramsBind));

	ExtensionUtil::RegisterFunction(inst, set);
}

} // namespace splink_udfs
