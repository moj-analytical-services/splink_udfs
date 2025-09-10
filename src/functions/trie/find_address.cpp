#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/exception.hpp"

#include "trie/address_trie_functions.hpp"
#include "trie/suffix_trie.hpp"
#include "trie/suffix_trie_cache.hpp"
#include "trie/trie_cache_utils.hpp"

#include <algorithm>
#include <cstdint>
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

// Binary search for child by token in a node's sorted kids vector
static inline PNode *FindChildByToken(PNode *node, const std::string &tok) {
	if (DUCKDB_UNLIKELY(node == nullptr)) {
		return nullptr;
	}
	auto &kids = node->kids;
	auto it = std::lower_bound(kids.begin(), kids.end(), tok,
	                           [](const std::pair<std::string, PNode *> &kv, const std::string &s) {
		                           return kv.first < s;
	                           });
	if (it == kids.end()) {
		return nullptr;
	}
	if (it->first != tok) {
		return nullptr;
	}
	return it->second;
}

static void FindAddressScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto &list_vec = args.data[0];
	auto &blob_vec = args.data[1];

	const idx_t count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetData<int64_t>(result);

	// Read token list columns
	UnifiedVectorFormat list_data;
	list_vec.ToUnifiedFormat(count, list_data);
	auto list_entries = ListVector::GetData(list_vec);
	auto &child_vec = ListVector::GetEntry(list_vec);
	UnifiedVectorFormat child_data;
	child_vec.ToUnifiedFormat(ListVector::GetListSize(list_vec), child_data);
	auto child_vals = UnifiedVectorFormat::GetData<string_t>(child_data);

	// Read blob column
	UnifiedVectorFormat blob_data;
	blob_vec.ToUnifiedFormat(count, blob_data);
	auto blob_vals = UnifiedVectorFormat::GetData<string_t>(blob_data);

    auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<FindAddressLocalState>();

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
		// Parse or fetch from cache
		auto parsed = GetOrParseTrie(lstate.cache, blob);
		if (!parsed || !parsed->root) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		PNode *node = parsed->root;
		bool failed = false;
		// Walk tokens in reverse order
		for (idx_t k = 0; k < le.length; ++k) {
			const idx_t cidx = child_data.sel->get_index(le.offset + (le.length - 1 - k));
			if (!child_data.validity.RowIsValid(cidx)) {
				failed = true;
				break;
			}
			const auto tok = child_vals[cidx].GetString();
			node = FindChildByToken(node, tok);
			if (node == nullptr) {
				failed = true;
				break;
			}
		}

		if (failed) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		if (node->term == 0) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		out[row] = static_cast<int64_t>(node->uprn);
	}
}

ScalarFunction GetFindAddressFunction() {
	ScalarFunction fn("find_address", {LogicalType::LIST(LogicalType::VARCHAR), LogicalType::BLOB},
	                  LogicalType::BIGINT, FindAddressScalar, nullptr, nullptr, nullptr, FindAddressInitLocal);
	return fn;
}

} // namespace duckdb
