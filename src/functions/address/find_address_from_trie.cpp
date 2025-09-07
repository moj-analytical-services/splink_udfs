#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

#include "trie/address_trie_functions.hpp"
#include "trie/suffix_trie_cache.hpp"
#include "trie/trie_cache_utils.hpp"
#include "trie/trie_nav.hpp"

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

struct FindAddrLocalState : public FunctionLocalState {
    TrieCache cache;
};

static unique_ptr<FunctionLocalState> FindAddrInitLocal(ExpressionState &, const BoundFunctionExpression &,
                                                        FunctionData *) {
    return make_uniq<FindAddrLocalState>();
}

// find_address_from_trie(tokens, trie) -> BIGINT (UPRN or NULL)
static void FindAddressExec(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &local = ExecuteFunctionState::GetFunctionState(state)->Cast<FindAddrLocalState>();

    Vector &list_vec = args.data[0];
    UnifiedVectorFormat list_uvf;
    list_vec.ToUnifiedFormat(args.size(), list_uvf);
    auto list_entries = ListVector::GetData(list_vec);

    auto &in_child = ListVector::GetEntry(list_vec);
    UnifiedVectorFormat child_uvf;
    in_child.ToUnifiedFormat(ListVector::GetListSize(list_vec), child_uvf);
    auto child_vals = UnifiedVectorFormat::GetData<string_t>(child_uvf);

    Vector &trie_vec = args.data[1];
    UnifiedVectorFormat trie_uvf;
    trie_vec.ToUnifiedFormat(args.size(), trie_uvf);
    auto trie_vals = UnifiedVectorFormat::GetData<string_t>(trie_uvf);

    auto out = FlatVector::GetData<int64_t>(result);

    std::vector<std::string> toks;

    for (idx_t i = 0; i < args.size(); ++i) {
        const auto rid = list_uvf.sel->get_index(i);
        if (!list_uvf.validity.RowIsValid(rid)) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        const auto trid = trie_uvf.sel->get_index(i);
        if (!trie_uvf.validity.RowIsValid(trid)) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        auto trie_ptr = GetOrParseTrie(local.cache, trie_vals[trid]);
        if (!trie_ptr || !trie_ptr->root) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        auto le = list_entries[rid];
        toks.clear();
        toks.reserve(le.length);
        for (idx_t k = 0; k < le.length; ++k) {
            const auto cidx = child_uvf.sel->get_index(le.offset + k);
            if (!child_uvf.validity.RowIsValid(cidx)) {
                continue;
            }
            toks.emplace_back(child_vals[cidx].GetString());
        }

        const PNode *n = WalkExact(*trie_ptr, toks);
        if (!n || n->term != 1 || n->uprn == 0) {
            FlatVector::SetNull(result, i, true);
            continue;
        }
        out[i] = static_cast<int64_t>(n->uprn);
    }
}

ScalarFunctionSet GetFindAddressFromTrieFunctionSet() {
    ScalarFunctionSet set("find_address_from_trie");
    const LogicalType tokens_type = LogicalType::LIST(LogicalType::VARCHAR);
    ScalarFunction f({tokens_type, LogicalType::BLOB}, LogicalType::BIGINT, FindAddressExec);
    f.init_local_state = FindAddrInitLocal;
    set.AddFunction(f);
    return set;
}

} // namespace duckdb

