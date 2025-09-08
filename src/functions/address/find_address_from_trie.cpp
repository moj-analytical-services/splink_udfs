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

// find_address_from_trie(tokens, trie [, allow_prefix=false [, max_skips=0]]) -> BIGINT (UPRN or NULL)
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

        bool allow_prefix = false;
        if (args.ColumnCount() >= 3) {
            UnifiedVectorFormat pref_uvf;
            Vector &pref_vec = args.data[2];
            pref_vec.ToUnifiedFormat(args.size(), pref_uvf);
            const auto pid = pref_uvf.sel->get_index(i);
            if (pref_uvf.validity.RowIsValid(pid)) {
                auto pref_vals = UnifiedVectorFormat::GetData<bool>(pref_uvf);
                allow_prefix = pref_vals[pid];
            }
        }

        int32_t max_skips = 0;
        if (args.ColumnCount() >= 4) {
            UnifiedVectorFormat skip_uvf;
            Vector &skip_vec = args.data[3];
            skip_vec.ToUnifiedFormat(args.size(), skip_uvf);
            const auto sid = skip_uvf.sel->get_index(i);
            if (skip_uvf.validity.RowIsValid(sid)) {
                auto skip_vals = UnifiedVectorFormat::GetData<int32_t>(skip_uvf);
                int32_t v = skip_vals[sid];
                if (v < 0) { v = 0; }
                if (v > 1) { v = 1; }
                max_skips = v;
            }
        }

        auto mr = GreedyWalkWithSkips(*trie_ptr, toks, allow_prefix, max_skips);
        const bool consumed_all = (mr.matched_len + mr.skipped == static_cast<int32_t>(toks.size()));
        if (!allow_prefix) {
            if (consumed_all && mr.last_node && mr.last_node->term == 1 && mr.last_node->uprn != 0) {
                out[i] = static_cast<int64_t>(mr.last_node->uprn);
            } else {
                FlatVector::SetNull(result, i, true);
            }
        } else {
            if (mr.deepest_unique) {
                out[i] = static_cast<int64_t>(mr.deepest_unique->uprn);
            } else {
                FlatVector::SetNull(result, i, true);
            }
        }
    }
}

ScalarFunctionSet GetFindAddressFromTrieFunctionSet() {
    ScalarFunctionSet set("find_address_from_trie");
    const LogicalType tokens_type = LogicalType::LIST(LogicalType::VARCHAR);
    {
        ScalarFunction f({tokens_type, LogicalType::BLOB}, LogicalType::BIGINT, FindAddressExec);
        f.init_local_state = FindAddrInitLocal;
        set.AddFunction(f);
    }
    {
        // 3-arg variant with allow_prefix boolean
        ScalarFunction f({tokens_type, LogicalType::BLOB, LogicalType::BOOLEAN}, LogicalType::BIGINT, FindAddressExec);
        f.init_local_state = FindAddrInitLocal;
        set.AddFunction(f);
    }
    {
        // 4-arg: allow_prefix + max_skips
        ScalarFunction f({tokens_type, LogicalType::BLOB, LogicalType::BOOLEAN, LogicalType::INTEGER},
                         LogicalType::BIGINT, FindAddressExec);
        f.init_local_state = FindAddrInitLocal;
        set.AddFunction(f);
    }
    return set;
}

} // namespace duckdb
