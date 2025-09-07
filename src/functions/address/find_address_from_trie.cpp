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

// find_address_from_trie(tokens, trie [, allow_prefix=false]) -> BIGINT (UPRN or NULL)
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

        // Walk path right→left. If allow_prefix is false, require full match and terminal==1.
        // If allow_prefix is true, reset to root when a token is missing (skip unmatched leading tokens)
        // and return the deepest unique terminal encountered while consuming the longest matching suffix.
        const PNode *node = trie_ptr->root;
        const PNode *deepest_unique = nullptr;
        bool path_broken = false;

        if (!allow_prefix) {
            for (idx_t ti = toks.size(); ti > 0; --ti) {
                node = FindChild(node, toks[ti - 1]);
                if (!node) {
                    path_broken = true;
                    break;
                }
            }
            if (!path_broken && node && node->term == 1 && node->uprn != 0) {
                out[i] = static_cast<int64_t>(node->uprn);
            } else {
                FlatVector::SetNull(result, i, true);
            }
            continue;
        } else {
            // allow_prefix: skip non-matching leading tokens by resetting to root when a token misses
            node = trie_ptr->root;
            for (idx_t ti = toks.size(); ti > 0; --ti) {
                const PNode *next = FindChild(node, toks[ti - 1]);
                if (!next) {
                    // reset and continue scanning earlier tokens (shorter suffix)
                    node = trie_ptr->root;
                    continue;
                }
                node = next;
                if (node->term == 1 && node->uprn != 0) {
                    deepest_unique = node;
                }
            }
            if (deepest_unique) {
                out[i] = static_cast<int64_t>(deepest_unique->uprn);
            } else {
                FlatVector::SetNull(result, i, true);
            }
            continue;
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
    return set;
}

} // namespace duckdb
