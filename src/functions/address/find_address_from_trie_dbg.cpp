#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

#include "trie/address_trie_functions.hpp"
#include "trie/suffix_trie_cache.hpp"
#include "trie/trie_cache_utils.hpp"
#include "trie/trie_nav.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

struct FindAddrDbgLocalState : public FunctionLocalState {
    TrieCache cache;
};

static unique_ptr<FunctionLocalState> FindAddrDbgInitLocal(ExpressionState &, const BoundFunctionExpression &,
                                                           FunctionData *) {
    return make_uniq<FindAddrDbgLocalState>();
}

static void FindAddressDbgExec(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &local = ExecuteFunctionState::GetFunctionState(state)->Cast<FindAddrDbgLocalState>();

    // Output children: uprn BIGINT, matched_len INTEGER, is_terminal BOOLEAN, ambiguous BOOLEAN
    auto &entries = StructVector::GetEntries(result);
    D_ASSERT(entries.size() == 4);
    auto &uprn_vec = *entries[0];
    auto &mlen_vec = *entries[1];
    auto &term_vec = *entries[2];
    auto &amb_vec = *entries[3];

    auto *uprn_out = FlatVector::GetData<int64_t>(uprn_vec);
    auto *mlen_out = FlatVector::GetData<int32_t>(mlen_vec);
    auto *term_out = FlatVector::GetData<bool>(term_vec);
    auto *amb_out = FlatVector::GetData<bool>(amb_vec);

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

    bool has_prefix = args.ColumnCount() >= 3;
    UnifiedVectorFormat pref_uvf;
    const bool *pref_vals = nullptr;
    if (has_prefix) {
        Vector &pref_vec = args.data[2];
        pref_vec.ToUnifiedFormat(args.size(), pref_uvf);
        pref_vals = UnifiedVectorFormat::GetData<bool>(pref_uvf);
    }

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

        bool allow_prefix = false;
        if (has_prefix) {
            const auto pid = pref_uvf.sel->get_index(i);
            if (pref_uvf.validity.RowIsValid(pid)) {
                allow_prefix = pref_vals[pid];
            }
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

        const idx_t n = static_cast<idx_t>(toks.size());
        // Default output values
        uprn_out[i] = 0;
        FlatVector::SetNull(uprn_vec, i, true);
        mlen_out[i] = 0;
        term_out[i] = false;
        amb_out[i] = false;

        if (n == 0) {
            continue;
        }

        const PNode *node = trie_ptr->root;
        const PNode *last_node = nullptr;
        const PNode *deepest_unique = nullptr;
        int32_t matched_len = 0;

        if (!allow_prefix) {
            // Single pass right→left, stop at first miss
            for (idx_t ti = n; ti > 0; --ti) {
                const PNode *next = FindChild(node, toks[ti - 1]);
                if (!next) {
                    break;
                }
                node = next;
                last_node = node;
                matched_len++;
                if (node->term == 1 && node->uprn != 0) {
                    deepest_unique = node;
                }
            }

            mlen_out[i] = matched_len;
            if (last_node) {
                term_out[i] = last_node->term > 0;
                amb_out[i] = last_node->term > 1;
            }

            if (matched_len == static_cast<int32_t>(n) && last_node && last_node->term == 1 && last_node->uprn != 0) {
                uprn_out[i] = static_cast<int64_t>(last_node->uprn);
                FlatVector::SetNull(uprn_vec, i, false);
            }
        } else {
            // allow_prefix=true: scan right→left, resetting to root on miss;
            // report the longest matched suffix length and deepest unique terminal encountered
            int32_t current_len = 0;
            const PNode *best_last_node = nullptr;
            node = trie_ptr->root;
            for (idx_t ti = n; ti > 0; --ti) {
                const PNode *next = FindChild(node, toks[ti - 1]);
                if (!next) {
                    node = trie_ptr->root;
                    current_len = 0;
                    continue;
                }
                node = next;
                current_len++;
                last_node = node;
                if (current_len > matched_len) {
                    matched_len = current_len;
                    best_last_node = node;
                }
                if (node->term == 1 && node->uprn != 0) {
                    deepest_unique = node;
                }
            }

            mlen_out[i] = matched_len;
            if (best_last_node) {
                term_out[i] = best_last_node->term > 0;
                amb_out[i] = best_last_node->term > 1;
            }

            if (deepest_unique) {
                uprn_out[i] = static_cast<int64_t>(deepest_unique->uprn);
                FlatVector::SetNull(uprn_vec, i, false);
            }
        }
    }
}

ScalarFunctionSet GetFindAddressFromTrieDbgFunctionSet() {
    ScalarFunctionSet set("find_address_from_trie_dbg");
    const LogicalType tokens_type = LogicalType::LIST(LogicalType::VARCHAR);

    child_list_t<LogicalType> children;
    children.emplace_back("uprn", LogicalType::BIGINT);
    children.emplace_back("matched_len", LogicalType::INTEGER);
    children.emplace_back("is_terminal", LogicalType::BOOLEAN);
    children.emplace_back("ambiguous", LogicalType::BOOLEAN);

    LogicalType out_type = LogicalType::STRUCT(std::move(children));

    // 2-arg
    {
        ScalarFunction f({tokens_type, LogicalType::BLOB}, out_type, FindAddressDbgExec);
        f.init_local_state = FindAddrDbgInitLocal;
        set.AddFunction(f);
    }
    // 3-arg with allow_prefix
    {
        ScalarFunction f({tokens_type, LogicalType::BLOB, LogicalType::BOOLEAN}, out_type, FindAddressDbgExec);
        f.init_local_state = FindAddrDbgInitLocal;
        set.AddFunction(f);
    }
    return set;
}

} // namespace duckdb
