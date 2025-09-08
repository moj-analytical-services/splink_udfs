#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

#include "trie/address_trie_functions.hpp"
#include "trie/suffix_trie_cache.hpp"
#include "trie/trie_cache_utils.hpp"
#include "trie/trie_nav.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

struct FindAddrClassifyLocal : public FunctionLocalState {
    TrieCache cache;
};

static unique_ptr<FunctionLocalState> ClassifyInitLocal(ExpressionState &, const BoundFunctionExpression &, FunctionData *) {
    return make_uniq<FindAddrClassifyLocal>();
}

static void ClassifyExec(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &local = ExecuteFunctionState::GetFunctionState(state)->Cast<FindAddrClassifyLocal>();

    // Output struct children in fixed order:
    // status VARCHAR, uprn BIGINT, matched_len INTEGER, consumed_all_tokens BOOLEAN, node_cnt INTEGER, term INTEGER
    auto &fields = StructVector::GetEntries(result);
    D_ASSERT(fields.size() == 6);
    auto &status_vec = *fields[0];
    auto &uprn_vec = *fields[1];
    auto &mlen_vec = *fields[2];
    auto &cons_vec = *fields[3];
    auto &cnt_vec = *fields[4];
    auto &term_vec = *fields[5];

    auto status_out = FlatVector::GetData<string_t>(status_vec);
    auto uprn_out = FlatVector::GetData<int64_t>(uprn_vec);
    auto mlen_out = FlatVector::GetData<int32_t>(mlen_vec);
    auto cons_out = FlatVector::GetData<bool>(cons_vec);
    auto cnt_out = FlatVector::GetData<int32_t>(cnt_vec);
    auto term_out = FlatVector::GetData<int32_t>(term_vec);

    // Inputs
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

    bool has_skips = args.ColumnCount() >= 4;
    UnifiedVectorFormat skip_uvf;
    const int32_t *skip_vals = nullptr;
    if (has_skips) {
        Vector &skip_vec = args.data[3];
        skip_vec.ToUnifiedFormat(args.size(), skip_uvf);
        skip_vals = UnifiedVectorFormat::GetData<int32_t>(skip_uvf);
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

        int32_t max_skips = 0;
        if (has_skips) {
            const auto sid = skip_uvf.sel->get_index(i);
            if (skip_uvf.validity.RowIsValid(sid)) {
                int32_t v = skip_vals[sid];
                if (v < 0) {
                    v = 0;
                }
                if (v > 1) {
                    v = 1; // greedy: at most one
                }
                max_skips = v;
            }
        }

        auto trie_ptr = GetOrParseTrie(local.cache, trie_vals[trid]);
        if (!trie_ptr || !trie_ptr->root) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        // Gather tokens
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
        // Defaults (non-null row)
        FlatVector::SetNull(result, i, false);
        FlatVector::SetNull(uprn_vec, i, true);
        mlen_out[i] = 0;
        cons_out[i] = false;
        cnt_out[i] = 0;
        term_out[i] = 0;

        if (n == 0) {
            status_out[i] = StringVector::AddString(status_vec, "NO_PATH");
            continue;
        }

        const PNode *node = trie_ptr->root;
        const PNode *last_node = nullptr;
        const PNode *best_last_node = nullptr;
        const PNode *deepest_unique = nullptr;

        int32_t matched_len = 0;      // exact or running length
        int32_t best_matched_len = 0;  // for prefix mode
        int32_t current_len = 0;

        if (!allow_prefix) {
            int32_t skips_left = max_skips;
            for (idx_t ti = n; ti > 0; --ti) {
                const std::string &tok = toks[ti - 1];
                const PNode *next = FindChild(node, tok);
                if (next) {
                    node = next;
                    last_node = node;
                    matched_len++;
                    if (node->term == 1 && node->uprn != 0) {
                        deepest_unique = node;
                    }
                    continue;
                }
                if (skips_left > 0) {
                    skips_left--;
                    continue; // consume token without moving
                }
                break;
            }

            cons_out[i] = (matched_len == static_cast<int32_t>(n));
            mlen_out[i] = matched_len;
            if (last_node) {
                cnt_out[i] = static_cast<int32_t>(last_node->cnt);
                term_out[i] = static_cast<int32_t>(last_node->term);
            }

            if (cons_out[i]) {
                if (last_node && last_node->term == 1 && last_node->uprn != 0) {
                    status_out[i] = StringVector::AddString(status_vec, "EXACT");
                    uprn_out[i] = static_cast<int64_t>(last_node->uprn);
                    FlatVector::SetNull(uprn_vec, i, false);
                } else if (last_node && last_node->term == 0) {
                    status_out[i] = StringVector::AddString(status_vec, "INSUFFICIENT");
                } else {
                    status_out[i] = StringVector::AddString(status_vec, "AMBIGUOUS");
                }
            } else {
                if (matched_len == 0) {
                    status_out[i] = StringVector::AddString(status_vec, "NO_PATH");
                } else if (last_node && last_node->cnt > 1) {
                    status_out[i] = StringVector::AddString(status_vec, "AMBIGUOUS");
                } else {
                    status_out[i] = StringVector::AddString(status_vec, "NO_PATH");
                }
            }
        } else {
            // allow_prefix: scan right->left with reset on miss, track longest suffix and deepest unique
            node = trie_ptr->root;
            int32_t skips_left = max_skips;
            for (idx_t ti = n; ti > 0; --ti) {
                const std::string &tok = toks[ti - 1];
                bool reattempt = true;
                while (true) {
                    const PNode *next = FindChild(node, tok);
                    if (next) {
                        node = next;
                        last_node = node;
                        current_len++;
                        if (current_len > best_matched_len) {
                            best_matched_len = current_len;
                            best_last_node = node;
                        }
                        if (node->term == 1 && node->uprn != 0) {
                            deepest_unique = node;
                        }
                        break;
                    }
                    if (skips_left > 0) {
                        skips_left--;
                        break; // consume token but do not advance
                    }
                    if (node != trie_ptr->root && reattempt) {
                        node = trie_ptr->root;
                        skips_left = max_skips;
                        current_len = 0;
                        reattempt = false;
                        continue; // retry this token from root
                    }
                    node = trie_ptr->root;
                    skips_left = max_skips;
                    current_len = 0;
                    break;
                }
            }

            mlen_out[i] = best_matched_len;
            cons_out[i] = (best_matched_len == static_cast<int32_t>(n));
            const PNode *final = best_last_node;
            if (!final) {
                status_out[i] = StringVector::AddString(status_vec, "NO_PATH");
                continue;
            }
            cnt_out[i] = static_cast<int32_t>(final->cnt);
            term_out[i] = static_cast<int32_t>(final->term);

            if (cons_out[i]) {
                if (final->term == 1 && final->uprn != 0) {
                    status_out[i] = StringVector::AddString(status_vec, "EXACT");
                    uprn_out[i] = static_cast<int64_t>(final->uprn);
                    FlatVector::SetNull(uprn_vec, i, false);
                } else if (final->term == 0) {
                    status_out[i] = StringVector::AddString(status_vec, "INSUFFICIENT");
                } else {
                    status_out[i] = StringVector::AddString(status_vec, "AMBIGUOUS");
                }
            } else {
                if (final->cnt > 1) {
                    status_out[i] = StringVector::AddString(status_vec, "AMBIGUOUS");
                } else {
                    status_out[i] = StringVector::AddString(status_vec, "NO_PATH");
                }
            }
        }
    }
}

ScalarFunctionSet GetFindAddressFromTrieClassifyFunctionSet() {
    ScalarFunctionSet set("find_address_from_trie_classify");
    const LogicalType tokens_type = LogicalType::LIST(LogicalType::VARCHAR);

    child_list_t<LogicalType> children;
    children.emplace_back("status", LogicalType::VARCHAR);
    children.emplace_back("uprn", LogicalType::BIGINT);
    children.emplace_back("matched_len", LogicalType::INTEGER);
    children.emplace_back("consumed_all_tokens", LogicalType::BOOLEAN);
    children.emplace_back("node_cnt", LogicalType::INTEGER);
    children.emplace_back("term", LogicalType::INTEGER);
    LogicalType out_type = LogicalType::STRUCT(std::move(children));

    // 2-arg
    {
        ScalarFunction f({tokens_type, LogicalType::BLOB}, out_type, ClassifyExec);
        f.init_local_state = ClassifyInitLocal;
        set.AddFunction(f);
    }
    // 3-arg with allow_prefix
    {
        ScalarFunction f({tokens_type, LogicalType::BLOB, LogicalType::BOOLEAN}, out_type, ClassifyExec);
        f.init_local_state = ClassifyInitLocal;
        set.AddFunction(f);
    }
    // 4-arg with allow_prefix + max_skips
    {
        ScalarFunction f({tokens_type, LogicalType::BLOB, LogicalType::BOOLEAN, LogicalType::INTEGER}, out_type, ClassifyExec);
        f.init_local_state = ClassifyInitLocal;
        set.AddFunction(f);
    }

    return set;
}

} // namespace duckdb

