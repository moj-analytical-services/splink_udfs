#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

#include "trie/address_trie_functions.hpp"
#include "trie/suffix_trie_cache.hpp"
#include "trie/trie_cache_utils.hpp"
#include "trie/trie_nav.hpp"

#include <string>
#include <vector>
#include <memory>

namespace duckdb {

struct FormatTermLocalState : public FunctionLocalState {
    TrieCache cache;
};

static unique_ptr<FunctionLocalState> FormatTermInitLocal(ExpressionState &, const BoundFunctionExpression &,
                                                          FunctionData *) {
    return make_uniq<FormatTermLocalState>();
}

// format_address_with_term(tokens, trie, joiner=' -> ')
static void FormatAddressWithTermExec(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &local = ExecuteFunctionState::GetFunctionState(state)->Cast<FormatTermLocalState>();

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

    // Optional joiner
    const bool has_joiner = args.ColumnCount() >= 3;
    UnifiedVectorFormat join_uvf;
    const string_t *join_vals = nullptr;
    if (has_joiner) {
        Vector &join_vec = args.data[2];
        join_vec.ToUnifiedFormat(args.size(), join_uvf);
        join_vals = UnifiedVectorFormat::GetData<string_t>(join_uvf);
    }

    auto out = FlatVector::GetData<string_t>(result);

    std::vector<std::string> toks;
    std::vector<uint32_t> terms;
    std::vector<uint64_t> uprns;

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

        std::string joiner = " -> ";
        if (has_joiner) {
            const auto jid = join_uvf.sel->get_index(i);
            if (join_uvf.validity.RowIsValid(jid)) {
                joiner = join_vals[jid].GetString();
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
        if (n == 0) {
            out[i] = StringVector::AddString(result, "");
            continue;
        }

        // Traverse from rightmost to leftmost; record term/uprn for each suffix node visited.
        terms.assign(n, 0);
        uprns.assign(n, 0);
        const PNode *node = trie_ptr->root;
        idx_t depth_reached = 0;
        for (idx_t t = 0; t < n; ++t) {
            const std::string &tok = toks[n - 1 - t];
            node = FindChild(node, tok);
            if (!node) {
                break;
            }
            const idx_t idx = n - 1 - t;
            terms[idx] = node->term;
            uprns[idx] = node->uprn;
            depth_reached++;
        }

        if (depth_reached == 0) {
            out[i] = StringVector::AddString(result, "");
            continue;
        }

        const idx_t start = n - depth_reached;
        // precompute length
        size_t total_len = 0;
        for (idx_t j = start; j < n; ++j) {
            total_len += toks[j].size();
            // add metadata length conservatively
            total_len += 10; // for " (term=)"
            if (terms[j] == 1 && uprns[j] != 0) {
                total_len += 10; // space for " uprn=..."
            }
            if (j + 1 < n) {
                total_len += joiner.size();
            }
        }

        std::string out_str;
        out_str.reserve(total_len);
        for (idx_t j = start; j < n; ++j) {
            if (j > start) {
                out_str += joiner;
            }
            out_str += toks[j];
            out_str += " (term=";
            out_str += std::to_string(terms[j]);
            if (terms[j] == 1 && uprns[j] != 0) {
                out_str += " uprn=";
                out_str += std::to_string(uprns[j]);
            }
            out_str += ")";
        }

        out[i] = StringVector::AddString(result, out_str);
    }
}

ScalarFunctionSet GetFormatAddressWithTermFunctionSet() {
    ScalarFunctionSet set("format_address_with_term");
    const LogicalType tokens_type = LogicalType::LIST(LogicalType::VARCHAR);

    // 2-arg
    {
        ScalarFunction f({tokens_type, LogicalType::BLOB}, LogicalType::VARCHAR, FormatAddressWithTermExec);
        f.init_local_state = FormatTermInitLocal;
        set.AddFunction(f);
    }
    // 3-arg with joiner
    {
        ScalarFunction f({tokens_type, LogicalType::BLOB, LogicalType::VARCHAR}, LogicalType::VARCHAR,
                         FormatAddressWithTermExec);
        f.init_local_state = FormatTermInitLocal;
        set.AddFunction(f);
    }
    return set;
}

} // namespace duckdb
