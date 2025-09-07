Here’s a tight, LLM-ready implementation plan. It starts with **exactly what we’re trying to achieve**, then gives **small, self-contained, verifiable steps** you can follow and check after each change.

# What we’re trying to achieve (precisely)

We want a clean, QCK2-only pipeline that lets us:

1. **Build** a serialized suffix trie (“QCK2”) from the master canonical address list where each terminal node stores:

   * `term` = number of full addresses that end at this node (0, 1, or >1)
   * `uprn` = the unique UPRN when `term == 1` else 0

2. **Parse & cache** that trie efficiently at query time.

3. **Query** the trie with new functions, starting with:

   * `find_address_from_trie(tokens, trie)` → `BIGINT` (UPRN or NULL)

     * **Phase 1**: **Exact unique match only**. Return the UPRN if (and only if) the full token sequence ends at a node with `term == 1`; otherwise return NULL.
     * **Phase 2**: optional `allow_prefix` boolean to also accept deepest unique terminal as a **prefix** of the tokens (useful for “house number + street + city” when extra trailing tokens are present).

We’ll implement this in tiny steps so that after each step the code compiles and we can run a small verifiable SQL check.

---

# Step-by-step plan (small, self-contained, verifiable)

Progress

- [x] 0) Rename parser to ParseQCK2
- [x] 1) Add minimal nav helpers
- [x] 2) Exact unique lookup → BIGINT
- [x] 3) Debug formatter with term/uprn
- [x] 4) Prefix option (allow_prefix)
- [x] 5) Debug struct return
- [ ] 6) SQL test snippets
- [ ] 7) Perf/limits checks
- [ ] 8) Docs/examples

## 0) Housekeeping: rename the parser to QCK2 (if you haven’t already)

**Goal:** Eliminate legacy naming to avoid confusion.

**Edits**

* `src/include/trie/suffix_trie.hpp`

  * Rename:

    ```diff
    ```
* std::unique\_ptr<ParsedTrie> ParseQCK1(const string\_t \&blob);

- std::unique\_ptr<ParsedTrie> ParseQCK2(const string\_t \&blob);

  ```
  ```

* `src/common/trie/suffix_trie.cpp`

  * Rename the definition accordingly.

* Update any call sites (likely in `trie/trie_cache_utils.*`) to call `ParseQCK2`.

**Build/verify**

* Build the extension. Should compile cleanly.
* Grep for old symbol:

  * `git grep -n 'ParseQCK1'` should return no matches.

---

## 1) Add minimal trie navigation helpers (exact path walk)

**Goal:** Centralize token navigation so higher-level functions are tiny.

**Edits**

* `src/include/trie/trie_nav.hpp` — add declarations:

  ```cpp
  #pragma once
  #include "trie/suffix_trie.hpp"
  #include <string>
  #include <vector>

  namespace duckdb {
    // Returns child node matching token, or nullptr
    const PNode* FindChild(const PNode& node, const std::string& tok);

    // Walk an exact token path from root; returns the final node or nullptr if any token missing
    const PNode* WalkExact(const ParsedTrie& pt, const std::vector<std::string>& toks);
  }
  ```
* `src/common/trie/trie_nav.cpp` — implement:

  ```cpp
  #include "trie/trie_nav.hpp"

  namespace duckdb {

  const PNode* FindChild(const PNode& node, const std::string& tok) {
    const auto& kids = node.kids;
    size_t lo = 0, hi = kids.size();
    while (lo < hi) {
      size_t mid = (lo + hi) / 2;
      int cmp = tok.compare(kids[mid].first);
      if (cmp == 0) return kids[mid].second;
      if (cmp < 0) hi = mid; else lo = mid + 1;
    }
    return nullptr;
  }

  const PNode* WalkExact(const ParsedTrie& pt, const std::vector<std::string>& toks) {
    const PNode* n = pt.root;
    if (!n) return nullptr;
    for (auto& t : toks) {
      n = FindChild(*n, t);
      if (!n) return nullptr;
    }
    return n;
  }

  } // namespace duckdb
  ```

**Build/verify**

* Build. Should compile.
* No behavior change yet; just helpers.

---

## 2) Implement Phase 1 lookup: exact unique match → BIGINT UPRN

**Goal:** Create the simplest, strict function we can test end-to-end.

**Edits**

* `src/include/trie/address_trie_functions.hpp` — add declaration:

  ```cpp
  ScalarFunctionSet GetFindAddressFromTrieFunctionSet();
  ```

* **New file** `src/functions/address/find_address_from_trie.cpp`

  ```cpp
  #include "duckdb.hpp"
  #include "duckdb/function/function_set.hpp"
  #include "trie/address_trie_functions.hpp"
  #include "trie/suffix_trie_cache.hpp"
  #include "trie/trie_cache_utils.hpp"
  #include "trie/trie_nav.hpp"
  #include <vector>
  #include <string>
  #include <memory>

  namespace duckdb {

  struct FindAddrLocalState : public FunctionLocalState {
    TrieCache cache;
  };
  static unique_ptr<FunctionLocalState> FindAddrInitLocal(ExpressionState&, const BoundFunctionExpression&, FunctionData*) {
    return make_uniq<FindAddrLocalState>();
  }

  // find_address_from_trie(tokens, trie) -> BIGINT (UPRN or NULL)
  static void FindAddressExec(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &local = ExecuteFunctionState::GetFunctionState(state)->Cast<FindAddrLocalState>();

    // Inputs
    Vector &list_vec = args.data[0];
    UnifiedVectorFormat list_uvf; list_vec.ToUnifiedFormat(args.size(), list_uvf);
    auto list_entries = ListVector::GetData(list_vec);

    auto &in_child = ListVector::GetEntry(list_vec);
    UnifiedVectorFormat child_uvf; in_child.ToUnifiedFormat(ListVector::GetListSize(list_vec), child_uvf);
    auto child_vals = UnifiedVectorFormat::GetData<string_t>(child_uvf);

    Vector &trie_vec = args.data[1];
    UnifiedVectorFormat trie_uvf; trie_vec.ToUnifiedFormat(args.size(), trie_uvf);
    auto trie_vals = UnifiedVectorFormat::GetData<string_t>(trie_uvf);

    auto out = FlatVector::GetData<int64_t>(result);

    std::vector<std::string> toks;

    for (idx_t i = 0; i < args.size(); ++i) {
      const auto rid = list_uvf.sel->get_index(i);
      if (!list_uvf.validity.RowIsValid(rid)) { FlatVector::SetNull(result, i, true); continue; }

      const auto trid = trie_uvf.sel->get_index(i);
      if (!trie_uvf.validity.RowIsValid(trid)) { FlatVector::SetNull(result, i, true); continue; }

      auto trie_ptr = GetOrParseTrie(local.cache, trie_vals[trid]);
      if (!trie_ptr || !trie_ptr->root) { FlatVector::SetNull(result, i, true); continue; }

      auto le = list_entries[rid];
      toks.clear(); toks.reserve(le.length);
      for (idx_t k = 0; k < le.length; ++k) {
        const auto cidx = child_uvf.sel->get_index(le.offset + k);
        if (!child_uvf.validity.RowIsValid(cidx)) continue;
        toks.emplace_back(child_vals[cidx].GetString());
      }

      const PNode* n = WalkExact(*trie_ptr, toks);
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
  ```

* `src/splink_udfs_extension.cpp` — register the new function:

  ```diff
  + ExtensionUtil::RegisterFunction(instance, GetFindAddressFromTrieFunctionSet());
  ```

* `CMakeLists.txt` — add the new source to `EXTENSION_SOURCES`:

  ```diff
    src/functions/address/format_address_with_counts.cpp
  + src/functions/address/find_address_from_trie.cpp
  ```

**Build/verify**

* Build should succeed.

* **Smoke test SQL (exact unique):**

  ```sql
  -- Build a tiny canon trie
  WITH canon(uprn, toks) AS (
    VALUES
      (1, ['10','HIGH','STREET','LONDON']),
      (2, ['12','HIGH','STREET','LONDON']),
      (3, ['10','HIGH','ROAD','LONDON'])
  )
  SELECT build_suffix_trie(uprn, toks) AS trie FROM canon;
  ```

  Save the single blob as `trie`.

  ```sql
  -- Should return 1
  WITH t AS (SELECT build_suffix_trie(uprn, toks) AS trie FROM (
    VALUES
      (1, ['10','HIGH','STREET','LONDON']),
      (2, ['12','HIGH','STREET','LONDON']),
      (3, ['10','HIGH','ROAD','LONDON'])
  ) canon(uprn, toks))
  SELECT find_address_from_trie(['10','HIGH','STREET','LONDON'], trie) AS uprn FROM t;

  -- Not exact terminal → NULL
  WITH t AS (SELECT build_suffix_trie(uprn, toks) AS trie FROM (
    VALUES
      (1, ['10','HIGH','STREET','LONDON']),
      (2, ['12','HIGH','STREET','LONDON'])
  ) canon(uprn, toks))
  SELECT find_address_from_trie(['HIGH','STREET','LONDON'], trie) AS uprn FROM t;

  -- Wrong token → NULL
  WITH t AS (SELECT build_suffix_trie(uprn, toks) AS trie FROM (
    VALUES (1, ['10','HIGH','STREET','LONDON'])
  ) canon(uprn, toks))
  SELECT find_address_from_trie(['10','HIGH','STREET','PARIS'], trie) AS uprn FROM t;
  ```

**Acceptance**

* First query returns `1`
* The next two return `NULL`

---

## 3) Add a debug formatter to inspect `term/uprn` along a path (optional but helpful)

**Goal:** Quick visibility when assertions fail.

**Edits**

* New scalar: `format_address_with_term(tokens, trie, joiner=' -> ')` → `VARCHAR` like:

  ```
  "10 (term=1 uprn=1) -> HIGH (term=0) -> STREET (term=0) -> LONDON (term=0)"
  ```
* Implementation mirrors `format_address_with_counts.cpp` but:

  * Walk with `FindChild` step by step
  * Append `" (term=X uprn=Y)"` at each visited node
  * Stop when a child is missing

**Verify**

* Run on the same examples and eyeball output.

*(You can skip this if you’re confident, but it’s invaluable for pinpointing mismatches.)*

---

## 4) Phase 2: add `allow_prefix` option to return deepest unique terminal on the path

**Goal:** Useful for cases where the canonical address is a prefix of a longer token list.

**Edits**

* Extend `find_address_from_trie` with a 3rd boolean arg: `allow_prefix` (default `false`).

  * If `allow_prefix = false` (current behavior): only return UPRN when **final node** has `term == 1`.
  * If `allow_prefix = true`: while walking, keep track of the **deepest** node with `term == 1`; after consuming all tokens:

    * Return that UPRN if at least one unique terminal was encountered
    * Else return NULL

**Code sketch (inside the exec loop, replacing the exact check):**

```cpp
bool allow_prefix = args.ColumnCount() >= 3 ? BooleanVector::GetValue(args.data[2], i) : false;

const PNode* n = pt.root;
const PNode* deepest_unique = nullptr;
for (auto& t : toks) {
  n = FindChild(*n, t);
  if (!n) break;
  if (n->term == 1 && n->uprn != 0) deepest_unique = n;
}

if (!n) {
  if (allow_prefix && deepest_unique) out[i] = (int64_t)deepest_unique->uprn;
  else FlatVector::SetNull(result, i, true);
  continue;
}
if (!allow_prefix) {
  if (n->term == 1 && n->uprn != 0) out[i] = (int64_t)n->uprn;
  else FlatVector::SetNull(result, i, true);
} else {
  if (deepest_unique) out[i] = (int64_t)deepest_unique->uprn;
  else FlatVector::SetNull(result, i, true);
}
```

**Verify**

```sql
WITH t AS (SELECT build_suffix_trie(uprn, toks) AS trie FROM (
  VALUES (1, ['10','HIGH','STREET','LONDON'])
) canon(uprn, toks))
SELECT
  find_address_from_trie(['10','HIGH','STREET','LONDON','UK'], trie, true)  AS prefix_ok,   -- expect 1
  find_address_from_trie(['10','HIGH','STREET','LONDON','UK'], trie, false) AS prefix_fail;  -- expect NULL
```

---

## 5) Add structured debug return (optional, separate function name)

**Goal:** Aid downstream pipelines and tests without changing the simple BIGINT API.

**Edits**

* New scalar `find_address_from_trie_dbg(tokens, trie, allow_prefix=false)` → `STRUCT(
    uprn BIGINT,
    matched_len INTEGER,
    is_terminal BOOLEAN,
    ambiguous BOOLEAN
  )`

  * `matched_len`: number of tokens consumed before first miss (or `n` if fully consumed)
  * `is_terminal`: true if final node had `term > 0`
  * `ambiguous`: true if final node had `term > 1`

**Verify**

* Same test set; inspect fields.

---

## 6) Add unit/SQL test snippets to repo (manual harness is fine)

**Goal:** Make verification one command.

**Edits**

* Create `tests/sql/find_address_from_trie.sql` with the smoke tests above.
* Optionally add a tiny C++ unit test if you have a harness; otherwise, keep SQL.

**Verify**

* Run the SQL file and assert expected outputs.

---

## 7) Performance/limits pass (quick checks)

**Goal:** Ensure no sneaky regressions.

* Confirm iterator is O(len(tokens) · log(children)) thanks to binary search in `FindChild`.
* Confirm caching hits:

  * Call `find_address_from_trie` repeatedly with same blob; ensure no repeated parses (you can temporarily add a debug counter in the local state to assert cache usage if helpful).

---

## 8) Documentation strings & examples

**Goal:** Make it easy for others to use.

* In `splink_udfs_extension.cpp`, add short descriptions as comments near registration.
* Create a short markdown snippet with examples for:

  * building the trie
  * using `find_address_from_trie`
  * `allow_prefix` behavior
  * relation to `build_cleaned_address` / `format_address_with_counts`

---

# Why this order?

* We start with **exact match** because it’s unambiguous and immediately testable.
* We then add **prefix match** as a flag—no API break, easy to verify with one extra test.
* Debug views are optional but make troubleshooting much faster.
* Every step compiles independently and has a tiny SQL harness to confirm behavior.

If you want, I can now generate the exact diffs for the files from Steps 1–4 so you can paste them directly.
