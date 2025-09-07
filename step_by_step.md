# What we’re aiming to do (and why)

We will **fully migrate** your serialized trie format from **QCK1** (counts-only) to **QCK2** (counts + terminal metadata). QCK2 stores, per node:

* `cnt`: how many canonical addresses share this suffix (unchanged),
* `term`: how many canonical addresses **terminate** at this node,
* `uprn`: the terminal UPRN when `term == 1` (0 otherwise).

**Why this first?**
It unlocks safe acceptance rules (e.g., avoid “ANNEX 7 …” false positives), enables `find_address_from_trie` to return a real UPRN, and keeps all existing “count-only” features working (they still rely on `cnt`). Doing this first gives us a clean base; the greedy matcher becomes trivial later.

---

# Precise, LLM-ready implementation plan

Each step is small, self-contained, and has a clear verification. The plan **replaces QCK1 with QCK2 everywhere** (no dual support). If you have any persisted QCK1 blobs, you’ll regenerate them at the end.

> Conventions below:
>
> * Filenames are relative to your repo exactly as you shared.
> * “Build & run” always means: recompile the extension and run the provided SQL snippets in DuckDB.
> * All steps are behavior-preserving for existing functions (they still compute counts from the trie).

---



## Step 1 — Extend the in-memory node with terminal metadata

**Edit**: `src/include/trie/suffix_trie.hpp`

**Changes**

1. Add terminal fields to `PNode`, and define the new magic:

```cpp
// ---- On-disk format constants ----
static constexpr uint32_t QCK2_MAGIC = 0x324B4351u; // 'QCK2'

// ---- Parsed trie structures (immutable) ----
struct PNode {
    uint32_t cnt = 0;
    std::vector<std::pair<std::string, PNode *>> kids;

    // NEW: terminal metadata for QCK2
    uint32_t term = 0;    // number of addresses that end here
    uint64_t uprn = 0;    // valid when term == 1
};
```

**Why**

* This lets the parser populate terminals/U PRNs; existing users of `cnt` keep working.

**Verify**

* Rebuild. It should compile (no call sites changed yet).

---

## Step 2 — Implement QCK2 parser (new layout), remove QCK1

**Edit**: `src/common/trie/suffix_trie.cpp`

**Changes**

1. Keep the helper read functions.
2. Replace the old node parser with a QCK2 version:

```cpp
static bool ParseNodeQCK2(ParseCursor &c, ParsedTrie &out, PNode *&node_out) {
    auto node = make_uniq<PNode>();

    uint32_t cnt = 0;
    if (!ReadU32(c, cnt)) return false;
    node->cnt = cnt;

    uint32_t term = 0;
    if (!ReadU32(c, term)) return false;
    node->term = term;

    uint64_t uprn = 0;
    // Read U64 little-endian
    uint32_t lo = 0, hi = 0;
    if (!ReadU32(c, lo)) return false;
    if (!ReadU32(c, hi)) return false;
    uprn = (uint64_t)lo | ((uint64_t)hi << 32);
    node->uprn = uprn;

    uint32_t nchild = 0;
    if (!ReadU32(c, nchild)) return false;

    node->kids.reserve(nchild);
    for (uint32_t i = 0; i < nchild; ++i) {
        std::string tok;
        if (!ReadString(c, tok)) return false;
        PNode *child_ptr = nullptr;
        if (!ParseNodeQCK2(c, out, child_ptr)) return false;
        node->kids.emplace_back(std::move(tok), child_ptr);
    }

    out.arena.emplace_back(std::move(node));
    node_out = out.arena.back().get();
    return true;
}
```

3. Replace `ParseQCK1(...)` with a **QCK2-only** top-level parser:

```cpp
std::unique_ptr<ParsedTrie> ParseQCK1(const string_t &blob) {
    // (Name preserved to minimize edits, but now expects QCK2.)
    auto data_ptr = reinterpret_cast<const uint8_t *>(blob.GetDataUnsafe());
    auto data_len = blob.GetSize();
    if (data_len < 5) return nullptr;

    ParseCursor cur{data_ptr, data_ptr + data_len};

    uint32_t magic = 0;
    if (!ReadU32(cur, magic)) return nullptr;
    if (magic != QCK2_MAGIC) return nullptr;

    if (DUCKDB_UNLIKELY(cur.p >= cur.end)) return nullptr;
    uint8_t flags = *cur.p++; // keep a 1-byte flags field for future; currently 0
    if (flags != 0x00) return nullptr;

    auto parsed = make_uniq<ParsedTrie>();
    PNode *root_ptr = nullptr;
    if (!ParseNodeQCK2(cur, *parsed, root_ptr)) return nullptr;
    parsed->root = root_ptr;

    if (cur.p != cur.end) return nullptr; // strict consumption
    return std::move(parsed);
}
```

> We’re reusing the exported name `ParseQCK1` to avoid touching all include sites. It now enforces QCK2.

**Why**

* Introduces QCK2 reading while keeping call sites unchanged.

**Verify**

* Rebuild.
* Existing functions won’t be runnable until we also migrate the **builder** to emit QCK2 (next step).

---

## Step 3 — Change the builder aggregate to accept UPRN and write QCK2

**Edit**: `src/functions/trie/build_suffix_trie.cpp`

**Changes**

1. Change the **signature** from

   ```
   build_suffix_trie(LIST(VARCHAR)) -> BLOB
   ```

   to

   ```
   build_suffix_trie(BIGINT uprn, LIST(VARCHAR) tokens) -> BLOB
   ```

2. Update the state node used during aggregation:

```cpp
struct TrieNode {
    idx_t cnt = 0;
    uint32_t term = 0;
    uint64_t uprn = 0; // valid iff term == 1
    unordered_map<string, unique_ptr<TrieNode>> next;
};
```

3. Update the insert logic to bump `cnt` along the path and set terminal fields:

```cpp
static void InsertReversed(TrieNode &root, const vector<string> &toks, uint64_t uprn_val) {
    root.cnt++;
    auto *n = &root;
    for (idx_t i = toks.size(); i > 0; --i) {
        const string &tok = toks[i - 1]; // right→left
        auto &child = n->next[tok];
        if (!child) child = make_uniq<TrieNode>();
        n = child.get();
        n->cnt++;
    }
    n->term++;
    if (n->term == 1) {
        n->uprn = uprn_val;
    } else {
        n->uprn = 0; // ambiguous terminal
    }
}
```

4. Update `StateUpdate` to read two columns: `uprn` and `tokens`.

5. Replace the serializer with QCK2 layout (sorted children):

```cpp
static inline void W32(vector<uint8_t> &buf, uint32_t v) { /* as before */ }
static inline void W64(vector<uint8_t> &buf, uint64_t v) {
    W32(buf, (uint32_t)(v & 0xFFFFFFFFULL));
    W32(buf, (uint32_t)(v >> 32));
}
static inline void WStr(vector<uint8_t> &buf, const string &s) {
    W32(buf, (uint32_t)s.size());
    buf.insert(buf.end(), s.begin(), s.end());
}

static void SerializeNodeQCK2(const TrieNode &n, vector<uint8_t> &buf) {
    W32(buf, (uint32_t)n.cnt);
    W32(buf, (uint32_t)n.term);
    W64(buf, (uint64_t)n.uprn);

    // sorted children
    vector<pair<string, const TrieNode *>> items;
    items.reserve(n.next.size());
    for (auto &kv : n.next) items.emplace_back(kv.first, kv.second.get());
    sort(items.begin(), items.end(), [](auto &a, auto &b){ return a.first < b.first; });

    W32(buf, (uint32_t)items.size());
    for (auto &it : items) {
        WStr(buf, it.first);
        SerializeNodeQCK2(*it.second, buf);
    }
}

static void StateFinalize(Vector &state, AggregateInputData &, Vector &result, idx_t count, idx_t) {
    auto st_ptrs = FlatVector::GetData<data_ptr_t>(state);
    auto out = FlatVector::GetData<string_t>(result);

    for (idx_t i = 0; i < count; i++) {
        auto *st = reinterpret_cast<BuildTrieState *>(st_ptrs[i]);
        if (!st || !st->root) { FlatVector::SetNull(result, i, true); continue; }

        vector<uint8_t> bin; bin.reserve(1024);
        W32(bin, QCK2_MAGIC);
        bin.push_back(0x00); // flags
        SerializeNodeQCK2(*st->root, bin);

        out[i] = StringVector::AddString(result, reinterpret_cast<const char *>(bin.data()), bin.size());
        delete st->root; st->root = nullptr;
    }
}
```

6. Keep the destructor and merging logic unchanged except for struct field names.

**Why**

* Produces the new format consistently; parser from Step 2 can now read it.

**Verify (build & run)**

* Rebuild the extension.
* In DuckDB, run a tiny round-trip:

```sql
-- Build a tiny canonical set (no postcodes needed for the trie builder itself)
WITH canon(uprn, tokens) AS (
  SELECT 1, ['4','LOVE','LANE','KINGS','LANGLEY']::VARCHAR[] UNION ALL
  SELECT 2, ['7','LOVE','LANE','KINGS','LANGLEY']::VARCHAR[] UNION ALL
  SELECT 3, ['ANNEX','7','LOVE','LANE','KINGS','LANGLEY']::VARCHAR[]
)
SELECT build_suffix_trie(uprn, tokens) AS trie FROM canon
-- Expect 1-row aggregate: use GROUP BY to produce 1 blob
GROUP BY 'dummy';
```

* Use existing readers to confirm counts still resolve:

```sql
WITH canon(uprn, tokens) AS (
  SELECT 1, ['4','LOVE','LANE','KINGS','LANGLEY']::VARCHAR[] UNION ALL
  SELECT 2, ['7','LOVE','LANE','KINGS','LANGLEY']::VARCHAR[] UNION ALL
  SELECT 3, ['ANNEX','7','LOVE','LANE','KINGS','LANGLEY']::VARCHAR[]
),
trie_tbl AS (
  SELECT build_suffix_trie(uprn, tokens) AS trie FROM canon GROUP BY 1
)
SELECT format_address_with_counts(['4','LOVE','LANE','KINGS','LANGLEY'], trie) AS f
FROM trie_tbl;
-- Expect something like: "4 (1) -> LOVE (3) -> LANE (3) -> KINGS (3) -> LANGLEY (3)"
```

* Also sanity-check peeling (should peel nothing here):

```sql
SELECT peel_end_tokens(['4','LOVE','LANE','KINGS','LANGLEY'], trie) FROM trie_tbl;
-- Expect the same list back
```

If those work, your QCK2 read/write path is sound.

---

## Step 4 — (Refactor nicety) Centralize the “parse with cache” helper

**Add**:

* `src/include/trie/trie_cache_utils.hpp`
* `src/common/trie/trie_cache_utils.cpp`

**Code**

```cpp
// header
#pragma once
#include "trie/suffix_trie.hpp"
#include "trie/suffix_trie_cache.hpp"

namespace duckdb {
std::shared_ptr<const ParsedTrie>
GetOrParseTrie(TrieCache &cache, const string_t &blob);
} // namespace duckdb

// impl
#include "trie/trie_cache_utils.hpp"

namespace duckdb {
std::shared_ptr<const ParsedTrie>
GetOrParseTrie(TrieCache &cache, const string_t &blob) {
    auto data_ptr = reinterpret_cast<const uint8_t *>(blob.GetDataUnsafe());
    size_t data_len = blob.GetSize();
    uint64_t key = FNV1aHash64(data_ptr, data_len);
    if (auto got = cache.Get(key)) return got;
    auto parsed = ParseQCK1(blob); // now QCK2 under the hood
    if (!parsed) return nullptr;
    auto sp = std::shared_ptr<const ParsedTrie>(parsed.release());
    cache.Put(key, sp);
    return sp;
}
} // namespace duckdb
```

**Why**

* De-duplicates identical logic across the three existing functions and future ones.

**Verify**

* Rebuild; no behavior change yet.

---

## Step 5 — Switch existing functions to the helper (no behavior change)

**Edit**:

* `src/functions/trie/peel_end_tokens.cpp`
* `src/functions/address/build_cleaned_address.cpp`
* `src/functions/address/format_address_with_counts.cpp`

**Changes**

* Replace the local “ResolveTrieFromBlob(..)” with calls to `GetOrParseTrie(local_state.cache, trie_blob)`.
* No other logic changes.

**Why**

* Consolidates parsing; ensures all callers definitely read QCK2.

**Verify (build & run)**

* Rebuild.
* Re-run the SQL checks from Step 3 to confirm outputs unchanged.

---

## Step 6 — Add a minimal terminal-aware sanity query (optional, quick)

This step doesn’t change the extension; it validates that terminals exist in memory.

**What to do**

* Add a tiny throwaway debug query to check acceptance logic manually outside the extension:

  * For the path `['7','LOVE','LANE','KINGS','LANGLEY']`, `term` at node should be **1**.
  * For its parent `['LOVE','LANE','KINGS','LANGLEY']`, `term` should be **0** and `cnt` ≥ number of houses.

> Since terminals are not exposed by existing functions, your best current check is to **trust** the serializer/parser code path plus an integration test next step.

**Alternative (quick integration test idea)**

* Temporarily add a private function `debug_count_and_term(tokens, trie) → (cnt, term)`, gated under `#ifdef DEBUG` and call it in a scratch DuckDB session to confirm `term` at 7 is 1 and at “LOVE” is 0. Then remove it.
  (If you’d prefer to skip this, proceed—QCK2 will be validated again by the upcoming matcher work.)

---

## Step 7 — Remove all QCK1 code paths and constants

**Edit**

* `src/common/trie/suffix_trie.cpp`: delete the old QCK1 parser code if any remains.
* `src/include/trie/suffix_trie.hpp`: remove `QCK1_MAGIC` and `QCK1_FLAGS_EXPECTED` declarations.

**Why**

* Finish the migration; the codebase now only knows QCK2.

**Verify**

* Rebuild; all references to QCK1 should be gone.

---

## Step 8 — Rebuild any stored tries in your environment

**What to do**

* Wherever you persisted trie blobs, run a one-time rebuild to QCK2.

**Script template**

```sql
-- Example: per postgroup
CREATE OR REPLACE TABLE postgroup_tries AS
SELECT
  postgroup,
  build_suffix_trie(uprn, tokens) AS trie
FROM canonical_addresses
GROUP BY postgroup;

-- Optional: verify counts for a spot-check address
WITH trie_tbl AS (SELECT trie FROM postgroup_tries WHERE postgroup = 'WD4 9H*'),
check AS (
  SELECT format_address_with_counts(['7','LOVE','LANE','KINGS','LANGLEY'], trie) f
  FROM trie_tbl
)
SELECT * FROM check;
```

**Why**

* Existing blobs (QCK1) won’t parse anymore. This regenerates in QCK2.

**Verify**

* Spot-check a few groups with `format_address_with_counts` and `peel_end_tokens` to ensure counts are as expected.

---

## Step 9 — (Optional groundwork) Add tiny shared helpers for nav/peel

This is a quality-of-life refactor that will make `find_address_from_trie` \~straight-line code. It does **not** change behavior.

**Add**

* `src/include/trie/trie_nav.hpp` / `src/common/trie/trie_nav.cpp`

  * `const PNode *FindChild(const PNode*, const std::string&)` — binary search in `kids`
  * `bool HasChild(const PNode*, const std::string&)`
  * `void PrecomputeSuffixCounts(const ParsedTrie&, const std::vector<std::string>&, std::vector<uint32_t>&)`
* `src/include/trie/peel_utils.hpp` / `src/common/trie/peel_utils.cpp`

  * `void PeelEndTokensInPlace(std::vector<std::string>&, const ParsedTrie&, int32_t steps=4, int32_t max_k=2)`

**Update**

* Optionally call `PrecomputeSuffixCounts` from `format_address_with_counts.cpp` to remove duplicate logic (outputs must remain identical).
* Optionally rewrite internal peel loop in `peel_end_tokens.cpp` to delegate to `PeelEndTokensInPlace` (behavior identical).

**Why**

* Makes the upcoming matcher a 1-page function reusing these helpers.

**Verify**

* Rebuild; re-run the SQL checks to ensure outputs are unchanged.

---

## Step 10 — CMake updates & final tidy

**Edit**: `CMakeLists.txt`

* Add any new `src/common/*` files introduced in Steps 4 and 9 to `EXTENSION_SOURCES`.

**Verify**

* Clean build, run the same SQL smoke tests.

---

### What this sets you up for next

With QCK2 live everywhere:

* Implement `find_address_from_trie(tokens, trie) → STRUCT(uprn, consumed_path, consumed_path_counts)` in \~100–150 LoC:

  1. Gather tokens (skip NULLs).
  2. `PeelEndTokensInPlace`.
  3. Greedy exact walk R→L using `FindChild/HasChild`.
  4. Safe acceptance: `node->term == 1` **and** (exhausted **or** `!HasChild(next_tok)`).
  5. Return `uprn`, `consumed_path` (L→R), and counts (either captured on descent or via `PrecomputeSuffixCounts`).

No further format or API churn will be required.

---

## Quick checklist (per step)

- [x] S1: Compiles after adding `term/uprn`.
- [x] S2: Parser enforces `QCK2_MAGIC`; compiles.
- [ ] S3: Builder takes `(uprn, tokens)` and writes QCK2; round-trip SQL shows expected counts.
- [ ] S4–S5: All existing functions parse via the helper; prior SQL still works with the QCK2 trie.
- [ ] S7: No stray QCK1 references; full build succeeds.
- [ ] S8: All persisted tries are rebuilt; spot-checks succeed.
- [ ] S9–S10: Optional helper refactors; builds stay green.

If you’d like, I can turn any of the steps above into exact patches (minimal diffs) for each file.
