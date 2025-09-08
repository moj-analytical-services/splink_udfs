Here’s a tight, incremental plan to add **greedy skip-of-messy-tokens** powered by the trie, in a way that is easy to build & verify after each step.

# What we’re trying to achieve (precise)

Enable the matcher to **skip up to N “messy” tokens** inside a candidate (messy) address *iff* skipping them is supported by the trie—i.e., the next token after the skipped one would continue the path in the trie. Keep it **greedy and simple**:

* Only skip when the current token **doesn’t match** and the **next** token (one position closer to the leaf) **does** match from the current node.
* Use at most **one skip** by default (`max_skips=1`), but make it parameterizable.
* Apply the same semantics for both `allow_prefix = false` (strict suffix walk) and `allow_prefix = true` (longest-suffix scan with resets).

This should:

* Convert some “NO\_PATH”/early-break cases into “EXACT” or “AMBIGUOUS”.
* Leave already-exact matches unchanged.
* Preserve API backwards compatibility (only add overloads/params).

---

## Step-by-step implementation plan

### 1) Add a small reusable matcher helper (declaration)

**Change:** Create a reusable API that performs a right→left walk with greedy lookahead-based skips.

**Edit:** `src/include/trie/trie_nav.hpp`

```cpp
// Add near the end of the header
struct GreedySkipMatchResult {
  const PNode *last_node = nullptr;   // node reached after walk/best segment
  const PNode *deepest_unique = nullptr; // deepest node with term==1
  int32_t matched_len = 0;            // number of consumed tokens (right->left)
  int32_t skipped = 0;                // number of tokens skipped
};

GreedySkipMatchResult GreedyWalkWithSkips(
    const ParsedTrie &pt,
    const std::vector<std::string> &toks,
    bool allow_prefix,
    int32_t max_skips);
```

**Verify:** file compiles (header only).
*No behavior change yet.*

---

### 2) Implement the helper with **lookahead-based skip** (core feature)

**Change:** Implement greedy skip: on mismatch, if `skips_left>0` and `next` token exists as a child of the **current node**, consume the skip and advance with that next token. Otherwise (no lookahead support), behave as before (break or reset depending on `allow_prefix`).

**Edit:** `src/common/trie/trie_nav.cpp`

```cpp
GreedySkipMatchResult GreedyWalkWithSkips(
    const ParsedTrie &pt,
    const std::vector<std::string> &toks,
    bool allow_prefix,
    int32_t max_skips) {

  GreedySkipMatchResult res;
  if (!pt.root) return res;
  const PNode *root = pt.root;

  if (!allow_prefix) {
    const PNode *node = root;
    int32_t skips_left = std::max(0, max_skips);
    for (idx_t ti = toks.size(); ti > 0; --ti) {
      const std::string &tok = toks[ti - 1];              // current
      const PNode *next = FindChild(node, tok);
      if (next) {
        node = next;
        res.last_node = node;
        res.matched_len++;
        if (node->term == 1 && node->uprn != 0) {
          res.deepest_unique = node;
        }
        continue;
      }
      // lookahead skip: only if there *is* a next token to test
      if (skips_left > 0 && ti > 1) {
        const std::string &tok_after_skip = toks[ti - 2]; // next closer to leaf
        const PNode *after = FindChild(node, tok_after_skip);
        if (after) {
          // consume the skip, do NOT advance node with the skipped token,
          // but do advance with tok_after_skip
          res.skipped++;
          skips_left--;
          node = after;
          res.last_node = node;
          res.matched_len++;         // we consumed tok_after_skip
          if (node->term == 1 && node->uprn != 0) {
            res.deepest_unique = node;
          }
          // additionally, we must also decrement loop once more to account for the consumed token
          // we already consumed tok_after_skip (ti-2), so skip the next loop's decrement by one extra:
          --ti; // because we manually consumed one more token
          continue;
        }
      }
      // no match and no supported lookahead skip: stop
      break;
    }
    return res;
  }

  // allow_prefix = true : longest matching suffix with resets from root on miss
  const PNode *node = root;
  const PNode *best_last = nullptr;
  int32_t best_len = 0;
  int32_t curr_len = 0;
  int32_t skips_left = std::max(0, max_skips);

  for (idx_t ti = toks.size(); ti > 0; --ti) {
    const std::string &tok = toks[ti - 1];

    // Try direct match
    const PNode *next = FindChild(node, tok);
    if (next) {
      node = next;
      curr_len++;
      res.last_node = node;
      if (curr_len > best_len) {
        best_len = curr_len;
        best_last = node;
      }
      if (node->term == 1 && node->uprn != 0) {
        res.deepest_unique = node;
      }
      continue;
    }

    // Try lookahead skip (stay at current node, consume next token if it fits)
    if (skips_left > 0 && ti > 1) {
      const std::string &tok_after_skip = toks[ti - 2];
      const PNode *after = FindChild(node, tok_after_skip);
      if (after) {
        res.skipped++;
        skips_left--;
        node = after;
        curr_len++;
        res.last_node = node;
        if (curr_len > best_len) {
          best_len = curr_len;
          best_last = node;
        }
        if (node->term == 1 && node->uprn != 0) {
          res.deepest_unique = node;
        }
        --ti; // consumed one extra token
        continue;
      }
    }

    // Miss without supported skip: reset and start a new suffix segment
    node = root;
    skips_left = std::max(0, max_skips);
    curr_len = 0;

    // Re-try this same token from root (one reattempt is enough)
    next = FindChild(node, tok);
    if (next) {
      node = next;
      curr_len = 1;
      res.last_node = node;
      if (curr_len > best_len) {
        best_len = curr_len;
        best_last = node;
      }
      if (node->term == 1 && node->uprn != 0) {
        res.deepest_unique = node;
      }
      continue;
    }
    // If even root fails, just continue to the next earlier token (shorter suffix).
  }

  res.last_node = best_last ? best_last : res.last_node;
  res.matched_len = best_len;
  return res;
}
```

**Verify (logic-only):**

* Build compiles.
* No callers yet; behavior change not visible.

---

### 3) Refactor `find_address_from_trie_classify` to use the helper

**Change:** Replace its hand-rolled loops with `GreedyWalkWithSkips(...)`. Keep the **same output schema** (status/uprn/etc.) so downstream code stays unchanged.

**Edit (surgical):** `src/functions/address/find_address_from_trie_classify.cpp`

* Inside the row loop, after you materialize `toks` and read `allow_prefix` + `max_skips`, call:

```cpp
auto mr = GreedyWalkWithSkips(*trie_ptr, toks, allow_prefix, max_skips);
const bool consumed_all = (mr.matched_len == (int32_t)toks.size());
cons_out[i] = consumed_all;
mlen_out[i] = mr.matched_len;

if (mr.last_node) {
  cnt_out[i]  = (int32_t)mr.last_node->cnt;
  term_out[i] = (int32_t)mr.last_node->term;
}

if (consumed_all) {
  if (mr.last_node && mr.last_node->term == 1 && mr.last_node->uprn != 0) {
    status_out[i] = StringVector::AddString(status_vec, "EXACT");
    uprn_out[i]   = (int64_t)mr.last_node->uprn;
    FlatVector::SetNull(uprn_vec, i, false);
  } else if (mr.last_node && mr.last_node->term == 0) {
    status_out[i] = StringVector::AddString(status_vec, "INSUFFICIENT");
  } else {
    status_out[i] = StringVector::AddString(status_vec, "AMBIGUOUS");
  }
} else {
  if (mr.matched_len == 0) {
    status_out[i] = StringVector::AddString(status_vec, "NO_PATH");
  } else if (mr.last_node && mr.last_node->cnt > 1) {
    status_out[i] = StringVector::AddString(status_vec, "AMBIGUOUS");
  } else {
    status_out[i] = StringVector::AddString(status_vec, "NO_PATH");
  }
}
```

**Verify:**

* Rebuild.
* Signature unchanged. Should compile clean.
* (Optional sanity) Compare outputs before/after on a small dataset to see some “NO\_PATH” flip to “EXACT” when `max_skips=1`.

---

### 4) Add `max_skips` to the simple finder (new overload, non-breaking)

**Change:** Add a **4-arg overload** to `find_address_from_trie(tokens, trie, allow_prefix, max_skips)` and route both the 2-arg and 3-arg variants to the helper with `max_skips=0`.

**Edit:** `src/functions/address/find_address_from_trie.cpp`

* In the row loop, replace the manual loop with:

```cpp
int32_t max_skips = 0;
if (args.ColumnCount() >= 4) {
  UnifiedVectorFormat skip_uvf;
  Vector &skip_vec = args.data[3];
  skip_vec.ToUnifiedFormat(args.size(), skip_uvf);
  const auto sid = skip_uvf.sel->get_index(i);
  if (skip_uvf.validity.RowIsValid(sid)) {
    auto skip_vals = UnifiedVectorFormat::GetData<int32_t>(skip_uvf);
    max_skips = std::max(0, std::min(1, skip_vals[sid])); // greedy: cap at 1
  }
}

auto mr = GreedyWalkWithSkips(*trie_ptr, toks, allow_prefix, max_skips);
if (mr.matched_len == (int32_t)toks.size()
    && mr.last_node && mr.last_node->term == 1 && mr.last_node->uprn != 0) {
  out[i] = (int64_t)mr.last_node->uprn;
} else {
  FlatVector::SetNull(result, i, true);
}
```

* Extend `GetFindAddressFromTrieFunctionSet()`:

```cpp
// existing 2-arg and 3-arg remain
{
  ScalarFunction f({tokens_type, LogicalType::BLOB, LogicalType::BOOLEAN, LogicalType::INTEGER},
                   LogicalType::BIGINT, FindAddressExec);
  f.init_local_state = FindAddrInitLocal;
  set.AddFunction(f);
}
```

**Verify:**

* Rebuild.
* Old SQL still works.
* New SQL compiles:

  ```sql
  select find_address_from_trie(tokens, trie_blob, true, 1) from t;
  ```

---

### 5) Add `max_skips` to the debug function (new overload, non-breaking)

**Change:** Mirror the helper usage and expose the 4-arg overload. Keep the struct shape (uprn, matched\_len, is\_terminal, ambiguous).

**Edit:** `src/functions/address/find_address_from_trie_dbg.cpp`

* Use `GreedyWalkWithSkips` to compute `mr`.
* Fill fields:

  * `matched_len = mr.matched_len`
  * If `mr.last_node`: `is_terminal = last_node->term > 0`; `ambiguous = last_node->term > 1`
  * `uprn` is non-null iff `consumed_all && last_node->term==1 && uprn!=0`.
* Add 4-arg overload in `GetFindAddressFromTrieDbgFunctionSet()` with an `INTEGER` as the 4th arg.

**Verify:**

* Rebuild.
* Compare old vs new behavior with `max_skips=0` (identical).

---

### 6) Update the classifier to cap `max_skips` at 1 (already in place)

**Change:** You already clamp `max_skips` to `[0..1]` in `find_address_from_trie_classify.cpp`. Leave as-is for greedy behavior.

**Verify:** build only.

---

### 7) Quick correctness checks with synthetic data (SQL-only)

**Goal:** Show the skip is used only when supported by the trie.

1. **Setup a tiny trie**:

```sql
-- Canonical tokens: [10, LOVE, LANE, KINGS, LANGLEY], [11, LOVE, LANE, KINGS, LANGLEY]
create table canon(uprn bigint, toks varchar[]);
insert into canon values
  (101, ['10','LOVE','LANE','KINGS','LANGLEY']),
  (111, ['11','LOVE','LANE','KINGS','LANGLEY']);

-- Build the trie
with blobs as (
  select build_suffix_trie(uprn, toks) as blob from canon
)
select blob into trie_tbl from blobs limit 1;
```

2. **Messy input with an internal extra token**:

```sql
-- EXTRA token between LOVE and LANE that should be skippable
with messy as (
  select ['10','LOVE','APT','LANE','KINGS','LANGLEY']::varchar[] as toks
),
T as (select (select blob from trie_tbl) as trie, toks from messy)

-- Before: STRICT, no skip: should be NULL
select find_address_from_trie(toks, trie, false) from T;

-- After: STRICT, allow skip=1: should return 101
select find_address_from_trie(toks, trie, false, 1) from T;

-- Classifier evidence:
select (find_address_from_trie_classify(toks, trie, false, 1)).* from T;
```

3. **Unsupported skip (should not skip blindly)**:

```sql
-- Insert a bogus token where lookahead doesn't help (wrong next token)
with messy as (
  select ['10','LOVE','APT','WRONG','KINGS','LANGLEY']::varchar[] as toks
),
T as (select (select blob from trie_tbl) as trie, toks from messy)
select (find_address_from_trie_classify(toks, trie, false, 1)).* from T; -- expect NO_PATH
```

4. **INSUFFICIENT** (no number):

```sql
with messy as (select ['LOVE','LANE','KINGS','LANGLEY']::varchar[] as toks),
T as (select (select blob from trie_tbl) as trie, toks from messy)
select (find_address_from_trie_classify(toks, trie, false, 1)).* from T; -- expect INSUFFICIENT
```

5. **AMBIGUOUS**:

```sql
with messy as (select ['LOVE','LANE','KINGS','LANGLEY','EXTRA']::varchar[] as toks),
T as (select (select blob from trie_tbl) as trie, toks from messy)
select (find_address_from_trie_classify(toks, trie, true, 1)).* from T; -- likely AMBIGUOUS
```

**Verify outcome:** The 2nd query returns 101; classifier shows `matched_len` equal to len(toks) due to one supported skip.

---

### 8) Keep `build_cleaned_address` & `peel_end_tokens` unchanged

They don’t need skip and should remain deterministic.
**Verify:** Re-run previous tests that exercise these functions—no regressions.

---

### 9) Performance sanity

**Goal:** Ensure the lookahead doesn’t blow up runtime.

* The lookahead adds only **O(1)** extra work per mismatch.
* Test on a few thousand rows; observe similar throughput vs. pre-change when `max_skips=0`.

---

### 10) Document the semantics (developer notes)

Add comments in each function set registration block:

* `max_skips (INTEGER, default 0, capped to 1): on mismatch, will skip at most one in-between token only if the following token continues the trie path from the current node. Greedy: the first supported skip is used; no backtracking. Works in both strict and allow_prefix modes.`

---

### 11) Optional: expose `skipped` in the classifier

If you want to externally verify that a skip happened, add a `skipped INTEGER` field to the struct in `find_address_from_trie_classify`. (Leave it out if you want zero API change.)
**Verify:** run the synthetic test in Step 7 and assert `skipped=1` only in the second query.

---

### 12) Backward compatibility checklist

* Existing SQL continues to work (2-arg and 3-arg versions still registered).
* New overloads simply add an optional 4th `INTEGER` arg.
* Classifier schema unchanged (unless you do Step 11).

---

## Why this order?

* We implement the **core behavior once** (Step 2) and reuse it everywhere → consistent semantics, fewer bugs.
* We **refactor the classifier first** (Step 3) because it provides rich evidence for verification.
* We then extend **simple** and **debug** versions (Steps 4–5) with minimal API changes.

If you want, I can turn Steps 2–5 into ready-to-paste diffs next.
