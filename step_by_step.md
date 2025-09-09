Here’s a tight, iterative plan can follow to strip the trie features down to **only** the builder + QCK2 (de)serialization, while **keeping the cache**. Each step is small, self-contained, and verifiable.

---

# What stays vs. what goes

**Keep (must continue to compile):**

* `src/functions/trie/build_suffix_trie.cpp` (builder + QCK2 serialization)
* `src/common/trie/suffix_trie.cpp` (QCK2 deserialization via `ParseQCK1`)
* `src/include/trie/suffix_trie.hpp` (structs + deserializer decl; we’ll prune it)
* `src/include/trie/suffix_trie_cache.hpp` (TrieCache)
* `src/common/trie/trie_cache_utils.cpp` (GetOrParseTrie)
* Anything non-trie you already have (soundex, diacritics, ngrams, etc.)

**Remove (features, and their headers/impls):**

* Peel functions:

  * `src/functions/trie/peel_end_tokens.cpp`
  * `src/common/trie/peel_utils.cpp`
  * `src/include/trie/peel_utils.hpp`
* Navigation helpers:

  * `src/common/trie/trie_nav.cpp`
  * `src/include/trie/trie_nav.hpp`
* Address helpers:

  * `src/functions/address/build_cleaned_address.cpp`
  * `src/functions/address/format_address_with_counts.cpp`
* Any declarations or registrations for:

  * `GetPeelEndTokensFunctionSet`
  * `GetBuildCleanedAddressFunctionSet`
  * `GetFormatAddressWithCountsFunctionSet`

We will also **prune** `address_trie_functions.hpp` so it declares only the builder’s function set.

---

# Step-by-step plan



## 1) Stop registering the features you’re about to remove (minimal impact, easy revert) — DONE

**File:** `src/splink_udfs_extension.cpp`

* Delete the three registration calls in `LoadInternal`:

  ```cpp
  // REMOVE these lines:
  // ExtensionUtil::RegisterFunction(instance, GetPeelEndTokensFunctionSet());
  // ExtensionUtil::RegisterFunction(instance, GetBuildCleanedAddressFunctionSet());
  // ExtensionUtil::RegisterFunction(instance, GetFormatAddressWithCountsFunctionSet());
  ```
* Keep the registration of `GetBuildSuffixTrieAggregateSet()`.

**Build & verify**

* Rebuild.
* **Verify at runtime:** In DuckDB, run:

  ```sql
  -- Should NOT list peel/address helpers anymore (0 rows):
  SELECT * FROM duckdb_functions() WHERE name IN
    ('peel_end_tokens','build_cleaned_address','format_address_with_counts');

  -- Builder should still be there (≥1 row):
  SELECT * FROM duckdb_functions() WHERE name = 'build_suffix_trie';
  ```

## 2) Prune the public *declarations* to only the builder (no behavior change yet) — DONE

**File:** `src/include/trie/address_trie_functions.hpp`

* Replace its contents with only:

  ```cpp
  #pragma once
  #include "duckdb.hpp"
  #include "duckdb/function/function_set.hpp"

  namespace duckdb {
  // Keep ONLY this declaration:
  AggregateFunctionSet GetBuildSuffixTrieAggregateSet();
  } // namespace duckdb
  ```

**Build & verify**

* Rebuild.
* **Verify:** Build still green. The extension loads; step 1’s SQL checks remain correct.

## 3) Stop compiling the removed features (CMake list edit only) — DONE

**File:** `CMakeLists.txt`

* In `set(EXTENSION_SOURCES ...)`, **remove** these entries:

  ```cmake
  # REMOVE:
  # src/functions/trie/peel_end_tokens.cpp
  # src/functions/address/build_cleaned_address.cpp
  # src/functions/address/format_address_with_counts.cpp
  # src/common/trie/trie_nav.cpp
  # src/common/trie/peel_utils.cpp
  ```

  Keep `src/common/trie/suffix_trie.cpp`, `src/common/trie/trie_cache_utils.cpp`, and the builder file.

**Build & verify**

* Rebuild.
* **Verify (link-time):** No undefined symbol errors for any of the removed functions.

## 4) Remove now-dead headers (safe cleanup of includes) — DONE

* **Delete files:**

  * `src/include/trie/peel_utils.hpp`
  * `src/include/trie/trie_nav.hpp`
* Search the repo for includes of those headers and remove any stray includes (they should only have been used by the files you just stopped compiling).

**Build & verify**

* Rebuild.
* **Verify:** Build still green.

## 5) (Optional but consistent with “only builder + QCK2”): prune unused `CountTail` — DONE

If you want to keep only QCK2 (de)serialization and the cache, remove the tail-count lookup which is purely for navigation/peeling.

* **File:** `src/include/trie/suffix_trie.hpp`

  * Remove the declaration:

    ```cpp
    // REMOVE:
    // uint32_t CountTail(const ParsedTrie &pt, const std::vector<std::string> &tail_reversed);
    ```
* **File:** `src/common/trie/suffix_trie.cpp`

  * Delete the entire `CountTail` function definition at the bottom of the file.

**Build & verify**

* Rebuild.
* **Verify:** Build green. (There should be no remaining references to `CountTail`.)

> If you’d rather keep `CountTail` around as a harmless internal (future) helper, you can skip Step 5. It’s not referenced after Step 3.

## 6) Keep the cache as-is (no code changes) — DONE

* Confirm we still compile these:

  * `src/include/trie/suffix_trie_cache.hpp`
  * `src/common/trie/trie_cache_utils.cpp`
* They depend only on `ParseQCK1` (which remains) and not on any removed features.

**Build & verify**

* Rebuild.
* **Verify:** Still green.

## 7) Runtime checks (verify the final surface) — DONE

Open DuckDB with your extension loaded, then:

* **Builder still works & returns a non-empty QCK2 blob**

  ```sql
  WITH t(uprn, toks) AS (
    VALUES (1, ['unit','a']), (2, ['unit','b'])
  )
  SELECT length(build_suffix_trie(uprn, toks)) > 5 AS ok
  FROM t;
  -- Expect: one row, ok = TRUE
  ```

* **Removed functions are gone**

  ```sql
  SELECT name
  FROM duckdb_functions()
  WHERE name IN ('peel_end_tokens','build_cleaned_address','format_address_with_counts');
  -- Expect: 0 rows
  ```

*(If you want to sanity-peek the QCK2 magic, the first 4 bytes are ‘QCK2’. A simple proxy check is the length > 5 assertion above; if you prefer a stricter check and your DuckDB build allows casting BLOB→VARCHAR safely for ASCII, you can `CAST` and compare `substr` to `'QCK2'`.)*

## 8) (Optional) Physically remove the implementation files from the repo — DONE

Now that the build no longer references them, you can delete:

* `src/functions/trie/peel_end_tokens.cpp`
* `src/functions/address/build_cleaned_address.cpp`
* `src/functions/address/format_address_with_counts.cpp`
* `src/common/trie/trie_nav.cpp`
* `src/common/trie/peel_utils.cpp`

**Build & verify**

* Rebuild.
* **Verify:** Still green. Git shows the deletions only.

## 9) Guardrail check: search for stragglers (scriptable) — DONE

Run a quick grep to ensure no references remain:

```bash
git grep -nE 'peel_end_tokens|build_cleaned_address|format_address_with_counts|trie_nav|PeelEndTokens|GetPeelEndTokens'
```

* **Verify:** No matches in active code (comments are okay if you want to keep history clean, remove them).

---

# Final state (what you’ll have)

* Trie **builder** aggregate: **present**
* QCK2 **serialization** (in builder) & **deserialization** (`ParseQCK1` in `suffix_trie.cpp`): **present**
* Trie **cache** (`TrieCache`, `GetOrParseTrie`): **present**
* Peel, address formatting/cleaning, trie navigation utils: **removed**
* Public header surface for trie functions reduced to **only**:

  ```cpp
  AggregateFunctionSet GetBuildSuffixTrieAggregateSet();
  ```

This plan lets you compile and load at each step, with clear checks after every change.
