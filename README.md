# Splink UDFs Extension for DuckDB

The `splink_udfs` extension is work in progress. It aims to offer a variety of function



This repo is based on the [DuckDB Extension Template](https://github.com/duckdb/extension-template)
## Installation

This is a custom DuckDB extension and not (yet) part of the official community extensions.

Once built and compiled locally, you can load it like this:

```sql
.load '/path/to/splink_udfs.duckdb_extension';
```

If you're using the DuckDB CLI or embedded DuckDB in your project, simply run:

```sql
SELECT soundex('Robert'); -- returns 'R163'
```

## API

### `soundex(VARCHAR) → VARCHAR`

Computes the Soundex code of a string. Always returns a 4-character string (e.g., `S540`, `J200`, `0000` for empty input).

Note that soundex ignores non-ascii chacters.  This means, for example, that `soundex('Émilie')` returns M400, which is the same as `soundex('milie')`.

If you have diacritics or other special characters, you should wrap you call like `soundex(strip_diacritics(first_name))`

### `strip_diacritics(VARCHAR) → VARCHAR`

Removes diacritical marks from a string using Unicode normalization. For example, `Jürgen` becomes `Jurgen`. This function does not transliterate distinct letters like `ø` or `ß`.

### `unaccent(VARCHAR) → VARCHAR`

Provides a more comprehensive transliteration of a string. It first strips all diacritics and then converts other special characters and ligatures (e.g., `Æ` → `AE`, `ø` → `o`, `ß` → `ss`) to their basic Latin equivalents.

### `double_metaphone(VARCHAR) → LIST(VARCHAR)`

Computes the Double Metaphone phonetic codes for a string. Double Metaphone is an advanced phonetic algorithm that generates up to two alternative pronunciations for English words, making it more accurate than Soundex for matching names that may have different pronunciations.

The function returns a list containing:
- The primary phonetic code (always present if input is non-empty)
- The alternate phonetic code (only included if different from the primary)

This allows for more flexible matching - you can check if any code from one word matches any code from another word.

### `levenshtein(VARCHAR, VARCHAR) → BIGINT`
### `levenshtein(VARCHAR, VARCHAR, BIGINT) → BIGINT`

Computes the Levenshtein distance between two strings. The Levenshtein distance is the minimum number of single-character edits (insertions, deletions, or substitutions) required to change one string into another.

The function has two variants:
- Two-argument version: Returns the exact Levenshtein distance
- Three-argument version: Returns the distance capped at the specified threshold (useful for performance when you only care if strings are within a certain distance)

### `damerau_levenshtein(VARCHAR, VARCHAR) → BIGINT`
### `damerau_levenshtein(VARCHAR, VARCHAR, BIGINT) → BIGINT`

Computes the Damerau-Levenshtein distance between two strings. This extends the Levenshtein distance by also allowing transposition of two adjacent characters as a single edit operation, making it more suitable for detecting common typing errors.

Like the Levenshtein function, it has two variants:
- Two-argument version: Returns the exact Damerau-Levenshtein distance
- Three-argument version: Returns the distance capped at the specified threshold

Both distance functions are implemented using the [rapidfuzz-cpp](https://github.com/rapidfuzz/rapidfuzz-cpp) library, which provides high-performance string similarity algorithms.

### `ngrams(LIST(any), BIGINT) → LIST(ARRAY(any, n))`

Generates n-grams from a list of elements. An n-gram is a contiguous sequence of `n` elements from the input list.

- The first argument must be a list of any type or `NULL`.
- The second argument is a constant positive integer `n` specifying the size of the n-grams.

The function returns a list of arrays, where each array contains `n` elements from the input list.

### Address Matching: Trie: Build & Lookup

This extension includes functions for address matching using a trie.

For full example code, see [here](https://github.com/moj-analytical-services/splink_udfs/pull/22)

**Workflow**

1. **Tokenize** each address into a `LIST(VARCHAR)` (e.g., `string_split(upper(address), ' ')`).
2. **Build** one shared trie for your dataset with the aggregate `build_suffix_trie(...)`. The result is a single `BLOB`.
3. **Lookup** each tokenized address with `find_address(...)`, which returns the matched `UPRN` (or `NULL` if no unambiguous match exists).

---

### `build_suffix_trie(BIGINT id, LIST(VARCHAR) tokens) → BLOB`  *(aggregate)*

Builds a compact, reversed-suffix trie across all input rows. Each row contributes:

* `id` (`BIGINT`): the unique identifier for the full address (e.g., UPRN).
* `tokens` (`LIST(VARCHAR)`): the tokenized address in **left-to-right** order (e.g., `['10','DOWNING','STREET','LONDON']`).

The trie stores:

* Path counts for suffixes (used internally), and
* Terminal metadata: if exactly one address ends at a node, that node carries its `id` (UPRN); if multiple end there, the node is marked ambiguous.

**Example**

```sql
-- Build one trie blob per group (often the whole table)
WITH tokenized AS (
  SELECT uprn,
         string_split(upper(address), ' ') AS toks
  FROM   my_addresses
)
SELECT build_suffix_trie(uprn, toks) AS trie_blob
FROM tokenized;
```

The result (`trie_blob`) can be joined back or stored for later lookups.

---

### `find_address(LIST(VARCHAR) tokens, BLOB trie) → BIGINT`  *(scalar)*

Looks up a tokenized address in the trie and returns the `id` (UPRN) **only if** the path ends at an **unambiguous** terminal. Otherwise returns `NULL`.

* `tokens` (`LIST(VARCHAR)`): tokenized address in left-to-right order.
* `trie` (`BLOB`): the blob produced by `build_suffix_trie`.

**Default usage**

```sql
-- Assume we persisted the trie in a single-row table `addr_trie(trie_blob BLOB)`
WITH q AS (
  SELECT string_split(upper('10 Downing Street London'), ' ') AS toks
)
SELECT find_address(toks, t.trie_blob) AS uprn
FROM q CROSS JOIN addr_trie t;
```

#### Optional parameters

The eight-argument overload exposes the tuning knobs DuckDB uses internally. Each optional `BIGINT` mirrors the default behaviour when set to the values below.

```
find_address(tokens, trie,
             skip_min_local_count,
             skip_max_in_walk,
             min_matched_tokens,
             entry_min_local_count,
             max_trailing_tokens_ignored,
             max_trie_entry_depth)
```

- `skip_min_local_count` (default `10`): only allow skipping over a token when the landing trie node has at least this many descendants. Example: leaving the default lets the messy input `1 LOVE LANE KINGS SKIP LANGLEY` match by skipping `SKIP`; lowering it to `0` would also allow skipping near specific house numbers.
- `skip_max_in_walk` (default `2`): cap on how many tokens may be skipped inside one lookup. Example: setting it to `0` forces every token to match, so `1 LOVE LANE KINGS SKIP LANGLEY` would fail.
- `min_matched_tokens` (default `2`): minimum tokens that must align before accepting a result. Example: increasing it to `3` means a lookup like `ANNEX 7 LOVE LANE ...` stops matching once only two tokens align.
- `entry_min_local_count` (default `10`): only seed the walk from trie nodes that have at least this many descendants. Example: with the default we can start matching from the `LANGLEY` branch; raising it to `100` skips that entry point and can miss valid addresses.
- `max_trailing_tokens_ignored` (default `2`): number of trailing tokens from the input that may be ignored. Example: the default lets `1 LOVE LANE KINGS LANGLEY EXTRA` match; setting it to `0` makes the same lookup fail.
- `max_trie_entry_depth` (default `2`): how far below the root we seed alternative entry points. Example: with the default, `1 LOVE LANE KINGS` can match an address that ends with `LANGLEY`; setting it to `0` forces matches to start at the root so the same input fails.

**Explicit parameter usage**

```sql
WITH q AS (
  SELECT string_split(upper('10 Downing Street London Extra'), ' ') AS toks
)
SELECT find_address(
         toks,
         t.trie_blob,
         10,  -- skip_min_local_count
         2,   -- skip_max_in_walk
         2,   -- min_matched_tokens
         10,  -- entry_min_local_count
         2,   -- max_trailing_tokens_ignored
         2    -- max_trie_entry_depth
       ) AS uprn
FROM q CROSS JOIN addr_trie t;
```

All parameters are optional; omit them to keep the historical behaviour.

**Notes**

- If multiple addresses share the same terminal path, the node is ambiguous and `find_address` returns `NULL`.
- Empty token lists or invalid trie blobs also return `NULL`.


#### Example Usage

```sql
-- Soundex examples
SELECT soundex('William'); -- returns 'W450'
SELECT soundex('Joe');     -- returns 'J000'
SELECT soundex('Charlie'); -- returns 'C640'

-- Diacritic and Unaccent examples
SELECT strip_diacritics('Jürgen Thérèse'); -- returns 'Jurgen Therese'
SELECT unaccent('Ærøskøbing Groß');       -- returns 'AEroskobing Gross'

-- Comparing the behavior of strip_diacritics and unaccent
SELECT
    strip_diacritics('Ærø') AS stripped,
    unaccent('Ærø') AS unaccented;
-- returns 'Ærø', 'AEro'

-- Double Metaphone examples
SELECT double_metaphone('Smith'); -- returns ['SM0']
SELECT double_metaphone('Schmidt'); -- returns ['XMT', 'SMT']
SELECT double_metaphone('Johnson'); -- returns ['JNSN']
SELECT double_metaphone('Jackson'); -- returns ['JKSN']

-- Levenshtein distance examples
SELECT levenshtein('kitten', 'sitting'); -- returns 3
SELECT levenshtein('Saturday', 'Sunday'); -- returns 3
SELECT levenshtein('hello', 'hello'); -- returns 0

-- Levenshtein with max threshold (performance optimization)
SELECT levenshtein('kitten', 'sitting', 2); -- returns 3 (exceeds threshold)
SELECT levenshtein('hello', 'helo', 2); -- returns 1 (within threshold)

-- Damerau-Levenshtein distance examples
SELECT damerau_levenshtein('CA', 'AC'); -- returns 1 (transposition)
SELECT damerau_levenshtein('kitten', 'sitting'); -- returns 3
SELECT damerau_levenshtein('hello', 'ehllo'); -- returns 1 (transposition)

-- Damerau-Levenshtein with max threshold
SELECT damerau_levenshtein('CA', 'AC', 2); -- returns 1

-- ngrams
SELECT ngrams([1, 2, 3, 4], 2); -- returns [[1, 2], [2, 3], [3, 4]]
SELECT ngrams(['a', 'b', 'c', 'd'], 3); -- returns [['a', 'b', 'c'], ['b', 'c', 'd']]

```

## Testing

This extension uses [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html) for validation.

To run the tests:

```bash
make test
```

You’ll find the test cases in the `test/sql/` directory.

## Build Instructions

To build the extension:

```bash
GEN=ninja make
```

This produces:

* `build/release/duckdb` — DuckDB shell with extension loaded
* `build/release/extension/splink_udfs/splink_udfs.duckdb_extension` — loadable binary

To run:

```bash
./build/release/duckdb
.load 'build/release/extension/splink_udfs/splink_udfs.duckdb_extension'
```

## License

**splink_udfs** is primarily licensed under the [MIT License](LICENSE).



**Exception:**
The Double Metaphone implementation in `src/phonetics/double_metaphone.cpp` (and its accompanying header) is a literal C++ translation of `org.apache.commons.codec.language.DoubleMetaphone` see [here](https://javadoc.io/doc/commons-codec/commons-codec/1.6/org/apache/commons/codec/language/DoubleMetaphone.html) and is therefore licensed separately under the [Apache License 2.0](LICENSE-APACHE). See the top of `double_metaphone.cpp` for the full Apache header, and the project’s `NOTICE` file for attribution details.


**RapidFuzz‑CPP** is MIT‑licensed implementation of Levenshtein families of algorithms.  Source is vendored under `third_party/rapidfuzz` and its license is [here](LICENSE_RAPIDFUZZ)