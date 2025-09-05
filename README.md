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

### Address Cleaning and Standardization

This suite of functions is designed for cleaning and standardizing address data, particularly by identifying and removing common, low-information trailing components (like city, county, or country names). They operate on lists of address tokens and use a trie structure to enable various types of cleaning.

**Workflow:**

1.  **Tokenize** your addresses into lists of strings (e.g., using `string_split`).
2.  **Build** a suffix trie from all your tokenized addresses using the `build_suffix_trie` aggregate function. This creates a single `BLOB` containing the trie.
3.  **Apply** one of the cleaning functions (`peel_end_tokens` or `build_cleaned_address`) to each tokenized address, providing the trie `BLOB` to guide the cleaning process.

### `build_suffix_trie(LIST(VARCHAR)) → BLOB`

An **aggregate function** that builds a compressed suffix trie from a column of token lists. A suffix trie stores the frequency counts of every possible trailing sequence of tokens. For example, for the address `['10', 'DOWNING', 'STREET', 'LONDON']`, it would increment the counts for `['LONDON']`, `['STREET', 'LONDON']`, `['DOWNING', 'STREET', 'LONDON']`, and so on.

The result is a single binary `BLOB` that can be passed to the other address cleaning functions.

### `format_address_with_counts(LIST(VARCHAR), BLOB, [VARCHAR]) → VARCHAR`

A utility function for inspecting the data within a suffix trie. For a given list of tokens, it returns a formatted string showing each token followed by the frequency count of the suffix starting with that token.

- `LIST(VARCHAR)`: The tokenized address.
- `BLOB`: The suffix trie generated by `build_suffix_trie`.
- `VARCHAR` (optional): A joiner string, which defaults to `' -> '`.

### `peel_end_tokens(LIST(VARCHAR), BLOB, [INTEGER steps], [INTEGER max_k]) → LIST(VARCHAR)`

Iteratively removes ("peels") tokens from the end of a list if they are statistically common suffixes according to the trie. The goal is to remove generic components like "LONDON UK" while preserving the more specific parts of the address.

- `LIST(VARCHAR)`: The tokenized address.
- `BLOB`: The suffix trie.
- `steps` (optional `INTEGER`, default 4): The maximum number of peeling iterations to perform.
- `max_k` (optional `INTEGER`, default 2): The maximum number of tokens to consider peeling in a single step.

The algorithm works by checking if a suffix (e.g., `['LONDON', 'UK']`) is less informative than the token preceding it (e.g., `['STREET']`). If so, it peels the suffix.

### `build_cleaned_address(LIST(VARCHAR), BLOB, INTEGER, [VARCHAR]) → VARCHAR`

A more direct approach to cleaning addresses. It finds the longest suffix whose frequency in the trie is at or above a given threshold and truncates the address there. It then joins the remaining tokens into a single string.

- `LIST(VARCHAR)`: The tokenized address.
- `BLOB`: The suffix trie.
- `INTEGER`: The `drop_above_count` threshold. The function will truncate the address at the first token of a suffix whose count is `>=` this value.
- `VARCHAR` (optional): A joiner string, which defaults to a space `' '`.

This function includes rules to prevent over-truncation, such as always keeping a minimum of three tokens if the original address is long enough.


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