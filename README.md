# Splink UDFs Extension for DuckDB

The `splink_udfs` extension is work in progress. It aims to offer a variety of function**Exception:**
The Double Metaphone implementation in `src/include/phonetic/double_metaphone.hpp` is a literal C++ translation of `org.apache.commons.codec.language.DoubleMetaphone` and is therefore licensed separately under the [Apache License 2.0](LICENSE_APACHE). See the top of the header file for the full Apache license notice, and the project's `NOTICE` file for attribution details.that are useful for the purpose of data linkage, including support for the [Soundex](https://en.wikipedia.org/wiki/Soundex) algorithm, [Double Metaphone](https://en.wikipedia.org/wiki/Metaphone#Double_Metaphone) phonetic encoding, diacritic stripping, and text transliteration. This allows similarity matching of names and other words based on how they sound, rather than how they are spelled.

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

### Example Usage

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
