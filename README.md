# Splink UDFs Extension for DuckDB

The `splink_udfs` extension is work in progress. It aims to offer a variety of functions that are useful for the purpose of data linkage, including support for the [Soundex](https://en.wikipedia.org/wiki/Soundex) algorithm, diacritic stripping, and text transliteration. This allows similarity matching of names and other words based on how they sound, rather than how they are spelled.

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

### `strip_diacritics(VARCHAR) → VARCHAR`

Removes diacritical marks from a string using Unicode normalization. For example, `Jürgen` becomes `Jurgen`. This function does not transliterate distinct letters like `ø` or `ß`.

### `unaccent(VARCHAR) → VARCHAR`

Provides a more comprehensive transliteration of a string. It first strips all diacritics and then converts other special characters and ligatures (e.g., `Æ` → `AE`, `ø` → `o`, `ß` → `ss`) to their basic Latin equivalents.

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