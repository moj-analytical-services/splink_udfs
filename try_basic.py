#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "duckdb",
#     "jellyfish",
# ]
# ///

import os
import duckdb
import jellyfish

# ───────────────────────────────────────────────────────────
# 1.  Locate and load the custom DuckDB soundex extension
# ───────────────────────────────────────────────────────────
ext_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "build/release/extension/splink_udfs/splink_udfs.duckdb_extension",
    )
)

con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
con.load_extension(ext_path)

con.sql("select soundex('Robin') as s").show(max_width=10000)


def compare_soundex(input_str):
    """Compare DuckDB and Jellyfish soundex results for a given input."""
    # Get DuckDB soundex result
    duckdb_result = con.sql(f"select soundex('{input_str}') as s").fetchone()[0]

    # Get Jellyfish soundex result
    jellyfish_result = jellyfish.soundex(input_str)

    # Check if results match
    match = duckdb_result == jellyfish_result

    # Print formatted message
    status = "🟢" if match else "🔴"
    print(
        f"Input: '{input_str}' | DuckDB: {duckdb_result} | Jellyfish: {jellyfish_result} | Match: {status}"
    )


# Test cases
compare_soundex("Robert")

sql = """
select strip_diacritics('Café') as stripped
"""
con.sql(sql).show(max_width=10000)

sql = """
select strip_diacritics('ßærøskøbing éëôÉ') as stripped
"""
con.sql(sql).show(max_width=10000)
