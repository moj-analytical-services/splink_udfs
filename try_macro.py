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

import pandas as pd

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

con.sql("select len(double_metaphone(NULL)) as a").show(max_width=10000)


df = pd.DataFrame(
    [
        {"unique_id": "A", "original_address_concat_l": "123 MAIN STREET"},
        {"unique_id": "A", "original_address_concat_l": "MAIN STREET 456"},
        {"unique_id": "B", "original_address_concat_l": "FOO BAR BAZ"},
    ]
)
con.register("input_data", df)

# 2) Tokenise into arrays of words
tokenised = con.sql("""

    SELECT
      unique_id,
      regexp_split_to_array(upper(original_address_concat_l), '\\s+') AS tokens
    FROM input_data;
""")

tokenised.show()


# ---------------------------------------------------------------------
# 4) use the macro in one shot
# ---------------------------------------------------------------------
result = con.sql("""
    SELECT
      unique_id,
      tokens,
      histogram_using_within_group_counts(tokens, unique_id) AS hist_filtered_to_row
    FROM tokenised
    ORDER BY unique_id
""")
result
