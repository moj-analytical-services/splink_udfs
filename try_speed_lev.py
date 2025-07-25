#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "duckdb",
#     "jellyfish",
# ]
# ///

import os
import time
import duckdb


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


# ───────────────────────────────────────────────────────────
# 2.  Build a 10-million-row table of random 5-letter words
#     (all a-z).  Everything happens inside DuckDB, so it’s
#     highly parallel and very fast.
# ───────────────────────────────────────────────────────────
print("Generating 10 000 000 random 5-letter strings …")

long_string = "chr(97 + (random() * 26)::INT)," * 25 + "chr(97 + (random() * 26)::INT)"

con.execute(
    f"""
    CREATE TABLE words AS
    SELECT
        concat(
            {long_string}
        ) AS word1,
        concat(
            {long_string}
        ) AS word2
    FROM range(10_000_000);
    """
)
print("Table created.\n")


# ───────────────────────────────────────────────────────────
# 3.  Time DuckDB’s soundex across the full column.
#     We calculate a cheap aggregate (sum of lengths) so the
#     work happens in DuckDB but only 1 scalar result crosses
#     the Python boundary.
# ───────────────────────────────────────────────────────────
t0 = time.perf_counter()

sql = """
SELECT count(*) as c, lev(word1, word2) AS distance
FROM words
GROUP BY distance
"""
con.sql(sql).show()
print("Counts of soundex by first letter:")

duckdb_elapsed = time.perf_counter() - t0
print(f"DuckDB lev on 10 M rows: {duckdb_elapsed:,.2f} s")


sql = """
SELECT count(*) as c, lev(word1, word2, 1) AS distance
FROM words
GROUP BY distance
"""
con.sql(sql).show()
print("Counts of lev by first letter:")

duckdb_elapsed = time.perf_counter() - t0
print(f"DuckDB lev on 10 M rows: {duckdb_elapsed:,.2f} s")


sql = """
SELECT count(*) as c, levenshtein(word1, word2) AS distance
FROM words
GROUP BY distance
"""
con.sql(sql).show()
print("Counts of lev by first letter:")

duckdb_elapsed = time.perf_counter() - t0
print(f"DuckDB lev on 10 M rows: {duckdb_elapsed:,.2f} s")


# ───────────────────────────────────────────────────────────
# 3.  Time DuckDB’s soundex across the full column.
#     We calculate a cheap aggregate (sum of lengths) so the
#     work happens in DuckDB but only 1 scalar result crosses
#     the Python boundary.
# ───────────────────────────────────────────────────────────
t0 = time.perf_counter()

sql = """
SELECT count(*) as c, dlev(word1, word2) AS distance
FROM words
GROUP BY distance
"""
con.sql(sql).show()
print("Counts of dlev by first letter:")

duckdb_elapsed = time.perf_counter() - t0
print(f"DuckDB dlev on 10 M rows: {duckdb_elapsed:,.2f} s")


sql = """
SELECT count(*) as c, dlev(word1, word2, 1) AS distance
FROM words
GROUP BY distance
"""
con.sql(sql).show()
print("Counts of dlev by first letter:")

duckdb_elapsed = time.perf_counter() - t0
print(f"DuckDB dlev on 10 M rows: {duckdb_elapsed:,.2f} s")


sql = """
SELECT count(*) as c, damerau_levenshtein(word1, word2) AS distance
FROM words
GROUP BY distance
"""
con.sql(sql).show()
print("Counts of dlev by first letter:")

duckdb_elapsed = time.perf_counter() - t0
print(f"DuckDB dlev on 10 M rows: {duckdb_elapsed:,.2f} s")
