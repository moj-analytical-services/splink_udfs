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
print("Generating  random  strings …")

sql = """
CREATE TABLE ten_mil AS (
    SELECT '' AS thing
    FROM range(10_000_000)
)
"""

con.execute(sql)

sql = """
CREATE TABLE words AS
WITH word_generation AS (
    SELECT
        -- Generate random word length between 3 and 30
        (3 + (random() * 28)::INT) AS word_length,
        -- Generate base word from random letters
        list_transform(
            range(3 + (random() * 28)::INT),
            x -> chr(97 + (random() * 26)::INT)
        ) AS base_letters,
        -- Generate random word length between 3 and 30
        (3 + (random() * 28)::INT) AS word_length,
        -- Generate base word from random letters
        list_transform(
            range(3 + (random() * 28)::INT),
            x -> chr(97 + (random() * 26)::INT)
        ) AS base_letter2,
        -- Random seed for mutation decisions
        random() AS mutation_seed
    FROM ten_mil
),
word_processing AS (
    SELECT
        base_letters,
        base_letter2,
        -- Create word1 by joining the base letters
        list_reduce(base_letters, (acc, x) -> acc || x) AS word1,

        -- Create word2 with mutations - guarantee non-empty result
        CASE
            -- 30% chance: keep word1 identical
            WHEN mutation_seed < 0.3 THEN base_letters

            -- 25% chance: random deletions with guaranteed minimum length
            WHEN mutation_seed < 0.55 THEN
                CASE
                    WHEN len(base_letters) <= 2 THEN base_letters  -- Keep short words intact
                    ELSE
                        -- Ensure we keep at least 2 characters by limiting deletion probability
                        CASE
                            WHEN len(list_filter(base_letters, (x, i) -> i = 1 OR random() > 0.3)) >= 1
                            THEN list_filter(base_letters, (x, i) -> i = 1 OR random() > 0.3)
                            ELSE [base_letters[1], base_letters[2]]  -- Fallback: keep first two chars
                        END
                END

            -- 25% chance: random substitutions (safe - same length)
            WHEN mutation_seed < 0.8 THEN
                list_transform(
                    base_letters,
                    x -> CASE
                        WHEN random() < 0.2 THEN chr(97 + (random() * 26)::INT)
                        ELSE x
                    END
                )

            -- 20% chance: combination with extra safety
            ELSE
                CASE
                    WHEN len(base_letters) <= 2 THEN
                        -- For short words, just do substitution
                        list_transform(
                            base_letters,
                            x -> CASE
                                WHEN random() < 0.3 THEN chr(97 + (random() * 26)::INT)
                                ELSE x
                            END
                        )
                    ELSE
                        -- For longer words, do deletion + substitution with safety
                        CASE
                            WHEN len(list_filter(base_letters, (x, i) -> i = 1 OR random() > 0.2)) >= 1
                            THEN list_transform(
                                list_filter(base_letters, (x, i) -> i = 1 OR random() > 0.2),
                                x -> CASE
                                    WHEN random() < 0.15 THEN chr(97 + (random() * 26)::INT)
                                    ELSE x
                                END
                            )
                            ELSE [base_letters[1]]  -- Ultimate fallback
                        END
                END
        END AS word2_letters
    FROM word_generation
)
SELECT
    word1,
    case when random() < 0.5 then
        list_reduce(word2_letters, (acc, x) -> acc || x)
    else
    list_reduce(base_letter2, (acc, x) -> acc || x)

    end
    as word2
FROM word_processing
"""

con.execute(sql)

print("Table created.\n")
con.sql("SELECT * from words").show()


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
con.sql(sql).df()


duckdb_elapsed = time.perf_counter() - t0
print(f"lev on 10 M rows: {duckdb_elapsed:,.2f} s")


sql = """
SELECT count(*) as c, lev(word1, word2, 1) AS distance
FROM words
GROUP BY distance
"""
con.sql(sql).df()


duckdb_elapsed = time.perf_counter() - t0
print(f"lev,1 on 10 M rows: {duckdb_elapsed:,.2f} s")


sql = """
SELECT count(*) as c, levenshtein(word1, word2) AS distance
FROM words
GROUP BY distance
"""
con.sql(sql).df()


duckdb_elapsed = time.perf_counter() - t0
print(f"levenshtein on 10 M rows: {duckdb_elapsed:,.2f} s")


# ───────────────────────────────────────────────────────────
# 3.  Time DuckDB’s soundex across the full column.
#     We calculate a cheap aggregate (sum of lengths) so the
#     work happens in DuckDB but only 1 scalar result crosses
#     the Python boundary.
# ───────────────────────────────────────────────────────────
print("-----")
t0 = time.perf_counter()

sql = """
SELECT count(*) as c, dlev(word1, word2) AS distance
FROM words
GROUP BY distance
"""
con.sql(sql).df()


duckdb_elapsed = time.perf_counter() - t0
print(f"dlev on 10 M rows: {duckdb_elapsed:,.2f} s")


sql = """
SELECT count(*) as c, dlev(word1, word2, 1) AS distance
FROM words
GROUP BY distance
"""
con.sql(sql).df()


duckdb_elapsed = time.perf_counter() - t0
print(f"dlev,1 on 10 M rows: {duckdb_elapsed:,.2f} s")


sql = """
SELECT count(*) as c, damerau_levenshtein(word1, word2) AS distance
FROM words
GROUP BY distance
"""
con.sql(sql).df()


duckdb_elapsed = time.perf_counter() - t0
print(f"DuckDB damerau_levenshtein on 10 M rows: {duckdb_elapsed:,.2f} s")
