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

sql = """
  WITH t AS (SELECT build_suffix_trie(uprn, toks) AS trie FROM (
    VALUES
      (1, ['10','HIGH','STREET','LONDON']),
      (2, ['12','HIGH','STREET','LONDON']),
      (3, ['10','HIGH','ROAD','LONDON'])
  ) canon(uprn, toks))
  SELECT
    format_address_with_term(['10','HIGH','STREET','LONDON'], trie, ' -> ') AS dbg_exact,
    format_address_with_term(['120','HIGH','STREET','LONDON'], trie, ' -> ') AS dbg_partial
  FROM t;

"""
con.sql(sql).show(max_width=10000)


rows = [
    ("LOVE LANE KINGS LANGLEY", "WD4 9HW"),
    ("ANNEX 7 LOVE LANE KINGS LANGLEY", "WD4 9HW"),
    ("1 LOVE LANE KINGS LANGLEY", "WD4 9HW"),
    ("2 LOVE LANE KINGS LANGLEY", "WD4 9HW"),
    ("3 LOVE LANE KINGS LANGLEY", "WD4 9HW"),
    ("4 LOVE LANE KINGS LANGLEY", "WD4 9HW"),
    ("5 LOVE LANE KINGS LANGLEY", "WD4 9HW"),
    ("6 LOVE LANE KINGS LANGLEY UNITED KINGDOM", "WD4 9HW"),
    ("7 LOVE LANE KINGS LANGLEY", "WD4 9HW"),
    ("MY LONG BUSINESS NAME 9 LOVE LANE KINGS LANGLEY", "WD4 9HW"),
]


def to_tokens(address: str):
    # simple tokenization: uppercase, split on whitespace
    return [tok for tok in address.upper().split() if tok]


def sql_array_literal(tokens):
    # tokens are plain strings (no embedded single quotes expected here)
    esc = [t.replace("'", "''") for t in tokens]
    return "['" + "', '".join(esc) + "']"


values_sql = []
for addr, pc in rows:
    arr = sql_array_literal(to_tokens(addr))
    values_sql.append(f"('{pc}', {arr})")


sql_create_t = "CREATE TEMPORARY TABLE t(postcode_group VARCHAR, tokens VARCHAR[]);"

con.sql(sql_create_t)

sql_insert = (
    "INSERT INTO t(postcode_group, tokens) VALUES\n  " + ",\n  ".join(values_sql) + ";"
)
print(sql_insert)
con.sql(sql_insert)


sql = """
WITH input_data AS (
  SELECT
    -- if you want a coarser group (e.g., drop last 2 chars), use LEFT():
    LEFT(postcode_group, LENGTH(postcode_group) - 2) AS postcode_group,
    tokens,
    row_number() OVER () AS uprn
  FROM t
),
raw AS (
  SELECT
    postcode_group,
    build_suffix_trie(uprn, tokens) AS raw_trie
  FROM input_data
  GROUP BY postcode_group
),
peeled AS (
  SELECT
    i.postcode_group,
    i.tokens,
    i.uprn,
    peel_end_tokens(i.tokens, r.raw_trie, 6, 6) AS tokens_peeled
  FROM input_data i
  JOIN raw r USING (postcode_group)
),
big AS (
  -- rebuild the trie on the peeled tokens (the “larger” trie)
  SELECT
    postcode_group,
    build_suffix_trie(uprn, tokens_peeled) AS big_trie
  FROM peeled
  GROUP BY postcode_group
)
SELECT
  p.postcode_group,
  p.tokens,
  p.tokens_peeled,
  build_cleaned_address(p.tokens_peeled, b.big_trie, 20) AS final_address,
  format_address_with_counts(p.tokens_peeled, b.big_trie, ' -> ') AS with_counts

FROM peeled p
JOIN big b USING (postcode_group);

"""
con.sql(sql).show(max_width=10000)


sql = """
WITH input_data AS (
  SELECT
    -- if you want a coarser group (e.g., drop last 2 chars), use LEFT():
    LEFT(postcode_group, LENGTH(postcode_group) - 2) AS postcode_group,
    tokens,
    row_number() OVER () AS uprn
  FROM t
),
raw AS (
  SELECT
    postcode_group,
    build_suffix_trie(uprn, tokens) AS raw_trie
  FROM input_data
  GROUP BY postcode_group
),
peeled AS (
  SELECT
    i.postcode_group,
    i.tokens,
    i.uprn,
    peel_end_tokens(i.tokens, r.raw_trie, 6, 6) AS tokens_peeled
  FROM input_data i
  JOIN raw r USING (postcode_group)
),
big AS (
  -- rebuild the trie on the peeled tokens (the “larger” trie)
  SELECT
    postcode_group,
    build_suffix_trie(uprn, tokens_peeled) AS big_trie
  FROM peeled
  GROUP BY postcode_group
)
select big_trie as trie from big
"""
df = con.sql(sql).df()
df


# Get the BLOB column as bytes
blob = df["trie"].iloc[0]
blob = bytes(blob)


# 1) Define (or import) the parser once
def parse_trie(data: bytes):
    off = 0

    def u32():
        nonlocal off
        v = int.from_bytes(data[off : off + 4], "little")
        off += 4
        return v

    def rbytes(n):
        nonlocal off
        b = data[off : off + n]
        off += n
        return b

    # header
    if rbytes(4) != b"QCK2":
        raise ValueError("Bad magic (expected QCK2)")
    flags = data[off]
    off += 1

    def u64():
        nonlocal off
        v = int.from_bytes(data[off : off + 8], "little")
        off += 8
        return v

    def node():
        cnt = u32()
        term = u32()
        uprn = u64()
        nchild = u32()
        children = []
        for _ in range(nchild):
            L = u32()
            tok = rbytes(L).decode("utf-8")
            children.append((tok, node()))
        return {"cnt": cnt, "term": term, "uprn": uprn, "children": children}

    return node()


# 2) Get the blob and coerce to bytes (handles memoryview/bytearray too)
blob = df["trie"].iloc[0]
blob = bytes(blob)

# 3) Parse, then pretty-print
trie = parse_trie(blob)


def print_trie_pretty(
    trie,
    *,
    collapse_chains=False,  # collapse single-child runs: A/B/C
    sep=" →  ",
    show_pct=True,  # show % of parent count
    max_depth=None,  # limit total printed edges from root
    min_count=1,  # hide subtrees with count < min_count
    max_children=None,  # show only top-N children by count per node
):
    def fmt_label(tok, node, parent_cnt):
        cnt = node.get("cnt", 0)
        term = node.get("term", 0)
        uprn = node.get("uprn", 0)
        if show_pct and parent_cnt:
            pct = 100.0 * cnt / parent_cnt if parent_cnt else 0.0
            # Keep simple; include pct at end if desired in future
        # Print all metadata in requested order: uprn, count, term
        return f"{tok} [uprn={uprn} cnt={cnt} term={term}]"

    def sorted_children(node):
        kids = [(tok, ch) for tok, ch in node["children"] if ch["cnt"] >= min_count]
        kids.sort(key=lambda kv: kv[1]["cnt"], reverse=True)
        extra = 0
        if max_children is not None and len(kids) > max_children:
            extra = len(kids) - max_children
            kids = kids[:max_children]
        return kids, extra

    def compress_chain(tok, node, remaining_depth):
        """Return (collapsed_token, tail_node, depth_used)."""
        if not collapse_chains:
            return tok, node, 1
        parts = [tok]
        n = node
        used = 1
        # Respect max_depth if provided (don’t overshoot)
        while len(n["children"]) == 1 and (max_depth is None or used < remaining_depth):
            t2, n2 = n["children"][0]
            parts.append(t2)
            n = n2
            used += 1
        return sep.join(parts), n, used

    def print_children(node, prefix, depth_used):
        # Stop if depth budget is exhausted
        if max_depth is not None and depth_used >= max_depth:
            if node["children"]:
                print(prefix + "┆ …")
            return

        kids, extra = sorted_children(node)
        total = len(kids)

        for i, (tok, child) in enumerate(kids):
            last = i == total - 1 and extra == 0
            branch = "└── " if last else "├── "
            child_prefix = prefix + ("    " if last else "│   ")

            # Chain compression (respect remaining depth budget)
            remaining = max_depth - depth_used if max_depth is not None else 10**9
            ctok, tail, used = compress_chain(tok, child, remaining)

            # Print the (possibly collapsed) child label
            print(prefix + branch + fmt_label(ctok, tail, node.get("cnt", 0)))

            # Recurse into the tail’s children
            print_children(tail, child_prefix, depth_used + used)

        if extra:
            # indicate there were more siblings omitted
            print(prefix + "└── " + f"… {extra} more")

    # Root
    print(fmt_label("root", trie, 0))
    print_children(trie, prefix="", depth_used=0)


print_trie_pretty(trie)
