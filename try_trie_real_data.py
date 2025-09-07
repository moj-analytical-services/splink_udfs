import os
import duckdb
import pandas as pd

# ───────────────────────────────────────────────────────────
# Paths (adjust if your local layout differs)
# ───────────────────────────────────────────────────────────
OS_PARQUET = "/Users/robin.linacre/Documents/data_linking/uk_address_matcher/secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet"
FHRS_PATH = "/Users/robin.linacre/Documents/data_linking/uk_address_matcher/example_data/fhrs_addresses_sample.parquet"


# Peel & clean params
PEEL_STEPS = 4
PEEL_MAX_K = 3
DROP_ABOVE = 40  # e.g. 15 drops BLACKPOOL in your example, 10 drops BELA GROVE BLACKPOOL, 40 is stricter
STRIP_REDUNDANT_COUNT_ONE_TOKENS = "True"
PC_TRIM = 1
# ───────────────────────────────────────────────────────────
# Load your extension
# ───────────────────────────────────────────────────────────
ext_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "build/release/extension/splink_udfs/splink_udfs.duckdb_extension",
    )
)

con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
con.load_extension(ext_path)

con.sql(f"select * from read_parquet('{FHRS_PATH}') limit 3").show()


# ───────────────────────────────────────────────────────────
# Pick a random postcode *prefix* (drop last char) from fhrs
# ───────────────────────────────────────────────────────────
pc_sample = con.sql(f"""
SELECT LEFT(postcode, LENGTH(postcode)-{PC_TRIM}) AS pc
FROM read_parquet('{FHRS_PATH}')
WHERE postcode IS NOT NULL AND LENGTH(TRIM(postcode)) > 0
ORDER BY random()
LIMIT 1
""").fetchone()[0]

print(f"Sampled postcode prefix: {pc_sample}")

# ───────────────────────────────────────────────────────────
# Build the dataset (OS + fhrs), normalize address text, tokenize
# ───────────────────────────────────────────────────────────
# Notes:
# - OS: remove the postcode from the address string
# - Both: remove 'FLAT', punctuation→space, collapse whitespace, trim, then UPPER + split to tokens
# - postcode_group = LEFT(postcode, LENGTH(postcode)-2) to coarsen grouping
# ───────────────────────────────────────────────────────────
sql_rows = f"""
WITH fhrs_raw AS (
  SELECT
    'fhrs-' || CAST(unique_id AS VARCHAR) AS row_id,
    postcode,
    -- normalize string but DO NOT remove postcode (fhrs address_concat usually doesn't duplicate it)
    address_concat
      .regexp_replace('FLAT', '', 'g')
      .regexp_replace('[,.]', ' ', 'g')
      .regexp_replace('\\s+', ' ', 'g')
      .trim()                                       AS addr_norm
  FROM read_parquet('{FHRS_PATH}')
  WHERE LEFT(postcode, LENGTH(postcode)-{PC_TRIM}) = '{pc_sample}'
),
os_raw AS (
  SELECT
    'OS-' || CAST(uprn AS VARCHAR) AS row_id,
    postcode,
    -- normalize and remove postcode from the OS address string
    fulladdress
      .regexp_replace('FLAT', '', 'g')
      .regexp_replace('[,.]', ' ', 'g')
      .regexp_replace('\\s+', ' ', 'g')
      .trim()
      .regexp_replace(postcode, '', 'gi')
      .trim()                                       AS addr_norm
  FROM read_parquet('{OS_PARQUET}')
  WHERE LEFT(postcode, LENGTH(postcode)-{PC_TRIM}) = '{pc_sample}'
),
all_rows AS (
  SELECT * FROM fhrs_raw
  UNION ALL
  SELECT * FROM os_raw
),
tokenised AS (
  SELECT
    row_id,
    postcode,
    LEFT(postcode, LENGTH(postcode)-{PC_TRIM}) AS postcode_group,
    addr_norm,
    -- UPPER then split on single spaces (we already collapsed whitespace)
    regexp_split_to_array(UPPER(addr_norm), ' ')    AS tokens
  FROM all_rows
)
SELECT * FROM tokenised;
"""

rows_df = con.sql(sql_rows).df()
print(f"Loaded rows: {len(rows_df)}")

# ───────────────────────────────────────────────────────────
# Register this intermediate as a view and run the full UDF pipeline in SQL
# raw -> peeled -> big -> final
# ───────────────────────────────────────────────────────────
con.register("tokenised", rows_df)

sql_pipeline = f"""
WITH input_data AS (
  SELECT t.*, row_number() OVER () AS uprn
  FROM tokenised t
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
    i.row_id,
    i.postcode,
    i.postcode_group,
    i.addr_norm,
    i.tokens,
    i.uprn,
    peel_end_tokens(i.tokens, r.raw_trie, {PEEL_STEPS}, {PEEL_MAX_K}) AS tokens_peeled
  FROM input_data i
  JOIN raw r USING (postcode_group)
),
big AS (
  SELECT
    postcode_group,
    build_suffix_trie(uprn, tokens_peeled) AS big_trie
  FROM peeled
  GROUP BY postcode_group
),
final AS (
  SELECT
    p.row_id,
    p.postcode,
    p.postcode_group,
    p.addr_norm,
    p.tokens,
    p.tokens_peeled,
    -- final cleaned string: drop the LONGEST tail with count >= DROP_ABOVE
    build_cleaned_address(p.tokens_peeled, b.big_trie, {DROP_ABOVE}) AS final_address,
    format_address_with_counts(p.tokens_peeled, b.big_trie, ' -> ') AS with_counts
  FROM peeled p
  JOIN big b USING (postcode_group)
)
SELECT * FROM final
ORDER BY postcode, row_id;
"""

con.sql(sql_pipeline).show(max_width=10000, max_rows=50)

con.sql(sql_pipeline).filter("row_id LIKE 'fhrs-%'").show(max_width=10000, max_rows=20)

final_df = con.sql(sql_pipeline).df()
final_df


# Build a friendlier DF with ORIGINAL/PEELED/AFTER_COMMON_ELIM columns like your Python reference
def join_tokens(col):
    return [" ".join(x) if isinstance(x, list) else x for x in col]


df = pd.DataFrame(
    {
        "row_id": final_df["row_id"],
        "POSTCODE": final_df["postcode"],
        "ORIGINAL": final_df["addr_norm"],
        "PEELED": join_tokens(final_df["tokens_peeled"]),
        "AFTER_COMMON_ELIM": final_df["final_address"],
    }
)


# fhrs↔OS match status by AFTER_COMMON_ELIM + POSTCODE
sql_match = """
WITH res AS (
  SELECT
    fhrs.row_id        AS fhrs_row_id,
    fhrs.ORIGINAL      AS fhrs_original,
    os.row_id         AS os_row_id,
    os.ORIGINAL       AS os_original,
    fhrs.AFTER_COMMON_ELIM,
    fhrs.POSTCODE,
    CASE
      WHEN os.row_id IS NULL THEN 'no match'
      WHEN fhrs.ORIGINAL = os.ORIGINAL THEN 'exact match'
      ELSE 'trimmed match'
    END AS match_status
  FROM df fhrs
  LEFT JOIN df os
    ON  fhrs.AFTER_COMMON_ELIM = os.AFTER_COMMON_ELIM
    AND fhrs.POSTCODE = os.POSTCODE
    AND os.row_id LIKE 'OS-%'
  WHERE fhrs.row_id LIKE 'fhrs-%'
)
SELECT * FROM res;
"""
res_df = con.sql(sql_match).df()

# Quick summary
con.register("res", res_df)
print("\n--- Match breakdown ---")
con.sql("""
WITH counts AS (
  SELECT match_status, COUNT(*) AS cnt
  FROM res
  GROUP BY match_status
),
total AS (SELECT SUM(cnt) AS total FROM counts)
SELECT
  c.match_status,
  c.cnt,
  (ROUND(100.0 * c.cnt / t.total, 2) || '%') AS pct
FROM counts c, total t
ORDER BY c.cnt DESC;
""").show(max_width=10000, max_rows=50)

# Peek some examples
print("\n--- Examples (trimmed + no match) ---")
con.sql("""
WITH t1 AS (
  SELECT * FROM res
  WHERE match_status = 'trimmed match'
  LIMIT 10
),
t2 AS (
  SELECT * FROM res
  WHERE match_status = 'no match'
  LIMIT 10
)
SELECT * FROM t1
UNION ALL
SELECT * FROM t2;
""").show(max_width=10000, max_rows=40)


def parse_qck2_trie(blob: bytes):
    """Parse QCK2 (magic 'QCK2', flags=0x00) into a simple nested dict with metadata."""
    off = 0

    def need(n):
        nonlocal off
        if off + n > len(blob):
            raise ValueError("Unexpected EOF while parsing")

    def u32():
        nonlocal off
        need(4)
        v = int.from_bytes(blob[off : off + 4], "little")
        off += 4
        return v

    def rbytes(n):
        nonlocal off
        need(n)
        b = blob[off : off + n]
        off += n
        return b

    # Header
    if rbytes(4) != b"QCK2":
        raise ValueError("Bad magic (expected 'QCK2')")
    flags = blob[off]
    off += 1
    if flags != 0:
        # The extension currently writes flags=0x00; treat other values as unsupported
        raise ValueError(f"Unsupported flags byte: {flags}")

    def u64():
        nonlocal off
        need(8)
        v = int.from_bytes(blob[off : off + 8], "little")
        off += 8
        return v

    # Node: u32 cnt, u32 term, u64 uprn, u32 nchildren, then children...
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

    root = node()
    # We ignore any trailing bytes on purpose; the writer currently emits none after the root.
    return root


def print_trie_pretty(trie, *, min_count=1, max_children=None):
    """ASCII render of the trie including metadata: uprn, cnt, term."""

    def fmt(tok, node):
        cnt = node.get("cnt", 0)
        term = node.get("term", 0)
        uprn = node.get("uprn", 0)
        return f"{tok} [uprn={uprn} cnt={cnt} term={term}]"

    def sorted_children(node):
        kids = [(tok, ch) for tok, ch in node["children"] if ch["cnt"] >= min_count]
        kids.sort(key=lambda kv: kv[1]["cnt"], reverse=True)
        extra = 0
        if max_children is not None and len(kids) > max_children:
            extra = len(kids) - max_children
            kids = kids[:max_children]
        return kids, extra

    counter = 1

    def walk(node, prefix=""):
        nonlocal counter
        kids, extra = sorted_children(node)
        for i, (tok, ch) in enumerate(kids):
            last = (i == len(kids) - 1) and (extra == 0)
            branch = "└── " if last else "├── "
            child_prefix = prefix + ("    " if last else "│   ")
            print(prefix + branch + fmt(tok, ch))
            counter += 1
            if counter > 30:
                return
            walk(ch, child_prefix)

        if extra:
            print(prefix + "└── " + f"… {extra} more")

    # Root line
    print(fmt("root", trie))
    walk(trie)


# ───────────────────────────────────────────────────────────
# Fetch a BLOB for the current sampled group and pretty-print
# ───────────────────────────────────────────────────────────
# Our pipeline used LEFT(postcode, LEN-2) as postcode_group,
# and pc_sample is LEFT(postcode, LEN-1). So group_key = pc_sample[:-1].
group_key = pc_sample

blob_row = con.sql(f"""
WITH input_data AS (
  SELECT t.*, row_number() OVER () AS uprn
  FROM tokenised t
),
raw AS (
  SELECT postcode_group, build_suffix_trie(uprn, tokens) AS raw_trie
  FROM input_data
  GROUP BY postcode_group
),
peeled AS (
  SELECT t.postcode_group,
         t.uprn,
         peel_end_tokens(t.tokens, r.raw_trie, {PEEL_STEPS}, {PEEL_MAX_K}) AS tokens_peeled
  FROM input_data t
  JOIN raw r USING (postcode_group)
),
big AS (
  SELECT postcode_group, build_suffix_trie(uprn, tokens_peeled) AS big_trie
  FROM peeled
  GROUP BY postcode_group
)
SELECT big_trie
FROM big
WHERE postcode_group = '{group_key}'
LIMIT 1
""").fetchone()

if blob_row is None:
    print("No big_trie found for group:", group_key)
else:
    blob_bytes = bytes(blob_row[0])
    trie_obj = parse_qck2_trie(blob_bytes)
    print("\n=== Pretty trie for group:", group_key, "===\n")
    # Adjust min_count / max_children to taste:
    print_trie_pretty(trie_obj, min_count=1, max_children=None)
