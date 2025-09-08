import os
import time
import duckdb


EXT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "build/release/extension/splink_udfs/splink_udfs.duckdb_extension",
    )
)

# Path to OS addresses Parquet (as used in your examples)
OS_PARQUET_PATH = "/Users/robin.linacre/Documents/data_linking/uk_address_matcher/secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet"
EPC_GLOB = "/Users/robin.linacre/Documents/data_linking/uk_address_matcher/secret_data/epc/address_concat/*.parquet"


CLEAN_PIPELINE_SQL = """
.upper()
-- replace commas, periods, and apostrophes with spaces
.regexp_replace('[,.'']', ' ', 'g')
.regexp_replace('\\s+', ' ', 'g')
.trim()
.str_split(' ')
"""


def timed(con: duckdb.DuckDBPyConnection, sql: str, label: str) -> float:
    t0 = time.perf_counter()
    con.execute(sql)
    dt = time.perf_counter() - t0
    print(f"{label:<35s}: {dt:8.3f} s")
    return dt


def timed_select(con: duckdb.DuckDBPyConnection, sql: str, label: str) -> float:
    """Time a SELECT by forcing materialization into a small dataframe."""
    t0 = time.perf_counter()
    _ = con.sql(sql).df()
    dt = time.perf_counter() - t0
    print(f"{label:<35s}: {dt:8.3f} s")
    return dt


wall_start = time.perf_counter()

# ───────────────────────────────────────────────────────
# Init DuckDB + load extension
# ───────────────────────────────────────────────────────
con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})

con.load_extension(EXT_PATH)


sql = f"""
select
    unique_id ,
    address_concat{CLEAN_PIPELINE_SQL} AS tokens,
    postcode,
    LEFT(postcode, LENGTH(postcode)-1) AS postcode_group

from read_parquet('{EPC_GLOB}')
where postcode is not null
limit 100
"""
messy_addresses = con.sql(sql)
messy_addresses.create("messy_addresses")


sql = f"""
with pc_cleaned as (
select
uprn,
fulladdress.regexp_replace(postcode, '', 'gi').trim() as fulladdressnopc,
postcode
from read_parquet('{OS_PARQUET_PATH}')
where postcode in (select postcode from messy_addresses)
)
select
uprn,
fulladdressnopc{CLEAN_PIPELINE_SQL} AS tokens,
postcode,
LEFT(postcode, LENGTH(postcode)-1) AS postcode_group
from pc_cleaned
"""
os_addresses = con.sql(sql)
os_addresses.create("os_addresses")
con.table("os_addresses").show(max_width=10000, max_rows=5)

sql = """
SELECT postcode_group, build_suffix_trie(uprn, tokens) AS trie
FROM os_addresses
GROUP BY postcode_group;
"""
trie = con.sql(sql)
trie.create("trie")
con.table("trie").show(max_width=10000, max_rows=5)

sql = """
SELECT find_address_from_trie(m.tokens, r.trie, TRUE, 1) AS uprn,
m.tokens

FROM messy_addresses m
JOIN trie r USING (postcode_group)

"""
found = con.sql(sql)
found.create("found")
con.table("found").show(max_width=10000, max_rows=5)


sql = """
select
    array_to_string(f.tokens, ' ') as addr_found,
    o.uprn,
    array_to_string(o.tokens, ' ') as add_os,

from found f
left join os_addresses o  on o.uprn = f.uprn
where array_to_string(f.tokens, ' ') != array_to_string(o.tokens, ' ')

"""
comparison = con.sql(sql)
comparison.create("comparison")
con.table("comparison").show(max_width=10000, max_rows=5)
