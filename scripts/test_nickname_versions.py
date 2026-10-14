#!/usr/bin/env python3
"""Validate generation, then optionally exercise a test-only extension with two versions.

Generate the fixture header with --fixtures test/nicknames/versions.json, build with
-DSPLINK_NICKNAME_TEST_HEADER=/absolute/path/test-data.hpp, and pass --library/--extension.
Never distribute that test artifact.
"""
import argparse
import ast
import concurrent.futures
import ctypes as c
import json
import tempfile
import sys
from pathlib import Path
import generate_nicknames as g
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "test/nicknames"))
from duckdb_capi import Database, Result, literal


def generator_checks():
    # Validate exact C++ escaping, including adjacent digits after octal escapes.
    for data in (bytes(range(256)), b'quote" backslash\\ null\x00012 trigraph??/', 'António אברהם'.encode('utf8')):
        assert ast.literal_eval('b' + g.cpp_string_literal(data)) == data
    manifest = ROOT / 'test/nicknames/versions.json'
    versions, states = g.load_versions(manifest, synthetic=True)
    assert states['fixture_a'] == {('alice', 'ally'), ('robert', 'bob'), ('robert', 'rob')}
    assert states['fixture_b'] == {('carol', 'caz'), ('robert', 'bobby'), ('robert', 'rob')}
    assert g.generate(manifest) == g.generate(manifest)
    assert 'fixture_a' not in g.generate()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)
        for source in manifest.parent.iterdir():
            if source.is_file():
                (path / source.name).write_bytes(source.read_bytes())
        config = json.loads((path / 'versions.json').read_text())
        for version in config['versions']:
            version['behavior'] = str(ROOT / 'data/nicknames/behavior-v1.json')
        def write():
            (path / 'versions.json').write_text(json.dumps(config))
        def rejected():
            try:
                g.load_versions(path / 'versions.json', synthetic=True)
            except ValueError:
                return
            raise AssertionError('Invalid immutable data accepted')
        write()
        config['versions'][0]['mapping_sha256'] = '0' * 64
        write(); rejected()
        config['versions'][0]['mapping_sha256'] = versions[0]['mapping_sha256']
        write()
        (path / 'fixture-b-remove.csv').write_bytes(g.mapping_bytes({('absent', 'missing')}))
        rejected()
        (path / 'fixture-b-remove.csv').write_bytes((manifest.parent / 'fixture-b-remove.csv').read_bytes())
        (path / 'fixture-b-add.csv').write_bytes(g.mapping_bytes({('robert', 'rob')}))
        rejected()
        (path / 'fixture-b-add.csv').write_bytes((manifest.parent / 'fixture-b-add.csv').read_bytes())
        config['versions'][0]['behavior_sha256'] = '0' * 64
        write(); rejected()
    # Import a new version only in a temporary repository; the published manifest stays v1.
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)
        for source in (ROOT / 'data/nicknames').iterdir():
            (path / source.name).write_bytes(source.read_bytes())
        before = {source.name: source.read_bytes() for source in path.iterdir()}
        production = g.load_versions(path / 'versions.json')[1]['v1']
        changed = set(production)
        changed.remove(('robert', 'bob'))
        changed.add(('newperson', 'newnick'))
        import csv, argparse
        upstream = path / 'external.csv'
        with upstream.open('w', newline='', encoding='utf8') as handle:
            writer = csv.writer(handle)
            writer.writerow(['base_given_name', 'nickname'])
            writer.writerows(sorted(changed))
        old_data = g.DATA
        try:
            g.DATA = path
            arguments = argparse.Namespace(version='v99999', parent='v1', commit='1' * 40, import_upstream=upstream)
            g.import_upstream(arguments)
            versions, states = g.load_versions(path / 'versions.json')
            assert states['v1'] == production and states['v99999'] == changed
            assert g.read_pairs(path / 'v99999-add.csv') == {('newperson', 'newnick')}
            assert g.read_pairs(path / 'v99999-remove.csv') == {('robert', 'bob')}
            for name, data in before.items():
                if name != 'versions.json':
                    assert (path / name).read_bytes() == data
            assert json.loads((path / 'versions.json').read_text())['versions'][0] == json.loads(before['versions.json'])['versions'][0]
            try:
                g.import_upstream(arguments)
            except ValueError:
                pass
            else:
                raise AssertionError('Published version overwritten')
        finally:
            g.DATA = old_data
    print('Generation: deterministic; production excludes fixtures; checksum and invalid delta rejection passed')


def runtime_checks(library, extension):
    a, b = Database(library.resolve()), Database(library.resolve())
    try:
        for db in (a, b):
            db.query('LOAD ' + literal(extension.resolve()))
        # The test build's one-entry cache evicts A when B binds. Prepared queries
        # retain A's shared pointer, and both versions execute concurrently.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            tasks = [pool.submit(db.query, f"SELECT count(*) FROM range(10000) WHERE len(_get_nicknames(range::VARCHAR, '{version}'))=0", True) for db, version in ((a, 'fixture_a'), (b, 'fixture_b'))]
            for task in tasks:
                assert task.result()[2] == [['10000']]
        a.query("PREPARE lookup_a AS SELECT count(*) FROM range(10000) WHERE _get_nicknames(CASE WHEN range%2=0 THEN 'Robert' ELSE '  ROBERT  ' END, 'fixture_a') = ['bob','rob']")
        b.query("PREPARE lookup_b AS SELECT count(*) FROM range(10000) WHERE _get_nicknames(CASE WHEN range%2=0 THEN 'Robert' ELSE '  ROBERT  ' END, 'fixture_b') = ['bobby','rob']")
        def run(db, statement):
            for _ in range(50):
                assert db.query(statement, True)[2] == [['10000']]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            tasks = [pool.submit(run, a, 'EXECUTE lookup_a'), pool.submit(run, b, 'EXECUTE lookup_b')]
            for task in tasks:
                task.result()
        expected = [['[ally]', '[]', '[]', '[caz]', '[bob, rob]', '[bobby, rob]']]
        assert a.query("SELECT _get_nicknames('Alice','fixture_a')::VARCHAR, _get_nicknames('Alice','fixture_b')::VARCHAR, _get_nicknames('Carol','fixture_a')::VARCHAR, _get_nicknames('Carol','fixture_b')::VARCHAR, _get_nicknames('Robert','fixture_a')::VARCHAR, _get_nicknames('Robert','fixture_b')::VARCHAR", True)[2] == expected
        # Retain a materialized list result, evict its lookup, then read the result.
        result = Result()
        try:
            assert not a.lib.duckdb_query(a.connection, b"SELECT _get_nicknames(name,'fixture_a') FROM (VALUES ('Alice'),('Robert')) t(name)", c.byref(result))
            b.query("SELECT _get_nicknames('Robert','fixture_b')")
            signatures = {
                'duckdb_result_get_chunk': (c.c_void_p, [Result, c.c_uint64]),
                'duckdb_data_chunk_get_vector': (c.c_void_p, [c.c_void_p, c.c_uint64]),
                'duckdb_list_vector_get_child': (c.c_void_p, [c.c_void_p]),
                'duckdb_vector_get_data': (c.c_void_p, [c.c_void_p]),
                'duckdb_destroy_data_chunk': (None, [c.POINTER(c.c_void_p)]),
            }
            for name, (restype, argtypes) in signatures.items():
                function = getattr(a.lib, name)
                function.restype, function.argtypes = restype, argtypes
            chunk = c.c_void_p(a.lib.duckdb_result_get_chunk(result, 0))
            try:
                vector = a.lib.duckdb_data_chunk_get_vector(chunk, 0)
                entries = c.cast(a.lib.duckdb_vector_get_data(vector), c.POINTER(c.c_uint64))
                offset, length = entries[0], entries[1]
                assert length == 1
                child = a.lib.duckdb_list_vector_get_child(vector)
                # duckdb_string_t: uint32 length followed by 12 inline bytes.
                data = a.lib.duckdb_vector_get_data(child) + offset * 16
                assert c.c_uint32.from_address(data).value == 4
                assert c.string_at(data + 4, 4) == b'ally'
            finally:
                a.lib.duckdb_destroy_data_chunk(c.byref(chunk))
        finally:
            a.lib.duckdb_destroy_result(c.byref(result))
        for _ in range(10):
            assert a.query("SELECT _get_nicknames('Alice','fixture_a')::VARCHAR,_get_nicknames('Alice','fixture_b')::VARCHAR", True)[2] == [['[ally]', '[]']]
        print('Runtime: additions/removals, old results, simultaneous prepared queries, eviction/reconstruction and retained result ownership passed')
    finally:
        a.close(); b.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--library', type=Path)
    parser.add_argument('--extension', type=Path)
    args = parser.parse_args()
    generator_checks()
    if args.library and args.extension:
        runtime_checks(args.library, args.extension)
