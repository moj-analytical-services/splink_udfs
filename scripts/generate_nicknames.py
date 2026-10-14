#!/usr/bin/env python3
"""Validate immutable nickname snapshots and generate compact embedded data."""
import argparse
import csv
import hashlib
import io
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data/nicknames'
TARGET = ROOT / 'src/include/nicknames/nickname_data.hpp'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def mapping_bytes(pairs):
    stream = io.StringIO(newline='')
    writer = csv.writer(stream, lineterminator='\n')
    writer.writerow(['name', 'nickname'])
    writer.writerows(sorted(pairs, key=lambda pair: tuple(x.encode('utf8') for x in pair)))
    return stream.getvalue().encode('utf8')


def read_pairs(path):
    with path.open(encoding='utf8', newline='') as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != ['name', 'nickname']:
            raise ValueError(f'{path}: expected name,nickname columns')
        rows = [(r['name'], r['nickname']) for r in reader]
    pairs = set(rows)
    if len(pairs) != len(rows) or any(not a or not b or '\0' in a or '\0' in b for a, b in pairs):
        raise ValueError(f'{path}: duplicate or empty mappings')
    if path.read_bytes() != mapping_bytes(pairs):
        raise ValueError(f'{path}: noncanonical CSV bytes or ordering')
    return pairs


def normalize(text, behavior, nickname=False):
    spaces = set(behavior['space_codepoints'])
    start, end = 0, len(text)
    while start < end and ord(text[start]) in spaces:
        start += 1
    while end > start and ord(text[end - 1]) in spaces:
        end -= 1
    table = dict(behavior['nickname_lower_map' if nickname else 'case_map'])
    return ''.join(chr(table.get(ord(c), ord(c) + 32 if 'A' <= c <= 'Z' else ord(c))) for c in text[start:end])


def load_versions(path, synthetic=False):
    manifest = json.loads(path.read_text())
    if manifest['format'] != 1:
        raise ValueError('Unsupported nickname manifest format')
    versions, states = [], {}
    for item in manifest['versions']:
        version = dict(item)
        name = version['version']
        if name in states or (not synthetic and not re.fullmatch(r'v[1-9][0-9]*', name)):
            raise ValueError(f'Invalid or duplicate version: {name}')
        behavior_path = path.parent / version['behavior']
        behavior_bytes = behavior_path.read_bytes()
        if sha(behavior_bytes) != version['behavior_sha256']:
            raise ValueError(f'{name}: frozen behavior checksum mismatch')
        behavior = json.loads(behavior_bytes)
        supported = {
            'normalization': 'per-codepoint simple uppercase then lowercase; trim leading/trailing space separators; no accent stripping or NFC',
            'ordering': 'ascending UTF-8 bytes',
            'deduplication': 'unique normalized directional pairs',
            'null_input': 'NULL',
            'unknown_or_empty_input': 'empty VARCHAR list',
            'direction': 'name -> nickname only',
        }
        if any(behavior.get(key) != value for key, value in supported.items()):
            raise ValueError(f'{name}: unsupported behavior; implement a new runtime algorithm first')
        for key in ('case_map', 'nickname_lower_map'):
            table = behavior[key]
            if table != sorted(table) or len({a for a, _ in table}) != len(table):
                raise ValueError(f'{name}: invalid frozen character map')
        if behavior['space_codepoints'] != sorted(set(behavior['space_codepoints'])):
            raise ValueError('Invalid frozen space map')
        parent = version['parent']
        if parent is None:
            if versions and not synthetic:
                raise ValueError('Only the first published version can have a full baseline')
            additions = read_pairs(path.parent / version['baseline'])
            removals = set()
            state = set()
        else:
            if parent not in states:
                raise ValueError(f'{name}: parent must precede this version')
            additions = read_pairs(path.parent / version['additions'])
            removals = read_pairs(path.parent / version['removals'])
            state = set(states[parent])
            if not removals.issubset(state) or additions & state or additions & removals:
                raise ValueError(f'{name}: invalid additions/removals')
        for a, b in additions | removals:
            if normalize(a, behavior) != a or normalize(b, behavior, True) != b:
                raise ValueError(f'{name}: mappings do not match frozen normalization')
        state.difference_update(removals)
        state.update(additions)
        if any(normalize(a, behavior) != a or normalize(b, behavior, True) != b for a, b in state):
            raise ValueError(f'{name}: inherited mappings do not match frozen normalization')
        if sha(mapping_bytes(state)) != version['mapping_sha256'] or len(state) != version['mapping_count']:
            raise ValueError(f'{name}: immutable mapping checksum/count mismatch')
        if not synthetic and not re.fullmatch('[0-9a-f]{40}', version['upstream_commit']):
            raise ValueError(f'{name}: upstream commit must be an exact SHA')
        version.update(behavior_data=behavior, additions_data=additions, removals_data=removals)
        versions.append(version)
        states[name] = state
    return versions, states


def cpp_string_literal(data):
    """Readable ASCII with byte-exact escapes for UTF-8/control bytes on all compilers."""
    characters = []
    for byte in data:
        if byte == ord('"'):
            characters.append('\\"')
        elif byte == ord('\\'):
            characters.append('\\\\')
        elif 32 <= byte < 127 and byte != ord('?'):
            characters.append(chr(byte))
        else:
            # Exactly three octal digits: a following digit cannot extend the escape.
            characters.append('\\%03o' % byte)
    return '"' + ''.join(characters) + '"'


def wrapped_entries(entries, width=100):
    """Pack generated records without splitting literals or records across lines."""
    lines, line = [], '    '
    for entry in entries:
        if len(line) > 4 and len(line) + 1 + len(entry) > width:
            lines.append(line)
            line = '    '
        line += (' ' if len(line) > 4 else '') + entry
    if len(line) > 4:
        lines.append(line)
    return lines


def generate(fixtures=None):
    versions, _ = load_versions(DATA / 'versions.json')
    if fixtures:
        test_versions, _ = load_versions(fixtures, synthetic=True)
        versions += test_versions
    ids = {v['version']: i for i, v in enumerate(versions)}
    if len(ids) != len(versions):
        raise ValueError('Duplicate published/test version IDs')
    strings = sorted({s for v in versions for pair in v['additions_data'] | v['removals_data'] for s in pair}, key=lambda s: s.encode('utf8'))
    string_ids = {s: i for i, s in enumerate(strings)}
    pool, offsets = bytearray(), []
    for value in strings:
        offsets.append(len(pool))
        pool.extend(value.encode('utf8') + b'\0')
    if len(pool) >= 2**32 or len(strings) >= 2**32:
        raise ValueError('Nickname string pool exceeds uint32 capacity')
    behaviors = {}
    for v in versions:
        ident = v['behavior_data']['id']
        if ident in behaviors and behaviors[ident] != v['behavior_data']:
            raise ValueError('One behavior ID cannot describe different behavior')
        behaviors[ident] = v['behavior_data']
    lines = ['// Generated by scripts/generate_nicknames.py; immutable sources are in data/nicknames/.', '#pragma once', '', '#include "duckdb/common/typedefs.hpp"', '', 'namespace duckdb {', 'namespace nicknames {', '',
             'struct Relationship {', '\tuint32_t name;', '\tuint32_t nickname;', '};',
             'struct CharacterMapping {', '\tuint32_t source;', '\tuint32_t target;', '};',
             'struct FrozenBehavior {', '\tconst char *id;', '\tconst CharacterMapping *characters;', '\tidx_t character_count;', '\tconst uint32_t *spaces;', '\tidx_t space_count;', '};',
             'struct DatasetVersion {', '\tconst char *version;', '\tconst char *repository;', '\tconst char *commit;', '\tconst char *source_file;', '\tconst char *checksum;', '\tconst char *behavior_checksum;', '\tidx_t behavior;', '\tint32_t parent;', '\tidx_t add_offset;', '\tidx_t add_count;', '\tidx_t remove_offset;', '\tidx_t remove_count;', '\tidx_t mapping_count;', '};', '',
             '// Generated arrays are packed for review; the CSV and manifest are the sources.',
             '// Non-ASCII bytes are escaped so the pool does not depend on compiler source encoding.',
             '// clang-format off', 'static constexpr char STRING_POOL[] =']
    lines += wrapped_entries(cpp_string_literal(value.encode('utf8') + b'\0') for value in strings)
    lines[-1] += ';'
    lines += ['static constexpr uint32_t STRING_OFFSETS[] = {']
    lines += wrapped_entries(f'{offset},' for offset in offsets)
    lines += ['};']
    behavior_ids = {}
    for index, (ident, behavior) in enumerate(behaviors.items()):
        behavior_ids[ident] = index
        lines += [f'static constexpr CharacterMapping CASE_MAP_{index}[] = {{']
        lines += wrapped_entries(f'{{{a}, {b}}},' for a, b in behavior['case_map'])
        lines += ['};', f'static constexpr uint32_t SPACES_{index}[] = {{' + ', '.join(map(str, behavior['space_codepoints'])) + '};']
    quote = lambda s: json.dumps(s, ensure_ascii=True)
    lines += ['static constexpr FrozenBehavior BEHAVIORS[] = {']
    for ident, index in behavior_ids.items():
        behavior = behaviors[ident]
        lines.append(f'    {{{quote(ident)}, CASE_MAP_{index}, {len(behavior["case_map"])}, SPACES_{index}, {len(behavior["space_codepoints"])}}},')
    lines += ['};']
    additions, removals, descriptors = [], [], []
    for version in versions:
        a = sorted((string_ids[x], string_ids[y]) for x, y in version['additions_data'])
        r = sorted((string_ids[x], string_ids[y]) for x, y in version['removals_data'])
        parent = -1 if version['parent'] is None else ids[version['parent']]
        fields = [quote(version[k]) for k in ('version','upstream_repository','upstream_commit','source_file','mapping_sha256','behavior_sha256')]
        fields += list(map(str, [behavior_ids[version['behavior_data']['id']], parent, len(additions), len(a), len(removals), len(r), version['mapping_count']]))
        descriptors.append('    {' + ', '.join(fields) + '},')
        additions += a
        removals += r
    for label, records in [('ADDITIONS', additions), ('REMOVALS', removals)]:
        lines += [f'static constexpr Relationship {label}[] = {{']
        lines += wrapped_entries(f'{{{a}, {b}}},' for a, b in records) or ['    {0, 0}, // Unused sentinel for an empty action array.']
        lines += ['};']
    lines += ['static constexpr DatasetVersion VERSIONS[] = {'] + descriptors + ['};', '// clang-format on', '', '} // namespace nicknames', '} // namespace duckdb', '']
    return '\n'.join(lines)


def import_upstream(args):
    manifest_path = DATA / 'versions.json'
    manifest = json.loads(manifest_path.read_text())
    versions, states = load_versions(manifest_path)
    if not re.fullmatch(r'v[1-9][0-9]*', args.version or '') or args.version in states:
        raise ValueError('Specify a new, never-published --version')
    if args.parent not in states or not re.fullmatch('[0-9a-f]{40}', args.commit or ''):
        raise ValueError('Specify an existing --parent and exact upstream --commit')
    parent = next(v for v in versions if v['version'] == args.parent)
    behavior = parent['behavior_data']
    with args.import_upstream.open(encoding='utf8', newline='') as handle:
        pairs = {(normalize(r['base_given_name'], behavior), normalize(r['nickname'], behavior, True)) for r in csv.DictReader(handle)}
    if any(not a or not b or '\0' in a or '\0' in b for a, b in pairs):
        raise ValueError('Upstream contains empty normalized mappings')
    additions, removals = pairs - states[args.parent], states[args.parent] - pairs
    version = {k: parent[k] for k in ('behavior', 'behavior_sha256', 'upstream_repository', 'source_file')}
    version.update(version=args.version, parent=args.parent, additions=args.version+'-add.csv', removals=args.version+'-remove.csv', upstream_commit=args.commit, source_sha256=sha(args.import_upstream.read_bytes()), mapping_sha256=sha(mapping_bytes(pairs)), mapping_count=len(pairs))
    if any((DATA / version[k]).exists() for k in ('additions', 'removals')):
        raise ValueError('Delta files already exist; immutable files cannot be overwritten')
    for key, values in [('additions', additions), ('removals', removals)]:
        path = DATA / version[key]
        with path.open('xb') as handle:
            handle.write(mapping_bytes(values))
    manifest['versions'].append(version)
    manifest_path.write_text(json.dumps(manifest, indent=2)+'\n')
    print(f'{args.version}: {len(additions)} additions, {len(removals)} removals; {version["mapping_sha256"]}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--fixtures', type=Path, help='Test-only manifest; never used for the published header')
    parser.add_argument('--output', type=Path, default=TARGET)
    parser.add_argument('--import-upstream', type=Path, help='Read an upstream file from outside the vendored baseline')
    parser.add_argument('--version')
    parser.add_argument('--parent')
    parser.add_argument('--commit')
    args = parser.parse_args()
    if args.import_upstream:
        import_upstream(args)
    content = generate(args.fixtures)
    if args.check:
        if not args.output.exists() or args.output.read_text() != content:
            parser.exit(1, 'Generated nickname data is stale; run scripts/generate_nicknames.py\n')
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(content)


if __name__ == '__main__':
    main()
