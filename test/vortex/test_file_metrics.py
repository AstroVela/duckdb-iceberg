"""Validate Vortex manifest metrics and file pruning with the native shell."""

import argparse
import json
import math
from pathlib import Path
import struct
import tempfile
import unittest

from test_local_catalog import local_catalog, quote
from test_parquet_pushdown import run_sql


def file_metrics(shell, metadata):
    snapshot = next(s for s in metadata['snapshots'] if s['snapshot-id'] == metadata['current-snapshot-id'])
    manifests = run_sql(shell, f"SELECT manifest_path FROM read_avro({quote(snapshot['manifest-list'])});")
    files = []
    for manifest in manifests:
        files.extend(
            run_sql(
                shell,
                "SELECT data_file.file_path AS path, data_file.file_format AS format, "
                "data_file.record_count AS rows, data_file.null_value_counts AS nulls, "
                "json_extract(to_json(data_file), '$.value_counts') AS counts, "
                "json_extract(to_json(data_file), '$.nan_value_counts') AS nans, "
                "map_from_entries(list_transform(map_entries(data_file.lower_bounds), "
                "lambda e: {'key': e.key, 'value': hex(e.value)})) AS lower, "
                "map_from_entries(list_transform(map_entries(data_file.upper_bounds), "
                "lambda e: {'key': e.key, 'value': hex(e.value)})) AS upper "
                f"FROM read_avro({quote(manifest['manifest_path'])}) WHERE status != 2;",
            )
        )
    return files


class FileMetricsTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='iceberg-vortex-metrics-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def sql(self, sql):
        return run_sql(DUCKDB, sql)

    def export(self, name, query, format='vortex', setup=''):
        table = self.root / name
        self.sql(setup + f'COPY ({query}) TO {quote(table)} (FORMAT iceberg, DATA_FORMAT {format});')
        path = next((table / 'metadata').glob('*.metadata.json'))
        return path, json.loads(path.read_text())

    def check_bounds(self, file, field_id, dtype, values):
        key = str(field_id)
        self.assertEqual((file['counts'] or {}).get(key), len(values))
        self.assertEqual(file['nulls'].get(key), values.count(None))
        floating = dtype in ('FLOAT', 'DOUBLE')
        if floating:
            self.assertEqual((file['nans'] or {}).get(key), sum(v is not None and math.isnan(v) for v in values))
        else:
            self.assertNotIn(key, file['nans'] or {})
        valid = [v for v in values if v is not None and not (floating and math.isnan(v))]
        if not valid:
            self.assertNotIn(key, file['lower'])
            self.assertNotIn(key, file['upper'])
            return
        fmt = {'INTEGER': '<i', 'BIGINT': '<q', 'FLOAT': '<f', 'DOUBLE': '<d'}[dtype]
        low, high = min(valid), max(valid)
        if floating and low == 0 and any(math.copysign(1, v) < 0 for v in valid if v == 0):
            low = -0.0
        if floating and high == 0 and any(math.copysign(1, v) > 0 for v in valid if v == 0):
            high = 0.0
        self.assertEqual(file['lower'][key], struct.pack(fmt, low).hex().upper())
        self.assertEqual(file['upper'][key], struct.pack(fmt, high).hex().upper())

    def test_copy_bounds_and_counts(self):
        cases = {
            'INTEGER': [[-2147483648, 2147483647, None], [None, None]],
            'BIGINT': [[-9223372036854775808, 9223372036854775807, None], [None, None]],
            'FLOAT': [
                [-1.5, 0.0, float('nan'), float('nan'), None],
                [-0.0, 0.0],
                [float('-inf'), float('inf'), None],
                [float('nan'), None],
                [None, None],
                [1.401298464324817e-45],
            ],
            'DOUBLE': [
                [-1.5, 0.0, float('nan'), float('nan'), None],
                [-0.0, 0.0],
                [float('-inf'), float('inf'), None],
                [float('nan'), None],
                [None, None],
                [5e-324],
            ],
        }
        for dtype, groups in cases.items():
            for index, values in enumerate(groups):
                with self.subTest(dtype=dtype, index=index):
                    rows = ', '.join(f'({"NULL" if v is None else quote(v)}::{dtype})' for v in values)
                    _, metadata = self.export(
                        f'{dtype}-{index}',
                        f'''SELECT 'ignored' AS s, x AS "x.dot""name" FROM (VALUES {rows}) t(x)''',
                    )
                    field = metadata['schemas'][0]['fields'][1]
                    files = file_metrics(DUCKDB, metadata)
                    self.assertEqual(len(files), 1)
                    self.assertEqual(files[0]['rows'], len(values))
                    self.check_bounds(files[0], field['id'], dtype, values)
                    self.assertNotIn(str(metadata['schemas'][0]['fields'][0]['id']), files[0]['nulls'])

    def test_insert_metrics_modes(self):
        for mode in ('none', 'counts', 'full', 'truncate(16)'):
            with self.subTest(mode=mode):
                path, metadata = self.export(mode, 'SELECT 1::INTEGER id, -1::DOUBLE x')
                old = file_metrics(DUCKDB, metadata)
                with local_catalog(path) as (server, attach):
                    server.metadata['properties']['write.metadata.metrics.default'] = mode
                    server.metadata['properties']['write.metadata.metrics.column.id'] = 'none'
                    self.sql(attach + "INSERT INTO lake.main.items VALUES (2, 'NaN'), (3, 0), (4, NULL);")
                    files = file_metrics(DUCKDB, server.metadata)
                    self.assertEqual(next(f for f in files if f['path'] == old[0]['path']), old[0])
                    file = next(f for f in files if f['path'] != old[0]['path'])
                    ids = {f['name']: str(f['id']) for f in metadata['schemas'][0]['fields']}
                    for key in ('counts', 'nulls', 'nans', 'lower', 'upper'):
                        self.assertNotIn(ids['id'], file[key] or {})
                    if mode == 'none':
                        for key in ('counts', 'nulls', 'nans', 'lower', 'upper'):
                            self.assertFalse(file[key])
                    else:
                        self.assertEqual(file['counts'][ids['x']], 3)
                        self.assertEqual(file['nulls'][ids['x']], 1)
                        self.assertEqual(file['nans'][ids['x']], 1)
                        self.assertEqual(ids['x'] in file['lower'], mode != 'counts')
                        self.assertEqual(ids['x'] in file['upper'], mode != 'counts')
                    self.assertFalse(server.errors)

    def test_field_ids_and_column_override(self):
        path, metadata = self.export('field-ids', '''SELECT 0::INTEGER id, 0::FLOAT AS "x.dot""name" WHERE false''')
        # Model an empty imported table whose field IDs differ from column positions.
        for field, field_id in zip(metadata['schemas'][0]['fields'], (11, 37)):
            field['id'] = field_id
        metadata['last-column-id'] = 37
        metadata['snapshots'] = []
        metadata['snapshot-log'] = []
        metadata['refs'] = {}
        metadata.pop('current-snapshot-id', None)
        metadata['last-sequence-number'] = 0
        path.write_text(json.dumps(metadata))
        with local_catalog(path) as (server, attach):
            server.metadata['properties'].update(
                {
                    'write.metadata.metrics.default': 'none',
                    'write.metadata.metrics.column.id': 'counts',
                    'write.metadata.metrics.column.x.dot"name': 'full',
                }
            )
            self.sql(attach + "INSERT INTO lake.main.items VALUES (1, '-0.0'), (2, 'NaN'), (3, NULL);")
            files = file_metrics(DUCKDB, server.metadata)
            self.assertEqual(len(files), 1)
            file = files[0]
            self.assertEqual(file['counts'], {'11': 3, '37': 3})
            self.assertEqual(file['nulls'], {'11': 0, '37': 1})
            self.assertEqual(file['nans'], {'37': 1})
            self.assertNotIn('11', file['lower'])
            self.assertNotIn('11', file['upper'])
            self.check_bounds(file, 37, 'FLOAT', [-0.0, float('nan'), None])
            self.assertEqual(
                self.sql(attach + '''SELECT id FROM lake.main.items WHERE "x.dot""name" = 0;'''), [{'id': 1}]
            )
            self.assertFalse(server.errors)

    def test_pruning_and_mixed_snapshots(self):
        for dtype in ('FLOAT', 'DOUBLE'):
            for initial_format in ('parquet', 'vortex'):
                with self.subTest(dtype=dtype, initial_format=initial_format):
                    path, metadata = self.export(
                        f'{dtype}-{initial_format}', f'SELECT 1::INTEGER id, -1::{dtype} x', initial_format
                    )
                    old_snapshot = metadata['current-snapshot-id']
                    with local_catalog(path) as (server, attach):
                        for row in ("(2, 'NaN')", '(3, 100)', '(4, NULL)', "(5, '-0.0'), (6, '0.0')"):
                            self.sql(attach + f'INSERT INTO lake.main.items VALUES {row};')
                        source = f'iceberg_scan({quote(server.metadata_path)})'
                        for predicate, ids in (
                            ('x > 0', [2, 3]),
                            ('x = 0', [5, 6]),
                            ('x < 0', [1]),
                            ("x = 'NaN'", [2]),
                            ('x IS NULL', [4]),
                            ('x IS NOT NULL', [1, 2, 3, 5, 6]),
                        ):
                            self.assertEqual(
                                self.sql(f'SELECT id FROM {source} WHERE {predicate} ORDER BY id'),
                                [{'id': i} for i in ids],
                            )
                        # NaN-only and NULL-only files have no bounds; the existing
                        # comparison pruner keeps them conservatively.
                        for predicate, skipped in (
                            ('x = 100', 2),
                            ('x > 0', 2 if initial_format == 'vortex' else 1),
                        ):
                            result = self.sql(
                                "SET enable_logging=false; CALL enable_logging('Iceberg', storage='memory'); "
                                f'SELECT * FROM {source} WHERE {predicate}; '
                                "SELECT count(*) AS skipped FROM duckdb_logs() WHERE type='Iceberg' "
                                "AND contains(message, 'skipped ''data_file''');"
                            )
                            self.assertEqual(result[0]['skipped'], skipped, predicate)
                        old_source = f'iceberg_scan({quote(server.metadata_path)}, snapshot_from_id={old_snapshot})'
                        self.assertEqual(self.sql(f'SELECT id FROM {old_source} WHERE x < 0'), [{'id': 1}])
                        self.assertFalse(server.errors)

    def test_parallel_batches(self):
        query = (
            "SELECT i::BIGINT id, CASE WHEN i % 13=0 THEN NULL WHEN i % 7=0 THEN 'NaN'::DOUBLE "
            "ELSE i::DOUBLE END AS x FROM range(400000) t(i)"
        )
        path, metadata = self.export(
            'parallel',
            'SELECT * FROM source',
            setup='CREATE TABLE source AS ' + query + '; SET threads=4; ',
        )
        self.assertGreater(len(file_metrics(DUCKDB, metadata)), 1)
        with local_catalog(path) as (server, attach):
            self.sql(
                attach + 'CREATE TABLE source AS ' + query + '; '
                'SET threads=4; INSERT INTO lake.main.items SELECT * FROM source;'
            )
            fields = {f['name']: f['id'] for f in metadata['schemas'][0]['fields']}
            files = file_metrics(DUCKDB, server.metadata)
            self.assertGreater(len(files), 2)
            self.assertEqual(sum(f['rows'] for f in files), 800000)
            for file in files:
                x = '"__iceberg_vortex_v1_field_' + str(fields['x']) + '"'
                expected = self.sql(
                    f'SELECT count(*) FILTER (WHERE {x} IS NULL) AS n, '
                    f'count(*) FILTER (WHERE isnan({x})) AS nan, min({x}) AS low, '
                    f'max({x}) FILTER (WHERE NOT isnan({x})) AS high FROM read_vortex({quote(file["path"])});'
                )[0]
                key = str(fields['x'])
                self.assertEqual(file['counts'][key], file['rows'])
                self.assertEqual(file['nulls'][key], expected['n'])
                self.assertEqual(file['nans'][key], expected['nan'])
                self.assertEqual(file['lower'][key], struct.pack('<d', expected['low']).hex().upper())
                self.assertEqual(file['upper'][key], struct.pack('<d', expected['high']).hex().upper())
            self.assertFalse(server.errors)

    def test_filtered_constants_and_empty_copy(self):
        _, metadata = self.export(
            'filtered',
            'SELECT i::BIGINT id, -0.0::DOUBLE x, NULL::FLOAT n FROM range(10000) t(i) WHERE i % 3 = 1',
        )
        fields = {f['name']: f['id'] for f in metadata['schemas'][0]['fields']}
        file = file_metrics(DUCKDB, metadata)[0]
        values = list(range(1, 10000, 3))
        self.assertEqual(file['rows'], len(values))
        self.check_bounds(file, fields['id'], 'BIGINT', values)
        self.check_bounds(file, fields['x'], 'DOUBLE', [-0.0] * len(values))
        self.check_bounds(file, fields['n'], 'FLOAT', [None] * len(values))
        path, _ = self.export('empty', 'SELECT 1::BIGINT id, 0::DOUBLE x WHERE false')
        self.assertEqual(self.sql(f'SELECT count(*) AS n FROM iceberg_scan({quote(path)});'), [{'n': 0}])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb', required=True, type=Path)
    args = parser.parse_args()
    DUCKDB = args.duckdb.resolve()
    unittest.main(argv=['test_file_metrics.py'], verbosity=2)
