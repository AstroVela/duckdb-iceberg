"""Check NULL file pruning and retained row filters with native Iceberg builds."""

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
import unittest

from test_file_metrics import file_metrics
from test_local_catalog import local_catalog, quote
from test_parquet_pushdown import iceberg_scans, profile, run_sql


class NullPruningTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='iceberg-null-pruning-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def sql(self, sql):
        return run_sql(DUCKDB, sql)

    def without_metric(self, metadata_path, metric):
        """Create a legacy manifest with one optional count field absent."""
        metadata = json.loads(metadata_path.read_text())
        snapshot = metadata['snapshots'][0]
        manifest_list = snapshot['manifest-list']
        manifest = self.sql(f'SELECT manifest_path path FROM read_avro({quote(manifest_list)})')[0]['path']
        fields = {
            'content': '134',
            'file_path': '100',
            'file_format': '101',
            'partition': '{__duckdb_field_id: 102}',
            'record_count': '103',
            'file_size_in_bytes': '104',
            'null_value_counts': '{__duckdb_field_id: 110, key: 121, value: 122}',
            'value_counts': '{__duckdb_field_id: 109, key: 119, value: 120}',
        }
        del fields[metric]
        new_manifest = metadata_path.parent / f'{metric}.avro'
        columns = ', '.join(f'{name}: data_file.{name}' for name in fields)
        ids = ', '.join(f'{name}: {value}' for name, value in fields.items())
        self.sql(
            f'COPY (SELECT status, snapshot_id, sequence_number, file_sequence_number, '
            f'{{{columns}}} AS data_file FROM read_avro({quote(manifest)})) TO {quote(new_manifest)} '
            '(FORMAT avro, FIELD_IDS {status: 0, snapshot_id: 1, sequence_number: 3, file_sequence_number: 4, '
            f'data_file: {{__duckdb_field_id: 2, {ids}}}}});'
        )
        new_list = metadata_path.parent / f'{metric}-list.avro'
        list_fields = {
            'manifest_path': 500,
            'manifest_length': 501,
            'partition_spec_id': 502,
            'content': 517,
            'sequence_number': 515,
            'min_sequence_number': 516,
            'added_snapshot_id': 503,
            'added_files_count': 504,
            'existing_files_count': 505,
            'deleted_files_count': 506,
            'added_rows_count': 512,
            'existing_rows_count': 513,
            'deleted_rows_count': 514,
        }
        columns = ', '.join(list_fields)
        ids = ', '.join(f'{name}: {value}' for name, value in list_fields.items())
        self.sql(
            f'COPY (SELECT * REPLACE ({quote(new_manifest)} AS manifest_path, '
            f'{new_manifest.stat().st_size}::BIGINT AS manifest_length) '
            f'FROM (SELECT {columns} FROM read_avro({quote(manifest_list)}))) '
            f'TO {quote(new_list)} (FORMAT avro, FIELD_IDS {{{ids}}});'
        )
        snapshot['manifest-list'] = str(new_list)
        new_metadata = metadata_path.parent / f'{metric}.metadata.json'
        new_metadata.write_text(json.dumps(metadata))
        return new_metadata

    @contextmanager
    def table(self, name, formats, mode='full'):
        path = self.root / name
        self.sql(
            f'COPY (SELECT * FROM (VALUES (1, NULL::DOUBLE), (2, NULL)) t(id, x)) '
            f'TO {quote(path)} (FORMAT iceberg, DATA_FORMAT {formats[0]});'
        )
        metadata = next((path / 'metadata').glob('*.metadata.json'))
        self.assertEqual(json.loads(metadata.read_text())['sort-orders'], [{'order-id': 0, 'fields': []}])
        with local_catalog(metadata) as (server, attach):
            old_snapshot = server.metadata['current-snapshot-id']
            server.metadata['properties']['write.metadata.metrics.default'] = mode
            for format, rows in zip(formats[1:], ("(3, 0), (4, 'NaN')", "(5, NULL), (6, '-0.0')")):
                server.metadata['properties']['write.format.default'] = format
                self.sql(attach + f'INSERT INTO lake.main.items VALUES {rows};')
            yield server, attach, old_snapshot
            self.assertFalse(server.errors)

    def check_query(self, source, predicate, ids, skipped=None, setup=''):
        query = f'SELECT id FROM {source} WHERE {predicate} ORDER BY id;'
        self.assertEqual(self.sql(setup + query), [{'id': i} for i in ids], predicate)
        if skipped is not None:
            result = self.sql(
                setup
                + "SET enable_logging=false; CALL enable_logging('Iceberg', storage='memory'); "
                + query
                + "SELECT count(*) skipped FROM duckdb_logs() WHERE type='Iceberg' "
                "AND contains(message, 'skipped ''data_file''');"
            )
            self.assertEqual(result, [{'skipped': skipped}], predicate)

    def test_results_pruning_and_snapshots(self):
        formats = [('parquet',) * 3]
        if MODE == 'ON':
            formats.extend([('vortex',) * 3, ('parquet', 'vortex', 'parquet'), ('vortex', 'parquet', 'vortex')])
        for index, sequence in enumerate(formats):
            with self.subTest(formats=sequence), self.table(str(index), sequence) as (server, attach, old):
                source = f'iceberg_scan({quote(server.metadata_path)})'
                for predicate, ids in (
                    ('x IS NULL', [1, 2, 5]),
                    ('x IS NOT NULL', [3, 4, 6]),
                ):
                    self.check_query(source, predicate, ids, 1)
                    self.check_query('lake.main.items', predicate, ids, 1, attach)
                for predicate, ids in (
                    ('NOT (x IS NULL)', [3, 4, 6]),
                    ('x IS NULL AND id >= 2', [2, 5]),
                    ('x IS NOT NULL AND x = 0', [3, 6]),
                    ("x IS NOT NULL AND isnan(x)", [4]),
                    ('x IS NULL OR id = 3', [1, 2, 3, 5]),
                    ('x IS NULL OR x = 0', [1, 2, 3, 5, 6]),
                    ('NULLIF(x, 0) IS NULL', [1, 2, 3, 5, 6]),
                    ('coalesce(x, 0) IS NOT NULL', [1, 2, 3, 4, 5, 6]),
                ):
                    self.check_query(source, predicate, ids)
                self.check_query(source, 'x IS NULL AND id >= 2', [2, 5], 1)
                # Projection order and aliases must resolve to the scan's column IDs.
                self.check_query(f'(SELECT x AS n, id FROM {source})', 'n IS NULL', [1, 2, 5], 1)
                old_source = f'iceberg_scan({quote(server.metadata_path)}, snapshot_from_id={old})'
                self.check_query(old_source, 'x IS NOT NULL', [], 1)
                self.check_query(old_source, 'x IS NULL', [1, 2], 0)
                scans = iceberg_scans(profile(DUCKDB, f'SELECT id FROM {source} WHERE x IS NULL'))
                self.assertEqual(len(scans), 1)
                self.assertEqual('Filters' in scans[0]['extra_info'], all(f == 'parquet' for f in sequence))

    def test_counts_without_bounds_and_missing_metrics(self):
        formats = ['parquet'] + (['vortex'] if MODE == 'ON' else [])
        for format in formats:
            for mode in ('counts', 'none'):
                with self.subTest(format=format, mode=mode), self.table(format + mode, (format,) * 3, mode) as (
                    server,
                    _,
                    old,
                ):
                    source = f'iceberg_scan({quote(server.metadata_path)})'
                    files = file_metrics(DUCKDB, server.metadata)
                    old_file = file_metrics(DUCKDB, {**server.metadata, 'current-snapshot-id': old})[0]['path']
                    for file in files:
                        if file['path'] == old_file:
                            continue
                        self.assertFalse(file['lower'])
                        self.assertFalse(file['upper'])
                    self.check_query(source, 'x IS NULL', [1, 2, 5], 1 if mode == 'counts' else 0)
                    self.check_query(source, 'x IS NOT NULL', [3, 4, 6], 1)

    def test_all_null_file_without_any_bounds(self):
        for format in ['parquet'] + (['vortex'] if MODE == 'ON' else []):
            with self.subTest(format=format):
                path = self.root / format
                self.sql(
                    f'COPY (SELECT NULL::INTEGER x FROM range(3)) TO {quote(path)} '
                    f'(FORMAT iceberg, DATA_FORMAT {format});'
                )
                source = f'iceberg_scan({quote(path)})'
                self.assertEqual(self.sql(f'SELECT count(*) n FROM {source} WHERE x IS NOT NULL'), [{'n': 0}])
                result = self.sql(
                    "SET enable_logging=false; CALL enable_logging('Iceberg', storage='memory'); "
                    f'SELECT * FROM {source} WHERE x IS NOT NULL; '
                    "SELECT count(*) skipped FROM duckdb_logs() WHERE type='Iceberg' "
                    "AND contains(message, 'skipped ''data_file''');"
                )
                self.assertEqual(result, [{'skipped': 1}])

    def test_independently_missing_counts(self):
        for format in ['parquet'] + (['vortex'] if MODE == 'ON' else []):
            for nulls in (True, False):
                with self.subTest(format=format, nulls=nulls):
                    path = self.root / f'{format}-{nulls}'
                    x = 'NULL::DOUBLE' if nulls else "'NaN'::DOUBLE"
                    self.sql(
                        f'COPY (SELECT i::INTEGER id, {x} x FROM range(2) t(i)) TO {quote(path)} '
                        f'(FORMAT iceberg, DATA_FORMAT {format});'
                    )
                    metadata = next((path / 'metadata').glob('*.metadata.json'))
                    for metric in ('value_counts', 'null_value_counts'):
                        legacy = self.without_metric(metadata, metric)
                        source = f'iceberg_scan({quote(legacy)})'
                        self.check_query(
                            source, 'x IS NULL', [0, 1] if nulls else [], int(not nulls and metric == 'value_counts')
                        )
                        self.check_query(source, 'x IS NOT NULL', [] if nulls else [0, 1], 0)

    def test_parquet_nested_and_derived_null_checks(self):
        path = self.root / 'nested'
        self.sql(
            "COPY (SELECT * FROM (VALUES (1, {'n': NULL::INTEGER}, []::INTEGER[], 'bad'), "
            "(2, {'n': 1}, [1], '2'), (3, NULL, NULL, NULL), (4, {'n': 4}, [NULL], 'bad')) "
            f't(id, s, a, label)) TO {quote(path)} (FORMAT iceberg, DATA_FORMAT parquet);'
        )
        source = f'iceberg_scan({quote(path)})'
        for predicate, ids in (
            ('s IS NULL', [3]),
            ('s.n IS NULL', [1, 3]),
            ('a IS NULL', [3]),
            ('a[1] IS NULL', [1, 3, 4]),
            ('TRY_CAST(label AS INTEGER) IS NULL', [1, 3, 4]),
        ):
            self.check_query(source, predicate, ids)
        metadata = json.loads(next((path / 'metadata').glob('*.metadata.json')).read_text())
        fields = {f['name']: str(f['id']) for f in metadata['schemas'][0]['fields']}
        self.assertEqual(file_metrics(DUCKDB, metadata)[0]['counts'], {fields['id']: 4, fields['label']: 4})

    def test_prepared_queries_after_transaction_append(self):
        path = self.root / 'prepared'
        self.sql(f'COPY (SELECT 1::INTEGER id, NULL::DOUBLE x) TO {quote(path)} (FORMAT iceberg);')
        metadata = next((path / 'metadata').glob('*.metadata.json'))
        with local_catalog(metadata) as (server, attach):
            server.metadata['properties']['write.format.default'] = 'vortex' if MODE == 'ON' else 'parquet'
            prepare = ''
            for name, predicate in (('n', 'x IS NULL'), ('v', 'x IS NOT NULL')):
                prepare += (
                    f'PREPARE {name} AS SELECT CASE WHEN coalesce(list(id ORDER BY id), []::INTEGER[]) = $1::INTEGER[] '
                    "THEN true ELSE error('NULL pruning prepared query mismatch') END ok "
                    f'FROM lake.main.items WHERE {predicate}; '
                )
            self.assertEqual(
                self.sql(
                    attach + prepare + 'EXECUTE n([1]); EXECUTE v([]); BEGIN; '
                    "INSERT INTO lake.main.items VALUES (2, 'NaN'), (3, NULL); "
                    'EXECUTE n([1, 3]); EXECUTE v([2]); COMMIT; EXECUTE n([1, 3]); EXECUTE v([2]);'
                ),
                [{'ok': True}],
            )
            self.assertEqual(len(server.commits), 1)
            self.assertFalse(server.errors)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb', required=True, type=Path)
    parser.add_argument('--mode', required=True, choices=('ON', 'OFF'))
    args = parser.parse_args()
    DUCKDB = args.duckdb.resolve()
    MODE = args.mode
    unittest.main(argv=['test_null_pruning.py'], verbosity=2)
