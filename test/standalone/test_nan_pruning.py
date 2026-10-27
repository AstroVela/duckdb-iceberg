#!/usr/bin/env python3
"""Exercise optional NaN metrics with the native shell; no Python packages required."""

import argparse
import json
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'vortex'))
from test_local_catalog import local_catalog


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def blob(value, dtype):
    return f"from_hex('{struct.pack('<f' if dtype == 'FLOAT' else '<d', value).hex()}')"


PREDICATES = (
    'x > 0',
    '0 < x',
    'x >= 1',
    "x = 'NaN'",
    "x >= 'NaN'",
    "x > 'NaN'",
    "x < 'NaN'",
    "x <= 'NaN'",
    "x != 'NaN'",
    "x IN ('NaN', 2)",
    "x > 0 AND x < 'NaN'",
    "x > 'Infinity'",
    "x < '-Infinity'",
    'x = 0',
    'x < 0',
    'x IS NULL',
    'x IS NOT NULL',
    'id > 100',
)


class NaNPruningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='iceberg-nan-pruning-')
        cls.root = Path(cls.temp.name)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def sql(self, sql):
        result = subprocess.run(
            [str(DUCKDB), '-unsigned', '-batch', '-bail', '-json', '-c', 'SET threads=1; ' + sql],
            text=True,
            capture_output=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr + '\n' + sql)
        values = []
        remaining = result.stdout.strip()
        while remaining:
            value, end = json.JSONDecoder().raw_decode(remaining)
            values.append(value)
            remaining = remaining[end:].strip()
        return values

    def parquet(self, path, dtype, values):
        rows = ', '.join(f'({i}, {literal(x) if x is not None else "NULL"}::{dtype})' for i, x in values)
        self.sql(
            f'COPY (SELECT * FROM (VALUES {rows}) t(id, x)) TO {literal(path)} '
            '(FORMAT PARQUET, FIELD_IDS {id: 17, x: 42});'
        )

    def manifest(self, path, dtype, files, metric, partitioned):
        rows = []
        for data, values, partition in files:
            finite = [float(x) for _, x in values if x is not None and x != 'NaN']
            bounds = []
            for fn in (min, max):
                entries = [f'17: from_hex(\'{struct.pack("<i", fn(i for i, _ in values)).hex()}\')']
                if finite:
                    bound = fn(finite)
                    if bound == 0:
                        bound = -0.0 if fn is min else 0.0
                    entries.append(f'42: {blob(bound, dtype)}')
                bounds.append('MAP {' + ', '.join(entries) + '}')
            nan_count = sum(x == 'NaN' for _, x in values)
            nan_metrics = {
                'absent': '',
                'null': ', nan_value_counts: NULL::MAP(INTEGER, BIGINT)',
                'empty': ', nan_value_counts: MAP {}::MAP(INTEGER, BIGINT)',
                'other_field': ', nan_value_counts: MAP {17: 0::BIGINT}',
                'known': f', nan_value_counts: MAP {{42: {nan_count}::BIGINT}}',
            }[metric]
            part = f'{{x: {literal(partition) if partition is not None else "NULL"}::{dtype}}}'
            if not partitioned:
                part = '{__duckdb_empty_struct_marker: NULL::INTEGER}'
            rows.append(
                f"SELECT 1 AS status, 1::BIGINT AS snapshot_id, {{content: 0, file_path: {literal(data)}, "
                f"file_format: 'PARQUET', partition: {part}, record_count: {len(values)}::BIGINT, "
                f"file_size_in_bytes: {data.stat().st_size}::BIGINT, lower_bounds: {bounds[0]}, "
                f"upper_bounds: {bounds[1]}{nan_metrics}}} AS data_file"
            )
        nan_ids = ', nan_value_counts: {__duckdb_field_id: 137, key: 138, value: 139}' if metric != 'absent' else ''
        partition_ids = '{__duckdb_field_id: 102, x: 1000}' if partitioned else '{__duckdb_field_id: 102}'
        self.sql(
            f"COPY ({' UNION ALL '.join(rows)}) TO {literal(path)} (FORMAT AVRO, FIELD_IDS {{"
            'status: 0, snapshot_id: 1, data_file: {__duckdb_field_id: 2, content: 134, '
            f'file_path: 100, file_format: 101, partition: {partition_ids}, '
            'record_count: 103, file_size_in_bytes: 104, '
            'lower_bounds: {__duckdb_field_id: 125, key: 126, value: 127}, '
            'upper_bounds: {__duckdb_field_id: 128, key: 129, value: 130}' + nan_ids + '}});'
        )

    def table(self, directory, dtype, files, metric='known', summary=None):
        directory.mkdir()
        manifest = directory / 'manifest.avro'
        manifest_list = directory / 'manifest-list.avro'
        metadata = directory / 'metadata.json'
        partitioned = summary is not None
        self.manifest(manifest, dtype, files, metric, partitioned)
        partitions = ''
        partition_ids = ''
        if partitioned:
            nan_field = '' if summary == 'absent' else f', contains_nan: {summary}::BOOLEAN'
            nan_id = '' if summary == 'absent' else ', contains_nan: 518'
            partitions = (
                ', [{contains_null: true, '
                f'lower_bound: {blob(-1, dtype)}, upper_bound: {blob(0, dtype)}{nan_field}}}] AS partitions'
            )
            partition_ids = (
                ', partitions: {__duckdb_field_id: 507, list: {__duckdb_field_id: 508, '
                'contains_null: 509, lower_bound: 510, upper_bound: 511' + nan_id + '}}'
            )
        self.sql(
            f'COPY (SELECT {literal(manifest)} AS manifest_path, {manifest.stat().st_size}::BIGINT AS manifest_length, '
            f'0 AS partition_spec_id, 1::BIGINT AS added_snapshot_id, {len(files)} AS added_files_count, '
            f'0 AS existing_files_count, 0 AS deleted_files_count, 0 AS content, 1::BIGINT AS sequence_number, '
            f'1::BIGINT AS min_sequence_number, {sum(len(values) for _, values, _ in files)}::BIGINT AS added_rows_count, '
            f'0::BIGINT AS existing_rows_count, 0::BIGINT AS deleted_rows_count{partitions}) TO {literal(manifest_list)} '
            '(FORMAT AVRO, FIELD_IDS {manifest_path: 500, manifest_length: 501, partition_spec_id: 502, '
            'added_snapshot_id: 503, added_files_count: 504, existing_files_count: 505, deleted_files_count: 506, '
            'content: 517, sequence_number: 515, min_sequence_number: 516, '
            'added_rows_count: 512, existing_rows_count: 513, deleted_rows_count: 514' + partition_ids + '});'
        )
        metadata.write_text(
            json.dumps(
                {
                    'format-version': 2,
                    'table-uuid': '11111111-2222-3333-4444-555555555555',
                    'location': str(directory),
                    'last-updated-ms': 1700000000000,
                    'last-sequence-number': 1,
                    'properties': {},
                    'last-column-id': 42,
                    'schemas': [
                        {
                            'type': 'struct',
                            'schema-id': 0,
                            'fields': [
                                {'id': 17, 'name': 'id', 'type': 'int', 'required': False},
                                {'id': 42, 'name': 'x', 'type': dtype.lower(), 'required': False},
                            ],
                        }
                    ],
                    'current-schema-id': 0,
                    'partition-specs': [
                        {
                            'spec-id': 0,
                            'fields': (
                                [
                                    {'source-id': 42, 'field-id': 1000, 'name': 'x', 'transform': 'identity'},
                                ]
                                if partitioned
                                else []
                            ),
                        }
                    ],
                    'default-spec-id': 0,
                    'last-partition-id': 1000,
                    'current-snapshot-id': 1,
                    'snapshots': [
                        {
                            'snapshot-id': 1,
                            'sequence-number': 1,
                            'timestamp-ms': 1700000000000,
                            'manifest-list': str(manifest_list),
                            'schema-id': 0,
                            'summary': {'operation': 'append'},
                        }
                    ],
                    'sort-orders': [{'order-id': 0, 'fields': []}],
                    'default-sort-order-id': 0,
                }
            )
        )
        return metadata

    def check_filters(self, metadata, files):
        reference = 'read_parquet([' + ','.join(literal(data) for data, _, _ in files) + '])'
        iceberg = f'iceberg_scan({literal(metadata)})'
        queries = []
        for predicate in PREDICATES:
            for source in (reference, iceberg):
                queries.append(f'SELECT list(id ORDER BY id) AS ids FROM {source} WHERE {predicate};')
        results = self.sql(' '.join(queries))
        self.assertEqual(len(results), len(PREDICATES) * 2)
        for i, predicate in enumerate(PREDICATES):
            self.assertEqual(results[2 * i + 1], results[2 * i], f'{metadata}: {predicate}')

    def check_pruning(self, metadata, predicate, kind, count):
        message = literal(f"skipped '{kind}'")
        results = self.sql(
            "SET enable_logging=false; CALL enable_logging('Iceberg', storage='memory'); "
            f'SELECT count(*) FROM iceberg_scan({literal(metadata)}) WHERE {predicate}; '
            f"SELECT count(*) AS skipped FROM duckdb_logs() WHERE type = 'Iceberg' "
            f"AND contains(message, {message});"
        )
        self.assertEqual(results[-1], [{'skipped': count}], str(metadata))

    def test_file_metrics(self):
        for dtype in ('FLOAT', 'DOUBLE'):
            for has_nan in (False, True):
                values = [(1, '-1'), (2, '-0'), (3, '0'), (4, None)]
                if has_nan:
                    values.append((5, 'NaN'))
                data = self.root / f'{dtype}-{has_nan}.parquet'
                self.parquet(data, dtype, values)
                files = [(data, values, None)]
                for metric in ('absent', 'null', 'empty', 'other_field', 'known'):
                    with self.subTest(dtype=dtype, has_nan=has_nan, metric=metric):
                        metadata = self.table(self.root / f'file-{dtype}-{has_nan}-{metric}', dtype, files, metric)
                        self.check_filters(metadata, files)
                        self.check_pruning(metadata, 'x < -2', 'data_file', 1)
                        self.check_pruning(metadata, 'id > 100', 'data_file', 1)
                        self.check_pruning(metadata, 'x > 0', 'data_file', int(metric == 'known' and not has_nan))

    def test_manifest_summaries(self):
        for dtype in ('FLOAT', 'DOUBLE'):
            files = []
            for i, value in enumerate(('-1', '0', 'NaN', None), 1):
                data = self.root / f'partition-{dtype}-{i}.parquet'
                values = [(i, value)]
                self.parquet(data, dtype, values)
                files.append((data, values, value))
            for summary in ('absent', 'NULL', 'true', 'false'):
                with self.subTest(dtype=dtype, summary=summary):
                    selected = [file for file in files if summary != 'false' or file[2] != 'NaN']
                    metadata = self.table(self.root / f'manifest-{dtype}-{summary}', dtype, selected, summary=summary)
                    self.check_filters(metadata, selected)
                    self.check_pruning(metadata, 'x < -2', 'manifest_file', 1)
                    self.check_pruning(metadata, 'x > 0', 'manifest_file', int(summary == 'false'))
                    expected_nan = None if summary in ('absent', 'NULL') else summary == 'true'
                    self.assertEqual(
                        self.sql(f'SELECT contains_nan FROM iceberg_partition_stats({literal(metadata)});')[-1],
                        [{'contains_nan': expected_nan}],
                    )

    def test_infinite_and_missing_bounds(self):
        for dtype in ('FLOAT', 'DOUBLE'):
            for case, values in (
                ('infinite', [(1, '-Infinity'), (2, 'Infinity'), (3, 'NaN'), (4, None)]),
                ('nan_only', [(1, 'NaN'), (2, None)]),
                ('null_only', [(1, None)]),
            ):
                data = self.root / f'{case}-{dtype}.parquet'
                self.parquet(data, dtype, values)
                files = [(data, values, None)]
                for metric in ('absent', 'known'):
                    with self.subTest(dtype=dtype, case=case, metric=metric):
                        metadata = self.table(self.root / f'{case}-{dtype}-{metric}', dtype, files, metric)
                        self.check_filters(metadata, files)

    def test_partition_summary_writer(self):
        for dtype in ('FLOAT', 'DOUBLE'):
            with self.subTest(dtype=dtype):
                files = []
                for i, value in enumerate(('-1', '0', 'NaN', None), 1):
                    data = self.root / f'writer-{dtype}-{i}.parquet'
                    values = [(i, value)]
                    self.parquet(data, dtype, values)
                    files.append((data, values, value))
                metadata = self.table(self.root / f'writer-{dtype}', dtype, files, summary='NULL')
                (metadata.parent / 'metadata').mkdir()
                with local_catalog(metadata) as (server, attach):
                    # The shared HTTP fixture defaults to Vortex; exercise Parquet in both builds.
                    server.metadata['properties']['write.format.default'] = 'parquet'
                    for rows, bound in (("(10, 'NaN'), (11, 0), (12, NULL)", 0), ("(13, 'NaN'), (14, NULL)", None)):
                        self.sql(attach + f'INSERT INTO lake.main.items VALUES {rows};')
                        self.assertEqual(server.errors, [])
                        snapshot = server.metadata['snapshots'][-1]
                        manifest_list = literal(snapshot['manifest-list'])
                        summaries = self.sql(
                            'SELECT partitions[1].contains_nan AS nan, partitions[1].contains_null AS nulls, '
                            'hex(partitions[1].lower_bound) AS lo, hex(partitions[1].upper_bound) AS hi '
                            f'FROM read_avro({manifest_list}) WHERE added_snapshot_id = {snapshot["snapshot-id"]};'
                        )[-1]
                        bound_hex = (
                            None if bound is None else struct.pack('<f' if dtype == 'FLOAT' else '<d', bound).hex()
                        )
                        self.assertEqual(summaries, [{'nan': True, 'nulls': True, 'lo': bound_hex, 'hi': bound_hex}])
                        # Rewriting the manifest list must preserve an old unknown contains_nan as NULL.
                        self.assertEqual(
                            self.sql(
                                f'SELECT partitions[1].contains_nan AS nan FROM read_avro({manifest_list}) '
                                'WHERE added_snapshot_id = 1;'
                            )[-1],
                            [{'nan': None}],
                        )
                    self.assertEqual(
                        self.sql(attach + 'SELECT count(*) AS n FROM lake.main.items WHERE x > 0;')[-1], [{'n': 3}]
                    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb', required=True, type=Path)
    args, remaining = parser.parse_known_args()
    DUCKDB = args.duckdb.resolve()
    unittest.main(argv=[__file__, *remaining])
