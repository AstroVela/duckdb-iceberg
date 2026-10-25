"""Verify conservative Vortex row-filter copies through native Iceberg scans."""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from test_local_catalog import local_catalog, quote
from test_parquet_pushdown import iceberg_scans, profile, run_sql


def source_query(start, end):
    return (
        'SELECT i::INTEGER id, (i::BIGINT - 10000) * 1000000000 k, '
        "CASE i % 5 WHEN 0 THEN NULL WHEN 1 THEN 0.0::DOUBLE WHEN 2 THEN '-0.0'::DOUBLE "
        "WHEN 3 THEN 'NaN'::DOUBLE ELSE i::DOUBLE END x, 'payload-' || i::VARCHAR payload "
        f'FROM range({start}, {end}) t(i)'
    )


class StaticFilterTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='iceberg-static-filters-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def table(self, mixed=False):
        table = self.root / ('mixed' if mixed else 'vortex')
        first_format = 'parquet' if mixed else 'vortex'
        end = 10000 if mixed else 20000
        run_sql(
            DUCKDB, f'COPY ({source_query(0, end)}) TO {quote(table)} (FORMAT iceberg, DATA_FORMAT {first_format});'
        )
        metadata = next((table / 'metadata').glob('*.metadata.json'))
        if mixed:
            with local_catalog(metadata) as (catalog, attach):
                run_sql(DUCKDB, attach + 'INSERT INTO lake.main.items ' + source_query(10000, 20000))
                metadata = catalog.metadata_path
                self.assertFalse(catalog.errors)
        return f'iceberg_scan({quote(metadata)})'

    def check(self, source, predicate, ids, scan_rows):
        query = f'SELECT payload, id FROM {source} WHERE {predicate} ORDER BY id'
        expected = [{'payload': f'payload-{i}', 'id': i} for i in ids]
        self.assertEqual(run_sql(DUCKDB, query), expected)
        scans = iceberg_scans(profile(DUCKDB, query))
        self.assertEqual(len(scans), 1)
        self.assertEqual(scans[0]['operator_cardinality'], scan_rows, predicate)
        # DuckDB retains the complete row predicate; the copied filter is only an optimization.
        self.assertNotIn('Filters', scans[0]['extra_info'])

    def test_static_ranges_reduce_vortex_output(self):
        source = self.table()
        self.check(source, 'id >= 7010 AND id < 7023', range(7010, 7023), 13)
        self.check(source, 'k >= -3000000000000 AND k < -2990000000000', range(7000, 7010), 10)
        self.check(source, 'id < 10 OR id >= 19990', [*range(10), *range(19990, 20000)], 20)

    def test_nulls_and_float_residuals(self):
        source = self.table()
        self.check(source, 'x IS NULL', range(0, 20000, 5), 4000)
        self.check(source, 'x IS NOT NULL', [i for i in range(20000) if i % 5], 16000)
        self.check(source, 'x = 0', [i for i in range(20000) if i % 5 in (1, 2)], 20000)
        self.check(
            source,
            'id >= 7000 AND id < 7100 AND x = 0',
            [i for i in range(7000, 7100) if i % 5 in (1, 2)],
            100,
        )
        self.check(
            source,
            'id < 10 OR x = 0',
            [i for i in range(20000) if i < 10 or i % 5 in (1, 2)],
            20000,
        )

    def test_mixed_files_keep_parquet_behavior(self):
        source = self.table(mixed=True)
        self.check(source, 'id >= 9000 AND id < 11000', range(9000, 11000), 11000)
        self.check(source, 'x IS NULL', range(0, 20000, 5), 12000)

    def test_scalar_null_types(self):
        values = (
            '42::INTEGER',
            '42000000000::BIGINT',
            'true',
            "'text'::VARCHAR",
            "'bytes'::BLOB",
            "DATE '2026-01-01'",
            "TIMESTAMP '2026-01-01 12:00:00'",
            "TIME '12:00:00'",
            '42.25::DECIMAL(8,2)',
        )
        for index, value in enumerate(values):
            with self.subTest(value=value):
                table = self.root / f'null-{index}'
                run_sql(
                    DUCKDB,
                    f'COPY (SELECT i::INTEGER id, CASE WHEN i % 2 = 0 THEN NULL ELSE {value} END x '
                    f'FROM range(32) t(i)) TO {quote(table)} (FORMAT iceberg, DATA_FORMAT vortex);',
                )
                metadata = next((table / 'metadata').glob('*.metadata.json'))
                source = f'iceberg_scan({quote(metadata)})'
                for predicate, start in (('x IS NULL', 0), ('x IS NOT NULL', 1)):
                    query = f'SELECT id FROM {source} WHERE {predicate} ORDER BY id'
                    self.assertEqual(run_sql(DUCKDB, query), [{'id': i} for i in range(start, 32, 2)])
                    scans = iceberg_scans(profile(DUCKDB, query))
                    self.assertEqual(scans[0]['operator_cardinality'], 16)

    def test_prepared_parameters_do_not_reuse_old_filters(self):
        source = self.table()
        sql = (
            f'PREPARE p AS SELECT count(*) n, sum(id)::BIGINT s FROM {source} WHERE id >= $1 AND id < $2; '
            'EXECUTE p(11, 23); EXECUTE p(12001, 12013);'
        )
        result = subprocess.run(
            [str(DUCKDB), '-no-init', '-unsigned', '-batch', '-bail', '-json', '-c', 'SET threads=1; ' + sql],
            text=True,
            capture_output=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        output = result.stdout.strip()
        batches = []
        while output:
            rows, end = json.JSONDecoder().raw_decode(output)
            batches.append(rows)
            output = output[end:].strip()
        self.assertEqual(
            batches[-2:],
            [[{'n': 12, 's': sum(range(11, 23))}], [{'n': 12, 's': sum(range(12001, 12013))}]],
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb', required=True, type=Path)
    args, remaining = parser.parse_known_args()
    DUCKDB = args.duckdb.resolve()
    unittest.main(argv=[__file__, *remaining])
