"""Check original physical row positions through native Iceberg Vortex scans."""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from test_local_catalog import local_catalog, quote
from test_parquet_pushdown import iceberg_scans, profile, run_sql


ROWS = 65537


def source_query(start=0, end=ROWS):
    # The primary value deliberately differs from its zero-based physical position.
    return f"SELECT (3 * i + 100)::BIGINT id, 'row-' || i payload FROM range({start}, {end}) t(i)"


class RowPositionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='iceberg-row-positions-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def table(self, *, end=ROWS, name='table'):
        table = self.root / name
        run_sql(DUCKDB, f'COPY ({source_query(end=end)}) TO {quote(table)} (FORMAT iceberg, DATA_FORMAT vortex);')
        metadata = next((table / 'metadata').glob('*.metadata.json'))
        return table, metadata, f'iceberg_scan({quote(metadata)})'

    def test_positions_survive_native_filters_and_projection_order(self):
        _, _, scan = self.table()
        for projection in ('file_row_number pos, payload, id', 'payload, id, file_row_number pos'):
            query = (
                f'SELECT {projection} FROM {scan} WHERE id >= {3 * 12001 + 100} '
                f'AND id < {3 * 12014 + 100} ORDER BY pos'
            )
            self.assertEqual(
                run_sql(DUCKDB, query),
                [{'pos': i, 'payload': f'row-{i}', 'id': 3 * i + 100} for i in range(12001, 12014)],
            )
            # Prove that the row positions survived filtering inside Vortex, not only a SQL residual.
            scans = iceberg_scans(profile(DUCKDB, query))
            self.assertEqual(len(scans), 1)
            self.assertEqual(scans[0]['operator_cardinality'], 13)

        self.assertEqual(
            run_sql(DUCKDB, f"SELECT file_row_number pos FROM {scan} WHERE payload = 'row-50001'"),
            [{'pos': 50001}],
        )

    def test_position_only_scan_and_chunk_boundaries(self):
        _, _, scan = self.table()
        for threads in (1, 4):
            with self.subTest(threads=threads):
                self.assertEqual(
                    run_sql(
                        DUCKDB,
                        f'SET threads={threads}; SELECT count(*) n, count(DISTINCT file_row_number) distinct_n, '
                        'min(file_row_number) lo, max(file_row_number) hi, '
                        f'sum(file_row_number)::BIGINT total FROM {scan}',
                    ),
                    [{'n': ROWS, 'distinct_n': ROWS, 'lo': 0, 'hi': ROWS - 1, 'total': ROWS * (ROWS - 1) // 2}],
                )
                self.assertEqual(
                    run_sql(
                        DUCKDB,
                        f'SET threads={threads}; SELECT count(*) n FROM {scan} '
                        'WHERE id != 3 * file_row_number + 100',
                    ),
                    [{'n': 0}],
                )
        self.assertEqual(run_sql(DUCKDB, f'SELECT typeof(file_row_number) t FROM {scan} LIMIT 1'), [{'t': 'BIGINT'}])
        self.assertEqual(
            run_sql(
                DUCKDB, f'SELECT file_row_number pos FROM {scan} WHERE file_row_number IN (0, 2048, 65536) ORDER BY pos'
            ),
            [{'pos': i} for i in (0, 2048, 65536)],
        )
        self.assertEqual(run_sql(DUCKDB, f'SELECT count(*) n FROM {scan} WHERE file_row_number < 0'), [{'n': 0}])
        self.assertEqual(run_sql(DUCKDB, f'SELECT count(*) n FROM {scan} WHERE id = 101'), [{'n': 0}])
        self.assertEqual(run_sql(DUCKDB, f'SELECT count(*) n FROM {scan}'), [{'n': ROWS}])

    def test_files_reset_positions_and_old_snapshot_keeps_its_addresses(self):
        for append_format in ('vortex', 'parquet'):
            with self.subTest(append_format=append_format):
                table, metadata, old_scan = self.table(name=append_format)
                first_file = str(next((table / 'data').glob('*.vortex')))
                old_snapshot = json.loads(metadata.read_text())['current-snapshot-id']
                with local_catalog(metadata) as (catalog, attach):
                    catalog.metadata['properties']['write.format.default'] = append_format
                    run_sql(DUCKDB, attach + 'INSERT INTO lake.main.items ' + source_query(ROWS, 2 * ROWS))
                    current = catalog.metadata_path
                    self.assertFalse(catalog.errors)
                    # Exercise the catalog scan as well as iceberg_scan.
                    sources = ('lake.main.items', f'iceberg_scan({quote(current)})')
                    for scan in sources:
                        actual = run_sql(
                            DUCKDB,
                            attach + f'SET threads=4; SELECT filename, count(*) n, min(file_row_number) lo, '
                            'max(file_row_number) hi, count(DISTINCT file_row_number) distinct_n, '
                            'sum(CASE WHEN file_row_number != ((id - 100) // 3) % '
                            f'{ROWS} THEN 1 ELSE 0 END)::BIGINT bad FROM {scan} GROUP BY filename ORDER BY filename',
                        )
                        self.assertEqual(len(actual), 2)
                        self.assertIn(first_file, [row['filename'] for row in actual])
                        for row in actual:
                            self.assertEqual(
                                {key: value for key, value in row.items() if key != 'filename'},
                                {'n': ROWS, 'lo': 0, 'hi': ROWS - 1, 'distinct_n': ROWS, 'bad': 0},
                            )
                    snapshot_scan = f'iceberg_scan({quote(current)}, snapshot_from_id={old_snapshot})'
                    for scan in (old_scan, snapshot_scan):
                        self.assertEqual(
                            run_sql(DUCKDB, f'SELECT filename, file_row_number pos FROM {scan} WHERE id = 151'),
                            [{'filename': first_file, 'pos': 17}],
                        )

    def test_prepared_query_restarts_positions(self):
        _, _, scan = self.table()
        sql = (
            f'PREPARE p AS SELECT file_row_number pos, id FROM {scan} WHERE id >= $1 AND id < $2 ORDER BY pos; '
            'EXECUTE p(121, 127); EXECUTE p(180103, 180109); EXECUTE p(121, 127);'
        )
        result = subprocess.run(
            [str(DUCKDB), '-no-init', '-unsigned', '-batch', '-bail', '-json', '-c', 'SET threads=4; ' + sql],
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
        first = [{'pos': i, 'id': 3 * i + 100} for i in (7, 8)]
        later = [{'pos': i, 'id': 3 * i + 100} for i in (60001, 60002)]
        self.assertEqual(batches, [first, later, first])

    def test_empty_export(self):
        _, _, scan = self.table(end=0)
        self.assertEqual(
            run_sql(DUCKDB, f'SELECT count(file_row_number) n, min(file_row_number) lo FROM {scan}'),
            [{'n': 0, 'lo': None}],
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb', required=True, type=Path)
    args, remaining = parser.parse_known_args()
    DUCKDB = args.duckdb.resolve()
    unittest.main(argv=[__file__, *remaining])
