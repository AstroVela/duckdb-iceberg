#!/usr/bin/env python3
"""Compare Vortex writes and multi-file pruning with native Iceberg builds."""

import argparse
import json
from pathlib import Path
import statistics
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'test/vortex'))
from test_local_catalog import local_catalog, quote
from test_parquet_pushdown import run_sql


def profile(shell, sql, expected):
    with tempfile.TemporaryDirectory(prefix='iceberg-metrics-profile-') as directory:
        path = Path(directory) / 'profile.json'
        result = run_sql(
            shell,
            "SET enable_profiling='json'; SET profiling_mode='detailed'; "
            f'SET profiling_output={quote(path)}; ' + sql,
        )
        assert result == expected, result
        return json.loads(path.read_text())['latency'] * 1000


def data_query(start, end, null_rows=0):
    x = f'CASE WHEN i < {null_rows} THEN NULL ELSE i::DOUBLE END' if null_rows else 'i::DOUBLE'
    return f'SELECT i::BIGINT id, {x} x FROM range({start}, {end}) t(i)'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb', action='append', required=True, metavar='LABEL=PATH')
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--files', type=int, default=8)
    parser.add_argument('--rows-per-file', type=int, default=250000)
    parser.add_argument('--null-files', type=int, default=0, help='use NULL predicates with this many all-NULL x files')
    parser.add_argument('--repeats', type=int, default=7)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.files < 2 or args.rows_per_file < 1000 or args.repeats < 1:
        parser.error('need at least 2 files, 1000 rows per file and 1 repeat')
    if not 0 <= args.null_files < args.files:
        parser.error('--null-files must be between 0 and --files - 1')
    shells = {}
    for item in args.duckdb:
        label, separator, path = item.partition('=')
        if not separator or not label or label in shells:
            parser.error('--duckdb requires a unique LABEL=PATH')
        shells[label] = Path(path).resolve()
    args.directory.mkdir(parents=True, exist_ok=True)
    report = {
        'files': args.files,
        'rows_per_file': args.rows_per_file,
        'null_files': args.null_files,
        'threads': 1,
        'repeats': args.repeats,
        'runs': {},
    }
    rows = args.files * args.rows_per_file
    null_rows = args.null_files * args.rows_per_file
    scans = {}
    for index, (label, shell) in enumerate(shells.items()):
        table = args.directory.resolve() / f'table-{index}'
        if table.exists():
            parser.error(f'use a fresh directory: {table} exists')
        writes = []
        for repetition in range(args.repeats + 1):
            with tempfile.TemporaryDirectory(prefix='iceberg-metrics-write-', dir=args.directory) as directory:
                target = Path(directory).resolve() / 'table'
                sql = (
                    f'COPY ({data_query(0, rows, null_rows)}) TO {quote(target)} (FORMAT iceberg, DATA_FORMAT vortex);'
                )
                elapsed = profile(shell, sql, None)
                assert run_sql(
                    shell,
                    f'SELECT count(*) n, count(x) nonnull, sum(id)::BIGINT total FROM iceberg_scan({quote(target)})',
                ) == [{'n': rows, 'nonnull': rows - null_rows, 'total': rows * (rows - 1) // 2}]
                if repetition:
                    writes.append(elapsed)
        run_sql(
            shell,
            f'COPY ({data_query(0, args.rows_per_file, null_rows)}) TO {quote(table)} '
            '(FORMAT iceberg, DATA_FORMAT vortex);',
        )
        metadata = next((table / 'metadata').glob('*.metadata.json'))
        with local_catalog(metadata) as (server, attach):
            for file_index in range(1, args.files):
                start = file_index * args.rows_per_file
                run_sql(
                    shell,
                    attach + 'INSERT INTO lake.main.items ' + data_query(start, start + args.rows_per_file, null_rows),
                )
            metadata = server.metadata_path
            assert not server.errors, server.errors
        scan = f'iceberg_scan({quote(metadata)})'
        assert run_sql(shell, f'SELECT count(*) n FROM {scan}') == [{'n': rows}]
        assert len(list((table / 'data').glob('*.vortex'))) == args.files
        scans[label] = scan
        report['runs'][label] = {'write_median_ms': statistics.median(writes), 'queries': {}}
    low = (args.files // 2) * args.rows_per_file + 100
    queries = [
        ('integer_range', f'id >= {low} AND id < {low + 100}', sum(range(low, low + 100))),
        ('float_range', f'x >= {low} AND x < {low + 100}', sum(range(low, low + 100))),
    ]
    if null_rows:
        null_sum = null_rows * (null_rows - 1) // 2
        queries = [
            ('is_null', 'x IS NULL', null_sum),
            ('is_not_null', 'x IS NOT NULL', rows * (rows - 1) // 2 - null_sum),
        ]
    queries.append(('full_scan', 'true', rows * (rows - 1) // 2))
    for name, predicate, expected in queries:
        sql = {label: f'SELECT sum(id)::BIGINT total FROM {scan} WHERE {predicate};' for label, scan in scans.items()}
        for label, shell in shells.items():
            profile(shell, sql[label], [{'total': expected}])
        timings = {label: [] for label in shells}
        for repetition in range(args.repeats):
            order = list(shells) if repetition % 2 == 0 else list(reversed(shells))
            for label in order:
                timings[label].append(profile(shells[label], sql[label], [{'total': expected}]))
        for label, shell in shells.items():
            skipped = run_sql(
                shell,
                "SET enable_logging=false; CALL enable_logging('Iceberg', storage='memory'); "
                + sql[label]
                + "SELECT count(*) skipped FROM duckdb_logs() WHERE type='Iceberg' "
                "AND contains(message, 'skipped ''data_file''');",
            )[0]['skipped']
            report['runs'][label]['queries'][name] = {
                'median_ms': statistics.median(timings[label]),
                'samples_ms': timings[label],
                'skipped_data_files': skipped,
            }
    for label, results in report['runs'].items():
        print(label + ': ' + json.dumps(results), flush=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
