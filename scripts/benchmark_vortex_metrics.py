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


def data_query(start, end):
    return f'SELECT i::BIGINT id, i::DOUBLE x FROM range({start}, {end}) t(i)'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb', action='append', required=True, metavar='LABEL=PATH')
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--files', type=int, default=8)
    parser.add_argument('--rows-per-file', type=int, default=250000)
    parser.add_argument('--repeats', type=int, default=7)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.files < 2 or args.rows_per_file < 1000 or args.repeats < 1:
        parser.error('need at least 2 files, 1000 rows per file and 1 repeat')
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
        'threads': 1,
        'repeats': args.repeats,
        'runs': {},
    }
    rows = args.files * args.rows_per_file
    for index, (label, shell) in enumerate(shells.items()):
        table = args.directory.resolve() / f'table-{index}'
        if table.exists():
            parser.error(f'use a fresh directory: {table} exists')
        writes = []
        for repetition in range(args.repeats + 1):
            with tempfile.TemporaryDirectory(prefix='iceberg-metrics-write-', dir=args.directory) as directory:
                target = Path(directory).resolve() / 'table'
                sql = f'COPY ({data_query(0, rows)}) TO {quote(target)} (FORMAT iceberg, DATA_FORMAT vortex);'
                elapsed = profile(shell, sql, None)
                assert run_sql(
                    shell, f'SELECT count(*) n, sum(id)::BIGINT total FROM iceberg_scan({quote(target)})'
                ) == [{'n': rows, 'total': rows * (rows - 1) // 2}]
                if repetition:
                    writes.append(elapsed)
        run_sql(
            shell,
            f'COPY ({data_query(0, args.rows_per_file)}) TO {quote(table)} ' '(FORMAT iceberg, DATA_FORMAT vortex);',
        )
        metadata = next((table / 'metadata').glob('*.metadata.json'))
        with local_catalog(metadata) as (server, attach):
            for file_index in range(1, args.files):
                start = file_index * args.rows_per_file
                run_sql(shell, attach + 'INSERT INTO lake.main.items ' + data_query(start, start + args.rows_per_file))
            metadata = server.metadata_path
            assert not server.errors, server.errors
        scan = f'iceberg_scan({quote(metadata)})'
        assert run_sql(shell, f'SELECT count(*) n FROM {scan}') == [{'n': rows}]
        assert len(list((table / 'data').glob('*.vortex'))) == args.files
        low = (args.files // 2) * args.rows_per_file + 100
        results = {'write_median_ms': statistics.median(writes), 'queries': {}}
        for name, predicate, expected in (
            ('integer_range', f'id >= {low} AND id < {low + 100}', sum(range(low, low + 100))),
            ('float_range', f'x >= {low} AND x < {low + 100}', sum(range(low, low + 100))),
            ('full_scan', 'true', rows * (rows - 1) // 2),
        ):
            query = f'SELECT sum(id)::BIGINT total FROM {scan} WHERE {predicate};'
            profile(shell, query, [{'total': expected}])
            timings = [profile(shell, query, [{'total': expected}]) for _ in range(args.repeats)]
            skipped = run_sql(
                shell,
                "SET enable_logging=false; CALL enable_logging('Iceberg', storage='memory'); "
                + query
                + "SELECT count(*) skipped FROM duckdb_logs() WHERE type='Iceberg' "
                "AND contains(message, 'skipped ''data_file''');",
            )[0]['skipped']
            results['queries'][name] = {'median_ms': statistics.median(timings), 'skipped_data_files': skipped}
        report['runs'][label] = results
        print(label + ': ' + json.dumps(results), flush=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
