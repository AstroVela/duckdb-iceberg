#!/usr/bin/env python3
"""Compare Parquet and Vortex Iceberg tables using one native DuckDB build."""

import argparse
import json
import math
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'test/vortex'))
from test_local_catalog import local_catalog, quote

FORMATS = ('parquet', 'vortex')


def decode_results(output):
    decoder = json.JSONDecoder()
    rows = None
    output = output.strip()
    while output:
        rows, end = decoder.raw_decode(output)
        output = output[end:].strip()
    return rows


class Runner:
    def __init__(self, shell, threads, memory_limit, time_binary):
        self.shell = str(shell)
        self.threads = threads
        self.memory_limit = memory_limit
        self.time_binary = time_binary

    def run(self, sql, *, measured=False, threads=None):
        with tempfile.TemporaryDirectory(prefix='iceberg-format-profile-') as directory:
            directory = Path(directory)
            profile_path = directory / 'profile.json'
            usage_path = directory / 'usage.txt'
            setup = (
                f'SET threads={threads or self.threads}; '
                f'SET memory_limit={quote(self.memory_limit)}; '
                'SET preserve_insertion_order=true; '
            )
            if measured:
                setup += (
                    "SET enable_profiling='json'; "
                    "SET profiling_mode='detailed'; "
                    f'SET profiling_output={quote(profile_path)}; '
                )
            command = [self.shell, '-no-init', '-unsigned', '-batch', '-bail', '-json']
            if measured:
                # GNU time measures the complete child, including Rust allocations.
                command = [self.time_binary, '-f', '%M %I', '-o', str(usage_path), *command]
            result = subprocess.run(command, input=setup + sql, text=True, capture_output=True, timeout=600)
            if result.returncode:
                raise RuntimeError(f'DuckDB exited with {result.returncode}: {result.stderr}\nSQL: {sql}')
            rows = decode_results(result.stdout)
            if not measured:
                return rows
            profile = json.loads(profile_path.read_text())
            peak_kib, input_blocks = map(int, usage_path.read_text().split())
            return rows, {
                'latency_ms': profile['latency'] * 1000,
                'peak_rss_bytes': peak_kib * 1024,
                'os_read_bytes': input_blocks * 512,
                'duckdb_reported_bytes_read': profile.get('total_bytes_read'),
                'duckdb_peak_buffer_memory_bytes': profile.get('system_peak_buffer_memory'),
            }


def data_query(start, end, rows, order):
    multiplier = 104729
    while math.gcd(multiplier, rows) != 1:
        multiplier += 2
    identifier = 'i' if order == 'sorted' else f'(i * {multiplier}) % {rows}'
    return (
        'SELECT id::BIGINT id, (id % 128)::INTEGER category, '
        'CASE WHEN id % 17 = 0 THEN NULL ELSE (id % 100000)::DOUBLE / 10 END measurement, '
        "'value-' || ((id * 48271) % 1000003)::VARCHAR payload, id % 2 = 0 flag "
        f'FROM (SELECT {identifier} id FROM range({start}, {end}) t(i)) source'
    )


def query_cases(rows):
    low = rows // 2
    high = min(rows, low + max(1, rows // 1000))
    return {
        'projection': 'SELECT sum(id)::BIGINT total FROM {source}',
        'selective_range': (
            'SELECT count(*) n, sum(length(payload))::BIGINT bytes FROM {source} ' f'WHERE id >= {low} AND id < {high}'
        ),
        'equality': 'SELECT count(*) n, sum(id)::BIGINT total FROM {source} WHERE category = 42',
        'null_filter': 'SELECT count(*) n, sum(id)::BIGINT total FROM {source} WHERE measurement IS NULL',
        'aggregate': (
            'SELECT category, count(*) n, sum(id)::BIGINT total, '
            'sum(measurement)::DECIMAL(38,2)::VARCHAR amount FROM {source} GROUP BY category ORDER BY category'
        ),
        'join': (
            'SELECT sum(e.id)::BIGINT total, sum(length(e.payload))::BIGINT bytes '
            'FROM {source} e JOIN (VALUES (3), (42), (99)) d(category) USING (category)'
        ),
        'full_scan': (
            'SELECT count(*) n, sum(id)::BIGINT total, sum(category)::BIGINT categories, '
            'count(measurement) nonnull, sum(measurement)::DECIMAL(38,2)::VARCHAR amount, '
            'sum(length(payload))::BIGINT bytes, count(*) FILTER (WHERE flag) flags, '
            'bit_xor(hash(id, category, measurement, payload, flag)) checksum FROM {source}'
        ),
    }


def summarize(samples):
    return {
        'median': {
            key: (
                statistics.median(sample[key] for sample in samples)
                if all(sample[key] is not None for sample in samples)
                else None
            )
            for key in samples[0]
        },
        'samples': samples,
    }


def create_table(runner, table, data_format, args):
    rows = args.files * args.rows_per_file
    _, initial = runner.run(
        f'COPY ({data_query(0, args.rows_per_file, rows, args.order)}) TO {quote(table)} '
        f'(FORMAT iceberg, DATA_FORMAT {data_format});',
        measured=True,
        threads=1,
    )
    profiles = [initial]
    metadata = next((table / 'metadata').glob('*.metadata.json'))
    with local_catalog(metadata) as (server, attach):
        # The append fixture normally selects Vortex; explicitly select both formats.
        server.metadata['properties']['write.format.default'] = data_format
        for index in range(1, args.files):
            start = index * args.rows_per_file
            _, profile = runner.run(
                attach
                + 'INSERT INTO lake.main.items '
                + data_query(start, start + args.rows_per_file, rows, args.order),
                measured=True,
                threads=1,
            )
            profiles.append(profile)
        metadata = server.metadata_path
        if server.errors:
            raise RuntimeError(f'Benchmark catalog failed: {server.errors}')
    files = list((table / 'data').glob('*'))
    if len(files) != args.files or any(path.suffix != '.' + data_format for path in files):
        raise RuntimeError(f'{data_format}: expected {args.files} data files, found {files}')
    scan = f'iceberg_scan({quote(metadata)})'
    latency = sum(profile['latency_ms'] for profile in profiles)
    if latency <= 0:
        raise RuntimeError('Write profiling returned a nonpositive latency')
    return scan, {
        'latency_ms': latency,
        'rows_per_second': rows * 1000 / latency,
        'peak_rss_bytes': max(profile['peak_rss_bytes'] for profile in profiles),
        'data_bytes': sum(path.stat().st_size for path in files),
        'metadata_bytes': sum(path.stat().st_size for path in (table / 'metadata').iterdir() if path.is_file()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb', required=True, type=Path)
    parser.add_argument('--directory', required=True, type=Path, help='new directory for the retained benchmark tables')
    parser.add_argument('--files', type=int, default=8)
    parser.add_argument('--rows-per-file', type=int, default=250000)
    parser.add_argument('--order', choices=('sorted', 'permuted'), default='sorted')
    parser.add_argument(
        '--threads', type=int, default=1, help='query threads; writes use one thread for equal file counts'
    )
    parser.add_argument('--memory-limit', default='1GB', help='DuckDB buffer manager limit, not a process memory limit')
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if min(args.files, args.rows_per_file, args.threads, args.repeats) < 1:
        parser.error('files, rows-per-file, threads and repeats must be positive')
    if platform.system() != 'Linux' or not shutil.which('time'):
        parser.error('this benchmark requires Linux and GNU time for comparable process memory measurements')
    if not args.duckdb.is_file():
        parser.error('--duckdb must name a native DuckDB shell built with ICEBERG_ENABLE_VORTEX=ON')
    if args.directory.exists():
        parser.error('--directory must be new; existing benchmark data is never overwritten')
    args.directory = args.directory.resolve()
    args.directory.mkdir(parents=True)
    runner = Runner(args.duckdb.resolve(), args.threads, args.memory_limit, shutil.which('time'))
    rows = args.files * args.rows_per_file
    queries = query_cases(rows)
    reference = f'({data_query(0, rows, rows, args.order)})'
    expected = {name: runner.run(query.format(source=reference)) for name, query in queries.items()}
    report = {
        'duckdb': str(args.duckdb.resolve()),
        'version': runner.run('PRAGMA version;'),
        'extensions': runner.run('SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded;'),
        'platform': platform.platform(),
        'rows': rows,
        'files': args.files,
        'rows_per_file': args.rows_per_file,
        'order': args.order,
        'query_threads': args.threads,
        'write_threads': 1,
        'memory_limit': args.memory_limit,
        'repeats': args.repeats,
        'cache': 'warm OS cache, fresh connection/process per query; no cache eviction',
        'notes': [
            'Default compression for each format; identical input rows and order, schema and data file count.',
            'Write latency sums COPY/INSERT query profiles, including source generation; REST catalog is a local fixture.',
            'Peak RSS covers the entire shell including Rust; memory_limit only limits the DuckDB buffer manager.',
            'OS read bytes are physical Linux block reads and may be zero with a warm cache.',
            'DuckDB reported I/O and buffer memory do not cover all Vortex allocations or reads; do not compare them as totals.',
        ],
        'formats': {data_format: {'queries': {}} for data_format in FORMATS},
    }
    writes = {data_format: [] for data_format in FORMATS}
    scans = {}
    for repetition in range(args.repeats + 1):
        # The first pair is warmup. Alternate first writer to reduce order bias.
        order = FORMATS if repetition % 2 == 0 else tuple(reversed(FORMATS))
        with tempfile.TemporaryDirectory(prefix='write-', dir=args.directory) as temporary:
            for data_format in order:
                parent = args.directory if repetition == args.repeats else Path(temporary)
                scan, sample = create_table(runner, parent / data_format, data_format, args)
                actual = runner.run(queries['full_scan'].format(source=scan))
                if actual != expected['full_scan']:
                    raise AssertionError(f'{data_format} write validation: {actual} != {expected["full_scan"]}')
                if repetition:
                    writes[data_format].append(sample)
                if repetition == args.repeats:
                    scans[data_format] = scan
        print(f'Write pair {repetition}/{args.repeats} validated', flush=True)
    for data_format in FORMATS:
        report['formats'][data_format]['write'] = summarize(writes[data_format])
    for name, query in queries.items():
        samples = {data_format: [] for data_format in FORMATS}
        for repetition in range(args.repeats + 1):
            order = FORMATS if repetition % 2 == 0 else tuple(reversed(FORMATS))
            for data_format in order:
                actual, sample = runner.run(query.format(source=scans[data_format]), measured=True)
                if actual != expected[name]:
                    raise AssertionError(f'{data_format} {name}: {actual} != {expected[name]}')
                if repetition:
                    samples[data_format].append(sample)
        for data_format in FORMATS:
            report['formats'][data_format]['queries'][name] = summarize(samples[data_format])
        print(f'{name}: both formats validated', flush=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(f'Report: {args.output}', flush=True)


if __name__ == '__main__':
    main()
