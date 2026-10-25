#!/usr/bin/env python3
"""Compare native Vortex filter changes against identical retained benchmark data."""

import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess

from benchmark_vortex_formats import decode_results, query_cases, quote


def checksum(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def latest_metadata(directory):
    candidates = list((directory / 'metadata').glob('*.metadata.json'))
    if not candidates:
        raise ValueError(f'No benchmark metadata in {directory}')
    return max(candidates, key=lambda path: json.loads(path.read_text()).get('last-sequence-number', 0))


def run_profile(binary, query, destination):
    setup = (
        "SET threads=1; SET enable_profiling='json'; SET profiling_mode='detailed'; "
        f'SET profiling_output={quote(destination)}; '
    )
    result = subprocess.run(
        [str(binary), '-no-init', '-unsigned', '-batch', '-bail', '-json'],
        input=setup + query + ';',
        text=True,
        capture_output=True,
        timeout=120,
    )
    if result.returncode:
        raise RuntimeError(result.stderr)
    profile = json.loads(destination.read_text())
    scans = []

    def walk(node):
        if node.get('extra_info', {}).get('Function') == 'ICEBERG_SCAN':
            scans.append(node)
        for child in node.get('children', []):
            walk(child)

    walk(profile)
    if len(scans) != 1:
        raise ValueError(f'Expected one Iceberg scan: {destination}')
    return decode_results(result.stdout), {
        'latency_ms': profile['latency'] * 1000,
        'cpu_ms': profile['cpu_time'] * 1000,
        'binding_ms': profile['planner_binding'] * 1000,
        'scan_output_rows': scans[0]['operator_cardinality'],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--before', required=True, type=Path)
    parser.add_argument('--after', required=True, type=Path)
    parser.add_argument('--data-directory', required=True, type=Path, help='retained format benchmark directory')
    parser.add_argument('--output-directory', required=True, type=Path)
    parser.add_argument('--rows', type=int, default=2000000)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--queries', nargs='+', choices=tuple(query_cases(1)))
    args = parser.parse_args()
    if min(args.rows, args.repeats) < 1:
        parser.error('rows and repeats must be positive')
    if not args.before.is_file() or not args.after.is_file():
        parser.error('before and after must name native DuckDB binaries')
    if args.output_directory.exists():
        parser.error('output-directory must be new')
    args.output_directory.mkdir(parents=True)
    before, after = args.before.resolve(), args.after.resolve()
    data = args.data_directory.resolve()
    metadata = {fmt: latest_metadata(data / fmt) for fmt in ('parquet', 'vortex')}
    variants = {
        'before_vortex': (before, 'vortex'),
        'after_vortex': (after, 'vortex'),
        'after_parquet': (after, 'parquet'),
    }
    report = {
        'before': {'path': str(before), 'sha256': checksum(before)},
        'after': {'path': str(after), 'sha256': checksum(after)},
        'metadata': {fmt: {'path': str(path), 'sha256': checksum(path)} for fmt, path in metadata.items()},
        'rows': args.rows,
        'repeats': args.repeats,
        'threads': 1,
        'cache': 'warm OS cache, fresh process per query; one warmup, rotating variant order',
        'queries': {},
    }
    queries = query_cases(args.rows)
    if args.queries:
        queries = {name: queries[name] for name in args.queries}
    for name, template in queries.items():
        samples = {variant: [] for variant in variants}
        expected = None
        for repetition in range(args.repeats + 1):
            order = list(variants)
            shift = repetition % len(order)
            for variant in order[shift:] + order[:shift]:
                binary, fmt = variants[variant]
                source = f'iceberg_scan({quote(metadata[fmt])})'
                destination = args.output_directory.resolve() / f'{name}-{variant}-{repetition}.json'
                actual, sample = run_profile(binary, template.format(source=source), destination)
                if expected is None:
                    expected = actual
                if actual != expected:
                    raise AssertionError(f'{name} {variant}: result mismatch: {actual} != {expected}')
                if repetition:
                    samples[variant].append(sample)
        report['queries'][name] = {
            variant: {
                'median': {key: statistics.median(sample[key] for sample in values) for key in values[0]},
                'samples': values,
            }
            for variant, values in samples.items()
        }
        print(
            name,
            {variant: round(result['median']['latency_ms'], 3) for variant, result in report['queries'][name].items()},
            flush=True,
        )
    destination = args.output_directory / 'results.json'
    destination.write_text(json.dumps(report, indent=2) + '\n')
    print(f'Report: {destination}', flush=True)


if __name__ == '__main__':
    main()
