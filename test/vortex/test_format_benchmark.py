"""Smoke-test the format comparison against real native Iceberg readers/writers."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb', required=True, type=Path)
    args = parser.parse_args()
    script = Path(__file__).resolve().parents[2] / 'scripts/benchmark_vortex_formats.py'
    with tempfile.TemporaryDirectory(prefix='iceberg-format-smoke-') as directory:
        directory = Path(directory)
        output = directory / 'results.json'
        command = [
            sys.executable,
            str(script),
            '--duckdb',
            str(args.duckdb.resolve()),
            '--directory',
            str(directory / 'tables'),
            '--files',
            '2',
            '--rows-per-file',
            '257',
            '--order',
            'permuted',
            '--repeats',
            '1',
            '--output',
            str(output),
        ]
        subprocess.run(command, check=True, timeout=240)
        report = json.loads(output.read_text())
        assert report['rows'] == 514
        for data_format in ('parquet', 'vortex'):
            result = report['formats'][data_format]
            assert result['write']['median']['data_bytes'] > 0
            assert len(list((directory / 'tables' / data_format / 'data').glob('*.' + data_format))) == 2
            assert set(result['queries']) == {
                'projection',
                'selective_range',
                'equality',
                'null_filter',
                'aggregate',
                'join',
                'full_scan',
            }
            for query in result['queries'].values():
                assert len(query['samples']) == 1
                assert query['median']['latency_ms'] > 0
                assert query['median']['peak_rss_bytes'] > 0
        # Reusing an output directory must fail before changing the retained data/report.
        before = output.read_bytes()
        retry = subprocess.run(command, capture_output=True, text=True, timeout=10)
        assert retry.returncode != 0 and '--directory must be new' in retry.stderr, retry
        assert output.read_bytes() == before
    print('Format benchmark smoke test passed')


if __name__ == '__main__':
    main()
