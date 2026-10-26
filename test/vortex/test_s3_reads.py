"""Check authenticated range reads, snapshots and failure handling without external services."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest

from s3_test_server import s3_server
from test_local_catalog import local_catalog, quote
from test_parquet_pushdown import run_sql


DATA_FIELDS = {
    'content': '134',
    'file_path': '100',
    'file_format': '101',
    'partition': '{__duckdb_field_id: 102}',
    'record_count': '103',
    'file_size_in_bytes': '104',
    'null_value_counts': '{__duckdb_field_id: 110, key: 121, value: 122}',
    'value_counts': '{__duckdb_field_id: 109, key: 119, value: 120}',
}
LIST_FIELDS = {
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


def upload_table(shell, metadata_path, server, destination):
    """Rewrite real Avro manifests to exact S3 paths and publish all retained snapshots."""
    metadata = json.loads(metadata_path.read_text())
    table = Path(metadata['location'])
    prefix = table.name + '/'
    for path in (table / 'data').iterdir():
        server.objects[prefix + 'data/' + path.name] = path.read_bytes()
    rewritten = {}
    for index, snapshot in enumerate(metadata['snapshots']):
        manifest_list = snapshot['manifest-list']
        manifests = run_sql(shell, f'SELECT manifest_path path FROM read_avro({quote(manifest_list)})')
        for entry in manifests:
            old_path = entry['path']
            if old_path in rewritten:
                continue
            local = destination / f'manifest-{len(rewritten)}.avro'
            key = prefix + 'metadata/' + local.name
            fields = dict(DATA_FIELDS)
            columns = ', '.join(
                (
                    f'{name}: replace(data_file.file_path, {quote(str(table) + "/")}, {quote(server.uri(prefix))})'
                    if name == 'file_path'
                    else f'{name}: data_file.{name}'
                )
                for name in fields
            )
            ids = ', '.join(f'{name}: {value}' for name, value in fields.items())
            run_sql(
                shell,
                f'COPY (SELECT status, snapshot_id, sequence_number, file_sequence_number, '
                f'{{{columns}}} data_file FROM read_avro({quote(old_path)})) TO {quote(local)} '
                '(FORMAT avro, FIELD_IDS {status: 0, snapshot_id: 1, sequence_number: 3, file_sequence_number: 4, '
                f'data_file: {{__duckdb_field_id: 2, {ids}}}}});',
            )
            server.objects[key] = local.read_bytes()
            rewritten[old_path] = (server.uri(key), local.stat().st_size)
        paths = (
            'CASE manifest_path '
            + ' '.join(f'WHEN {quote(old)} THEN {quote(new)}' for old, (new, _) in rewritten.items())
            + ' END'
        )
        sizes = (
            'CASE manifest_path '
            + ' '.join(f'WHEN {quote(old)} THEN {size}' for old, (_, size) in rewritten.items())
            + ' END'
        )
        local_list = destination / f'list-{index}.avro'
        columns = ', '.join(LIST_FIELDS)
        ids = ', '.join(f'{name}: {value}' for name, value in LIST_FIELDS.items())
        run_sql(
            shell,
            f'COPY (SELECT * REPLACE ({paths} AS manifest_path, ({sizes})::BIGINT AS manifest_length) '
            f'FROM (SELECT {columns} FROM read_avro({quote(manifest_list)}))) TO {quote(local_list)} '
            f'(FORMAT avro, FIELD_IDS {{{ids}}});',
        )
        key = prefix + 'metadata/' + local_list.name
        server.objects[key] = local_list.read_bytes()
        snapshot['manifest-list'] = server.uri(key)
    metadata['location'] = server.uri(prefix.rstrip('/'))
    local_metadata = destination / 'table.metadata.json'
    local_metadata.write_text(json.dumps(metadata))
    key = prefix + 'metadata/table.metadata.json'
    server.objects[key] = local_metadata.read_bytes()
    return server.uri(key), local_metadata


class S3ReadTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='iceberg-s3-read-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.server = self.enterContext(s3_server())

    def secret(self, *, secret_key=None, token=None, scope=None, name='iceberg_s3'):
        server = self.server
        return (
            f'CREATE OR REPLACE SECRET {name} (TYPE s3, KEY_ID {quote(server.access_key)}, '
            f'SECRET {quote(secret_key or server.secret_key)}, '
            f'SESSION_TOKEN {quote(token or server.session_token)}, REGION \'us-east-1\', '
            f'ENDPOINT {quote("127.0.0.1:" + str(server.server_port))}, USE_SSL false, URL_STYLE \'path\', '
            f'SCOPE {quote(scope or server.uri(""))}); '
        )

    def command(self, sql):
        return [
            str(DUCKDB),
            '-no-init',
            '-unsigned',
            '-batch',
            '-bail',
            '-json',
            '-c',
            'SET allow_persistent_secrets=false; '
            "SET threads=1; SET http_proxy=''; SET http_retries=0; SET http_timeout=3; "
            f'SET s3_endpoint={quote("127.0.0.1:" + str(self.server.server_port))}; '
            "SET s3_url_style='path'; SET s3_use_ssl=false; SET s3_region='us-east-1'; "
            'SET force_download=false; SET force_download_threshold=0; '
            'SET auto_fallback_to_full_download=false; ' + sql,
        ]

    def environment(self):
        # The test must prove connection-scoped credentials, without ambient AWS fallback.
        env = {key: value for key, value in os.environ.items() if not key.startswith('AWS_')}
        env['AWS_EC2_METADATA_DISABLED'] = 'true'
        return env

    def sql(self, sql, *, error=None):
        result = subprocess.run(self.command(sql), capture_output=True, text=True, env=self.environment(), timeout=30)
        for credential in (self.server.secret_key, self.server.session_token):
            self.assertNotIn(credential, result.stdout + result.stderr, 'credential leaked into query output')
        if error:
            self.assertNotEqual(result.returncode, 0, 'expected the remote read to fail')
            self.assertIn(error, result.stderr)
            return None
        self.assertEqual(result.returncode, 0, result.stderr)
        output = result.stdout.strip()
        rows = None
        while output:
            rows, end = json.JSONDecoder().raw_decode(output)
            output = output[end:].strip()
        return rows

    def table(self, *, mixed=False, large=False):
        table = self.root / 'table'
        query = (
            "SELECT i::BIGINT id, sha256('a' || i) || sha256('b' || i) || "
            "sha256('c' || i) || sha256('d' || i) payload FROM range(100000) t(i)"
            if large
            else "SELECT * FROM (VALUES (1::BIGINT, 0.0::DOUBLE), (2, '-0.0'::DOUBLE), "
            "(3, 'NaN'::DOUBLE), (4, NULL::DOUBLE)) t(id, x)"
        )
        run_sql(DUCKDB, f'COPY ({query}) TO {quote(table)} (FORMAT iceberg, DATA_FORMAT vortex);')
        metadata = next((table / 'metadata').glob('*.metadata.json'))
        old_snapshot = json.loads(metadata.read_text())['current-snapshot-id']
        if mixed:
            with local_catalog(metadata) as (catalog, attach):
                catalog.metadata['properties']['write.format.default'] = 'parquet'
                run_sql(DUCKDB, attach + 'INSERT INTO lake.main.items VALUES (5, 1), (6, NULL);')
                metadata = catalog.metadata_path
                self.assertFalse(catalog.errors)
        destination = self.root / 'remote'
        destination.mkdir()
        remote, local = upload_table(DUCKDB, metadata, self.server, destination)
        return f'iceberg_scan({quote(remote)})', remote, local, old_snapshot

    def test_temporary_credentials_and_range_projection(self):
        scan, _, _, _ = self.table(large=True)
        actual = self.sql(self.secret() + f'SELECT sum(id)::BIGINT n FROM {scan} WHERE id >= 100 AND id < 200;')
        self.assertEqual(actual, [{'n': sum(range(100, 200))}])
        key = next(key for key in self.server.objects if key.endswith('.vortex'))
        reads = [request for request in self.server.requests if request['key'] == key and request['method'] == 'GET']
        self.assertTrue(reads)
        self.assertTrue(all(request['status'] == 206 and request['range'] for request in reads), reads)
        self.assertLess(sum(request['bytes'] for request in reads), len(self.server.objects[key]) // 2)

    def test_mixed_snapshots_and_catalog(self):
        scan, remote, local, old = self.table(mixed=True)
        query = f'SELECT id FROM {scan} WHERE x = 0 OR x IS NULL ORDER BY id;'
        self.assertEqual(self.sql(self.secret() + query), [{'id': i} for i in (1, 2, 4, 6)])
        self.assertEqual(self.sql(self.secret() + f'SELECT id FROM {scan} WHERE isnan(x);'), [{'id': 3}])
        self.assertEqual(
            self.sql(self.secret() + f'SELECT count(*) n FROM iceberg_scan({quote(remote)}, snapshot_from_id={old});'),
            [{'n': 4}],
        )
        with local_catalog(local) as (catalog, attach):
            self.assertEqual(
                self.sql(self.secret() + attach + 'SELECT sum(id)::BIGINT n FROM lake.main.items;'), [{'n': 21}]
            )
            self.assertFalse(catalog.errors)

    def test_physical_row_positions_with_native_filter(self):
        scan, _, _, _ = self.table(large=True)
        self.assertEqual(
            self.sql(
                self.secret() + f'SELECT file_row_number pos, id FROM {scan} '
                'WHERE id >= 50001 AND id < 50004 ORDER BY pos;'
            ),
            [{'pos': i, 'id': i} for i in range(50001, 50004)],
        )
        self.assertEqual(self.sql(self.secret() + f'SELECT max(file_row_number) pos FROM {scan};'), [{'pos': 99999}])

    def test_mixed_files_keep_original_row_positions(self):
        scan, remote, _, old = self.table(mixed=True)
        self.assertEqual(
            self.sql(self.secret() + f'SELECT id, file_row_number pos FROM {scan} WHERE id >= 2 ORDER BY id;'),
            [{'id': i, 'pos': pos} for i, pos in ((2, 1), (3, 2), (4, 3), (5, 0), (6, 1))],
        )
        self.assertEqual(
            self.sql(
                self.secret() + f'SELECT file_row_number pos FROM iceberg_scan({quote(remote)}, '
                f'snapshot_from_id={old}) WHERE x IS NULL;'
            ),
            [{'pos': 3}],
        )

    def test_credentials_are_scoped_and_rechecked(self):
        scan, _, _, _ = self.table()
        query = f'SELECT sum(id)::BIGINT n FROM {scan};'
        self.sql(self.secret(secret_key='wrong-test-secret') + query, error='403')
        self.sql(self.secret(token='wrong-test-token') + query, error='403')
        self.sql(self.secret(scope='s3://different-bucket/') + query, error='403')
        self.assertEqual(self.sql(self.secret() + query), [{'n': 10}])
        # Bind with working credentials, then invalidate only the Vortex object's scoped secret.
        key = next(key for key in self.server.objects if key.endswith('.vortex'))
        self.sql(
            self.secret()
            + f'PREPARE q AS {query} EXECUTE q; '
            + self.secret(secret_key='wrong-test-secret', scope=self.server.uri(key), name='data_only')
            + 'EXECUTE q;',
            error='403',
        )

    def test_prepared_access_policy_is_rechecked(self):
        scan, _, _, _ = self.table()
        self.sql(
            self.secret() + f'PREPARE q AS SELECT sum(id)::BIGINT n FROM {scan}; EXECUTE q; '
            "SET disabled_filesystems='S3FileSystem'; EXECUTE q;",
            error='disabled',
        )

    def test_integer_filter_keeps_signed_zero_and_nulls(self):
        scan, _, _, _ = self.table()
        self.assertEqual(
            self.sql(self.secret() + f'SELECT id FROM {scan} WHERE id >= 2 AND (x = 0 OR x IS NULL) ORDER BY id;'),
            [{'id': 2}, {'id': 4}],
        )

    def test_short_read_fails_without_partial_results(self):
        scan, _, _, _ = self.table()
        self.server.short_read_key = next(key for key in self.server.objects if key.endswith('.vortex'))
        self.sql(self.secret() + f'SELECT sum(id)::BIGINT n FROM {scan};', error='HTTP')

    def test_remote_append_remains_rejected(self):
        _, _, local, _ = self.table()
        with local_catalog(local) as (catalog, attach):
            before = dict(self.server.objects)
            self.sql(
                self.secret() + attach + 'INSERT INTO lake.main.items VALUES (5, 1);', error='literal local file paths'
            )
            self.assertEqual(catalog.commits, [])
            self.assertEqual(self.server.objects, before)

    @unittest.skipUnless(os.name == 'posix', 'requires shell SIGINT support')
    def test_interrupt_during_range_read(self):
        scan, _, _, _ = self.table()
        self.server.block_key = next(key for key in self.server.objects if key.endswith('.vortex'))
        process = subprocess.Popen(
            self.command(self.secret() + f'SELECT sum(id)::BIGINT n FROM {scan};'),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=self.environment(),
        )
        try:
            self.assertTrue(self.server.blocked.wait(timeout=10), 'query did not reach the Vortex range request')
            process.send_signal(signal.SIGINT)
            self.server.release.set()
            stdout, stderr = process.communicate(timeout=10)
            # The shell suppresses further output after SIGINT; a handled query error exits 1.
            self.assertEqual(process.returncode, 1, stderr)
            self.assertEqual(json.loads(stdout), [{'Success': True}], 'interrupted query returned rows')
            self.assertNotIn(self.server.secret_key, stdout + stderr)
            self.assertNotIn(self.server.session_token, stdout + stderr)
        finally:
            self.server.release.set()
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=10)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb', required=True, type=Path)
    args, remaining = parser.parse_known_args()
    DUCKDB = args.duckdb.resolve()
    unittest.main(argv=[__file__, *remaining])
