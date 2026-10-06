"""Exercise Vortex appends through the REST catalog API without external services.

Run with a shell built with ICEBERG_ENABLE_VORTEX=ON:
    python3 test/vortex/test_local_catalog.py --duckdb build/vortex/duckdb

The test catalog implements only load-table, snapshot and property commits.
It is not a substitute for qualifying a production catalog's acceptance of Vortex files.
"""

import argparse
import copy
from contextlib import contextmanager
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import subprocess
import tempfile
import threading
from urllib.parse import urlparse


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


class AppendCatalog(HTTPServer):
    def __init__(self, metadata_path):
        super().__init__(("127.0.0.1", 0), CatalogHandler)
        self.metadata_path = metadata_path
        self.metadata = json.loads(metadata_path.read_text())
        self.metadata["properties"]["write.format.default"] = "vortex"
        self.metadata["schemas"][0]["fields"][0]["required"] = True
        self.commits = []
        self.errors = []

    def load_table(self):
        return {"metadata-location": str(self.metadata_path), "metadata": self.metadata}

    def commit(self, body):
        metadata = copy.deepcopy(self.metadata)
        for requirement in body["requirements"]:
            kind = requirement["type"]
            if kind == "assert-table-uuid":
                assert requirement["uuid"] == metadata["table-uuid"]
            elif kind == "assert-ref-snapshot-id":
                assert requirement["ref"] == "main"
                assert requirement["snapshot-id"] == metadata["current-snapshot-id"]
            elif kind == "assert-current-schema-id":
                assert requirement["current-schema-id"] == metadata["current-schema-id"]
            else:
                raise AssertionError(f"Unexpected commit requirement: {requirement}")
        for update in body["updates"]:
            action = update["action"]
            if action == "add-snapshot":
                snapshot = update["snapshot"]
                metadata["snapshots"].append(snapshot)
                metadata["last-sequence-number"] = snapshot["sequence-number"]
                metadata["last-updated-ms"] = snapshot["timestamp-ms"]
            elif action == "set-snapshot-ref":
                assert update["ref-name"] == "main"
                metadata["current-snapshot-id"] = update["snapshot-id"]
                metadata.setdefault("refs", {})["main"] = {
                    "snapshot-id": update["snapshot-id"],
                    "type": "branch",
                }
            elif action == "set-properties":
                metadata["properties"].update(update["updates"])
            else:
                raise AssertionError(f"Unexpected commit update: {update}")
        metadata.setdefault("snapshot-log", []).append(
            {
                "timestamp-ms": metadata["last-updated-ms"],
                "snapshot-id": metadata["current-snapshot-id"],
            }
        )
        path = self.metadata_path.parent / f"test-commit-{len(self.commits)}.metadata.json"
        path.write_text(json.dumps(metadata))
        self.metadata_path = path
        self.metadata = metadata
        self.commits.append(body)
        return self.load_table()


class CatalogHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def reply(self, body, status=200):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/v1/config":
            self.reply(
                {
                    "defaults": {},
                    "overrides": {},
                    "endpoints": [
                        "GET /v1/{prefix}/namespaces",
                        "GET /v1/{prefix}/namespaces/{namespace}",
                        "GET /v1/{prefix}/namespaces/{namespace}/tables",
                        "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
                        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
                    ],
                }
            )
        elif path == "/v1/namespaces":
            self.reply({"namespaces": [["main"]]})
        elif path == "/v1/namespaces/main":
            self.reply({"namespace": ["main"], "properties": {}})
        elif path == "/v1/namespaces/main/tables":
            self.reply({"identifiers": [{"namespace": ["main"], "name": "items"}]})
        elif path == "/v1/namespaces/main/tables/items":
            self.reply(self.server.load_table())
        else:
            self.server.errors.append(f"Unexpected GET {path}")
            self.reply({"error": {"message": path, "type": "NoSuchTableException", "code": 404}}, 404)

    def do_POST(self):
        try:
            assert urlparse(self.path).path == "/v1/namespaces/main/tables/items"
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            self.reply(self.server.commit(body))
        except Exception as error:
            self.server.errors.append(str(error))
            self.reply({"error": {"message": str(error), "type": "CommitFailedException", "code": 409}}, 409)


def run_sql(shell, sql, error=None, cwd=None):
    result = subprocess.run(
        [
            str(shell),
            "-unsigned",
            "-batch",
            "-bail",
            "-json",
            "-c",
            "LOAD parquet; LOAD avro; LOAD vortex; LOAD iceberg; " + sql,
        ],
        text=True,
        capture_output=True,
        timeout=120,
        cwd=cwd,
    )
    if error is not None:
        assert result.returncode > 0, f"Exit {result.returncode}: {result.stderr}\n{result.stdout}"
        assert error in result.stderr, result.stderr
        return None
    assert result.returncode == 0, f"Exit {result.returncode}: {result.stderr}\n{result.stdout}\nSQL: {sql}"
    output = result.stdout.strip()
    if not output:
        return None
    decoder = json.JSONDecoder()
    rows = None
    while output:
        rows, end = decoder.raw_decode(output)
        output = output[end:].strip()
    return rows


@contextmanager
def local_catalog(metadata_path):
    server = AppendCatalog(metadata_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}"
    attach = (
        f"ATTACH '' AS lake (TYPE iceberg, ENDPOINT {quote(endpoint)}, "
        "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none', DEFAULT_SCHEMA 'main'); "
    )
    try:
        yield server, attach
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def check_append(shell, root, initial_format):
    table = root / initial_format
    run_sql(
        shell,
        f"COPY (SELECT i::BIGINT id, 'row-' || i payload FROM range(3) t(i)) "
        f"TO {quote(table)} (FORMAT iceberg, DATA_FORMAT {initial_format});",
    )
    original_path = next((table / "metadata").glob("*.metadata.json"))
    original_contents = original_path.read_bytes()
    with local_catalog(original_path) as (server, attach):
        assert run_sql(shell, attach + "SELECT count(*) n, sum(id)::BIGINT s FROM lake.main.items;") == [
            {"n": 3, "s": 3}
        ]
        run_sql(shell, attach + "INSERT INTO lake.main.items VALUES (3, NULL), (4, 'row-4');")
        assert len(server.commits) == 1
        assert original_path.read_bytes() == original_contents
        assert run_sql(
            shell, attach + "SELECT count(*) n, sum(id)::BIGINT s, count(payload) p FROM lake.main.items;"
        ) == [{"n": 5, "s": 10, "p": 4}]
        assert run_sql(
            shell, attach + "SELECT id FROM lake.main.items WHERE md5(payload) = md5('row-2') ORDER BY id;"
        ) == [{"id": 2}]
        assert run_sql(shell, f"SELECT count(*) n, sum(id)::BIGINT s FROM iceberg_scan({quote(original_path)});") == [
            {"n": 3, "s": 3}
        ]
        formats = run_sql(
            shell,
            f"SELECT DISTINCT file_format FROM iceberg_metadata({quote(server.metadata_path)}) ORDER BY file_format;",
        )
        expected = ["vortex"] if initial_format == "vortex" else ["parquet", "vortex"]
        assert formats == [{"file_format": item} for item in expected], formats
        manifest_list = server.metadata["snapshots"][-1]["manifest-list"]
        manifests = run_sql(shell, f"SELECT manifest_path FROM read_avro({quote(manifest_list)});")
        for manifest in manifests:
            files = run_sql(
                shell,
                "SELECT data_file.file_path path, data_file.file_size_in_bytes size "
                f"FROM read_avro({quote(manifest['manifest_path'])});",
            )
            for file in files:
                assert Path(file["path"]).stat().st_size == file["size"], file
        run_sql(shell, attach + "DELETE FROM lake.main.items WHERE id = 1;", error="append only")
        run_sql(shell, attach + "UPDATE lake.main.items SET payload = 'changed' WHERE id = 1;", error="append only")
        run_sql(
            shell, attach + "INSERT INTO lake.main.items VALUES (NULL, 'invalid');", error="NOT NULL constraint failed"
        )
        assert len(server.commits) == 1
        server.metadata["schemas"].append({**server.metadata["schemas"][0], "schema-id": 1})
        server.metadata["current-schema-id"] = 1
        run_sql(shell, attach + "INSERT INTO lake.main.items VALUES (5, 'row-5');", error="fixed schema")
        run_sql(shell, attach + "SELECT payload FROM lake.main.items ORDER BY id;", error="fixed schema")
        assert len(server.commits) == 1
        assert not server.errors, server.errors


def check_append_after_deletes(shell, root, same_transaction):
    table = root / ("transaction_deletes" if same_transaction else "committed_deletes")
    run_sql(
        shell,
        f"COPY (SELECT i::BIGINT id, 'row-' || i payload FROM range(3) t(i)) TO {quote(table)} (FORMAT iceberg);",
    )
    original_path = next((table / "metadata").glob("*.metadata.json"))
    with local_catalog(original_path) as (server, attach):
        server.metadata["properties"]["write.format.default"] = "parquet"
        if same_transaction:
            # Property updates take effect at commit, so this insert still writes Parquet.
            assert run_sql(
                shell,
                attach + "BEGIN; DELETE FROM lake.main.items WHERE id = 1; "
                "CALL set_iceberg_table_properties(lake.main.items, {'write.format.default': 'vortex'}); "
                "INSERT INTO lake.main.items VALUES (3, 'row-3'); "
                "SELECT value FROM iceberg_table_properties(lake.main.items) WHERE key = 'write.format.default'; "
                "COMMIT;",
            ) == [{"value": "parquet"}]
            assert server.metadata["properties"]["write.format.default"] == "vortex"
            assert not list((table / "data").glob("*.vortex"))
            expected_ids = [{"id": 0}, {"id": 2}, {"id": 3}]
        else:
            run_sql(shell, attach + "DELETE FROM lake.main.items WHERE id = 1;")
            server.metadata["properties"]["write.format.default"] = "vortex"
            expected_ids = [{"id": 0}, {"id": 2}]
        assert len(server.commits) == 1
        manifest_list = server.metadata["snapshots"][-1]["manifest-list"]
        assert run_sql(shell, f"SELECT count(*) n FROM read_avro({quote(manifest_list)}) WHERE content = 1;") == [
            {"n": 1}
        ]
        before_metadata = copy.deepcopy(server.metadata)
        before_files = set(table.rglob("*"))
        run_sql(
            shell,
            attach + "INSERT INTO lake.main.items VALUES (4, 'row-4');",
            error="Vortex Iceberg appends do not yet support delete files",
        )
        assert len(server.commits) == 1
        assert server.metadata == before_metadata
        assert set(table.rglob("*")) == before_files
        assert run_sql(shell, attach + "SELECT id FROM lake.main.items ORDER BY id;") == expected_ids
        server.metadata["properties"]["write.format.default"] = "parquet"
        run_sql(shell, attach + "INSERT INTO lake.main.items VALUES (4, 'row-4');")
        assert len(server.commits) == 2
        assert run_sql(shell, attach + "SELECT id FROM lake.main.items ORDER BY id;") == expected_ids + [{"id": 4}]
        assert not server.errors, server.errors


def check_float_filters(shell, root):
    table = root / "mixed_floats"
    run_sql(
        shell,
        f"COPY (SELECT 1::BIGINT id, '0.0'::FLOAT x, '0.0'::DOUBLE y) TO {quote(table)} (FORMAT iceberg);",
    )
    with local_catalog(next((table / "metadata").glob("*.metadata.json"))) as (server, attach):
        run_sql(shell, attach + "INSERT INTO lake.main.items VALUES (2, '-0.0', '-0.0'), (3, NULL, NULL);")
        for column in ("x", "y"):
            assert run_sql(shell, attach + f"SELECT id FROM lake.main.items WHERE {column} = 0 ORDER BY id;") == [
                {"id": 1},
                {"id": 2},
            ]
            assert run_sql(shell, attach + f"SELECT count(*) n FROM lake.main.items WHERE {column} < 0;") == [{"n": 0}]
        assert len(server.commits) == 1
        assert not server.errors, server.errors


def check_timestamp_appends(shell, root, dtype):
    table = root / f"timestamps_{dtype}"
    run_sql(
        shell,
        f"COPY (SELECT 0::BIGINT id, '2000-01-01'::{dtype} t) TO {quote(table)} (FORMAT iceberg);",
    )
    with local_catalog(next((table / "metadata").glob("*.metadata.json"))) as (server, attach):
        before_metadata = copy.deepcopy(server.metadata)
        for value in ("-infinity", "infinity", "30000-01-01"):
            run_sql(
                shell,
                attach + f"INSERT INTO lake.main.items VALUES (1, {quote(value)}::{dtype});",
                error="Vortex Iceberg timestamp value is outside the supported range",
            )
            assert not server.commits
            assert server.metadata == before_metadata
        assert run_sql(shell, attach + "SELECT id FROM lake.main.items;") == [{"id": 0}]
        run_sql(shell, attach + f"INSERT INTO lake.main.items VALUES (1, '2001-01-01'::{dtype}), (2, NULL);")
        assert run_sql(shell, attach + "SELECT count(*) n, count(t) timestamps FROM lake.main.items;") == [
            {"n": 3, "timestamps": 2}
        ]
        assert len(server.commits) == 1
        assert not server.errors, server.errors


def check_time_appends(shell, root):
    table = root / "times"
    run_sql(
        shell,
        f"COPY (SELECT 0::BIGINT id, TIME '24:00:00' t) TO {quote(table)} (FORMAT iceberg);",
    )
    with local_catalog(next((table / "metadata").glob("*.metadata.json"))) as (server, attach):
        before_metadata = copy.deepcopy(server.metadata)
        for insert in (
            "INSERT INTO lake.main.items VALUES (1, TIME '24:00:00');",
            "INSERT INTO lake.main.items SELECT i + 1, "
            "CASE WHEN i = 2048 THEN TIME '24:00:00' ELSE TIME '12:00:00' END FROM range(4099) v(i);",
        ):
            run_sql(shell, attach + insert, error="Vortex Iceberg time value is outside the supported range")
            assert not server.commits
            assert server.metadata == before_metadata
            assert run_sql(shell, attach + "SELECT id, t::VARCHAR t FROM lake.main.items;") == [
                {"id": 0, "t": "24:00:00"}
            ]
        run_sql(
            shell,
            attach + "INSERT INTO lake.main.items VALUES (1, TIME '00:00:00'), "
            "(2, TIME '23:59:59.999999'), (3, NULL);",
        )
        assert run_sql(shell, attach + "SELECT id, t::VARCHAR t FROM lake.main.items ORDER BY id;") == [
            {"id": 0, "t": "24:00:00"},
            {"id": 1, "t": "00:00:00"},
            {"id": 2, "t": "23:59:59.999999"},
            {"id": 3, "t": None},
        ]
        assert len(server.commits) == 1
        assert not server.errors, server.errors


def check_local_paths(shell, root):
    error = "Vortex Iceberg data paths cannot contain URL-escaped characters"
    for name in ("space name", "hash#name", "percent%name", "encoded%20name", "unicode-\u76ee\u5f55"):
        table = root / name
        run_sql(
            shell, f"COPY (SELECT 1::BIGINT id) TO {quote(table)} (FORMAT iceberg, DATA_FORMAT vortex);", error=error
        )
        assert not any(path.is_file() for path in table.rglob("*"))
        run_sql(shell, f"COPY (SELECT 1::BIGINT id) TO {quote(table)} (FORMAT iceberg);")
        with local_catalog(next((table / "metadata").glob("*.metadata.json"))) as (server, attach):
            before_metadata = copy.deepcopy(server.metadata)
            before_files = set(table.rglob("*"))
            run_sql(shell, attach + "INSERT INTO lake.main.items VALUES (2);", error=error)
            assert not server.commits
            assert server.metadata == before_metadata
            assert set(table.rglob("*")) == before_files
            assert run_sql(shell, attach + "SELECT id FROM lake.main.items;") == [{"id": 1}]
            server.metadata["properties"]["write.format.default"] = "parquet"
            run_sql(shell, attach + "INSERT INTO lake.main.items VALUES (2);")
            assert len(server.commits) == 1
            assert run_sql(shell, attach + "SELECT id FROM lake.main.items ORDER BY id;") == [{"id": 1}, {"id": 2}]
            assert not server.errors, server.errors

    # Relative paths inherit any unsafe characters in the current working directory.
    working_directory = root / "working directory"
    working_directory.mkdir()
    run_sql(
        shell,
        "COPY (SELECT 1::BIGINT id) TO 'relative_table' (FORMAT iceberg, DATA_FORMAT vortex);",
        error=error,
        cwd=working_directory,
    )
    assert not any(path.is_file() for path in working_directory.rglob("*"))

    # Punctuation that is preserved in a file URL should still be usable.
    safe_relative_path = "safe-+'&=@,;()"
    run_sql(
        shell,
        f"COPY (SELECT 42::BIGINT id) TO {quote(safe_relative_path)} (FORMAT iceberg, DATA_FORMAT vortex);",
        cwd=root,
    )
    assert run_sql(shell, f"SELECT id FROM iceberg_scan({quote(safe_relative_path)});", cwd=root) == [{"id": 42}]


def check_aggregate_appends(shell, root):
    table = root / "aggregate_appends"
    run_sql(shell, f"COPY (SELECT 0::BIGINT id, 0::DECIMAL(38,0) total) TO {quote(table)} (FORMAT iceberg);")
    with local_catalog(next((table / "metadata").glob("*.metadata.json"))) as (server, attach):
        run_sql(shell, attach + "INSERT INTO lake.main.items SELECT 1, sum(i) FROM range(10) t(i);")
        run_sql(shell, attach + "INSERT INTO lake.main.items SELECT 2, sum(i) FROM range(0) t(i);")
        assert run_sql(shell, attach + "SELECT id, total::VARCHAR total FROM lake.main.items ORDER BY id;") == [
            {"id": 0, "total": "0"},
            {"id": 1, "total": "45"},
            {"id": 2, "total": None},
        ]
        assert len(server.commits) == 2
        assert not server.errors, server.errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path)
    args = parser.parse_args()
    shell = args.duckdb.resolve()
    with tempfile.TemporaryDirectory(prefix="iceberg-vortex-") as directory:
        for initial_format in ("vortex", "parquet"):
            check_append(shell, Path(directory), initial_format)
        for same_transaction in (False, True):
            check_append_after_deletes(shell, Path(directory), same_transaction)
        check_float_filters(shell, Path(directory))
        for dtype in ("TIMESTAMP", "TIMESTAMPTZ"):
            check_timestamp_appends(shell, Path(directory), dtype)
        check_time_appends(shell, Path(directory))
        check_local_paths(shell, Path(directory))
        check_aggregate_appends(shell, Path(directory))
    print("Vortex catalog append, filter, delete, temporal, path and cast regressions passed.")


if __name__ == "__main__":
    main()
