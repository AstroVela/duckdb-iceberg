"""Metadata guards and opt-in real Iceberg/Tantivy integration tests.

python3 -m unittest discover -s test/vortex -p test_fts.py -v
For native integration set VORTEX_EXTENSION and ICEBERG_EXTENSION to artifacts
built from the paired PRs, and run from the repository root with DuckDB installed.
"""
import copy
import json
import os
from pathlib import Path
import tempfile
import subprocess
import sys
from contextlib import contextmanager
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
if "--duckdb" in sys.argv:
    position = sys.argv.index("--duckdb")
    os.environ["FTS_DUCKDB_SHELL"] = str(Path(sys.argv[position + 1]).resolve())
    del sys.argv[position:position + 2]
from tools import iceberg_fts as fts


class ShellConnection:
    def execute(self, sql):
        result = subprocess.run([os.environ["FTS_DUCKDB_SHELL"], "-unsigned", "-batch", "-bail", "-json", "-c",
            "LOAD parquet; LOAD avro; LOAD vortex; LOAD iceberg; " + sql], text=True, capture_output=True)
        if result.returncode:
            raise RuntimeError(result.stderr)
        output = result.stdout.strip()
        rows = []
        while output:
            rows, end = json.JSONDecoder().raw_decode(output)
            output = output[end:].strip()
        self.rows = [tuple(row.values()) for row in rows]
        return self

    def fetchall(self):
        return self.rows


@contextmanager
def connection():
    if os.environ.get("FTS_DUCKDB_SHELL"):
        yield ShellConnection()
    else:
        import duckdb
        with duckdb.connect(config={"allow_unsigned_extensions": "true"}) as conn:
            conn.execute("LOAD " + fts._literal(os.environ["VORTEX_EXTENSION"]))
            conn.execute("LOAD " + fts._literal(os.environ["ICEBERG_EXTENSION"]))
            yield conn



class MetadataGuards(unittest.TestCase):
    def setUp(self):
        self.metadata = {
            "table-uuid": "table-123", "current-snapshot-id": 9,
            "schemas": [{"schema-id": 0, "fields": [
                {"id": 1, "name": "id", "type": "long"},
                {"id": 2, "name": "title", "type": "string"}]}],
            "snapshots": [{"snapshot-id": 9, "schema-id": 0, "manifest-list": "manifest.avro"}],
        }

    def test_expired_snapshot_and_field_type_fail(self):
        for snapshot, fields in [(10, ["title"]), (True, ["title"]), (9, ["id"]), (9, []),
                                 (9, ["title", "title"]), (9, ["title'); DROP TABLE x;--"])]:
            with self.assertRaises(ValueError):
                fts._identity(self.metadata, snapshot, fields)

    def test_new_current_snapshot_keeps_explicit_old_identity(self):
        old = fts._identity(self.metadata, 9, ["title"])
        self.metadata["snapshots"].append({"snapshot-id": 10, "schema-id": 0, "manifest-list": "new.avro"})
        self.metadata["current-snapshot-id"] = 10
        self.assertEqual(fts._identity(self.metadata, 9, ["title"]), old)
        self.assertNotEqual(fts._identity(self.metadata, 10, ["title"]), old)

    def test_reused_name_new_field_id_and_uuid_do_not_match(self):
        old = fts._identity(copy.deepcopy(self.metadata), 9, ["title"])
        self.metadata["schemas"][0]["fields"][1]["id"] = 20
        self.assertNotEqual(fts._identity(self.metadata, 9, ["title"]), old)
        self.metadata["table-uuid"] = "other-table"
        self.assertNotEqual(fts._identity(self.metadata, 9, ["title"]), old)

    def test_failure_cleanup_and_exclusive_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            metadata = root / "metadata.json"
            metadata.write_text(json.dumps(self.metadata))
            conn = Mock()
            conn.execute.side_effect = RuntimeError("scan failed")
            with self.assertRaisesRegex(RuntimeError, "scan failed"):
                fts.build(conn, metadata, 9, root / "new-index", ["title"])
            self.assertFalse((root / "new-index").exists())
            (root / "existing").mkdir()
            (root / "existing" / "keep").write_text("original")
            with self.assertRaises(FileExistsError):
                fts.build(conn, metadata, 9, root / "existing", ["title"])
            self.assertEqual((root / "existing" / "keep").read_text(), "original")

    def test_sql_literal_and_symlink_rejection(self):
        self.assertEqual(fts._literal("a'b"), "'a''b'")
        with self.assertRaises(ValueError):
            fts._literal("\0")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / "link").symlink_to(root)
            with self.assertRaises(ValueError):
                fts._local(root / "link")


@unittest.skipUnless(os.environ.get("FTS_DUCKDB_SHELL") or (os.environ.get("VORTEX_EXTENSION") and os.environ.get("ICEBERG_EXTENSION")),
                     "set native extension paths to run Iceberg/Tantivy end-to-end tests")
class NativeIntegration(unittest.TestCase):
    def test_visible_rows_across_equality_delete_snapshots(self):
        with connection() as conn, tempfile.TemporaryDirectory() as directory:
            metadata = Path("data/persistent/equality_deletes/warehouse/mydb/mytable/metadata/v7.metadata.json").resolve()
            table = json.loads(metadata.read_text())
            snapshots = [table["snapshots"][0], table["snapshots"][-1]]
            # A global '*' query must return exactly the snapshot's visible rows,
            # including null text, never any equality-deleted row.
            for number, snapshot in enumerate(snapshots):
                sid = snapshot["snapshot-id"]
                index = Path(directory).resolve() / str(number)
                expected = conn.execute(f"SELECT id, name FROM iceberg_scan({fts._literal(str(metadata))}, snapshot_from_id := {sid}) ORDER BY id").fetchall()
                built = fts.build(conn, metadata, sid, index, ["name"])
                self.assertEqual(built["rows"], len(expected))
                hits = fts.search(conn, index, metadata, sid, "*", 100)
                self.assertEqual(sorted((h[4]["id"], h[4]["name"]) for h in hits), expected)
                if number:
                    with self.assertRaisesRegex(ValueError, "does not cover"):
                        fts.search(conn, index, metadata, snapshots[0]["snapshot-id"], "*", 1)
                (index / "snapshot.vortex").write_bytes(b"corrupt")
                with self.assertRaises(Exception):
                    fts.search(conn, index, metadata, sid, "*", 1)

    def test_parquet_position_delete_before_top_k(self):
        from test_local_catalog import local_catalog
        with connection() as conn, tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            table = root / "parquet"
            conn.execute(f"COPY (SELECT i::BIGINT id, 'rust search' AS title FROM range(3) t(i)) TO {fts._literal(str(table))} (FORMAT iceberg)").fetchall()
            original = next((table / "metadata").glob("*.metadata.json"))
            with local_catalog(original) as (server, attach):
                server.metadata["properties"]["write.format.default"] = "parquet"
                conn.execute(attach + "DELETE FROM lake.main.items WHERE id = 0;").fetchall()
                metadata = server.metadata_path
                sid = json.loads(metadata.read_text())["current-snapshot-id"]
                index = root / "index"
                self.assertEqual(fts.build(conn, metadata, sid, index, ["title"])["rows"], 2)
                hits = fts.search(conn, index, metadata, sid, "rust", 10)
                self.assertEqual(sorted(hit[4]["id"] for hit in hits), [1, 2])
                self.assertIn(fts.search(conn, index, metadata, sid, "rust", 1)[0][4]["id"], [1, 2])

    @unittest.skipUnless(os.environ.get("FTS_DUCKDB_SHELL"), "repository shell required for Vortex Iceberg format")
    def test_vortex_snapshot(self):
        with connection() as conn, tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            table = root / "vortex"
            conn.execute(f"COPY (SELECT i::BIGINT id, 'rust search' AS title FROM range(3) t(i)) TO {fts._literal(str(table))} (FORMAT iceberg, DATA_FORMAT vortex)").fetchall()
            metadata = next((table / "metadata").glob("*.metadata.json"))
            sid = json.loads(metadata.read_text())["current-snapshot-id"]
            index = root / "index"
            self.assertEqual(fts.build(conn, metadata, sid, index, ["title"])["rows"], 3)
            hits = fts.search(conn, index, metadata, sid, "rust", 10)
            self.assertEqual(sorted(hit[4]["id"] for hit in hits), [0, 1, 2])


if __name__ == "__main__":
    unittest.main()
