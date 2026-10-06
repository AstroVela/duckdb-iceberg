"""Check schema-commit rejection for Vortex and schema evolution for Parquet.

Run with an enabled shell:
    python3 test/vortex/test_schema_changes.py --duckdb build/vortex/duckdb
"""

import argparse
import copy
import json
from pathlib import Path
import tempfile

from test_local_catalog import local_catalog, quote, run_sql

ALTERS = {
    "add": "ADD COLUMN extra BIGINT",
    "rename": "RENAME COLUMN payload TO renamed",
    "drop": "DROP COLUMN payload",
    "type": "ALTER COLUMN id TYPE BIGINT",
    "nullable": "ALTER COLUMN id DROP NOT NULL",
}
ROW = {"id": 1, "payload": "row-1"}


def export_table(shell, table, fmt, empty=False, value=1):
    run_sql(
        shell,
        f"COPY (SELECT {value}::INTEGER id, 'row-{value}' payload {'WHERE false' if empty else ''}) "
        f"TO {quote(table)} (FORMAT iceberg, DATA_FORMAT {fmt});",
    )
    return next((table / "metadata").glob("*.metadata.json"))


def check_schema_change(shell, root, mode, action):
    table = root / f"{mode}_{action}"
    fmt = "parquet" if mode in ("parquet", "mixed") else "vortex"
    original_path = export_table(shell, table, fmt)
    with local_catalog(original_path) as (server, attach):
        expected = [ROW]
        if mode == "mixed":
            run_sql(shell, attach + "INSERT INTO lake.main.items VALUES (2, 'row-2');")
            expected += [{"id": 2, "payload": "row-2"}]
        if mode == "parquet":
            server.metadata["properties"]["write.format.default"] = "parquet"
        elif mode != "vortex":
            run_sql(
                shell,
                attach + "CALL set_iceberg_table_properties(lake.main.items, {'write.format.default': 'parquet'});",
            )
            assert server.metadata["properties"]["write.format.default"] == "parquet"

        before_metadata = copy.deepcopy(server.metadata)
        before_files = set(table.rglob("*"))
        before_commits = len(server.commits)
        statement = attach + f"ALTER TABLE lake.main.items {ALTERS[action]};"
        if mode == "parquet":
            run_sql(shell, statement)
            assert len(server.commits) == before_commits + 1
            assert len(server.metadata["schemas"]) == 2
            assert server.metadata["current-schema-id"] == 1
            if action == "add":
                expected = [{**ROW, "extra": None}]
            elif action == "rename":
                expected = [{"id": 1, "renamed": "row-1"}]
            elif action == "drop":
                expected = [{"id": 1}]
            elif action == "type":
                assert server.metadata["schemas"][-1]["fields"][0]["type"] == "long"
            elif action == "nullable":
                assert not server.metadata["schemas"][-1]["fields"][0]["required"]
        else:
            run_sql(shell, statement, error="require a fixed schema")
            assert len(server.commits) == before_commits
            assert server.metadata == before_metadata
            assert set(table.rglob("*")) == before_files

        assert run_sql(shell, attach + "SELECT * FROM lake.main.items ORDER BY id;") == expected
        old_snapshot = before_metadata["current-snapshot-id"]
        assert run_sql(
            shell,
            f"SELECT * FROM iceberg_scan({quote(server.metadata_path)}, snapshot_from_id={old_snapshot}) ORDER BY id;",
        ) == ([ROW] if mode == "parquet" else expected)
        if mode != "parquet":
            run_sql(shell, attach + "INSERT INTO lake.main.items VALUES (3, 'row-3');")
            assert run_sql(shell, attach + "SELECT * FROM lake.main.items ORDER BY id;") == expected + [
                {"id": 3, "payload": "row-3"}
            ]
        assert not server.errors, server.errors


def check_retained_snapshot(shell, root):
    # The latest snapshot is entirely Parquet, while an older retained snapshot uses Vortex.
    old_path = export_table(shell, root / "retained_vortex", "vortex", value=7)
    old = json.loads(old_path.read_text())
    current_path = export_table(shell, root / "retained_parquet", "parquet")
    with local_catalog(current_path) as (server, attach):
        server.metadata["properties"]["write.format.default"] = "parquet"
        current = server.metadata["snapshots"][0]
        current["parent-snapshot-id"] = old["current-snapshot-id"]
        current["sequence-number"] = 2
        server.metadata["last-sequence-number"] = 2
        server.metadata["snapshots"].insert(0, old["snapshots"][0])
        server.metadata["snapshot-log"] = [
            {"timestamp-ms": snapshot["timestamp-ms"], "snapshot-id": snapshot["snapshot-id"]}
            for snapshot in server.metadata["snapshots"]
        ]
        current_path.write_text(json.dumps(server.metadata))
        before = copy.deepcopy(server.metadata)
        for action in ("add", "rename"):
            run_sql(shell, attach + f"ALTER TABLE lake.main.items {ALTERS[action]};", error="require a fixed schema")
            assert server.metadata == before
            assert not server.commits
            assert run_sql(shell, attach + "SELECT * FROM lake.main.items;") == [ROW]
            assert run_sql(
                shell,
                f"SELECT * FROM iceberg_scan({quote(current_path)}, snapshot_from_id={old['current-snapshot-id']});",
            ) == [{"id": 7, "payload": "row-7"}]
        assert not server.errors, server.errors


def check_pending_append(shell, root):
    table = root / "pending_vortex_append"
    path = export_table(shell, table, "parquet", empty=True)
    with local_catalog(path) as (server, attach):
        before_metadata = copy.deepcopy(server.metadata)
        before_files = set(table.rglob("*"))
        run_sql(
            shell,
            attach + "BEGIN; INSERT INTO lake.main.items VALUES (1, 'row-1'); "
            "CALL set_iceberg_table_properties(lake.main.items, {'write.format.default': 'parquet'}); "
            "ALTER TABLE lake.main.items ADD COLUMN extra BIGINT; COMMIT;",
            error="require a fixed schema",
        )
        assert not server.commits
        assert server.metadata == before_metadata
        assert set(table.rglob("*")) == before_files
        assert run_sql(shell, attach + "SELECT count(*) n FROM lake.main.items;") == [{"n": 0}]
        run_sql(shell, attach + "INSERT INTO lake.main.items VALUES (1, 'row-1');")
        assert run_sql(shell, attach + "SELECT * FROM lake.main.items;") == [ROW]
        assert not server.errors, server.errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path)
    args = parser.parse_args()
    shell = args.duckdb.resolve()
    with tempfile.TemporaryDirectory(prefix="iceberg-vortex-schema-") as directory:
        root = Path(directory)
        for mode in ("parquet", "vortex", "parquet_default", "mixed"):
            for action in ALTERS:
                check_schema_change(shell, root, mode, action)
        check_retained_snapshot(shell, root)
        check_pending_append(shell, root)
    print("Parquet schema evolution and Vortex schema-commit rejection regressions passed.")


if __name__ == "__main__":
    main()
