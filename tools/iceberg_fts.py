"""Explicit Iceberg snapshot full-text indexes backed by DuckDB, Vortex and Tantivy.

The connection must have iceberg and a Tantivy-enabled vortex extension loaded.
Only immutable, explicit metadata JSON files and snapshot IDs are accepted. The
index stores the snapshot's visible rows; row addresses refer to this materialized
Vortex file, not to Iceberg data files. See docs/TANTIVY_FTS_USER_GUIDE.md.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
from typing import Any

MAX_METADATA_BYTES = 16 * 1024 * 1024
MAX_SOURCE_BYTES = 512 * 1024 * 1024


def _literal(value: str) -> str:
    if "\0" in value:
        raise ValueError("NUL is not allowed in SQL strings")
    return "'" + value.replace("'", "''") + "'"


def _local(path: str | Path, *, exists: bool = True) -> Path:
    path = Path(path).absolute()
    if ".." in path.parts or any(p.is_symlink() for p in [path, *path.parents]):
        raise ValueError("Index and metadata paths must not contain symlinks or '..'")
    if exists and not path.exists():
        raise ValueError(f"Path does not exist: {path}")
    return path


def _read_json(path: Path) -> tuple[dict, bytes]:
    if not path.is_file() or path.stat().st_size > MAX_METADATA_BYTES:
        raise ValueError("Expected a regular JSON file of at most 16 MiB")
    with path.open("rb") as stream:
        raw = stream.read(MAX_METADATA_BYTES + 1)
    if len(raw) > MAX_METADATA_BYTES:
        raise ValueError("Metadata exceeds 16 MiB")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("Metadata must be an object")
    return value, raw


def _identity(metadata: dict, snapshot_id: int, fields: list[str]) -> dict:
    if isinstance(snapshot_id, bool) or not isinstance(snapshot_id, int) or snapshot_id <= 0:
        raise ValueError("A positive explicit snapshot ID is required")
    if not metadata.get("table-uuid"):
        raise ValueError("Iceberg table UUID is required")
    matches = [s for s in metadata.get("snapshots", []) if s["snapshot-id"] == snapshot_id]
    if len(matches) != 1:
        raise ValueError("Snapshot is absent or expired; never fall back to the current snapshot")
    snapshot = matches[0]
    schema_id = snapshot.get("schema-id")
    if schema_id is None:
        raise ValueError("Snapshot must carry an explicit schema-id")
    schemas = [s for s in metadata.get("schemas", []) if s["schema-id"] == schema_id]
    if len(schemas) != 1:
        raise ValueError("Snapshot schema is missing or ambiguous")
    schema = schemas[0]
    columns = {f["name"]: f for f in schema["fields"]}
    if not fields or len(fields) > 16 or len(set(fields)) != len(fields):
        raise ValueError("Choose 1..16 unique text fields")
    for name in fields:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name) or name in {"_vortex_row_id", "_vortex_file_id"}:
            raise ValueError("Text fields require ASCII identifiers without reserved row-address names")
        if name not in columns or columns[name]["type"] != "string":
            raise ValueError(f"Field is not a top-level Iceberg string in this snapshot: {name}")
    return {"table_uuid": metadata["table-uuid"], "snapshot": snapshot, "schema": schema,
            "fields": [{"id": columns[n]["id"], "name": n} for n in fields]}


def _publish(path: Path, value: dict) -> None:
    pending = path.with_suffix(".pending")
    with pending.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(pending, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def build(connection: Any, metadata_path: str | Path, snapshot_id: int,
          index_dir: str | Path, fields: list[str]) -> dict:
    """Publish an immutable index of visible rows from exactly one Iceberg snapshot.

    No writes occur at SQL bind time. This orchestration explicitly executes a
    snapshot scan and COPY before native vortex_fts_build. An existing destination
    is never replaced. Failure removes only the directory this invocation created.
    The caller must not concurrently execute queries on the supplied connection.
    """
    metadata_path = _local(metadata_path)
    metadata, raw = _read_json(metadata_path)
    identity = _identity(metadata, snapshot_id, fields)
    root = _local(index_dir, exists=False)
    _local(root.parent)
    root.mkdir(mode=0o700)  # exclusive reservation; don't catch FileExistsError
    try:
        source = root / "snapshot.vortex"
        reference = root / "index.json"
        # Use the original path so relative manifest paths retain their meaning.
        # Metadata files are immutable under Iceberg's commit protocol.
        scan = f"iceberg_scan({_literal(str(metadata_path))}, snapshot_from_id := {snapshot_id})"
        connection.execute(f"COPY (SELECT * FROM {scan} LIMIT 1000001) TO {_literal(str(source))} (FORMAT vortex)").fetchall()
        if source.stat().st_size > MAX_SOURCE_BYTES:
            raise ValueError("Materialized snapshot exceeds the 512 MiB source budget")
        # The provider rejects the extra row rather than silently truncating snapshots.
        indexed = connection.execute(
            f"SELECT * FROM vortex_fts_build([{_literal(str(source))}], {_literal(str(reference))}, "
            f"[{','.join(_literal(f) for f in fields)}])").fetchall()
        if _read_json(metadata_path)[1] != raw:
            raise ValueError("Iceberg metadata changed during build")
        manifest = {"format_version": 1, "identity": identity,
                    "metadata_path": str(metadata_path), "metadata_sha256": hashlib.sha256(raw).hexdigest(),
                    "reference_sha256": hashlib.sha256(reference.read_bytes()).hexdigest(),
                    "rows": int(indexed[0][2]), "row_address_space": "materialized-snapshot.vortex"}
        (root / "iceberg-metadata.json").write_bytes(raw)
        _publish(root / "iceberg-fts.json", manifest)
        return manifest
    except BaseException:
        shutil.rmtree(root)
        raise


def search(connection: Any, index_dir: str | Path, metadata_path: str | Path,
           snapshot_id: int, query: str, k: int = 10, *, field_boosts: dict[str, float] | None = None) -> list:
    """Return rank, materialized file_id/row_offset, BM25 score and snapshot row.

    The caller supplies the metadata version and snapshot it intends to read.
    A retained historical snapshot is valid; a different/expired snapshot or any
    change in its schema, field IDs or manifest identity fails closed. A new
    current snapshot never silently reuses an old index.
    """
    if isinstance(k, bool) or not isinstance(k, int) or not 1 <= k <= 10000:
        raise ValueError("k must be between 1 and 10000")
    if not isinstance(query, str) or not query.strip() or len(query.encode()) > 65536:
        raise ValueError("query must contain 1..65536 UTF-8 bytes")
    root = _local(index_dir)
    manifest, _ = _read_json(_local(root / "iceberg-fts.json"))
    if manifest.get("format_version") != 1 or manifest.get("row_address_space") != "materialized-snapshot.vortex":
        raise ValueError("Unsupported Iceberg FTS generation")
    identity = manifest["identity"]
    fields = [f["name"] for f in identity["fields"]]
    metadata, _ = _read_json(_local(metadata_path))
    if _identity(metadata, snapshot_id, fields) != identity:
        raise ValueError("Index does not cover the requested Iceberg snapshot and schema; rebuild")
    reference = _local(root / "index.json")
    _, reference_bytes = _read_json(reference)
    if hashlib.sha256(reference_bytes).hexdigest() != manifest["reference_sha256"]:
        raise ValueError("Index reference changed since publication")
    options = json.dumps({"field_boosts": field_boosts or {}}, allow_nan=False)
    return connection.execute(
        f"SELECT * FROM vortex_fts_search({_literal(str(reference))}, {_literal(query)}, {k}, "
        f"query_options := {_literal(options)}) ORDER BY rank").fetchall()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vortex-extension", required=True, help="Tantivy-enabled vortex.duckdb_extension")
    parser.add_argument("--iceberg-extension", required=True)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ["build", "search"]:
        p = sub.add_parser(name)
        p.add_argument("--metadata", required=True)
        p.add_argument("--snapshot", required=True, type=int)
        p.add_argument("--index", required=True)
        if name == "build":
            p.add_argument("--fields", nargs="+", required=True)
        else:
            p.add_argument("--query", required=True)
            p.add_argument("-k", type=int, default=10)
    args = parser.parse_args()
    import duckdb
    with duckdb.connect(config={"allow_unsigned_extensions": "true"}) as connection:
        connection.execute(f"LOAD {_literal(str(_local(args.vortex_extension)))}")
        connection.execute(f"LOAD {_literal(str(_local(args.iceberg_extension)))}")
        if args.command == "build":
            result = build(connection, args.metadata, args.snapshot, args.index, args.fields)
        else:
            result = search(connection, args.index, args.metadata, args.snapshot, args.query, args.k)
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
