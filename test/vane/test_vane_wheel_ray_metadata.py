#!/usr/bin/env python3
"""Metadata table-function regressions using installed wheels and only the Ray runner.

Expected results come from committed fixtures, Python and the REST catalog.
The head has no CPUs, so the singleton scans must execute on a worker node.
"""

from __future__ import annotations

import copy
import datetime as dt
import gzip
import json
import os
import shutil
import sys
import tempfile
import urllib.request
import uuid
from importlib import import_module
from importlib.metadata import entry_points
from pathlib import Path

TEST_DIRECTORY = Path(__file__).resolve().parent
sys.path.insert(0, str(TEST_DIRECTORY))
try:
    from test_vane_wheel_ray_iceberg import (
        CATALOG_ENDPOINT,
        MINIO_READY_ENDPOINT,
        configure_coordinator_s3,
        configure_worker_session_environment,
        create_two_worker_cluster,
        execution_node_ids,
        require_equal,
        run_scenario,
        sql_string,
        wait_for_http_endpoint,
    )
finally:
    sys.path.pop(0)

REPOSITORY_ROOT = TEST_DIRECTORY.parents[1]
LINEITEM = REPOSITORY_ROOT / "data/persistent/iceberg/lineitem_iceberg"
CATALOG = "vane_ray_metadata"


def open_connection(vane: object) -> object:
    # Load the descriptor graph without querying coordinator-only system tables,
    # so every result-producing query in this suite can use Ray.
    from vane.extensions import DynamicExtensionResolver

    connection = vane.connect(
        ":memory:",
        config={
            "allow_unsigned_extensions": "false",
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    try:
        installed = tuple(entry_points(group="vane.dynamic_extension_providers"))
        providers, descriptors = [], {}
        trust_identity = os.environ["VANE_EXPECTED_EXTENSION_TRUST_IDENTITY"]
        for name in ("avro", "iceberg"):
            matches = [entry for entry in installed if entry.name == name]
            require_equal(len(matches), 1, f"installed {name} providers")
            entry = matches[0]
            provider = entry.load()()
            descriptor = import_module(entry.module).descriptor()
            require_equal(descriptor.name, name, "provider name")
            require_equal(descriptor.trust_identity, trust_identity, "provider trust root")
            require_equal(provider.find(descriptor.identity).descriptor, descriptor, "provider artifact")
            providers.append(provider)
            descriptors[name] = descriptor
        require_equal(descriptors["avro"].dependencies, (), "Avro dependency graph")
        require_equal(
            tuple(dependency.identity for dependency in descriptors["iceberg"].dependencies),
            (descriptors["avro"].identity,),
            "Iceberg dependency graph",
        )
        DynamicExtensionResolver(trusted_identities={trust_identity}, providers=providers).load(
            connection, descriptors["iceberg"]
        )
        connection.execute("LOAD httpfs")
        configure_coordinator_s3(connection)
        connection.execute("SET TimeZone = 'UTC'")
        connection.execute(
            f"ATTACH '' AS {CATALOG} "
            "(TYPE ICEBERG, CLIENT_ID 'admin', CLIENT_SECRET 'password', "
            f"ENDPOINT '{CATALOG_ENDPOINT}', stage_create_tables true)"
        )
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.default")
        return connection
    except BaseException:
        connection.close()
        raise


class MetadataHarness:
    def __init__(self, connection: object, runner: object):
        self.connection = connection
        self.runner = runner
        self.original_read = runner.run_iter_tables
        self.reads = 0
        self.before_dispatch = None
        runner.run_iter_tables = self.dispatch

    def dispatch(self, *args: object, **kwargs: object) -> object:
        self.reads += 1
        hook, self.before_dispatch = self.before_dispatch, None
        if hook is not None:
            # Vane has already bound and copied the native plan at this point.
            hook()
        return self.original_read(*args, **kwargs)

    def query(self, sql: str, expected: list[tuple], *, params: list | None = None, before_dispatch=None) -> None:
        before = self.reads
        self.before_dispatch = before_dispatch
        try:
            result = self.connection.sql(sql, params=params).fetchall()
            require_equal(result, expected, sql)
            require_equal(self.reads, before + 1, "Ray query dispatch count")
            require_equal(self.before_dispatch, None, "post-bind hook consumed")
        finally:
            self.before_dispatch = None

    def close(self) -> None:
        self.runner.run_iter_tables = self.original_read


def snapshot_rows(metadata: dict) -> list[tuple]:
    return sorted(
        (
            snapshot.get("sequence-number", 0),
            snapshot["snapshot-id"],
            dt.datetime(1970, 1, 1) + dt.timedelta(milliseconds=snapshot["timestamp-ms"]),
            snapshot["manifest-list"],
            snapshot["summary"]["operation"],
        )
        for snapshot in metadata.get("snapshots", [])
    )


def metadata_source(path: Path, options: str = "") -> str:
    return f"iceberg_metadata({sql_string(path)}, allow_moved_paths=true{options})"


def check_files(harness: MetadataHarness) -> None:
    metadata = json.loads((LINEITEM / "metadata/v2.metadata.json").read_text())
    harness.query(
        "SELECT * FROM iceberg_snapshots(?) ORDER BY sequence_number", snapshot_rows(metadata), params=[str(LINEITEM)]
    )
    expected = [
        (
            "lineitem_iceberg/metadata/179b4fb1-0366-4f7d-ad35-99ee8da0abf5-m1.avro",
            2,
            "DATA",
            "ADDED",
            "EXISTING",
            "lineitem_iceberg/data/00000-5-dad9988f-2a3b-464c-adb6-6034de93da19-00001.parquet",
            "PARQUET",
            51793,
        ),
        (
            "lineitem_iceberg/metadata/179b4fb1-0366-4f7d-ad35-99ee8da0abf5-m0.avro",
            2,
            "DATA",
            "DELETED",
            "EXISTING",
            "lineitem_iceberg/data/00000-1-66fee7c2-c97c-4af9-963d-930afd99ace4-00001.parquet",
            "PARQUET",
            60175,
        ),
    ]
    harness.query(f"SELECT * FROM {metadata_source(LINEITEM)} ORDER BY status", expected)
    harness.query(
        f"SELECT file_format, sum(record_count)::BIGINT FROM {metadata_source(LINEITEM)} WHERE status='ADDED' GROUP BY file_format",
        [("PARQUET", 51793)],
    )
    harness.query(
        f"SELECT s.sequence_number, m.status, m.record_count FROM iceberg_snapshots({sql_string(LINEITEM)}) s "
        f"JOIN {metadata_source(LINEITEM)} m ON s.sequence_number=m.manifest_sequence_number ORDER BY m.status",
        [(2, "ADDED", 51793), (2, "DELETED", 60175)],
    )
    for options in (", version='1'", ", version='1', version_name_format='v%s%s.metadata.json'"):
        harness.query(f"SELECT record_count FROM {metadata_source(LINEITEM, options)}", [(60175,)])
    first = min(metadata["snapshots"], key=lambda snapshot: snapshot["timestamp-ms"])
    timestamp = dt.datetime(1970, 1, 1) + dt.timedelta(milliseconds=first["timestamp-ms"])
    for options in (
        f", snapshot_from_id={first['snapshot-id']}",
        f", snapshot_from_timestamp=TIMESTAMP {sql_string(timestamp)}",
    ):
        harness.query(f"SELECT record_count FROM {metadata_source(LINEITEM, options)}", [(60175,)])
    before_first_snapshot = ", snapshot_from_timestamp=TIMESTAMP '1900-01-01'"
    harness.query(f"SELECT * FROM {metadata_source(LINEITEM, before_first_snapshot)}", [])
    try:
        conflicting_options = before_first_snapshot + ", snapshot_from_id=1"
        harness.connection.sql(f"SELECT * FROM {metadata_source(LINEITEM, conflicting_options)}").fetchall()
    except Exception as error:
        if "Can't use 'snapshot_from_id' in combination" not in str(error):
            raise
    else:
        raise AssertionError("conflicting snapshot selectors were accepted")


def check_gzip(harness: MetadataHarness) -> None:
    path = REPOSITORY_ROOT / "data/persistent/iceberg/lineitem_iceberg_gz"
    metadata_path = path / "metadata/v2.gz.metadata.json"
    with gzip.open(metadata_path, "rt") as source:
        metadata = json.load(source)
    harness.query(
        f"SELECT * FROM iceberg_snapshots({sql_string(path)}, metadata_compression_codec='gzip') ORDER BY sequence_number",
        snapshot_rows(metadata),
    )
    harness.query(
        f"SELECT * FROM iceberg_snapshots({sql_string(metadata_path)}) ORDER BY sequence_number",
        snapshot_rows(metadata),
    )
    options = ", metadata_compression_codec='gzip'"
    harness.query(f"SELECT record_count FROM {metadata_source(path, options)}", [(111968,)])


def check_bound_version(harness: MetadataHarness, root: Path) -> None:
    table = root / "moved"
    shutil.copytree(LINEITEM / "metadata", table / "metadata")
    hint = table / "metadata/version-hint.text"
    for function, expected, projection in (
        ("iceberg_snapshots", [(1,)], "count(*)"),
        ("iceberg_metadata", [(60175,)], "sum(record_count)::BIGINT"),
    ):
        hint.write_text("1")
        suffix = ", allow_moved_paths=true" if function == "iceberg_metadata" else ""
        query = f"SELECT {projection} FROM {function}({sql_string(table)}{suffix})"
        harness.query(query, expected, before_dispatch=lambda: hint.write_text("2"))
        harness.query(query, [(2,)] if function == "iceberg_snapshots" else [(111968,)])


def check_many_snapshots(harness: MetadataHarness, root: Path) -> None:
    metadata = json.loads((LINEITEM / "metadata/v2.metadata.json").read_text())
    seed = metadata["snapshots"][0]
    snapshots = []
    for index in range(4101):
        snapshot = copy.deepcopy(seed)
        snapshot.update({"snapshot-id": index + 1, "sequence-number": index + 1})
        snapshots.append(snapshot)
    metadata.update({"snapshots": snapshots, "current-snapshot-id": 4101, "snapshot-log": [], "refs": {}})
    path = root / "many.metadata.json"
    path.write_text(json.dumps(metadata))
    harness.query(
        "SELECT * FROM iceberg_snapshots(?) ORDER BY sequence_number", snapshot_rows(metadata), params=[str(path)]
    )
    harness.query(
        "SELECT count(*), count(DISTINCT snapshot_id), sum(sequence_number)::BIGINT FROM iceberg_snapshots(?)",
        [(4101, 4101, 4101 * 4102 // 2)],
        params=[str(path)],
    )


def rest_metadata(name: str) -> dict:
    with urllib.request.urlopen(f"{CATALOG_ENDPOINT}/v1/namespaces/default/tables/{name}", timeout=10) as response:
        return json.load(response)["metadata"]


def check_catalog(harness: MetadataHarness, writer: object, format_version: int) -> None:
    name = f"metadata_v{format_version}_{uuid.uuid4().hex}"
    table = f"{CATALOG}.default.{name}"
    try:
        writer.execute(
            f"CREATE TABLE {table} (id INTEGER) WITH ('format-version'='{format_version}', "
            f"'write.data.path'='s3://warehouse/vane-ray-metadata/{name}/data')"
        )
        for function in ("iceberg_snapshots", "iceberg_metadata"):
            harness.query(f"SELECT * FROM {function}({sql_string(table)})", [])
        harness.query(
            f"SELECT * FROM iceberg_metadata({sql_string(table)})",
            [],
            before_dispatch=lambda: writer.sql("SELECT 1::INTEGER AS id").insert_into(table),
        )
        first = rest_metadata(name)
        harness.query(
            f"SELECT * FROM iceberg_snapshots({sql_string(table)}) ORDER BY sequence_number",
            snapshot_rows(first),
            before_dispatch=lambda: writer.sql("SELECT 2::INTEGER AS id").insert_into(table),
        )
        harness.query(
            f"SELECT * FROM iceberg_snapshots({sql_string(table)}) ORDER BY sequence_number",
            snapshot_rows(rest_metadata(name)),
        )
        harness.query(
            f"SELECT sum(record_count)::BIGINT FROM iceberg_metadata({sql_string(table)})",
            [(2,)],
            before_dispatch=lambda: writer.sql("SELECT 3::INTEGER AS id").insert_into(table),
        )
        harness.query(f"SELECT sum(record_count)::BIGINT FROM iceberg_metadata({sql_string(table)})", [(3,)])
        harness.query(
            f"SELECT sum(record_count)::BIGINT FROM iceberg_metadata({sql_string(table)}, snapshot_from_id={first['current-snapshot-id']})",
            [(1,)],
        )
        # Latest lookup uses the current schema, while time travel uses the
        # snapshot schema. Preserve both after schema-only catalog commits.
        writer.execute(f"ALTER TABLE {table} ADD COLUMN added VARCHAR")
        harness.query(f"SELECT sum(record_count)::BIGINT FROM iceberg_metadata({sql_string(table)})", [(3,)])
    finally:
        writer.execute(f"DROP TABLE IF EXISTS {table}")


def main() -> None:
    import ray
    import vane
    from vane import runners

    require_equal(os.environ.get("VANE_RUNNER"), "ray", "selected runner")
    configure_worker_session_environment()
    wait_for_http_endpoint(MINIO_READY_ENDPOINT)
    wait_for_http_endpoint(f"{CATALOG_ENDPOINT}/v1/config")
    cluster = create_two_worker_cluster(ray)
    connection = writer = harness = None
    try:
        execution_node_ids(ray)
        vane.set_runner_ray(noop_if_initialized=True)
        runner = runners.get_or_create_runner()
        require_equal(runner.name, "ray", "active runner")
        connection = open_connection(vane)
        writer = open_connection(vane)
        harness = MetadataHarness(connection, runner)
        with tempfile.TemporaryDirectory(prefix="vane-ray-metadata-") as directory:
            root = Path(directory)
            run_scenario("metadata/snapshots SQL and historical selectors", lambda: check_files(harness))
            run_scenario("gzip metadata", lambda: check_gzip(harness))
            run_scenario("bound version hint and moved paths", lambda: check_bound_version(harness, root))
            run_scenario(
                "snapshot chunk continuation and singleton cardinality", lambda: check_many_snapshots(harness, root)
            )
            for format_version in (2, 3):
                run_scenario(
                    f"v{format_version} empty/catalog/frozen snapshot/schema evolution",
                    lambda: check_catalog(harness, writer, format_version),
                )
        print(f"[vane-ray-metadata] PASS: {harness.reads} queries dispatched through Ray", flush=True)
    finally:
        if harness is not None:
            harness.close()
        if writer is not None:
            writer.close()
        if connection is not None:
            connection.close()
        vane.teardown_runner()
        ray.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
