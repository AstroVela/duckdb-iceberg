#!/usr/bin/env python3
"""Exercise Iceberg reads and writes through a packaged two-worker Vane Ray runtime."""

from __future__ import annotations

import os
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from pathlib import Path

CATALOG_ENDPOINT = "http://127.0.0.1:8181"
MINIO_ENDPOINT = "http://127.0.0.1:9000"
MINIO_READY_ENDPOINT = f"{MINIO_ENDPOINT}/minio/health/ready"
CATALOG_NAME = "vane_wheel_ray_catalog"
SOURCE_NAME = "vane_wheel_ray_source"
TARGET_NAME = "vane_wheel_ray_target"
EMPTY_NAME = "vane_wheel_ray_empty"
PARTITION_NAME = "vane_wheel_ray_partitioned"
SCHEMA_EVOLUTION_NAME = "vane_wheel_ray_schema_evolution"
CONFLICT_NAME = "vane_wheel_ray_conflict"
SOURCE_TABLE = f"{CATALOG_NAME}.default.{SOURCE_NAME}"
TARGET_TABLE = f"{CATALOG_NAME}.default.{TARGET_NAME}"
EMPTY_TABLE = f"{CATALOG_NAME}.default.{EMPTY_NAME}"
PARTITION_TABLE = f"{CATALOG_NAME}.default.{PARTITION_NAME}"
SCHEMA_EVOLUTION_TABLE = f"{CATALOG_NAME}.default.{SCHEMA_EVOLUTION_NAME}"
CONFLICT_TABLE = f"{CATALOG_NAME}.default.{CONFLICT_NAME}"
TABLES = (
    TARGET_TABLE,
    SOURCE_TABLE,
    EMPTY_TABLE,
    PARTITION_TABLE,
    SCHEMA_EVOLUTION_TABLE,
    CONFLICT_TABLE,
)
DATA_ROOT = "s3://warehouse/vane-wheel-ray-integration"
WORKER_COUNT = 2
SOURCE_PARTITIONS = 4
ROWS_PER_PARTITION = 256
SOURCE_ROW_COUNT = SOURCE_PARTITIONS * ROWS_PER_PARTITION
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def require_equal(actual: object, expected: object, description: str) -> None:
    if actual != expected:
        raise AssertionError(f"{description}: expected {expected!r}, got {actual!r}")


def require_true(value: bool, description: str) -> None:
    if not value:
        raise AssertionError(description)


def run_scenario(description: str, operation: Callable[[], None]) -> None:
    print(f"[vane-ray-iceberg] START {description}", flush=True)
    operation()
    print(f"[vane-ray-iceberg] PASS  {description}", flush=True)


def sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def persistent_scan(relative_path: str, options: str = "") -> str:
    path = sql_string(REPOSITORY_ROOT / relative_path)
    suffix = f", {options}" if options else ""
    return f"iceberg_scan({path}{suffix})"


def wait_for_http_endpoint(endpoint: str) -> None:
    deadline = time.monotonic() + 90
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(endpoint, timeout=5) as response:
                if 200 <= response.status < 300:
                    return
                last_error = RuntimeError(f"HTTP {response.status}")
        except (OSError, urllib.error.HTTPError, urllib.error.URLError) as error:
            last_error = error
        time.sleep(1)
    raise RuntimeError(f"fixture endpoint did not become ready: {endpoint}: {last_error}")


def verify_extension_is_wheel_linked(connection: object) -> None:
    extension = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() WHERE extension_name = 'iceberg'"
    ).fetchone()
    if extension is None:
        raise AssertionError("the packaged Vane wheel does not contain iceberg")
    require_equal(extension[1], "STATICALLY_LINKED", "iceberg install mode before LOAD")

    connection.execute("LOAD iceberg")
    loaded = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() WHERE extension_name = 'iceberg'"
    ).fetchone()
    require_equal(loaded, (True, "STATICALLY_LINKED"), "iceberg after LOAD")
    connection.execute("LOAD httpfs")


def configure_coordinator_s3(connection: object) -> None:
    connection.execute("SET s3_endpoint = '127.0.0.1:9000'")
    connection.execute("SET s3_use_ssl = false")
    connection.execute("SET s3_url_style = 'path'")
    connection.execute("SET s3_region = 'us-east-1'")
    connection.execute("SET s3_access_key_id = 'admin'")
    connection.execute("SET s3_secret_access_key = 'password'")


def configure_worker_session_environment() -> None:
    # Vane captures explicit AWS session settings with the connection and
    # replays them on each worker; no SecretManager entry is transferred.
    os.environ.update(
        {
            "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_REGION": "us-east-1",
        }
    )


def create_two_worker_cluster(ray: object) -> object:
    from ray.cluster_utils import Cluster

    cluster = Cluster(shutdown_at_exit=False)
    try:
        cluster.add_node(
            include_dashboard=False,
            num_cpus=0,
            num_gpus=0,
            object_store_memory=100 * 1024 * 1024,
        )
        for _ in range(WORKER_COUNT):
            cluster.add_node(
                include_dashboard=False,
                num_cpus=1,
                num_gpus=0,
                object_store_memory=100 * 1024 * 1024,
            )
        ray.init(
            address=cluster.address,
            ignore_reinit_error=False,
            log_to_driver=True,
        )
        return cluster
    except BaseException:
        cluster.shutdown()
        raise


def execution_node_ids(ray: object) -> set[str]:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        node_ids = {
            str(node["NodeID"])
            for node in ray.nodes()
            if node.get("Alive") and float((node.get("Resources") or {}).get("CPU", 0)) >= 1
        }
        if len(node_ids) == WORKER_COUNT:
            return node_ids
        time.sleep(0.25)
    raise AssertionError(f"expected {WORKER_COUNT} live Ray execution nodes")


def assert_vane_worker_topology(ray: object, runner: object) -> None:
    client = runner.query_driver_client
    if client is None:
        raise AssertionError("the Ray runner did not create a query driver client")
    stats = ray.get(client.runner.fragment_stats.remote())
    workers = stats.get("workers") if isinstance(stats, dict) else None
    if not isinstance(workers, dict):
        raise AssertionError(f"Vane fragment statistics do not expose workers: {stats!r}")
    require_equal(len(workers), WORKER_COUNT, "Vane Ray worker count")


class RayIcebergHarness:
    def __init__(self, vane: object, connection: object, runner: object):
        self.vane = vane
        self.connection = connection
        self.runner = runner
        self.read_dispatch_count = 0
        self.write_dispatch_count = 0
        self._plan_counter = 0
        self._original_run_iter_tables = runner.run_iter_tables
        self._original_run_write = runner.run_write

        def record_distributed_read(*args: object, **kwargs: object) -> object:
            self.read_dispatch_count += 1
            return self._original_run_iter_tables(*args, **kwargs)

        def record_distributed_write(*args: object, **kwargs: object) -> object:
            self.write_dispatch_count += 1
            return self._original_run_write(*args, **kwargs)

        self._record_distributed_write = record_distributed_write
        runner.run_iter_tables = record_distributed_read
        runner.run_write = record_distributed_write

    def require_query(
        self,
        query: str,
        description: str,
        expected: list[tuple[object, ...]] | None = None,
    ) -> list[tuple[object, ...]]:
        if expected is None:
            expected = self.connection.execute(query).fetchall()
        previous_count = self.read_dispatch_count
        actual = self.connection.sql(query).fetchall()
        require_equal(self.read_dispatch_count, previous_count + 1, f"{description} Ray dispatch count")
        require_equal(actual, expected, description)
        return actual

    def require_write(self, description: str, operation: Callable[[], object]) -> None:
        previous_count = self.write_dispatch_count
        operation()
        require_equal(self.write_dispatch_count, previous_count + 1, f"{description} Ray dispatch count")

    def require_rejected_write(
        self,
        description: str,
        operation: Callable[[], object],
        expected_message: str,
    ) -> None:
        previous_count = self.write_dispatch_count
        self.require_error(description, operation, expected_message)
        require_equal(self.write_dispatch_count, previous_count + 1, f"{description} Ray dispatch count")

    @staticmethod
    def require_error(description: str, operation: Callable[[], object], expected_message: str) -> None:
        try:
            operation()
        except Exception as error:
            if expected_message not in str(error):
                raise AssertionError(
                    f"{description}: expected error containing {expected_message!r}, got {error!r}"
                ) from error
        else:
            raise AssertionError(f"{description}: operation unexpectedly succeeded")

    def scan_split_counts(self, query: str) -> dict[str, int]:
        self._plan_counter += 1
        relation = self.connection.sql(query)
        plan = self.vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation,
            f"vane-wheel-ray-iceberg-plan-{self._plan_counter}",
        ).to_physical_plan(self.connection)
        return {str(scan_id): len(batches) for scan_id, batches in plan.scan_split_batch_map().items()}

    def capture_write_plan(self, operation: Callable[[], object]) -> object:
        captured: list[object] = []

        def capture(relation: object) -> dict[str, object]:
            captured.append(
                self.vane.ray_cxx.PyLogicalPlan.from_duckdb_write_relation(
                    relation,
                    f"vane-wheel-ray-iceberg-stale-{uuid.uuid4()}",
                )
            )
            return {}

        self.runner.run_write = capture
        try:
            operation()
        finally:
            self.runner.run_write = self._record_distributed_write
        require_equal(len(captured), 1, "captured distributed write plan count")
        return captured[0]


class AnnotateWorkerNode:
    """Zero-argument actor UDF that records the Ray node for each batch."""

    def __call__(self, table: object) -> object:
        import pyarrow as pa
        import ray

        # Several batches stay runnable long enough for both one-CPU worker
        # nodes to consume the Iceberg-backed stream.
        time.sleep(0.05)
        node_id = str(ray.get_runtime_context().get_node_id())
        return pa.table(
            {
                "id": table.column("id"),
                "worker_node_id": [node_id] * table.num_rows,
            }
        )


def seed_source_table_with_local_fast(harness: RayIcebergHarness) -> None:
    connection = harness.connection
    configured_runner = os.environ.get("VANE_RUNNER")
    previous_write_count = harness.write_dispatch_count

    # range() does not expose file-backed scan splits for a Ray extension write.
    # Select local-fast explicitly for setup, persist four Iceberg files, and
    # restore Ray before any distributed assertion.
    os.environ["VANE_RUNNER"] = "local-fast"
    try:
        connection.execute(
            f"CREATE TABLE {SOURCE_TABLE} (id INTEGER, payload VARCHAR) "
            f"WITH ('format-version' = '2', 'write.data.path' = '{DATA_ROOT}/source')"
        )
        for partition_index in range(SOURCE_PARTITIONS):
            start = partition_index * ROWS_PER_PARTITION
            stop = start + ROWS_PER_PARTITION
            source_batch = connection.sql(
                "SELECT i::INTEGER AS id, ('value-' || i::VARCHAR)::VARCHAR AS payload "
                f"FROM range({start}, {stop}) AS source(i)"
            )
            source_batch.insert_into(SOURCE_TABLE)
    finally:
        if configured_runner is None:
            os.environ.pop("VANE_RUNNER", None)
        else:
            os.environ["VANE_RUNNER"] = configured_runner

    require_equal(
        harness.write_dispatch_count,
        previous_write_count,
        "local-fast Iceberg seed Ray dispatch count",
    )
    require_equal(
        connection.execute(f"SELECT count(*)::BIGINT FROM {SOURCE_TABLE}").fetchone(),
        (SOURCE_ROW_COUNT,),
        "local-fast Iceberg seed row count",
    )


def exercise_persistent_distributed_reads(harness: RayIcebergHarness) -> None:
    equality_scan = persistent_scan(
        "data/persistent/equality_deletes/warehouse/mydb/mytable",
        "filename = true",
    )
    harness.require_query(
        f"SELECT id, name, bir::VARCHAR, filename FROM {equality_scan} ORDER BY id",
        "equality deletes with a materialized filename column",
    )

    partitioned_equality_scan = persistent_scan(
        "data/persistent/equality_deletes/warehouse/mydb/mytable_partitioned",
        "filename = true",
    )
    harness.require_query(
        f"SELECT id, name, bir::VARCHAR, filename FROM {partitioned_equality_scan} "
        "WHERE id IN (1, 4, 5) ORDER BY id",
        "partitioned equality deletes, filename materialization, and filter pushdown",
    )

    hidden_delete_column_scan = persistent_scan(
        "data/persistent/equality_delete_extra_column/warehouse/ns/t/metadata/vfinal.metadata.json"
    )
    harness.require_query(
        f"SELECT val FROM {hidden_delete_column_scan} ORDER BY val",
        "equality deletes whose key is not part of the query projection",
    )

    nested_scan = persistent_scan("data/persistent/column_mapping/warehouse/default.db/my_table")
    harness.require_query(
        f"SELECT id, attributes['height'], list_sum(scores)::BIGINT, profile.verified "
        f"FROM {nested_scan} ORDER BY id",
        "nested Iceberg types and field-id mapping",
    )

    defaults_scan = persistent_scan(
        "data/persistent/add_columns_with_defaults/default.db/add_columns_with_defaults/metadata/"
        "00003-3f1801a5-7dfb-4072-b14a-39cd12f9279b.metadata.json"
    )
    harness.require_query(
        f"SELECT col_boolean, col_integer, col_string FROM {defaults_scan} ORDER BY ALL",
        "initial values for evolved Iceberg columns",
    )

    large_scan = persistent_scan("data/persistent/large_partitioned_table/metadata/v2.metadata.json")
    full_split_count = sum(harness.scan_split_counts(f"SELECT id FROM {large_scan}").values())
    filtered_query = (
        f"SELECT count(id)::BIGINT, min(id), max(id), sum(id)::BIGINT FROM {large_scan} "
        "WHERE joined >= TIMESTAMPTZ '1984-12-01 00:00:00+01'"
    )
    filtered_split_count = sum(harness.scan_split_counts(filtered_query).values())
    require_true(full_split_count > 1, "large Iceberg fixture did not produce multiple Ray scan splits")
    require_true(
        1 <= filtered_split_count <= full_split_count,
        "filtered Iceberg planning produced an invalid Ray scan split count",
    )
    harness.require_query(filtered_query, "filtered aggregate over a multi-file Iceberg table")

    complex_query = (
        "WITH ranked AS ("
        "  SELECT id, row_number() OVER (ORDER BY id) AS row_number "
        f"  FROM {large_scan} "
        "  WHERE joined >= TIMESTAMPTZ '1984-12-01 00:00:00+01'"
        "), matched AS ("
        "  SELECT left_rows.id, left_rows.row_number "
        "  FROM ranked left_rows "
        f"  JOIN {large_scan} right_rows ON right_rows.id = left_rows.id - 1 "
        "  WHERE right_rows.name IS NOT NULL"
        ") "
        "SELECT count(*)::BIGINT, sum(id)::BIGINT, max(row_number)::BIGINT FROM matched"
    )
    complex_split_counts = harness.scan_split_counts(complex_query)
    require_true(
        len(complex_split_counts) >= 2 and all(split_count >= 1 for split_count in complex_split_counts.values()),
        f"complex Iceberg query did not preserve two independent Ray scan nodes: {complex_split_counts!r}",
    )
    harness.require_query(complex_query, "two-scan join, filter, aggregate, CTE, and window query")


def exercise_source_target_and_topology(
    harness: RayIcebergHarness,
    ray: object,
    expected_nodes: set[str],
) -> None:
    connection = harness.connection
    vane = harness.vane

    seed_source_table_with_local_fast(harness)

    harness.require_query(
        f"SELECT count(*)::BIGINT FROM {SOURCE_TABLE}",
        "distributed scan of the local-fast Iceberg seed",
        [(SOURCE_ROW_COUNT,)],
    )
    scan_split_count = sum(harness.scan_split_counts(f"SELECT id, payload FROM {SOURCE_TABLE}").values())
    require_equal(scan_split_count, SOURCE_PARTITIONS, "independently schedulable Iceberg file splits")

    previous_read_count = harness.read_dispatch_count
    annotated_rows = (
        connection.sql(f"SELECT id, payload FROM {SOURCE_TABLE}")
        .map_batches(
            AnnotateWorkerNode,
            schema={
                "id": vane.sqltype("INTEGER"),
                "worker_node_id": vane.sqltype("VARCHAR"),
            },
            batch_size=64,
            cpus=1.0,
            execution_backend="ray_actor",
            actor_number=WORKER_COUNT,
            target_max_batch_bytes=4096,
        )
        .fetchall()
    )
    require_equal(harness.read_dispatch_count, previous_read_count + 1, "annotated Iceberg scan Ray dispatch count")
    require_equal(len(annotated_rows), SOURCE_ROW_COUNT, "distributed Iceberg scan row count")
    observed_nodes = {str(row[1]) for row in annotated_rows}
    require_equal(observed_nodes, expected_nodes, "Ray nodes consuming the Iceberg scan")
    assert_vane_worker_topology(ray, harness.runner)

    ctas_source = connection.sql(f"SELECT id, payload FROM {SOURCE_TABLE}")
    harness.require_rejected_write(
        "distributed Iceberg CTAS without an explicit worker data path",
        lambda: ctas_source.create(TARGET_TABLE),
        "requires an explicit 'location' or 'write.data.path' table property",
    )
    target_exists = connection.execute(
        "SELECT count(*) FROM information_schema.tables "
        f"WHERE table_catalog = '{CATALOG_NAME}' AND table_schema = 'default' AND table_name = '{TARGET_NAME}'"
    ).fetchone()
    require_equal(target_exists, (0,), "rejected distributed CTAS catalog cleanup")

    # Vane's current Relation.create() API cannot carry Iceberg table
    # properties. Create the target as coordinator-side test setup, then
    # exercise its writes exclusively through distributed Relation APIs.
    connection.execute(
        f"CREATE TABLE {TARGET_TABLE} (id INTEGER, payload VARCHAR) "
        f"WITH ('format-version' = '2', 'write.data.path' = '{DATA_ROOT}/target')"
    )
    target_source = connection.sql(f"SELECT id, payload FROM {SOURCE_TABLE}")
    harness.require_write(
        "distributed Iceberg INSERT SELECT",
        lambda: target_source.insert_into(TARGET_TABLE),
    )
    harness.require_query(
        f"SELECT count(*)::BIGINT, sum(id)::BIGINT FROM {TARGET_TABLE}",
        "distributed Iceberg INSERT SELECT result",
        [(SOURCE_ROW_COUNT, SOURCE_ROW_COUNT * (SOURCE_ROW_COUNT - 1) // 2)],
    )

    harness.require_write(
        "distributed Iceberg UPDATE",
        lambda: connection.table(TARGET_TABLE).update(
            {"payload": vane.ConstantExpression("updated")},
            condition=(vane.ColumnExpression("id") % vane.ConstantExpression(64)) == vane.ConstantExpression(0),
        ),
    )
    harness.require_write(
        "distributed Iceberg DELETE",
        lambda: connection.table(TARGET_TABLE).delete(
            condition=(vane.ColumnExpression("id") % vane.ConstantExpression(64)) == vane.ConstantExpression(1)
        ),
    )
    removed_sum = sum(1 + 64 * index for index in range(SOURCE_ROW_COUNT // 64))
    harness.require_query(
        f"SELECT count(*)::BIGINT, sum(id)::BIGINT FROM {TARGET_TABLE}",
        "distributed Iceberg UPDATE and DELETE commit result",
        [
            (
                SOURCE_ROW_COUNT - SOURCE_ROW_COUNT // 64,
                SOURCE_ROW_COUNT * (SOURCE_ROW_COUNT - 1) // 2 - removed_sum,
            )
        ],
    )
    harness.require_query(
        f"SELECT count(*)::BIGINT FROM {TARGET_TABLE} WHERE payload = 'updated'",
        "distributed Iceberg UPDATE row count",
        [(SOURCE_ROW_COUNT // 64,)],
    )

    data_file_count = connection.execute(
        f"SELECT count(DISTINCT file_path) FROM iceberg_metadata({SOURCE_TABLE}) " "WHERE manifest_content = 'DATA'"
    ).fetchone()[0]
    require_true(data_file_count >= SOURCE_PARTITIONS, "source table did not retain its multi-file write layout")
    deleted_ids = tuple(partition * ROWS_PER_PARTITION + 7 for partition in range(SOURCE_PARTITIONS))
    harness.require_write(
        "distributed multi-file positional DELETE",
        lambda: connection.table(SOURCE_TABLE).delete(condition=vane.ColumnExpression("id").isin(*deleted_ids)),
    )
    delete_file_count = connection.execute(
        f"SELECT count(*) FROM iceberg_metadata({SOURCE_TABLE}) WHERE manifest_content = 'DELETE'"
    ).fetchone()[0]
    require_true(delete_file_count >= 1, "multi-file distributed DELETE did not commit delete metadata")
    deleted_id_list = ", ".join(str(value) for value in deleted_ids)
    harness.require_query(
        f"SELECT count(*)::BIGINT FROM {SOURCE_TABLE} WHERE id IN ({deleted_id_list})",
        "distributed positional deletes across multiple source files",
        [(0,)],
    )
    harness.require_query(
        f"SELECT count(*)::BIGINT, sum(id)::BIGINT FROM {SOURCE_TABLE} WHERE id % 3 = 1",
        "filter and aggregate after multi-file positional deletes",
    )


def exercise_empty_and_zero_match(harness: RayIcebergHarness) -> None:
    connection = harness.connection
    vane = harness.vane
    connection.execute(
        f"CREATE TABLE {EMPTY_TABLE} (id INTEGER, payload VARCHAR) "
        f"WITH ('format-version' = '2', 'write.data.path' = '{DATA_ROOT}/empty')"
    )
    empty_split_count = sum(harness.scan_split_counts(f"SELECT id FROM {EMPTY_TABLE}").values())
    require_equal(empty_split_count, 1, "empty Iceberg Ray sentinel split count")
    harness.require_query(
        f"SELECT count(*)::BIGINT FROM {EMPTY_TABLE}",
        "empty distributed Iceberg COUNT(*)",
        [(0,)],
    )

    snapshot_count = connection.execute(f"SELECT count(*) FROM iceberg_snapshots({EMPTY_TABLE})").fetchone()
    harness.require_write(
        "zero-match distributed Iceberg UPDATE",
        lambda: connection.table(EMPTY_TABLE).update(
            {"payload": vane.ConstantExpression("unreachable")},
            condition=vane.ColumnExpression("id") == vane.ConstantExpression(1),
        ),
    )
    harness.require_write(
        "zero-match distributed Iceberg DELETE",
        lambda: connection.table(EMPTY_TABLE).delete(
            condition=vane.ColumnExpression("id") == vane.ConstantExpression(1)
        ),
    )
    require_equal(
        connection.execute(f"SELECT count(*) FROM iceberg_snapshots({EMPTY_TABLE})").fetchone(),
        snapshot_count,
        "zero-match mutations must not create Iceberg snapshots",
    )


def exercise_partitioned_mutations(harness: RayIcebergHarness) -> None:
    connection = harness.connection
    vane = harness.vane
    connection.execute(
        f"CREATE TABLE {PARTITION_TABLE} (id INTEGER, category VARCHAR, ts TIMESTAMP, payload VARCHAR) "
        f"PARTITIONED BY (category, year(ts)) "
        f"WITH ('format-version' = '2', 'write.data.path' = '{DATA_ROOT}/partitioned')"
    )
    values = connection.sql(
        "SELECT id + 1 AS id, "
        "CASE id WHEN 0 THEN 'A' WHEN 2 THEN 'B' WHEN 3 THEN 'A' WHEN 5 THEN 'C' END::VARCHAR AS category, "
        "CASE id WHEN 0 THEN TIMESTAMP '2020-01-15' WHEN 2 THEN TIMESTAMP '2021-06-01' "
        "WHEN 3 THEN TIMESTAMP '2020-09-01' WHEN 4 THEN TIMESTAMP '2022-03-20' "
        "WHEN 5 THEN TIMESTAMP '2022-11-05' END AS ts, "
        "CASE id WHEN 0 THEN 'a' WHEN 1 THEN 'b' WHEN 2 THEN 'c' WHEN 3 THEN 'd' "
        "WHEN 4 THEN 'e' WHEN 5 THEN 'f' END::VARCHAR AS payload "
        f"FROM {SOURCE_TABLE} WHERE id < 6"
    )
    harness.require_write(
        "distributed identity-and-year-partitioned INSERT",
        lambda: values.insert_into(PARTITION_TABLE),
    )

    full_split_count = sum(harness.scan_split_counts(f"SELECT id FROM {PARTITION_TABLE}").values())
    filtered_split_count = sum(
        harness.scan_split_counts(
            f"SELECT id FROM {PARTITION_TABLE} WHERE category = 'A' "
            "AND ts >= TIMESTAMP '2020-01-01' AND ts < TIMESTAMP '2021-01-01'"
        ).values()
    )
    require_true(full_split_count > 1, "partitioned Iceberg table did not produce multiple Ray scan splits")
    require_true(
        1 <= filtered_split_count <= full_split_count,
        "identity/year partition filtering produced an invalid Ray scan split count",
    )
    harness.require_query(
        f"SELECT id, category, ts::VARCHAR, payload FROM {PARTITION_TABLE} WHERE category = 'A' "
        "AND ts >= TIMESTAMP '2020-01-01' AND ts < TIMESTAMP '2021-01-01' ORDER BY id",
        "identity/year-partition-filtered distributed Iceberg read",
        [(1, "A", "2020-01-15 00:00:00", "a"), (4, "A", "2020-09-01 00:00:00", "d")],
    )

    harness.require_write(
        "distributed UPDATE moving identity and transformed partitions",
        lambda: connection.table(PARTITION_TABLE).update(
            {
                "category": vane.ConstantExpression("MOVED"),
                "ts": vane.ConstantExpression("2023-01-15 00:00:00").cast("TIMESTAMP"),
                "payload": vane.ConstantExpression("updated-a"),
            },
            condition=vane.ColumnExpression("id") == vane.ConstantExpression(1),
        ),
    )
    harness.require_write(
        "distributed DELETE from null identity partitions",
        lambda: connection.table(PARTITION_TABLE).delete(condition=vane.ColumnExpression("id").isin(2, 5)),
    )
    harness.require_query(
        f"SELECT id, category, ts::VARCHAR, payload FROM {PARTITION_TABLE} ORDER BY id",
        "partitioned distributed UPDATE and DELETE result",
        [
            (1, "MOVED", "2023-01-15 00:00:00", "updated-a"),
            (3, "B", "2021-06-01 00:00:00", "c"),
            (4, "A", "2020-09-01 00:00:00", "d"),
            (6, "C", "2022-11-05 00:00:00", "f"),
        ],
    )


def exercise_schema_evolution(harness: RayIcebergHarness) -> None:
    connection = harness.connection
    connection.execute(
        f"CREATE TABLE {SCHEMA_EVOLUTION_TABLE} (id INTEGER, measure INTEGER, obsolete VARCHAR) "
        f"WITH ('format-version' = '2', 'write.data.path' = '{DATA_ROOT}/schema-evolution')"
    )
    initial_values = connection.sql(
        f"SELECT id + 1 AS id, id + 10 AS measure, 'old'::VARCHAR AS obsolete " f"FROM {SOURCE_TABLE} WHERE id = 0"
    )
    harness.require_write(
        "distributed INSERT before Iceberg schema evolution",
        lambda: initial_values.insert_into(SCHEMA_EVOLUTION_TABLE),
    )

    connection.execute(f"ALTER TABLE {SCHEMA_EVOLUTION_TABLE} RENAME measure TO metric")
    connection.execute(f"ALTER TABLE {SCHEMA_EVOLUTION_TABLE} ALTER metric TYPE BIGINT")
    connection.execute(f"ALTER TABLE {SCHEMA_EVOLUTION_TABLE} ADD COLUMN added VARCHAR")
    connection.execute(f"ALTER TABLE {SCHEMA_EVOLUTION_TABLE} DROP COLUMN obsolete")

    evolved_values = connection.sql(
        f"SELECT id + 1 AS id, 9223372036854770000::BIGINT AS metric, 'new'::VARCHAR AS added "
        f"FROM {SOURCE_TABLE} WHERE id = 1"
    )
    harness.require_write(
        "distributed INSERT after rename, promotion, add, and drop",
        lambda: evolved_values.insert_into(SCHEMA_EVOLUTION_TABLE),
    )
    split_count = sum(harness.scan_split_counts(f"SELECT id, metric, added FROM {SCHEMA_EVOLUTION_TABLE}").values())
    require_true(split_count >= 2, "schema-evolved Iceberg table did not preserve multiple data files")
    harness.require_query(
        f"SELECT id, metric, added FROM {SCHEMA_EVOLUTION_TABLE} ORDER BY id",
        "distributed read across Iceberg schema evolution",
        [(1, 10, None), (2, 9223372036854770000, "new")],
    )


def exercise_conflicts_and_fail_closed_writes(harness: RayIcebergHarness) -> None:
    connection = harness.connection
    vane = harness.vane
    connection.execute(
        f"CREATE TABLE {CONFLICT_TABLE} (id INTEGER, category VARCHAR) PARTITIONED BY (category) "
        f"WITH ('format-version' = '2', 'write.data.path' = '{DATA_ROOT}/conflict')"
    )
    initial_values = connection.sql(f"SELECT id + 1 AS id, 'A'::VARCHAR AS category FROM {SOURCE_TABLE} WHERE id = 0")
    harness.require_write("distributed conflict-table INSERT", lambda: initial_values.insert_into(CONFLICT_TABLE))

    stale_plan = harness.capture_write_plan(
        lambda: connection.table(CONFLICT_TABLE).update(
            {"category": vane.ConstantExpression("STALE")},
            condition=vane.ColumnExpression("id") == vane.ConstantExpression(1),
        )
    )
    # This direct SQL write is coordinator-side conflict setup. The stale
    # operation under test remains the captured Relation UPDATE submitted to Ray.
    connection.execute(f"INSERT INTO {CONFLICT_TABLE} VALUES (2, 'B')")
    snapshots_after_concurrent_commit = connection.execute(
        f"SELECT count(*) FROM iceberg_snapshots({CONFLICT_TABLE})"
    ).fetchone()
    client = harness.runner.query_driver_client
    if client is None:
        raise AssertionError("the Ray runner did not create a query driver client")
    harness.require_error(
        "stale distributed UPDATE source snapshot",
        lambda: client.run_copy_plan(stale_plan),
        "snapshot changed between the distributed UPDATE source scan and write planning",
    )
    require_equal(
        connection.execute(f"SELECT count(*) FROM iceberg_snapshots({CONFLICT_TABLE})").fetchone(),
        snapshots_after_concurrent_commit,
        "stale distributed UPDATE must not create an Iceberg snapshot",
    )

    connection.execute(f"ALTER TABLE {CONFLICT_TABLE} RESET PARTITIONED BY")
    snapshots_before_unpartitioned_insert = connection.execute(
        f"SELECT count(*) FROM iceberg_snapshots({CONFLICT_TABLE})"
    ).fetchone()
    unpartitioned_values = connection.sql(
        f"SELECT id + 1 AS id, 'C'::VARCHAR AS category FROM {SOURCE_TABLE} WHERE id = 2"
    )
    harness.require_write(
        "distributed INSERT after resetting partitioning",
        lambda: unpartitioned_values.insert_into(CONFLICT_TABLE),
    )
    require_equal(
        connection.execute(f"SELECT count(*) FROM iceberg_snapshots({CONFLICT_TABLE})").fetchone(),
        (snapshots_before_unpartitioned_insert[0] + 1,),
        "unpartitioned distributed INSERT snapshot count",
    )
    harness.require_query(
        f"SELECT id, category FROM {CONFLICT_TABLE} ORDER BY id",
        "conflict cleanup and partition-reset write result",
        [(1, "A"), (2, "B"), (3, "C")],
    )


def drop_test_tables(connection: object) -> None:
    for table in TABLES:
        connection.execute(f"DROP TABLE IF EXISTS {table}")


def main() -> None:
    if os.environ.get("VANE_RUNNER") != "ray":
        raise RuntimeError("the distributed wheel integration test requires VANE_RUNNER=ray")

    # Keep every file split independently schedulable so the topology case can
    # prove that both one-CPU Ray workers consume Iceberg input.
    os.environ["VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION"] = "1"

    wait_for_http_endpoint(MINIO_READY_ENDPOINT)
    wait_for_http_endpoint(f"{CATALOG_ENDPOINT}/v1/config")
    configure_worker_session_environment()

    import ray
    import vane
    from vane import runners

    if ray.is_initialized():
        raise RuntimeError("the Ray wheel integration test must own its Ray cluster")

    cluster = create_two_worker_cluster(ray)
    connection = None
    try:
        expected_nodes = execution_node_ids(ray)
        vane.set_runner_ray(noop_if_initialized=True)
        runner = runners.get_or_create_runner()
        require_equal(getattr(runner, "name", None), "ray", "configured Vane runner")

        connection = vane.connect(
            ":memory:",
            config={
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            },
        )
        for setting in ("autoinstall_known_extensions", "autoload_known_extensions"):
            value = connection.execute(f"SELECT current_setting('{setting}')").fetchone()
            require_equal(str(value[0]).lower(), "false", f"{setting} setting")

        verify_extension_is_wheel_linked(connection)
        configure_coordinator_s3(connection)
        connection.execute("SET unsafe_enable_version_guessing = true")
        connection.execute("SET TimeZone = 'UTC'")
        connection.execute(
            f"ATTACH '' AS {CATALOG_NAME} "
            "(TYPE ICEBERG, CLIENT_ID 'admin', CLIENT_SECRET 'password', "
            f"ENDPOINT '{CATALOG_ENDPOINT}', stage_create_tables true)"
        )
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.default")
        drop_test_tables(connection)

        harness = RayIcebergHarness(vane, connection, runner)
        run_scenario("persistent distributed reads", lambda: exercise_persistent_distributed_reads(harness))
        run_scenario(
            "source/target writes and worker topology",
            lambda: exercise_source_target_and_topology(harness, ray, expected_nodes),
        )
        run_scenario("empty and zero-match operations", lambda: exercise_empty_and_zero_match(harness))
        run_scenario("partitioned mutations", lambda: exercise_partitioned_mutations(harness))
        run_scenario("schema evolution", lambda: exercise_schema_evolution(harness))
        run_scenario(
            "conflicts and fail-closed writes",
            lambda: exercise_conflicts_and_fail_closed_writes(harness),
        )
        require_true(harness.read_dispatch_count >= 15, "comprehensive suite did not exercise enough Ray reads")
        require_true(harness.write_dispatch_count >= 14, "comprehensive suite did not exercise enough Ray writes")

        drop_test_tables(connection)
    finally:
        try:
            if connection is not None:
                try:
                    drop_test_tables(connection)
                except Exception:
                    # Preserve the primary test failure; the fixture volume is
                    # destroyed by the workflow's unconditional cleanup step.
                    pass
                connection.close()
        finally:
            try:
                vane.teardown_runner()
            finally:
                if ray.is_initialized():
                    ray.shutdown()
                cluster.shutdown()


if __name__ == "__main__":
    main()
