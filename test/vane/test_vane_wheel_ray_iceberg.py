#!/usr/bin/env python3
"""Exercise distributed Iceberg scans and commits from a packaged Vane wheel."""

from __future__ import annotations

import os
import time
import urllib.error
import urllib.request


CATALOG_ENDPOINT = "http://127.0.0.1:8181"
MINIO_ENDPOINT = "http://127.0.0.1:9000"
MINIO_READY_ENDPOINT = f"{MINIO_ENDPOINT}/minio/health/ready"
CATALOG_NAME = "vane_wheel_ray_catalog"
SOURCE_NAME = "vane_wheel_ray_source"
TARGET_NAME = "vane_wheel_ray_target"
SOURCE_TABLE = f"{CATALOG_NAME}.default.{SOURCE_NAME}"
TARGET_TABLE = f"{CATALOG_NAME}.default.{TARGET_NAME}"
SOURCE_DATA_PATH = "s3://warehouse/vane-wheel-ray-integration/source"
TARGET_DATA_PATH = "s3://warehouse/vane-wheel-ray-integration/target"
WORKER_COUNT = 2
SOURCE_PARTITIONS = 4
ROWS_PER_PARTITION = 256
SOURCE_ROW_COUNT = SOURCE_PARTITIONS * ROWS_PER_PARTITION


def require_equal(actual: object, expected: object, description: str) -> None:
    if actual != expected:
        raise AssertionError(f"{description}: expected {expected!r}, got {actual!r}")


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
        "SELECT loaded, install_mode FROM duckdb_extensions() "
        "WHERE extension_name = 'iceberg'"
    ).fetchone()
    if extension is None:
        raise AssertionError("the packaged Vane wheel does not contain iceberg")
    require_equal(extension[1], "STATICALLY_LINKED", "iceberg install mode before LOAD")

    connection.execute("LOAD iceberg")
    loaded = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() "
        "WHERE extension_name = 'iceberg'"
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


def count_distributed_scan_tasks(vane: object, connection: object) -> int:
    relation = connection.sql(f"SELECT id, payload FROM {SOURCE_TABLE}")
    plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
        relation,
        "vane-wheel-ray-iceberg-scan",
    ).to_physical_plan(connection)
    return sum(len(tasks) for tasks in plan.scan_task_descriptor_map().values())


def annotate_worker_node(table: object) -> object:
    import pyarrow as pa
    import ray

    # Several batches stay runnable long enough for both one-CPU worker nodes
    # to consume the Iceberg-backed stream.
    time.sleep(0.05)
    node_id = str(ray.get_runtime_context().get_node_id())
    return pa.table(
        {
            "id": table.column("id"),
            "worker_node_id": [node_id] * table.num_rows,
        }
    )


def assert_vane_worker_topology(ray: object, runner: object) -> None:
    client = runner.query_driver_client
    if client is None:
        raise AssertionError("the Ray runner did not create a query driver client")
    stats = ray.get(client.runner.fragment_stats.remote())
    workers = stats.get("workers") if isinstance(stats, dict) else None
    if not isinstance(workers, dict):
        raise AssertionError(f"Vane fragment statistics do not expose workers: {stats!r}")
    require_equal(len(workers), WORKER_COUNT, "Vane Ray worker count")


def main() -> None:
    if os.environ.get("VANE_RUNNER") != "ray":
        raise RuntimeError("the distributed wheel integration test requires VANE_RUNNER=ray")

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
        connection.execute(
            f"ATTACH '' AS {CATALOG_NAME} "
            "(TYPE ICEBERG, CLIENT_ID 'admin', CLIENT_SECRET 'password', "
            f"ENDPOINT '{CATALOG_ENDPOINT}', stage_create_tables true)"
        )
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.default")
        connection.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
        connection.execute(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")
        connection.execute(
            f"CREATE TABLE {SOURCE_TABLE} (id INTEGER, payload VARCHAR) "
            "WITH ('format-version' = '2', "
            f"'write.data.path' = '{SOURCE_DATA_PATH}')"
        )

        for partition_index in range(SOURCE_PARTITIONS):
            start = partition_index * ROWS_PER_PARTITION
            stop = start + ROWS_PER_PARTITION
            connection.execute(
                f"INSERT INTO {SOURCE_TABLE} "
                "SELECT i::INTEGER, ('value-' || i::VARCHAR)::VARCHAR "
                f"FROM range({start}, {stop}) AS source(i)"
            )

        require_equal(
            connection.execute(f"SELECT count(*) FROM {SOURCE_TABLE}").fetchone(),
            (SOURCE_ROW_COUNT,),
            "distributed Iceberg INSERT result",
        )
        scan_task_count = count_distributed_scan_tasks(vane, connection)
        if scan_task_count < SOURCE_PARTITIONS:
            raise AssertionError(
                f"expected at least {SOURCE_PARTITIONS} planned Iceberg scan tasks, got {scan_task_count}"
            )

        annotated_rows = connection.sql(f"SELECT id, payload FROM {SOURCE_TABLE}").map_batches(
            annotate_worker_node,
            schema={"id": vane.sqltype("INTEGER"), "worker_node_id": vane.sqltype("VARCHAR")},
            batch_size=64,
            cpus=1.0,
            execution_backend="ray_actor",
            actor_number=WORKER_COUNT,
            target_max_batch_bytes=4096,
        ).fetchall()
        require_equal(len(annotated_rows), SOURCE_ROW_COUNT, "distributed Iceberg scan row count")
        observed_nodes = {str(row[1]) for row in annotated_rows}
        require_equal(observed_nodes, expected_nodes, "Ray nodes consuming the Iceberg scan")
        assert_vane_worker_topology(ray, runner)

        connection.execute(
            f"CREATE TABLE {TARGET_TABLE} "
            "WITH ('format-version' = '2', "
            f"'write.data.path' = '{TARGET_DATA_PATH}') AS "
            f"SELECT id, payload FROM {SOURCE_TABLE}"
        )
        require_equal(
            connection.execute(f"SELECT count(*), sum(id) FROM {TARGET_TABLE}").fetchone(),
            (SOURCE_ROW_COUNT, SOURCE_ROW_COUNT * (SOURCE_ROW_COUNT - 1) // 2),
            "distributed Iceberg CTAS result",
        )

        connection.execute(
            f"UPDATE {TARGET_TABLE} "
            "SET payload = ('updated-' || id::VARCHAR)::VARCHAR "
            "WHERE id % 64 = 0"
        )
        connection.execute(f"DELETE FROM {TARGET_TABLE} WHERE id % 64 = 1")
        removed_sum = sum(1 + 64 * index for index in range(SOURCE_ROW_COUNT // 64))
        require_equal(
            connection.execute(
                f"SELECT count(*), "
                "sum(CASE WHEN starts_with(payload, 'updated-') THEN 1 ELSE 0 END), "
                f"sum(id) FROM {TARGET_TABLE}"
            ).fetchone(),
            (
                SOURCE_ROW_COUNT - SOURCE_ROW_COUNT // 64,
                SOURCE_ROW_COUNT // 64,
                SOURCE_ROW_COUNT * (SOURCE_ROW_COUNT - 1) // 2 - removed_sum,
            ),
            "distributed Iceberg UPDATE and DELETE commit result",
        )

        connection.execute(f"DROP TABLE {TARGET_TABLE}")
        connection.execute(f"DROP TABLE {SOURCE_TABLE}")
    finally:
        try:
            if connection is not None:
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
