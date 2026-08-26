#!/usr/bin/env python3
"""Exercise a statically linked Iceberg extension from a packaged Vane wheel."""

from __future__ import annotations

import os
import time
import urllib.error
import urllib.request


CATALOG_ENDPOINT = "http://127.0.0.1:8181"
MINIO_READY_ENDPOINT = "http://127.0.0.1:9000/minio/health/ready"
CATALOG_NAME = "vane_wheel_catalog"
TABLE_NAME = "vane_wheel_iceberg_integration"
TABLE = f"{CATALOG_NAME}.default.{TABLE_NAME}"
V3_TABLE = f"{CATALOG_NAME}.default.vane_wheel_iceberg_v3_local"


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
        "SELECT loaded, install_mode FROM duckdb_extensions() " "WHERE extension_name = 'iceberg'"
    ).fetchone()
    if extension is None:
        raise AssertionError("the packaged Vane wheel does not contain iceberg")
    require_equal(extension[1], "STATICALLY_LINKED", "iceberg install mode before LOAD")

    connection.execute("LOAD iceberg")
    loaded = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() " "WHERE extension_name = 'iceberg'"
    ).fetchone()
    require_equal(loaded, (True, "STATICALLY_LINKED"), "iceberg after LOAD")
    connection.execute("LOAD httpfs")


def main() -> None:
    if os.environ.get("VANE_RUNNER") != "local-fast":
        raise RuntimeError("the wheel integration test requires VANE_RUNNER=local-fast")

    wait_for_http_endpoint(MINIO_READY_ENDPOINT)
    wait_for_http_endpoint(f"{CATALOG_ENDPOINT}/v1/config")

    import vane

    connection = vane.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    try:
        for setting in ("autoinstall_known_extensions", "autoload_known_extensions"):
            value = connection.execute(f"SELECT current_setting('{setting}')").fetchone()
            require_equal(str(value[0]).lower(), "false", f"{setting} setting")

        verify_extension_is_wheel_linked(connection)
        connection.execute(
            "CREATE SECRET vane_wheel_iceberg_s3 "
            "(TYPE S3, KEY_ID 'admin', SECRET 'password', ENDPOINT '127.0.0.1:9000', "
            "URL_STYLE 'path', USE_SSL false)"
        )
        connection.execute(
            f"ATTACH '' AS {CATALOG_NAME} "
            "(TYPE ICEBERG, CLIENT_ID 'admin', CLIENT_SECRET 'password', "
            f"ENDPOINT '{CATALOG_ENDPOINT}')"
        )
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.default")
        connection.execute(f"DROP TABLE IF EXISTS {TABLE}")
        connection.execute(f"DROP TABLE IF EXISTS {V3_TABLE}")
        connection.execute(f"CREATE TABLE {TABLE} (id INTEGER, payload VARCHAR) " "WITH ('format-version' = '2')")
        connection.execute(f"INSERT INTO {TABLE} VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        require_equal(
            connection.execute(f"SELECT id, payload FROM {TABLE} ORDER BY id").fetchall(),
            [(1, "one"), (2, "two"), (3, "three")],
            "Iceberg scan after INSERT",
        )

        connection.execute(f"CALL set_iceberg_table_properties({TABLE}, {{'vane.integration': 'wheel'}})")
        require_equal(
            connection.execute(
                f"SELECT key, value FROM iceberg_table_properties({TABLE}) " "WHERE key = 'vane.integration'"
            ).fetchall(),
            [("vane.integration", "wheel")],
            "Iceberg metadata update",
        )

        connection.execute(f"DELETE FROM {TABLE} WHERE id = 2")
        require_equal(
            connection.execute(f"SELECT id, payload FROM {TABLE} ORDER BY id").fetchall(),
            [(1, "one"), (3, "three")],
            "Iceberg scan after DELETE",
        )

        # A Vane-enabled extension build must retain the independent local-fast
        # backend for v3 mutations. This Relation DELETE deliberately bypasses
        # the distributed provider and exercises the native Puffin writer.
        connection.execute(f"CREATE TABLE {V3_TABLE} (id INTEGER, payload VARCHAR) WITH ('format-version' = '3')")
        connection.execute(f"INSERT INTO {V3_TABLE} VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        connection.table(V3_TABLE).delete(condition=vane.ColumnExpression("id") == vane.ConstantExpression(2))
        require_equal(
            connection.execute(f"SELECT id, payload FROM {V3_TABLE} ORDER BY id").fetchall(),
            [(1, "one"), (3, "three")],
            "local-fast Iceberg v3 Relation DELETE",
        )

        connection.execute(f"DROP TABLE {TABLE}")
        connection.execute(f"DROP TABLE {V3_TABLE}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
