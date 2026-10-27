"""Check Iceberg scan predicates with native ON and OFF builds."""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile

from test_local_catalog import local_catalog, quote


def run_sql(shell, sql):
    result = subprocess.run(
        [str(shell), "-unsigned", "-batch", "-bail", "-json", "-c", "SET threads=1; " + sql],
        text=True,
        capture_output=True,
        timeout=120,
    )
    assert result.returncode == 0, f"Exit {result.returncode}: {result.stderr}\nSQL: {sql}"
    output = result.stdout.strip()
    decoder = json.JSONDecoder()
    rows = None
    while output:
        rows, end = decoder.raw_decode(output)
        output = output[end:].strip()
    return rows


def iceberg_scans(plan):
    scans = []
    if plan.get("extra_info", {}).get("Function") == "ICEBERG_SCAN":
        scans.append(plan)
    for child in plan.get("children", []):
        scans.extend(iceberg_scans(child))
    return scans


def profile(shell, query, setup=""):
    return run_sql(shell, setup + "EXPLAIN (ANALYZE, FORMAT JSON) " + query)


def check_parquet(shell, root):
    table = root / "parquet"
    run_sql(
        shell,
        f"COPY (SELECT i::BIGINT id, (i % 97)::BIGINT v FROM range(10000) t(i)) "
        f"TO {quote(table)} (FORMAT iceberg, DATA_FORMAT parquet);",
    )
    scan = f"iceberg_scan({quote(table)})"
    query = f"SELECT sum(v)::BIGINT total FROM {scan} WHERE id >= 4200 AND id < 4204"
    assert run_sql(shell, query) == [{"total": sum(i % 97 for i in range(4200, 4204))}]
    scans = iceberg_scans(profile(shell, query))
    assert len(scans) == 1, scans
    assert "Filters" in scans[0]["extra_info"], scans[0]
    assert scans[0]["operator_cardinality"] == 4, scans[0]

    query = f"SELECT sum(id)::BIGINT total FROM {scan} WHERE id % 2 = 0"
    assert run_sql(shell, query) == [{"total": sum(range(0, 10000, 2))}]
    scans = iceberg_scans(profile(shell, query))
    assert "Filters" in scans[0]["extra_info"], scans[0]
    assert scans[0]["operator_cardinality"] == 5000, scans[0]

    setup = "CREATE TABLE probe_keys AS SELECT i::BIGINT id FROM range(4200, 4204) t(i); "
    query = f"SELECT sum(v)::BIGINT total FROM {scan} JOIN probe_keys USING (id)"
    assert run_sql(shell, setup + query) == [{"total": sum(i % 97 for i in range(4200, 4204))}]
    scans = iceberg_scans(profile(shell, query, setup))
    assert "Dynamic Filters" in scans[0]["extra_info"], scans[0]
    assert scans[0]["operator_cardinality"] == 4, scans[0]


def float_values(dtype, ids):
    values = ("0.0", "-0.0", "-1.0", "1.0", "-infinity", "infinity", "NaN", None)
    return ", ".join(
        f"({index}, {quote(values[index - 1]) if values[index - 1] is not None else 'NULL'}::{dtype})" for index in ids
    )


def check_float_predicates(shell, source, setup, dtype, ids, pushdown):
    predicates = {
        "x = 0": [1, 2],
        "x < 0": [3, 5],
        "x <= 0": [1, 2, 3, 5],
        "x > 0": [4, 6, 7],
        "x = 'NaN'": [7],
        "x >= 'NaN'": [7],
        "x > 'NaN'": [],
        "x < 'NaN'": [1, 2, 3, 4, 5, 6],
        "x > 'infinity'": [7],
        "x IN (0, 'NaN')": [1, 2, 7],
        "isnan(x)": [7],
        "x IS NULL": [8],
        "x = 0 AND signbit(x)": [2],
    }
    for predicate, expected in predicates.items():
        query = f"SELECT id FROM {source} WHERE {predicate} ORDER BY id"
        actual = run_sql(shell, setup + query)
        assert actual == [{"id": index} for index in expected if index in ids], (predicate, actual)
    query = f"SELECT id FROM {source} WHERE x = 0"
    scans = iceberg_scans(profile(shell, query, setup))
    assert len(scans) == 1, scans
    assert ("Filters" in scans[0]["extra_info"]) == pushdown, scans[0]
    assert "Dynamic Filters" not in scans[0]["extra_info"], scans[0]
    for key, expected in (("0.0", (1, 2)), ("NaN", (7,)), ("infinity", (6,))):
        join_setup = setup + f"CREATE TABLE float_keys AS SELECT {quote(key)}::{dtype} x; "
        query = f"SELECT id FROM {source} JOIN float_keys USING (x) ORDER BY id"
        actual = run_sql(shell, join_setup + query)
        assert actual == [{"id": index} for index in expected if index in ids], (key, actual)
        if not pushdown:
            scans = iceberg_scans(profile(shell, query, join_setup))
            assert "Dynamic Filters" not in scans[0]["extra_info"], scans[0]
            assert "Filters" not in scans[0]["extra_info"], scans[0]


def check_parquet_floats(shell, root, dtype):
    table = root / f"parquet_float_{dtype}"
    ids = (1, 2, 3, 7, 8)
    run_sql(
        shell,
        f"COPY (SELECT * FROM (VALUES {float_values(dtype, ids)}) t(id, x)) "
        f"TO {quote(table)} (FORMAT iceberg, DATA_FORMAT parquet);",
    )
    check_float_predicates(shell, f"iceberg_scan({quote(table)})", "", dtype, ids, True)


def check_mixed_snapshots(shell, root, dtype, first_format):
    table = root / f"{dtype}_{first_format}"
    # The finite upper bound is zero: x > 0 must retain the NaN in this file.
    initial_ids = (1, 3, 7)
    run_sql(
        shell,
        f"COPY (SELECT * FROM (VALUES {float_values(dtype, initial_ids)}) t(id, x)) "
        f"TO {quote(table)} (FORMAT iceberg, DATA_FORMAT {first_format});",
    )
    path = next((table / "metadata").glob("*.metadata.json"))
    with local_catalog(path) as (server, attach):
        old_snapshot = server.metadata["current-snapshot-id"]
        next_format = "vortex" if first_format == "parquet" else "parquet"
        # Deliberately disagree with the stored format before the first append.
        server.metadata["properties"]["write.format.default"] = next_format
        check_float_predicates(shell, "lake.main.items", attach, dtype, initial_ids, first_format == "parquet")
        run_sql(shell, attach + f"INSERT INTO lake.main.items VALUES {float_values(dtype, (2, 4, 5, 6, 8))};")
        run_sql(
            shell, attach + "CALL set_iceberg_table_properties(lake.main.items, {'write.format.default': 'parquet'});"
        )
        check_float_predicates(shell, "lake.main.items", attach, dtype, range(1, 9), False)
        current = f"iceberg_scan({quote(server.metadata_path)})"
        check_float_predicates(shell, current, "", dtype, range(1, 9), False)
        old = f"iceberg_scan({quote(server.metadata_path)}, snapshot_from_id={old_snapshot})"
        check_float_predicates(shell, old, "", dtype, initial_ids, first_format == "parquet")

        # Both capabilities must coexist in one plan and survive prepared execution.
        query = f"SELECT id FROM {old} WHERE x = 0 UNION ALL SELECT id FROM {current} WHERE x = 0 ORDER BY id"
        scans = iceberg_scans(profile(shell, query))
        assert len(scans) == 2, scans
        assert sum("Filters" in entry["extra_info"] for entry in scans) == (first_format == "parquet"), scans
        assert run_sql(shell, f"PREPARE p AS {query}; EXECUTE p;") == [{"id": 1}, {"id": 1}, {"id": 2}]
        assert not server.errors, server.errors


def check_pending_append(shell, root, dtype, first_format):
    table = root / f"pending_append_{dtype}_{first_format}"
    run_sql(
        shell,
        f"COPY (SELECT 1::INTEGER id, '0.0'::{dtype} x) TO {quote(table)} "
        f"(FORMAT iceberg, DATA_FORMAT {first_format});",
    )
    path = next((table / "metadata").glob("*.metadata.json"))
    with local_catalog(path) as (server, attach):
        server.metadata["properties"]["write.format.default"] = "vortex" if first_format == "parquet" else "parquet"
        for predicate, expected in (("x = 0", [1, 2]), ("x > 0", [3]), ("x = 'NaN'", [3])):
            query = f"SELECT id FROM lake.main.items WHERE {predicate} ORDER BY id"
            setup = (
                attach + f"BEGIN; PREPARE p AS {query}; "
                f"INSERT INTO lake.main.items VALUES (2, '-0.0'::{dtype}), (3, 'NaN'::{dtype}); "
            )
            rows = [{"id": index} for index in expected]
            assert run_sql(shell, setup + query + "; ROLLBACK;") == rows
            assert run_sql(shell, setup + "EXECUTE p; ROLLBACK;") == rows
            scans = iceberg_scans(profile(shell, query, setup))
            assert "Filters" not in scans[0]["extra_info"], scans[0]
            assert "Dynamic Filters" not in scans[0]["extra_info"], scans[0]
        assert not server.commits, server.commits
        assert not server.errors, server.errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=("ON", "OFF"))
    args = parser.parse_args()
    shell = args.duckdb.resolve()
    with tempfile.TemporaryDirectory(prefix="iceberg-pushdown-") as directory:
        root = Path(directory)
        check_parquet(shell, root)
        for dtype in ("FLOAT", "DOUBLE"):
            check_parquet_floats(shell, root, dtype)
        if args.mode == "ON":
            for dtype in ("FLOAT", "DOUBLE"):
                for first_format in ("parquet", "vortex"):
                    check_mixed_snapshots(shell, root, dtype, first_format)
                    check_pending_append(shell, root, dtype, first_format)
    print(f"Iceberg predicate pushdown {args.mode}: passed")


if __name__ == "__main__":
    main()
