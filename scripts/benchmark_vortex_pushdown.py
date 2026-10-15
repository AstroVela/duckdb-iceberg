#!/usr/bin/env python3
"""Compare native Iceberg Parquet scan profiles using identical data and queries."""

import argparse
import json
from pathlib import Path
import statistics
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "test/vortex"))
from test_parquet_pushdown import iceberg_scans, quote, run_sql


def profile_query(shell, setup, query, expected):
    # Detailed profiling includes binding, where file-format classification runs.
    # EXPLAIN ANALYZE starts its latency timer after that work.
    with tempfile.TemporaryDirectory(prefix="iceberg-benchmark-") as directory:
        output = Path(directory) / "profile.json"
        settings = (
            "SET enable_profiling='json'; SET profiling_mode='detailed'; " f"SET profiling_output={quote(output)}; "
        )
        assert run_sql(shell, setup + settings + query) == [{"total": expected}]
        return json.loads(output.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=2_000_000)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.rows < 1000 or args.repeats < 1:
        parser.error("--rows must be at least 1000 and --repeats must be positive")
    shells = {}
    for item in args.duckdb:
        label, separator, path = item.partition("=")
        if not separator or not label or label in shells:
            parser.error("--duckdb requires a unique LABEL=PATH")
        shells[label] = Path(path).resolve()
    args.directory.mkdir(parents=True, exist_ok=True)
    table = args.directory.resolve() / "parquet"
    if not table.exists():
        run_sql(
            next(iter(shells.values())),
            f"COPY (SELECT i::BIGINT id, (i % 97)::BIGINT v FROM range({args.rows}) t(i)) "
            f"TO {quote(table)} (FORMAT iceberg, DATA_FORMAT parquet);",
        )
    scan = f"iceberg_scan({quote(table)})"
    low = args.rows // 2
    queries = {
        "selective_range": ("", f"SELECT sum(v)::BIGINT total FROM {scan} WHERE id >= {low} AND id < {low + 100}"),
        "dynamic_join": (
            f"CREATE TABLE probe_keys AS SELECT i::BIGINT id FROM range({low}, {low + 100}) t(i); ",
            f"SELECT sum(v)::BIGINT total FROM {scan} JOIN probe_keys USING (id)",
        ),
        "full_scan": ("", f"SELECT sum(v)::BIGINT total FROM {scan}"),
    }
    report = {"rows": args.rows, "threads": 1, "repeats": args.repeats, "runs": {}}
    for label, shell in shells.items():
        assert run_sql(shell, f"SELECT count(*) n FROM {scan}") == [{"n": args.rows}]
        results = {}
        for name, (setup, query) in queries.items():
            expected = sum(i % 97 for i in (range(args.rows) if name == "full_scan" else range(low, low + 100)))
            # Warm the OS file cache once, then measure fresh query binds/processes.
            profile_query(shell, setup, query, expected)
            profiles = [profile_query(shell, setup, query, expected) for _ in range(args.repeats)]
            scans = [iceberg_scans(plan) for plan in profiles]
            assert all(len(entries) == 1 for entries in scans), scans
            results[name] = {
                "median_latency_ms": statistics.median(plan["latency"] for plan in profiles) * 1000,
                "median_binding_ms": statistics.median(plan["planner_binding"] for plan in profiles) * 1000,
                "median_scan_ms": statistics.median(entries[0]["operator_timing"] for entries in scans) * 1000,
                "scan_output_rows": [entries[0]["operator_cardinality"] for entries in scans],
                "filters": scans[0][0]["extra_info"].get("Filters"),
                "dynamic_filters": scans[0][0]["extra_info"].get("Dynamic Filters"),
            }
            print(f"{label} {name}: {results[name]['median_latency_ms']:.3f} ms", flush=True)
        report["runs"][label] = {"binary": str(shell), "queries": results}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
