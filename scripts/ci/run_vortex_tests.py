#!/usr/bin/env python3
"""Run the native Iceberg Vortex CI suite, rejecting skipped SQL tests."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
PARQUET_TESTS = (
    "test/sql/copy/basic_copy.test",
    "test/sql/copy/empty_copy.test",
    "test/sql/copy/data_format.test",
    "test/sql/local/column_mapping.test",
    "test/sql/local/column_mapping_delete.test",
    "test/sql/local/equality_deletes.test",
    "test/sql/local/equality_delete_extra_column.test",
    "test/sql/local/equality_deletes_join.test",
)
PYTHON_TESTS = ("test_local_catalog.py", "test_file_access.py", "test_schema_changes.py")


def sql_assertions(report, expected):
    """Catch reports a skipped sqllogictest as a successful case with zero assertions."""
    root = ET.parse(report).getroot()
    cases = root.findall("./Group/TestCase")
    if len(cases) != 1 or not ("/" + cases[0].get("name", "")).endswith("/" + expected):
        raise ValueError(f"{expected}: expected exactly one matching test case in {report}")
    result = cases[0].find("OverallResult")
    totals = root.find("OverallResults")
    if (
        result is None
        or result.get("success") != "true"
        or totals is None
        or totals.get("failures") != "0"
        or totals.get("expectedFailures") != "0"
    ):
        raise ValueError(f"{expected}: unsuccessful or incomplete report: {report}")
    assertions = int(totals.get("successes", "0"))
    if assertions <= 0:
        raise ValueError(f"{expected}: zero assertions; test was skipped ({report})")
    return assertions


def check_build(build, mode):
    cache = (build / "CMakeCache.txt").read_text()
    if f"ICEBERG_ENABLE_VORTEX:BOOL={mode}" not in cache.splitlines():
        raise ValueError(f"{build}: expected ICEBERG_ENABLE_VORTEX:BOOL={mode}")
    targets = subprocess.check_output(["ninja", "-C", str(build), "-t", "targets", "all"], text=True)
    if mode == "ON":
        if "vortex_extension" not in targets or "iceberg_vortex.cpp.o" not in targets:
            raise ValueError("ON build is missing the Vortex extension or Iceberg adapter")
    else:
        if re.search(r"vortex_extension|vortex_duckdb|iceberg_vortex\.cpp|corrosion|cargo-build", targets):
            raise ValueError("OFF build includes Vortex or Rust build targets")
        for pattern in ("vortex_extension_fc-*", "corrosion-*"):
            if list((build / "_deps").glob(pattern)):
                raise ValueError(f"OFF build fetched a Vortex dependency: {pattern}")
    print(f"Build isolation: {mode} passed", flush=True)


def check_binary(build, mode, output):
    command = [
        str(build / "duckdb"),
        "-unsigned",
        "-batch",
        "-bail",
        "-json",
        "-c",
        "SELECT extension_name, install_mode FROM duckdb_extensions() WHERE loaded;",
    ]
    extensions = json.loads(subprocess.check_output(command, text=True, cwd=ROOT))
    static = {row["extension_name"] for row in extensions if row["install_mode"] == "STATICALLY_LINKED"}
    required = {"parquet", "avro", "iceberg", "httpfs"}
    if mode == "ON":
        required.add("vortex")
    if not required <= static or (mode == "OFF" and "vortex" in static):
        raise ValueError(f"Unexpected static extensions for {mode}: {sorted(static)}")
    (output / "extensions.json").write_text(json.dumps(extensions, indent=2) + "\n")
    if mode == "OFF":
        for binary in (build / "duckdb", build / "test/unittest"):
            symbols = subprocess.check_output(["nm", "-C", "--defined-only", str(binary)], text=True)
            if re.search(r"\b(?:duckdb::(?:IcebergVortex|VortexExtension)|vortex(?:_|::))", symbols):
                raise ValueError(f"OFF binary contains Vortex symbols: {binary}")
    print(f"Binary isolation: {mode} passed", flush=True)


def run_logged(command, log, env):
    with log.open("w") as stream:
        result = subprocess.run(command, cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT, timeout=300)
    if result.returncode:
        print(log.read_text(), file=sys.stderr)
        raise RuntimeError(f"Command failed ({result.returncode}); see {log}")


def run_tests(build, mode, output):
    output.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["ICEBERG_TEST_VORTEX"] = "1" if mode == "ON" else "0"
    env["DUCKDB_TEST_AUTOLOADING"] = "available"
    env["DUCKDB_TEST_SETTINGS"] = "[{'name':'autoload_known_extensions','value':'false'}]"
    tests = list(PARQUET_TESTS)
    if mode == "ON":
        vortex_tests = sorted((ROOT / "test/sql/local/vortex").glob("*.test"))
        if not vortex_tests:
            raise ValueError("No Vortex SQL tests found")
        tests.extend(str(path.relative_to(ROOT)) for path in vortex_tests)
    else:
        tests.append("test/sql/copy/vortex_disabled.test")
    assertions = 0
    for test in tests:
        if not (ROOT / test).is_file():
            raise ValueError(f"Missing test: {test}")
        report = output / (Path(test).stem + ".xml")
        # Remove previous output so a stale report cannot hide a missing test.
        report.unlink(missing_ok=True)
        run_logged(
            [str(build / "test/unittest"), "*" + test, "--reporter", "xml", "--out", str(report)],
            report.with_suffix(".log"),
            env,
        )
        count = sql_assertions(report, test)
        assertions += count
        print(f"PASS {test}: {count} assertions", flush=True)
    print(f"SQL {mode}: {len(tests)} cases, {assertions} assertions", flush=True)
    if mode == "ON":
        for test in PYTHON_TESTS:
            run_logged(
                [sys.executable, str(ROOT / "test/vortex" / test), "--duckdb", str(build / "duckdb")],
                output / (Path(test).stem + ".log"),
                env,
            )
            print(f"PASS {test}", flush=True)
    summary = f"Vortex {mode}: {len(tests)} SQL cases / {assertions} assertions passed.\n"
    if mode == "ON":
        summary += f"{len(PYTHON_TESTS)} local Python regressions passed.\n"
    (output / "summary.txt").write_text(summary)
    if "GITHUB_STEP_SUMMARY" in os.environ:
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
            stream.write(summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("ON", "OFF"))
    parser.add_argument("--build-dir", required=True, type=Path)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "build/ci-logs/tests")
    parser.add_argument("--check-build-only", action="store_true")
    args = parser.parse_args()
    build = args.build_dir.resolve()
    output = args.output_dir.resolve()
    check_build(build, args.mode)
    if args.check_build_only:
        return
    output.mkdir(parents=True, exist_ok=True)
    check_binary(build, args.mode, output)
    run_tests(build, args.mode, output)


if __name__ == "__main__":
    main()
