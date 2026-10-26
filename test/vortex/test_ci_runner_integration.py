"""Exercise CI skip detection with a real DuckDB sqllogictest runner."""

import argparse
from contextlib import redirect_stderr
import io
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts/ci"))
import run_vortex_tests as runner


class NativeRunnerTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="iceberg-ci-runner-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "test").mkdir()
        self.name = "test/ci_runner_probe.test"
        self.report = self.root / "report.txt"
        self.env = {key: value for key, value in os.environ.items() if not key.startswith("DUCKDB_TEST_")}
        self.env["ICEBERG_CI_SKIP_PROBE"] = "mismatch"
        root_patch = patch.object(runner, "ROOT", self.root)
        root_patch.start()
        self.addCleanup(root_patch.stop)

    def write_test(self, sql):
        (self.root / self.name).write_text(sql)

    def run_test(self):
        return runner.run_sql_test(self.binary, self.name, self.report, self.env)

    def test_complete_case(self):
        self.write_test("statement ok\nSELECT 1;\n\nstatement ok\nSELECT 2;\n")
        self.assertEqual(self.run_test(), 2)

    def test_late_require_env(self):
        self.write_test(
            "statement ok\nSELECT 1;\n\nrequire-env ICEBERG_CI_SKIP_PROBE required\n\nstatement ok\nSELECT 2;\n"
        )
        with redirect_stderr(io.StringIO()), self.assertRaisesRegex(ValueError, "test was skipped"):
            self.run_test()

    def test_default_error_skips_are_disabled(self):
        for message in ("HTTP regression probe", "Unable to connect regression probe"):
            with self.subTest(message=message):
                self.write_test(
                    f"statement ok\nSELECT 1;\n\nstatement ok\nSELECT error('{message}');\n\nstatement ok\nSELECT 2;\n"
                )
                with redirect_stderr(io.StringIO()), self.assertRaisesRegex(RuntimeError, "Command failed"):
                    self.run_test()

    def test_initial_skip(self):
        self.write_test("require-env ICEBERG_CI_SKIP_PROBE required\n\nstatement ok\nSELECT 1;\n")
        with redirect_stderr(io.StringIO()), self.assertRaisesRegex(ValueError, "test was skipped"):
            self.run_test()

    def test_missing_case_cannot_reuse_report(self):
        self.write_test("statement ok\nSELECT 1;\n")
        self.assertEqual(self.run_test(), 1)
        (self.root / self.name).unlink()
        with redirect_stderr(io.StringIO()), self.assertRaisesRegex(ValueError, "exactly one matching"):
            self.run_test()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unittest", required=True, type=Path)
    args = parser.parse_args()
    NativeRunnerTests.binary = args.unittest.resolve()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(NativeRunnerTests)
    sys.exit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
