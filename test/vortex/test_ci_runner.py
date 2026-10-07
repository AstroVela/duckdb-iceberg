"""Prevent the Vortex CI runner from accepting skipped or mismatched SQL reports."""

from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts/ci"))
from run_vortex_tests import sql_assertions


class ReportValidationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.report = Path(directory.name) / "report.txt"
        self.expected = "/checkout/test/sql/local/vortex/vortex_filters.test"

    def write_report(self, totals="All tests passed (24 assertions in 1 test case)", names=None):
        names = names if names is not None else [self.expected]
        cases = "".join(f"0.001 s: {name}\n" for name in names)
        self.report.write_text(f"Filters: {self.expected}\n{cases}{'=' * 79}\n{totals}\n\n")

    def test_successful_case(self):
        for assertions in (1, 24):
            with self.subTest(assertions=assertions):
                label = "assertion" if assertions == 1 else "assertions"
                self.write_report(f"All tests passed ({assertions} {label} in 1 test case)")
                self.assertEqual(sql_assertions(self.report, self.expected), assertions)

    def test_skipped_case_is_not_success(self):
        self.write_report(
            "All tests were skipped (total skipped 1)\n\nSkipped tests for the following reasons:\nrequire-env MISSING: 1"
        )
        with self.assertRaisesRegex(ValueError, "test was skipped"):
            sql_assertions(self.report, self.expected)

    def test_missing_wrong_or_multiple_cases(self):
        for names in (
            [],
            ["another.test"],
            [self.expected.removeprefix("/checkout/")],
            ["/another" + self.expected],
            [self.expected, self.expected],
        ):
            with self.subTest(names=names):
                self.write_report(names=names)
                with self.assertRaisesRegex(ValueError, "exactly one matching"):
                    sql_assertions(self.report, self.expected)

    def test_unsuccessful_or_incomplete_totals(self):
        for totals in (
            "No tests ran",
            "All tests passed (0 assertions in 1 test case)",
            "All tests passed (24 assertions in 2 test cases)",
            "All tests passed (1 skipped test, 24 assertions in 1 test case)",
            "test cases: 1 | 1 failed\nassertions: 2 | 1 passed | 1 failed",
            "test cases: 1 | 1 skipped\nassertions: 2 | 1 passed | 1 skipped",
            "",
            "All tests passed (24 assertions in 1 test case)\ntruncated output",
        ):
            with self.subTest(totals=totals):
                self.write_report(totals)
                with self.assertRaisesRegex(ValueError, "unsuccessful"):
                    sql_assertions(self.report, self.expected)


if __name__ == "__main__":
    unittest.main()
