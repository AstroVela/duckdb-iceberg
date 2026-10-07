"""Prevent the Vortex CI runner from accepting skipped or mismatched SQL reports."""

from pathlib import Path
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts/ci"))
from run_vortex_tests import sql_assertions


class ReportValidationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.report = Path(directory.name) / "report.xml"
        self.expected = "/checkout/test/sql/local/vortex/vortex_filters.test"

    def write_report(self, successes="24", failures="0", expected_failures="0", names=None):
        root = ET.Element("Catch")
        group = ET.SubElement(root, "Group")
        for name in names if names is not None else [self.expected]:
            case = ET.SubElement(group, "TestCase", name=name)
            ET.SubElement(case, "OverallResult", success="true" if failures == "0" else "false")
        ET.SubElement(
            root, "OverallResults", successes=successes, failures=failures, expectedFailures=expected_failures
        )
        ET.ElementTree(root).write(self.report)

    def test_successful_case(self):
        self.write_report()
        self.assertEqual(sql_assertions(self.report, self.expected), 24)

    def test_skipped_case_is_not_success(self):
        self.write_report(successes="0")
        with self.assertRaisesRegex(ValueError, "zero assertions"):
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

    def test_failed_and_expected_failure_assertions(self):
        for failures, expected_failures in (("1", "0"), ("0", "1")):
            with self.subTest(failures=failures, expected_failures=expected_failures):
                self.write_report(failures=failures, expected_failures=expected_failures)
                with self.assertRaisesRegex(ValueError, "unsuccessful"):
                    sql_assertions(self.report, self.expected)

    def test_truncated_report(self):
        self.report.write_text("<Catch><Group>")
        with self.assertRaises(ET.ParseError):
            sql_assertions(self.report, self.expected)


if __name__ == "__main__":
    unittest.main()
