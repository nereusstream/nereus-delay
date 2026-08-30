from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest


VALIDATOR_PATH = Path(__file__).with_name("ndip1-staging-skip-audit.py")
SPEC = importlib.util.spec_from_file_location("ndip1_staging_skip_audit", VALIDATOR_PATH)
assert SPEC is not None and SPEC.loader is not None
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)


class StagingSkipAuditTest(unittest.TestCase):
    def write_result(self, directory: Path, status: str) -> None:
        marker = {
            "PASS": "",
            "SKIPPED": '<skipped message="dependency unavailable" />',
            "FAILED": '<failure message="executor exited 137" />',
        }[status]
        (directory / f"TEST-{status}.xml").write_text(
            f'<testsuite><testcase classname="example" name="case">{marker}</testcase></testsuite>',
            encoding="utf-8",
        )

    def observations(self, *statuses: str):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result_dirs = []
            for index, status in enumerate(statuses):
                result_dir = root / str(index)
                result_dir.mkdir()
                self.write_result(result_dir, status)
                result_dirs.append(result_dir)
            cases = VALIDATOR.collect_cases(result_dirs)
            return cases[("example", "case")]

    def test_failure_cannot_be_masked_by_later_pass(self) -> None:
        observations = self.observations("FAILED", "PASS")

        self.assertEqual("FAILED", VALIDATOR.aggregate_status(observations))
        self.assertEqual(["FAILED", "PASS"], [item["status"] for item in observations])

    def test_all_duplicate_passes_remain_pass(self) -> None:
        observations = self.observations("PASS", "PASS")

        self.assertEqual("PASS", VALIDATOR.aggregate_status(observations))

    def test_skip_cannot_be_promoted_by_pass(self) -> None:
        observations = self.observations("SKIPPED", "PASS")

        self.assertEqual("SKIPPED", VALIDATOR.aggregate_status(observations))


if __name__ == "__main__":
    unittest.main()
