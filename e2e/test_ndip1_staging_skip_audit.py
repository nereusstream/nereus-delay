from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


VALIDATOR_PATH = Path(__file__).with_name("ndip1-staging-skip-audit.py")
SPEC = importlib.util.spec_from_file_location("ndip1_staging_skip_audit", VALIDATOR_PATH)
assert SPEC is not None and SPEC.loader is not None
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)


class StagingSkipAuditTest(unittest.TestCase):
    def write_result(
        self,
        directory: Path,
        status: str,
        classname: str = "example",
        name: str = "case",
    ) -> None:
        marker = {
            "PASS": "",
            "SKIPPED": '<skipped message="dependency unavailable" />',
            "FAILED": '<failure message="executor exited 137" />',
        }[status]
        (directory / f"TEST-{status}.xml").write_text(
            f'<testsuite><testcase classname="{classname}" name="{name}">{marker}</testcase></testsuite>',
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

    def test_verified_expected_termination_is_not_a_missing_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected_dir = root / "expected"
            pass_dir = root / "pass"
            recovery_dir = root / "recovery"
            expected_dir.mkdir()
            pass_dir.mkdir()
            recovery_dir.mkdir()
            self.write_result(
                expected_dir,
                "SKIPPED",
                VALIDATOR.LOCAL_STORAGE_TEST_CLASS,
                VALIDATOR.LOCAL_STORAGE_TEST_NAME,
            )
            self.write_result(
                pass_dir,
                "PASS",
                VALIDATOR.LOCAL_STORAGE_TEST_CLASS,
                VALIDATOR.LOCAL_STORAGE_TEST_NAME,
            )

            kill_receipt = root / "kill-receipt.json"
            kill_receipt.write_text(
                json.dumps(
                    {
                        "schema": "nereus-delay.ndip1-local-storage-kill",
                        "cell": "disaster-host-fault",
                        "signal": "SIGKILL",
                        "signalNumber": 9,
                        "targetPid": 123,
                        "exactTarget": True,
                    }
                ),
                encoding="utf-8",
            )
            recovery_run = recovery_dir / "run.json"
            recovery_run.write_text(
                json.dumps(
                    {
                        "schema": "nereus-delay.ndip1-test-run",
                        "label": "recovery",
                        "exitCode": 0,
                    }
                ),
                encoding="utf-8",
            )
            marker = {
                "schema": "nereus-delay.ndip1-expected-fault",
                "schemaGeneration": 1,
                "classification": "EXPECTED_TERMINATION",
                "cell": "disaster-host-fault",
                "signal": "SIGKILL",
                "signalNumber": 9,
                "testClass": "com.nereusstream.delay.store.LocalStorageDurableChaosTest",
                "testName": "localStorageFailureSurvivesFreshProcessRecovery()",
                "resultDir": str(expected_dir),
                "killReceipt": str(kill_receipt),
                "killReceiptSha256": hashlib.sha256(kill_receipt.read_bytes()).hexdigest(),
                "recoveryRunJson": str(recovery_run),
                "recoveryRunJsonSha256": hashlib.sha256(recovery_run.read_bytes()).hexdigest(),
            }
            (expected_dir / "expected-fault.json").write_text(
                json.dumps(marker), encoding="utf-8"
            )

            observations = VALIDATOR.collect_cases([expected_dir, pass_dir])[
                (VALIDATOR.LOCAL_STORAGE_TEST_CLASS, VALIDATOR.LOCAL_STORAGE_TEST_NAME)
            ]

            self.assertEqual("PASS", VALIDATOR.aggregate_status(observations))
            self.assertEqual(1, sum(item.get("expectedFault", False) for item in observations))


if __name__ == "__main__":
    unittest.main()
