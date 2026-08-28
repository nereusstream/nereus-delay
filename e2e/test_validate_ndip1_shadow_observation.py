from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest


VALIDATOR_PATH = Path(__file__).with_name("validate-ndip1-shadow-observation.py")
SPEC = importlib.util.spec_from_file_location("ndip1_shadow_validator", VALIDATOR_PATH)
assert SPEC is not None and SPEC.loader is not None
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)


class ShadowObservationValidatorTest(unittest.TestCase):
    def test_normalizes_gradle_junit_no_argument_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "TEST-example.xml"
            report.write_text(
                '<testsuite><testcase classname="example" name="case()" /></testsuite>',
                encoding="utf-8",
            )

            self.assertEqual(
                {("example", "case"): "PASS"},
                VALIDATOR.result_cases(Path(directory)),
            )

    def test_preserves_parameterized_case_names(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "TEST-example.xml"
            report.write_text(
                '<testsuite><testcase classname="example" name="case(1)" /></testsuite>',
                encoding="utf-8",
            )

            self.assertEqual(
                {("example", "case(1)"): "PASS"},
                VALIDATOR.result_cases(Path(directory)),
            )


if __name__ == "__main__":
    unittest.main()
