from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
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

    def test_policy_requires_external_key_and_monotonic_current_head(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            policy_dir = Path(directory) / "policy"
            policy_dir.mkdir()
            trusted_key = Path(directory) / "issuer.der"
            trusted_key.write_bytes(b"trusted-ed25519-der")
            scope = "12" * 32
            for version, phase in enumerate(
                    ("shadow-initial", "shadow-candidate-add", "shadow-candidate-cancel"), start=1):
                payload = (json.dumps({
                    "policySchema": "nereus-delay.handoff-policy-publication",
                    "policyStatus": "SHADOW",
                    "policyOxiaVersion": version,
                    "policyGeneration": str(version),
                    "policyScopeDigest": scope,
                }) + "\n").encode()
                envelope = {
                    "evidenceSchema": "nereus-delay.persistent-staging-evidence",
                    "keyGeneration": 1,
                    "publicKeyDerBase64": base64.b64encode(trusted_key.read_bytes()).decode(),
                    "payloadBase64": base64.b64encode(payload).decode(),
                    "payloadSha256": hashlib.sha256(payload).hexdigest(),
                }
                (policy_dir / f"{phase}.signed.json").write_text(json.dumps(envelope), encoding="utf-8")
                (policy_dir / f"{phase}-readback.log").write_text(
                    f"policyMode=SHADOW\npolicyOxiaVersion={version}\npolicyGeneration={version}\n",
                    encoding="utf-8",
                )

            VALIDATOR.verify_policy(policy_dir, trusted_key, 1)

            bad_key = Path(directory) / "bad.der"
            bad_key.write_bytes(b"different-ed25519-der")
            with self.assertRaises(SystemExit):
                VALIDATOR.verify_policy(policy_dir, bad_key, 1)


if __name__ == "__main__":
    unittest.main()
