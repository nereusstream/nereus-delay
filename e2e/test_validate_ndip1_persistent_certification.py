from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


VALIDATOR_PATH = Path(__file__).with_name("validate-ndip1-persistent-certification.py")
SPEC = importlib.util.spec_from_file_location("ndip1_persistent_validator", VALIDATOR_PATH)
assert SPEC is not None and SPEC.loader is not None
VALIDATOR = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = VALIDATOR
SPEC.loader.exec_module(VALIDATOR)


class PersistentCertificationValidatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.private_key = self.root / "private.der"
        self.public_key = self.root / "public.der"
        subprocess.run(
            ["openssl", "genpkey", "-algorithm", "ED25519", "-outform", "DER", "-out", str(self.private_key)],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "openssl",
                "pkey",
                "-in",
                str(self.private_key),
                "-inform",
                "DER",
                "-pubout",
                "-outform",
                "DER",
                "-out",
                str(self.public_key),
            ],
            check=True,
            capture_output=True,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_envelope(self, path: Path, payload: dict, key_generation: int = 1) -> None:
        payload_bytes = (json.dumps(payload, separators=(",", ":")) + "\n").encode()
        digest = hashlib.sha256(payload_bytes).digest()
        message = VALIDATOR.SIGNATURE_DOMAIN + key_generation.to_bytes(4, "big") + digest
        message_path = self.root / "signature-input.bin"
        signature_path = self.root / "signature.bin"
        message_path.write_bytes(message)
        subprocess.run(
            [
                "openssl",
                "pkeyutl",
                "-sign",
                "-inkey",
                str(self.private_key),
                "-keyform",
                "DER",
                "-rawin",
                "-in",
                str(message_path),
                "-out",
                str(signature_path),
            ],
            check=True,
            capture_output=True,
        )
        envelope = {
            "evidenceSchema": VALIDATOR.ENVELOPE_SCHEMA,
            "evidenceSchemaGeneration": 1,
            "keyGeneration": key_generation,
            "publicKeyDerBase64": base64.b64encode(self.public_key.read_bytes()).decode(),
            "payloadBase64": base64.b64encode(payload_bytes).decode(),
            "payloadSha256": digest.hex(),
            "signatureBase64": base64.b64encode(signature_path.read_bytes()).decode(),
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(envelope) + "\n", encoding="utf-8")

    def test_verifies_domain_separated_ed25519_envelope(self) -> None:
        path = self.root / "evidence.signed.json"
        self.write_envelope(path, {"status": "PASS"})

        verified = VALIDATOR.verify_envelope(path, self.public_key.read_bytes())

        self.assertEqual("PASS", verified.payload["status"])

    def test_rejects_tampered_envelope(self) -> None:
        path = self.root / "evidence.signed.json"
        self.write_envelope(path, {"status": "PASS"})
        envelope = json.loads(path.read_text(encoding="utf-8"))
        envelope["payloadBase64"] = base64.b64encode(b'{"status":"FAIL"}\n').decode()
        path.write_text(json.dumps(envelope), encoding="utf-8")

        with self.assertRaises(VALIDATOR.VerificationError):
            VALIDATOR.verify_envelope(path, self.public_key.read_bytes())

    def test_requires_monotonic_single_scope_policy_chain(self) -> None:
        policy_dir = self.root / "authority" / "policy"
        candidate = "12" * 20
        scope = "34" * 32
        phases = (
            ("shadow-initial", "SHADOW", 0, 0),
            ("shadow-candidate-add", "SHADOW", 0, 0),
            ("shadow-candidate-cancel", "SHADOW", 0, 0),
            ("enabled", "ENABLED", 7000, 3),
            ("disabled", "DISABLED", 0, 0),
        )
        for version, (phase, mode, lead, paths) in enumerate(phases, start=1):
            payload = {
                "policySchema": "nereus-delay.handoff-policy-publication",
                "policyStatus": mode,
                "candidateCommit": candidate,
                "policyOxiaVersion": version,
                "policyGeneration": str(version),
                "policyScopeDigest": scope,
                "policySnapshotDigest": f"{version:064x}",
                "effectiveLeadMs": lead,
                "allowedPathBits": paths,
                "validUntilEpochMs": 1000 if phase == "enabled" else 2000,
                "effectiveDisabledAfterEpochMs": 1000 if phase == "disabled" else 0,
            }
            self.write_envelope(policy_dir / f"{phase}.signed.json", payload)
            (policy_dir / f"{phase}-readback.log").write_text(
                f"policyMode={mode}\npolicyOxiaVersion={version}\npolicyGeneration={version}\n",
                encoding="utf-8",
            )

        policies, observed_scope = VALIDATOR.verify_policy_chain(
            self.root, self.public_key.read_bytes(), candidate
        )

        self.assertEqual(scope, observed_scope)
        self.assertEqual("DISABLED", policies["disabled"].payload["policyStatus"])

    def test_managed_handoff_requires_physical_interval_and_journal(self) -> None:
        path = self.root / "managed.json"
        digest = "56" * 32
        value = {
            "schema": "nereus-delay.managed-handoff-canary-evidence",
            "verdict": "PASS",
            "productionPath": True,
            "productionAuthority": False,
            "nativeAdmission": 1,
            "nativeSend": 1,
            "handedOff": 1,
            "deliveryContract": "PULSAR_NATIVE_DELIVERY",
            "actionAtEpochMs": 100,
            "brokerPersistenceTimeEpochMs": 110,
            "deliverAtEpochMs": 120,
            "policySnapshotDigest": digest,
            "p1SourceLock": "78" * 20,
            "destinationResponseLossResolved": True,
            "attemptJournalResponseLossRecoveries": 3,
            "sequenceId": 7,
            "journal": [
                {"kind": "MAPPED", "sequenceId": 7},
                {"kind": "OWNERSHIP_STARTED", "sequenceId": 7},
                {"kind": "PUBLISHED", "sequenceId": 7},
            ],
        }
        for field in (
            "publishAttemptId",
            "preparedPublishHash",
            "recordTemplateHash",
            "preparedRecordHash",
            "sendCommandSha256",
            "authenticatedResponseCommandSha256",
            "artifactSetDigest",
            "schedulePositionSha256",
            "admissionPositionSha256",
            "outcomePositionSha256",
        ):
            value[field] = "9a" * 32
        path.write_text(json.dumps(value), encoding="utf-8")

        VALIDATOR.verify_managed_handoff_evidence(path, digest, "78" * 20)
        value["brokerPersistenceTimeEpochMs"] = 120
        path.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaises(VALIDATOR.VerificationError):
            VALIDATOR.verify_managed_handoff_evidence(path, digest, "78" * 20)


if __name__ == "__main__":
    unittest.main()
