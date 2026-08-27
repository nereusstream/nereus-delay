#!/usr/bin/env python3
"""Focused closed-schema tests for verify-ndip-package.py."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
VERIFIER_PATH = ROOT / "scripts/verify-ndip-package.py"
SPEC = importlib.util.spec_from_file_location("verify_ndip_package", VERIFIER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load NDIP verifier")
VERIFIER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFIER)


class VerifyNdipPackageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.package_dir = ROOT / "docs/ndip/NDIP-1"
        self.receipt_path = self.package_dir / "acceptance-receipt.json"
        self.receipt = VERIFIER.load_receipt(self.receipt_path)

    def test_accepted_receipt_binds_exact_package_and_implementation_authority(self) -> None:
        _, paths, package_digest = VERIFIER.validate_receipt_shape(
            self.receipt, self.package_dir, self.receipt_path, ROOT
        )
        actual_digest, files = VERIFIER.calculate_package(paths, ROOT)
        VERIFIER.verify_file_digests(self.receipt, files)

        self.assertEqual(package_digest, actual_digest)
        self.assertEqual("PASS", self.receipt["authorization"]["gateB"])
        self.assertIs(
            True, self.receipt["authorization"]["implementationAuthorized"]
        )
        self.assertIs(
            True,
            self.receipt["authorization"]["localDisposableTestingAuthorized"],
        )

    def test_candidate_cannot_claim_implementation_authority(self) -> None:
        candidate = self._candidate()
        candidate["authorization"]["implementationAuthorized"] = True

        with self.assertRaisesRegex(
            VERIFIER.VerificationError,
            "candidate must not authorize H1 through H6 implementation",
        ):
            VERIFIER.validate_receipt_shape(
                candidate,
                self.package_dir,
                self.package_dir / "acceptance-receipt.candidate.json",
                ROOT,
            )

    def test_require_accepted_rejects_candidate_status(self) -> None:
        with self.assertRaisesRegex(
            VERIFIER.VerificationError, "explicit Accepted authority is required"
        ):
            VERIFIER.verify_required_status("CANDIDATE", True)

        VERIFIER.verify_required_status("ACCEPTED", True)

    def test_accepted_gate_b_must_authorize_implementation(self) -> None:
        receipt = copy.deepcopy(self.receipt)
        receipt["authorization"]["implementationAuthorized"] = False

        with self.assertRaisesRegex(
            VERIFIER.VerificationError,
            "accepted Gate B must authorize H1 through H6 implementation",
        ):
            VERIFIER.validate_receipt_shape(
                receipt, self.package_dir, self.receipt_path, ROOT
            )

    def test_gate_c_remains_required_before_shadow_and_enabled(self) -> None:
        for field in ("gateCRequiredBeforeShadow", "gateCRequiredBeforeEnabled"):
            with self.subTest(field=field):
                receipt = copy.deepcopy(self.receipt)
                receipt["authorization"][field] = False
                with self.assertRaisesRegex(
                    VERIFIER.VerificationError, field
                ):
                    VERIFIER.validate_receipt_shape(
                        receipt, self.package_dir, self.receipt_path, ROOT
                    )

    def _candidate(self) -> dict[str, object]:
        candidate = copy.deepcopy(self.receipt)
        candidate["receiptStatus"] = "CANDIDATE"
        candidate["authority"] = False
        candidate["governanceBridge"]["observedStatus"] = "DRAFT"
        candidate["decision"] = {
            "status": "PENDING",
            "acceptedBy": None,
            "acceptedAt": None,
            "decisionReference": None,
        }
        candidate["authorization"]["gateB"] = "PENDING"
        candidate["authorization"]["implementationAuthorized"] = False
        candidate["authorization"]["localDisposableTestingAuthorized"] = False
        return candidate


if __name__ == "__main__":
    unittest.main()
