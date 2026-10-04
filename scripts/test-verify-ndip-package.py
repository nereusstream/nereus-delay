#!/usr/bin/env python3
"""Focused closed-schema tests for verify-ndip-package.py."""

from __future__ import annotations

import copy
import importlib.util
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
VERIFIER_PATH = ROOT / "scripts/verify-ndip-package.py"
NDIP1_ACCEPTED_TRANSITION_COMMIT = "77ffa61136c0cd401244ebea4375a5a18b097b17"
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
        actual_digest, files = VERIFIER.calculate_package(
            paths, ROOT, NDIP1_ACCEPTED_TRANSITION_COMMIT
        )
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

    def test_ndip2_accepted_receipt_binds_current_package_without_deployment_authority(
        self,
    ) -> None:
        package_dir = ROOT / "docs/ndip/NDIP-2"
        receipt_path = package_dir / "acceptance-receipt.json"
        receipt = VERIFIER.load_receipt(receipt_path)
        proposal_id, paths, package_digest = VERIFIER.validate_receipt_shape(
            receipt, package_dir, receipt_path, ROOT
        )
        actual_digest, files = VERIFIER.calculate_package(paths, ROOT)
        VERIFIER.verify_file_digests(receipt, files)

        self.assertEqual("NDIP-2", proposal_id)
        self.assertEqual(package_digest, actual_digest)
        self.assertEqual("PASS", receipt["authorization"]["gateB"])
        self.assertIs(True, receipt["authorization"]["implementationAuthorized"])
        self.assertIs(False, receipt["authorization"]["deploymentAuthority"])

    def test_ndip3_candidate_binds_full_normative_package_without_authority(self) -> None:
        package_dir = ROOT / "docs/ndip/NDIP-3"
        receipt_path = package_dir / "acceptance-receipt.candidate.json"
        candidate = self._ndip3_candidate()

        proposal_id, paths, package_digest = VERIFIER.validate_receipt_shape(
            candidate, package_dir, receipt_path, ROOT
        )
        actual_digest, files = VERIFIER.calculate_package(paths, ROOT)
        VERIFIER.verify_file_digests(candidate, files)
        VERIFIER.verify_repository_status(proposal_id, "CANDIDATE", 4, ROOT)
        VERIFIER.verify_required_status("CANDIDATE", False)

        self.assertEqual("NDIP-3", proposal_id)
        self.assertEqual(10, len(paths))
        self.assertEqual(package_digest, actual_digest)
        self.assertIs(False, candidate["authority"])
        self.assertIs(False, candidate["authorization"]["implementationAuthorized"])
        self.assertIs(False, candidate["authorization"]["deploymentAuthority"])

    def test_repository_status_must_be_a_status_line(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            checks = {
                "docs/proposals/0002-register-ndip-governance.md": "- Status: Accepted"
            }
            checks.update(
                {
                    path: marker.format(status="Draft")
                    for path, marker in VERIFIER.STATUS_MARKERS["NDIP-3"].items()
                }
            )
            for path, marker in checks.items():
                document = root / path
                document.parent.mkdir(parents=True, exist_ok=True)
                document.write_text(
                    f"Historical prose mentions {marker} but does not set status.\n",
                    encoding="utf-8",
                )

            with self.assertRaisesRegex(
                VERIFIER.VerificationError, "proposal status line is missing"
            ):
                VERIFIER.verify_repository_status("NDIP-3", "CANDIDATE", 4, root)

    def test_ndip3_candidate_cannot_claim_implementation_or_deployment_authority(self) -> None:
        package_dir = ROOT / "docs/ndip/NDIP-3"
        receipt_path = package_dir / "acceptance-receipt.candidate.json"

        for field in ("implementationAuthorized", "deploymentAuthority"):
            with self.subTest(field=field):
                candidate = self._ndip3_candidate()
                candidate["authorization"][field] = True
                with self.assertRaises(VERIFIER.VerificationError):
                    VERIFIER.validate_receipt_shape(
                        candidate, package_dir, receipt_path, ROOT
                    )

    def test_ndip3_accepted_receipt_shape_never_grants_deployment_authority(self) -> None:
        package_dir = ROOT / "docs/ndip/NDIP-3"
        receipt_path = package_dir / "acceptance-receipt.json"
        receipt = self._ndip3_candidate()
        receipt["receiptStatus"] = "ACCEPTED"
        receipt["authority"] = True
        receipt["decision"] = {
            "status": "ACCEPTED",
            "acceptedBy": "test-only",
            "acceptedAt": "2026-09-30",
            "decisionReference": "test-only",
        }
        receipt["authorization"]["gateB"] = "PASS"
        receipt["authorization"]["implementationAuthorized"] = True

        proposal_id, paths, _ = VERIFIER.validate_receipt_shape(
            receipt, package_dir, receipt_path, ROOT
        )

        self.assertEqual("NDIP-3", proposal_id)
        self.assertEqual(10, len(paths))
        self.assertIs(False, receipt["authorization"]["deploymentAuthority"])

    def test_receipt_schema_generation_four_is_reserved_for_ndip3(self) -> None:
        candidate = self._ndip3_candidate()
        candidate["proposalId"] = "NDIP-2"
        package_dir = ROOT / "docs/ndip/NDIP-3"

        with self.assertRaisesRegex(
            VERIFIER.VerificationError,
            "receipt schema generation 4 is reserved for NDIP-3",
        ):
            VERIFIER.validate_receipt_shape(
                candidate,
                package_dir,
                package_dir / "acceptance-receipt.candidate.json",
                ROOT,
            )

    def test_candidate_cannot_claim_implementation_authority(self) -> None:
        candidate = self._candidate()
        candidate["authorization"]["implementationAuthorized"] = True

        with self.assertRaisesRegex(
            VERIFIER.VerificationError,
            "candidate must not authorize implementation",
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
            "accepted Gate B must authorize implementation",
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

    def _ndip3_candidate(self) -> dict[str, object]:
        paths = VERIFIER.EXPECTED_PACKAGES["NDIP-3"]
        package_digest, files = VERIFIER.calculate_package(paths, ROOT)
        return {
            "receiptSchema": VERIFIER.RECEIPT_SCHEMA,
            "receiptSchemaGeneration": 4,
            "proposalId": "NDIP-3",
            "receiptStatus": "CANDIDATE",
            "authority": False,
            "preparedAt": "2026-09-30",
            "governanceBridge": {
                "proposalId": "NDP-0002",
                "requiredStatus": "ACCEPTED",
                "observedStatus": "ACCEPTED",
            },
            "normativePackage": {
                "digestAlgorithm": "SHA-256",
                "digestDomain": VERIFIER.PACKAGE_DOMAIN_LABEL,
                "pathBase": "repository-root",
                "files": [
                    {"path": path, "sha256": digest} for path, digest in files
                ],
                "digest": package_digest,
            },
            "reviewBaseline": {
                "mainCommit": "1" * 40,
                "governanceBaselineCommit": "2" * 40,
            },
            "decision": {
                "status": "PENDING",
                "acceptedBy": None,
                "acceptedAt": None,
                "decisionReference": None,
            },
            "authorization": {
                "gateB": "PENDING",
                "implementationAuthorized": False,
                "deploymentAuthority": False,
            },
        }


if __name__ == "__main__":
    unittest.main()
