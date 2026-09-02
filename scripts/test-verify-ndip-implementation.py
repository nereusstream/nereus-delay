#!/usr/bin/env python3
"""Focused fail-closed tests for verify-ndip-implementation.py."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parent.parent
VERIFIER_PATH = ROOT / "scripts/verify-ndip-implementation.py"
SPEC = importlib.util.spec_from_file_location("verify_ndip_implementation", VERIFIER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load NDIP implementation verifier")
VERIFIER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFIER)


def build_gradle_sample(source_identity: bytes, documentation_governance: bytes) -> bytes:
    return b"".join(
        (
            b"plugins { id 'java-library' }\n",
            b"def runningArtifactIdentityDir = layout.buildDirectory.dir('generated')\n",
            source_identity,
            b"tasks.named('processResources').configure { dependsOn 'identity' }\n",
            b"dependencies { implementation 'example:runtime:1' }\n",
            b"def designDocumentPaths = [\n",
            documentation_governance,
            b"tasks.register('checkProjectVersionMarkers', Exec) { commandLine 'true' }\n",
            b"tasks.named('check').configure { dependsOn 'runtimeCheck' }\n",
        )
    )


class VerifyNdipImplementationTest(unittest.TestCase):
    def test_current_checkout_closes_the_exact_implemented_receipt(self) -> None:
        receipt = VERIFIER.load_json(
            ROOT / "docs/ndip/NDIP-1/implementation-receipt.json",
            "implementation receipt",
        )
        result = VERIFIER.verify_receipt(receipt, current_required=True)

        self.assertEqual(
            "b4e077e9978f262cdb93cf3562ea12eee32430e2",
            result["certifiedCommit"],
        )
        self.assertEqual(835, result["runtimeSourceFileCount"])
        self.assertEqual("20260901055333-78920", result["stagingRunId"])

    def test_receipt_unknown_root_field_fails_closed(self) -> None:
        receipt = VERIFIER.load_json(
            ROOT / "docs/ndip/NDIP-1/implementation-receipt.json",
            "implementation receipt",
        )
        receipt["unknown"] = "not allowed"

        with self.assertRaisesRegex(
            VERIFIER.VerificationError, "receipt keys are not closed"
        ):
            VERIFIER.verify_receipt(receipt, current_required=False)

    def test_non_runtime_documents_are_outside_the_runtime_scope(self) -> None:
        self.assertFalse(
            VERIFIER.selected(
                "docs/ndip/NDIP-1/README.md",
                VERIFIER.RUNTIME_PREFIXES,
                VERIFIER.RUNTIME_EXACT_PATHS,
            )
        )
        self.assertFalse(
            VERIFIER.selected(
                "docs/ndip/NDIP-1/06-Persistent-Staging-Gate-C-SHADOW-执行记录.md",
                VERIFIER.RUNTIME_PREFIXES,
                VERIFIER.RUNTIME_EXACT_PATHS,
            )
        )
        self.assertTrue(
            VERIFIER.selected(
                "src/main/java/example/Runtime.java",
                VERIFIER.RUNTIME_PREFIXES,
                VERIFIER.RUNTIME_EXACT_PATHS,
            )
        )
        self.assertFalse(
            VERIFIER.selected(
                "src/test/java/example/RuntimeTest.java",
                VERIFIER.RUNTIME_PREFIXES,
                VERIFIER.RUNTIME_EXACT_PATHS,
            )
        )
        self.assertFalse(
            VERIFIER.selected(
                ".editorconfig",
                VERIFIER.RUNTIME_PREFIXES,
                VERIFIER.RUNTIME_EXACT_PATHS,
            )
        )

    def test_governance_block_only_drift_keeps_build_projection_equal(self) -> None:
        before = build_gradle_sample(b"identity-before\n", b"        'old-doc.md'\n]\n")
        after = build_gradle_sample(b"identity-after\n", b"        'new-doc.md'\n]\n")

        self.assertEqual(
            VERIFIER.project_source("build.gradle", before),
            VERIFIER.project_source("build.gradle", after),
        )

    def test_runtime_build_input_drift_changes_build_projection(self) -> None:
        before = build_gradle_sample(b"identity\n", b"        'doc.md'\n]\n")
        after = before.replace(b"example:runtime:1", b"example:runtime:2")

        self.assertNotEqual(
            VERIFIER.project_source("build.gradle", before),
            VERIFIER.project_source("build.gradle", after),
        )

    def test_duplicate_governance_projection_marker_fails_closed(self) -> None:
        source = build_gradle_sample(b"identity\n", b"        'doc.md'\n]\n")
        source += b"def designDocumentPaths = [\n"

        with self.assertRaisesRegex(
            VERIFIER.VerificationError,
            "projection markers must occur exactly once",
        ):
            VERIFIER.project_source("build.gradle", source)

    def test_runtime_content_and_path_set_drift_change_digest(self) -> None:
        original = {"src/main/A.java": b"final class A {}\n"}
        changed = {"src/main/A.java": b"final class A { int value; }\n"}
        added = {
            "src/main/A.java": original["src/main/A.java"],
            "src/main/B.java": b"final class B {}\n",
        }
        original_digest, _ = VERIFIER.canonical_source_digest(
            tuple(original), original.__getitem__, VERIFIER.RUNTIME_DOMAIN
        )
        changed_digest, _ = VERIFIER.canonical_source_digest(
            tuple(changed), changed.__getitem__, VERIFIER.RUNTIME_DOMAIN
        )
        added_digest, _ = VERIFIER.canonical_source_digest(
            tuple(added), added.__getitem__, VERIFIER.RUNTIME_DOMAIN
        )

        self.assertNotEqual(original_digest, changed_digest)
        self.assertNotEqual(original_digest, added_digest)

        with self.assertRaisesRegex(
            VERIFIER.VerificationError, "source authority contains duplicate paths"
        ):
            VERIFIER.canonical_source_digest(
                ("src/main/A.java", "src/main/A.java"),
                original.__getitem__,
                VERIFIER.RUNTIME_DOMAIN,
            )

    def test_closure_rejects_production_authority_or_completed_future_work(self) -> None:
        receipt = VERIFIER.load_json(
            ROOT / "docs/ndip/NDIP-1/implementation-receipt.json",
            "implementation receipt",
        )
        for field, value in (
            ("productionAuthority", True),
            ("productionAuthority", 0),
            ("deployment", "COMPLETE"),
            ("performanceAndScale", "COMPLETE"),
            ("productionRollout", "COMPLETE"),
        ):
            with self.subTest(field=field):
                closure = copy.deepcopy(receipt["closure"])
                closure[field] = value
                with self.assertRaisesRegex(
                    VERIFIER.VerificationError,
                    "implementation closure boundaries are not closed",
                ):
                    VERIFIER.verify_closure(closure)

    def test_external_evidence_digest_mismatch_fails_closed(self) -> None:
        receipt = VERIFIER.load_json(
            ROOT / "docs/ndip/NDIP-1/implementation-receipt.json",
            "implementation receipt",
        )
        with TemporaryDirectory() as directory:
            run_dir = Path(directory)
            log_dir = run_dir / "logs"
            log_dir.mkdir()
            (log_dir / "disposable-receipt-verifier.log").write_text(
                "receipt_status=PASS\n", encoding="utf-8"
            )

            with self.assertRaisesRegex(
                VERIFIER.VerificationError,
                "retained disposable verifier log digest mismatch",
            ):
                VERIFIER.verify_external_evidence(
                    receipt,
                    run_dir,
                    run_dir / "missing-public-key.der",
                    None,
                )


if __name__ == "__main__":
    unittest.main()
