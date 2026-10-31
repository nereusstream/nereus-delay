#!/usr/bin/env python3
"""Focused NDP-0001 scanner regressions; no Gradle or service dependencies."""

import contextlib
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest


spec = importlib.util.spec_from_file_location(
    "version_markers", Path(__file__).with_name("check-project-version-markers.py")
)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class ProjectVersionMarkersTest(unittest.TestCase):
    def assert_allowed(self, text, path="docs/example.md"):
        self.assertEqual([], checker.violations(path, text))

    def assert_forbidden(self, text, path="docs/example.md"):
        self.assertTrue(checker.violations(path, text), text)

    def test_registered_format_revisions(self):
        for text in (
            "Manifest v2 preserves the v1 fields.",
            "Target Claim v2 has schema v2; v1 bytes are unchanged.",
            "TargetRecoveryProofRef v1/type1 is a closed canonical image.",
            "PUBLISH_OUTCOME v1 remains unchanged.",
        ):
            with self.subTest(text=text):
                self.assert_allowed(text)

    def test_format_members_are_bound_to_their_schema_owner(self):
        self.assert_allowed("V1_SCHEMA_VERSION = 1; byte[] V2_DOMAIN;",
                            "src/main/java/TargetMigrationArchiveRow.java")
        self.assert_allowed("TargetMigrationArchiveRow.V1_SCHEMA_VERSION")
        self.assert_forbidden("V1_SCHEMA_VERSION = 1;", "src/main/java/ProjectSettings.java")
        self.assert_forbidden("Manifest v2; int V2_NEW_PROJECT_MODE;")

    def test_frozen_domain_bytes_and_compatibility_explanation(self):
        self.assert_allowed('Bytes.utf8("nereus-delay-target-migration-root-v1\\0")')
        self.assert_allowed("The domain `nereus-delay-target-migration-root-v1` retains exact bytes.")
        self.assert_forbidden('Bytes.utf8("nereus-delay-new-product-v1\\0")')

    def test_fixed_external_api_and_locked_sdk_names(self):
        for text in (
            "transaction-v2; /admin/v2/brokers/ready; stateDiagram-v2",
            "`nereus/delay-guarded-producer-v1@5128f46a358b5c3e9991cd5a51f2d1d0ef15339a`",
            "../oxia-client-java-worktrees/nereus-delay-client-cleanup-v1",
            "go: downloading github.com/cockroachdb/pebble/v2 v2.1.0",
            '"${admin_url}/admin/v2/brokers/ready"',
            '"http://127.0.0.1:8080/admin/v2/brokers/ready"',
            "Cluster ID was `UoRJ3MEES_6a7lV2DA5NQg`",
        ):
            self.assert_allowed(text)

    def test_project_titles_and_internal_paths_are_forbidden(self):
        for text in (
            "# Nereus Delay V1", "# Nereus Delay V3", "docs/V2-PROTOCOL-REGISTRY.md",
            "# V2 Protocol Registry", "# Runbook V1", "# Design V2",
            "package com.nereusstream.delay.v2;", "public class V1Worker {}",
            "public class DelayRuntimeV2 {}", "NEREUS_DELAY_V2_ENABLED=true",
            "public class NereusDelayV2Worker {}", "public class DelayRuntimeV2Worker {}",
            "scripts/run-v1-worker.sh", "NEW_PROJECT_V2_MODE=true",
        ):
            self.assert_forbidden(text)

    def test_every_match_is_checked(self):
        for text in (
            "transaction-v2 /admin/v2/brokers/ready; Nereus Delay V2",
            "nereus/delay-guarded-producer-v1; Nereus Delay V1",
            "Manifest v2; NewProject V1",
            "Manifest v2; public class V1Worker {}",
            "Manifest v2; public class NereusDelayV2Worker {}",
            "Manifest v2; public class DelayRuntimeV2Worker {}",
            "Manifest v2; Nereus Delay / V1",
            "Manifest v2; NEREUS_DELAY_VERSION=V2",
            "Manifest v2; package com.nereusstream.delay.v2;",
            "Manifest v2; scripts/run-v1-worker.sh",
            "Manifest v2; unknown-v1",
            "Manifest v2; my-transaction-v2",
            "Manifest v2; transaction-v2-project",
            "Manifest v2; src/main/java/admin/v2/Project.java",
            "Manifest v2; http://DelayRuntimeV2Worker.example/admin/v2/brokers/ready",
            "Manifest v2\n# V1 Protocol Registry",
            "`nereus-delay-target-migration-root-v1`; NEREUS_DELAY_V2=true",
        ):
            self.assert_forbidden(text)

    def test_generic_schema_or_native_word_does_not_waive_a_marker(self):
        for text in ("Native v1", "schema v2", "TargetUnregistered v1", "unknown-v1"):
            self.assert_forbidden(text)

    def test_new_unknown_markers_do_not_inherit_an_allowance(self):
        self.assert_forbidden("nereus/delay-another-sdk-v1")
        self.assert_forbidden("Manifest v2\n\nV2_UNKNOWN_PROJECT_LINE")
        self.assert_forbidden("Manifest v2; NEREUS_DELAY_ROOT_RECOVERY_KEY_MATCH_V1=true")
        self.assert_forbidden("v2", "src/main/java/TargetClaimRecord.java")
        self.assert_forbidden("## TargetClaimRecord\n\nv2 new product")

    def test_physical_paths_cannot_use_external_text_allowances(self):
        with tempfile.TemporaryDirectory(prefix="project-marker-check-") as directory:
            root = Path(directory)
            (root / "docs").mkdir()
            (root / "docs/V2-PROTOCOL-REGISTRY.md").write_text("current design\n")
            (root / "docs/transaction-v2.md").write_text("current design\n")
            (root / "docs/V7-RUNBOOK.md").write_text("current design\n")
            (root / ".ignore").write_text("docs/V7-RUNBOOK.md\n")
            with contextlib.redirect_stderr(io.StringIO()) as output:
                self.assertEqual(1, checker.check(root))
            self.assertIn("V2-PROTOCOL-REGISTRY.md", output.getvalue())
            self.assertIn("transaction-v2.md", output.getvalue())
            self.assertIn("V7-RUNBOOK.md", output.getvalue())


if __name__ == "__main__":
    unittest.main()
