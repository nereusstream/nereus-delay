import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "e2e" / "run-ndip1-persistent-staging.sh"
WORKER_SMOKE = (
    ROOT
    / "src"
    / "real-pulsar"
    / "java"
    / "com"
    / "nereusstream"
    / "delay"
    / "transport"
    / "PulsarClientArtifactWorkerSmoke.java"
)


class PersistentStagingContractTest(unittest.TestCase):
    def test_shadow_receipt_uses_closed_numeric_and_boolean_types(self) -> None:
        runner = RUNNER.read_text(encoding="utf-8")
        shadow_receipt = runner.split(
            'jq -n --arg schema "nereus-delay.shadow-certification"', 1
        )[1].split('>"${run_dir}/authority/shadow-receipt.json"', 1)[0]
        self.assertIn("nativeAdmission:0,nativeSend:0,handedOff:0", shadow_receipt)
        self.assertIn(
            "unresolvedPublishing:false,\n"
            "      unresolvedUncertain:false,attemptJournalLeak:false,generationIncarnationMix:false",
            shadow_receipt,
        )
        self.assertNotIn('nativeAdmission:"0"', shadow_receipt)
        self.assertNotIn('unresolvedPublishing:"false"', shadow_receipt)

    def test_enabled_evidence_uses_canonical_p1_digest_and_closed_stale_head_error(self) -> None:
        runner = RUNNER.read_text(encoding="utf-8")
        enabled_canary = runner.split("run_enabled_canary() {", 1)[1].split(
            "disable_enabled_policy() {", 1
        )[0]
        self.assertEqual(2, enabled_canary.count('--arg p1Lock "${p1_source_lock_digest}"'))
        self.assertEqual(1, enabled_canary.count('--arg p1Lock "${p1_source_lock}"'))
        self.assertIn('rg -e "current (Oxia )?handoff policy"', runner)

    def test_managed_handoff_separates_static_profile_boundary_from_runtime_policy_lead(self) -> None:
        source = WORKER_SMOKE.read_text(encoding="utf-8")
        resolver = source.split("private static ScheduleResolver scheduleResolver(", 1)[1]
        self.assertIn(
            "intent.deliverAtEpochMs(),\n"
            "                                    PersistentStagingNativeCanaryIdentity.MAX_HANDOFF_LEAD_MS",
            resolver,
        )
        self.assertNotIn("intent.deliverAtEpochMs(), managedHandoff.effectiveLeadMs()", resolver)
        self.assertIn("bridge.managedHandoffSnapshot().effectiveLeadMs()", source)
        self.assertIn("effectiveLeadMs <= 0", source)
        self.assertIn(
            "effectiveLeadMs > PersistentStagingNativeCanaryIdentity.MAX_HANDOFF_LEAD_MS",
            source,
        )
        self.assertIn("MANAGED_HANDOFF_CANARY_DELAY_MS = 30_000", source)

    def test_managed_handoff_worker_binds_catalog_activation_and_channel_credential(self) -> None:
        source = WORKER_SMOKE.read_text(encoding="utf-8")
        shard_composition = source.split("final DelayShard delayShard = new DelayShard(", 1)[1].split(
            "final OwnerIdentity ownerIdentity", 1
        )[0]
        self.assertIn(
            "managedHandoff == null ? null : managedHandoff.profileCatalog()",
            shard_composition,
        )

        active_branch = source.split(
            "managedProfileActivation = managedHandoff == null", 1
        )[1].split("recoveredDestinationOutcome = null;", 1)[0]
        self.assertLess(
            active_branch.index("appendManagedProfileActivation("),
            active_branch.index('activeCommand = managedHandoff == null ? command(shard, "worker-active")'),
        )
        self.assertIn("requireManagedProfileActivationApplied(", source)
        self.assertIn("ProfileAcceptance.ACTIVE_FOR_FIRST_BINDING", source)
        self.assertIn("lease.requireBinding(exactBinding)", source)
        self.assertIn("lease.requireProtectedBy(exactProtection)", source)

        catalog = source.split("private record ManagedHandoffProfileCatalog(", 1)[1].split(
            "static final class PhysicalPublishBridge", 1
        )[0]
        self.assertIn("return destination.ref().equals(profile) ? head : null", catalog)
        self.assertIn("binding.bindingDigest(), head.bindingDigest()", catalog)
        self.assertNotIn("resolveHead(final ProfileRef profile) {\n                return null;", catalog)

    def test_managed_handoff_retries_bounded_source_propagation_without_changing_identity(self) -> None:
        source = WORKER_SMOKE.read_text(encoding="utf-8")
        helper = source.split(
            "private static WorkerShardRuntime.SourceBoundPhysicalPublishTurn "
            "awaitSourceBoundPhysicalPublish(",
            1,
        )[1].split(
            "private static WorkerShardRuntime.SourceBoundPhysicalPublishTurn "
            "finishAdmissionRecoveryHold(",
            1,
        )[0]
        self.assertIn("SourceBoundPhysicalPublishStatus.SOURCE_TURN_LIMIT", helper)
        self.assertIn("TimeUnit.SECONDS.toNanos(30)", helper)
        self.assertIn("TimeUnit.MILLISECONDS.sleep(25)", helper)
        self.assertIn("publishAttemptId,\n                    admissionPosition", helper)
        self.assertIn("ignored -> Optional.of(payload)", helper)
        self.assertNotIn("append", helper.lower())
        self.assertNotIn("submitPublish", helper)
        self.assertLess(
            source.index("source-applied PUBLISHING did not submit physical publish"),
            source.index("submitted physical publish did not retain its durable attempt"),
        )

    def test_rollback_reads_live_topic_stats_even_if_canary_capture_is_missing(self) -> None:
        runner = RUNNER.read_text(encoding="utf-8")
        rollback = runner.split("disable_enabled_policy() {", 1)[1].split(
            "run_independent_certification_validation() {", 1
        )[0]
        self.assertIn('rollback-native-topic-stats.json', rollback)
        self.assertIn(
            '"${admin_url}/admin/v2/persistent/public/default/${native_topic}/stats"',
            rollback,
        )
        self.assertNotIn('${run_dir}/canary/native-topic-stats.json', rollback)
        self.assertIn("nativeTopicStatsHttpStatus", rollback)
        self.assertIn("schemaGeneration:2", rollback)

    def test_manifest_rocksdb_identity_is_the_exact_worker_root(self) -> None:
        runner = RUNNER.read_text(encoding="utf-8")
        self.assertIn('rocksdb_resource="${run_dir}/worker-store"', runner)
        self.assertIn(
            'export NEREUS_DELAY_PULSAR_WORKER_ROOT="${rocksdb_resource}"',
            runner,
        )
        self.assertIn(
            '>"${rocksdb_resource}/incarnation"',
            runner,
        )
        self.assertIn(
            '>"${rocksdb_resource}/route-incarnation"',
            runner,
        )
        self.assertNotIn(
            'rocksdb_resource="${run_dir}/worker-store/rocksdb"',
            runner,
        )

    def test_persistent_staging_worker_does_not_delete_manifest_store(self) -> None:
        source = WORKER_SMOKE.read_text(encoding="utf-8")
        declaration = re.search(
            r"final boolean preserveWorkerRoot = (?P<body>.*?);",
            source,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(declaration)
        self.assertIn("isPersistentStaging()", declaration.group("body"))
        self.assertRegex(
            source,
            r"if \(!preserveWorkerRoot\) \{\s*deleteTree\(root\);\s*\}",
        )

    def test_final_checkpoint_is_bound_to_shard_and_owner_generation(self) -> None:
        source = WORKER_SMOKE.read_text(encoding="utf-8")
        self.assertNotIn('root.resolve("worker-final-checkpoint")', source)
        self.assertIn('root.resolve("worker-final-checkpoints")', source)
        self.assertIn(
            '.resolve(Bytes.hex(shard.routeIncarnation().bytes()))',
            source,
        )
        self.assertIn(
            '.resolve(Integer.toUnsignedString(shard.partition()))',
            source,
        )
        self.assertIn('.resolve(Long.toUnsignedString(ownerEpoch))', source)
        self.assertIn('Bytes.u64beBits(ownerEpoch)', source)


if __name__ == "__main__":
    unittest.main()
