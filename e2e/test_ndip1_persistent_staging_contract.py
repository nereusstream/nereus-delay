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
    def test_enabled_evidence_uses_canonical_p1_digest_and_closed_stale_head_error(self) -> None:
        runner = RUNNER.read_text(encoding="utf-8")
        enabled_canary = runner.split("run_enabled_canary() {", 1)[1].split(
            "disable_enabled_policy() {", 1
        )[0]
        self.assertEqual(2, enabled_canary.count('--arg p1Lock "${p1_source_lock_digest}"'))
        self.assertEqual(1, enabled_canary.count('--arg p1Lock "${p1_source_lock}"'))
        self.assertIn('rg -e "current (Oxia )?handoff policy"', runner)

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
