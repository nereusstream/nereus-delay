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


if __name__ == "__main__":
    unittest.main()
