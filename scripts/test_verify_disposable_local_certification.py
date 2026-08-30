from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


SCRIPT_DIR = Path(__file__).resolve().parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


BUILDER = load_module(
    "disposable_local_builder", SCRIPT_DIR / "build-disposable-local-certification.py"
)
VERIFIER = load_module(
    "disposable_local_verifier", SCRIPT_DIR / "verify-disposable-local-certification.py"
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class DisposableLocalEvidenceBindingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def matrix_receipt(self) -> dict:
        matrix = []
        for index, cell_id in enumerate(VERIFIER.REQUIRED_CELLS):
            log = self.root / f"matrix-{index}.log"
            evidence = self.root / f"matrix-{index}.json"
            log.write_text(f"log for {cell_id}\n", encoding="utf-8")
            evidence.write_text(f'{{"cellId":"{cell_id}"}}\n', encoding="utf-8")
            matrix.append(
                {
                    "id": cell_id,
                    "category": "native" if cell_id.startswith("native.") else "recovery",
                    "expected": "exact focused behavior",
                    "status": "EXECUTED_PASS",
                    "skipped": False,
                    "command": "focused-command",
                    "logPath": str(log),
                    "logSha256": sha256(log),
                    "evidencePath": str(evidence),
                    "resultSha256": sha256(evidence),
                    "reason": "exact focused behavior",
                }
            )
        return {"receiptSchemaGeneration": 3, "matrix": matrix}

    def supporting_receipt(self) -> dict:
        checks = []
        for index, check_id in enumerate(
            ("p1.compileRealPulsar", "p1.h0", "p1.nativeCoordinator")
        ):
            log = self.root / f"supporting-{index}.log"
            log.write_text(f"log for {check_id}\n", encoding="utf-8")
            checks.append(
                {
                    "id": check_id,
                    "command": "focused-command",
                    "status": "PASS",
                    "logPath": str(log),
                    "logSha256": sha256(log),
                }
            )
        return {"receiptSchemaGeneration": 3, "supportingChecks": checks}

    def test_builder_binds_matrix_and_supporting_logs(self) -> None:
        matrix_lines = []
        for index, cell_id in enumerate(BUILDER.REQUIRED_CELLS):
            log = self.root / f"builder-matrix-{index}.log"
            evidence = self.root / f"builder-matrix-{index}.json"
            log.write_text(f"log for {cell_id}\n", encoding="utf-8")
            evidence.write_text("{}\n", encoding="utf-8")
            matrix_lines.append(
                "\t".join(
                    (
                        cell_id,
                        "native" if cell_id.startswith("native.") else "recovery",
                        "expected",
                        "EXECUTED_PASS",
                        "0",
                        "command",
                        str(log),
                        str(evidence),
                        "reason",
                    )
                )
            )
        matrix_records = self.root / "matrix.tsv"
        matrix_records.write_text("\n".join(matrix_lines) + "\n", encoding="utf-8")

        supporting_lines = []
        for index, check_id in enumerate(
            ("p1.compileRealPulsar", "p1.h0", "p1.nativeCoordinator")
        ):
            log = self.root / f"builder-supporting-{index}.log"
            log.write_text(f"log for {check_id}\n", encoding="utf-8")
            supporting_lines.append("\t".join((check_id, "PASS", "command", str(log))))
        supporting_records = self.root / "supporting.tsv"
        supporting_records.write_text("\n".join(supporting_lines) + "\n", encoding="utf-8")

        matrix = BUILDER.read_records(matrix_records)
        supporting = BUILDER.read_supporting(supporting_records)
        self.assertEqual(sha256(Path(matrix[0]["logPath"])), matrix[0]["logSha256"])
        self.assertEqual(
            sha256(Path(supporting[0]["logPath"])), supporting[0]["logSha256"]
        )

    def test_matrix_log_mutation_is_rejected(self) -> None:
        receipt = self.matrix_receipt()
        VERIFIER.verify_matrix(receipt)
        Path(receipt["matrix"][0]["logPath"]).write_text("mutated\n", encoding="utf-8")
        with self.assertRaisesRegex(VERIFIER.VerificationError, "matrix log SHA-256 mismatch"):
            VERIFIER.verify_matrix(receipt)

    def test_supporting_log_mutation_is_rejected(self) -> None:
        receipt = self.supporting_receipt()
        VERIFIER.verify_supporting_checks(receipt)
        Path(receipt["supportingChecks"][0]["logPath"]).write_text(
            "mutated\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(
            VERIFIER.VerificationError, "supporting check log SHA-256 mismatch"
        ):
            VERIFIER.verify_supporting_checks(receipt)


if __name__ == "__main__":
    unittest.main()
