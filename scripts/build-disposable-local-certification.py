#!/usr/bin/env python3
"""Build the closed, non-authoritative disposable-local certification receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUIRED_CELLS = (
    "native.shared.strict",
    "native.shared.non_strict",
    "native.shared.disabled",
    "native.key_shared.strict",
    "native.key_shared.non_strict",
    "native.key_shared.disabled",
    "native.exclusive.immediate",
    "native.failover.immediate",
    "native.shared.ttl_expiry",
    "native.shared.retention_zero",
    "recovery.candidate_claim",
    "recovery.admission",
    "recovery.journal_mapping",
    "recovery.response_loss_after_send_async_before_ack",
    "recovery.response_loss_after_ack_before_outcome",
    "recovery.response_loss_after_outcome_before_handoff",
    "recovery.response_loss_handed_off_before_checkpoint",
    "recovery.worker_ownership_transfer",
    "recovery.broker_restart_failover",
    "recovery.oxia_restart_reopen",
    "recovery.oxia_minio_checkpoint",
    "recovery.oxia_minio_reaping",
    "recovery.minio_idempotent_restore",
    "recovery.rocksdb_reopen_retention",
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True, type=Path)
    parser.add_argument("--records", required=True, type=Path)
    parser.add_argument("--supporting", required=True, type=Path)
    parser.add_argument("--cleanup", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--delay-root", required=True, type=Path)
    parser.add_argument("--finished-at", required=True)
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON root is not an object: {path}")
    return value


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def package_digest(root: Path) -> tuple[str, list[dict[str, str]]]:
    paths = (
        "docs/ndip/NDIP-1/01-调查与决策记录.md",
        "docs/ndip/NDIP-1/02-NDIP-1-Pulsar-Native-Delivery.md",
        "docs/ndip/NDIP-1/03-实施计划.md",
        "docs/ndip/NDIP-1/04-代码级目标设计.md",
    )
    material = bytearray(b"nereus-delay-ndip-package\0")
    files: list[dict[str, str]] = []
    for path_text in paths:
        data = (root / path_text).read_bytes()
        path_bytes = path_text.encode("utf-8")
        file_digest = hashlib.sha256(data).digest()
        material.extend(len(path_bytes).to_bytes(4, byteorder="big"))
        material.extend(path_bytes)
        material.extend(file_digest)
        files.append({"path": path_text, "sha256": file_digest.hex()})
    return hashlib.sha256(material).hexdigest(), files


def read_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line:
            continue
        fields = line.split("\t")
        if len(fields) != 9:
            raise ValueError(f"matrix record {line_number} must have nine tab-separated fields")
        cell_id, category, expected, status, skipped, command, log_path, evidence_path, reason = fields
        log = Path(log_path)
        evidence = Path(evidence_path)
        if not log.is_file():
            raise ValueError(f"matrix log is missing: {cell_id}: {log}")
        if not evidence.is_file():
            raise ValueError(f"matrix evidence is missing: {cell_id}: {evidence}")
        records.append(
            {
                "id": cell_id,
                "category": category,
                "expected": expected,
                "status": status,
                "skipped": skipped == "1",
                "command": command,
                "logPath": log_path,
                "logSha256": sha256(log),
                "evidencePath": evidence_path,
                "resultSha256": sha256(evidence),
                "reason": reason,
            }
        )
    if [record["id"] for record in records] != list(REQUIRED_CELLS):
        raise ValueError("matrix records do not contain the exact required cells in order")
    return records


def read_supporting(path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line:
            continue
        fields = line.split("\t")
        if len(fields) != 4:
            raise ValueError(f"supporting record {line_number} must have four tab-separated fields")
        cell_id, status, command, log_path = fields
        log = Path(log_path)
        if not log.is_file():
            raise ValueError(f"supporting check log is missing: {log_path}")
        records.append(
            {
                "id": cell_id,
                "status": status,
                "command": command,
                "logPath": log_path,
                "logSha256": sha256(log),
            }
        )
    expected_ids = ["p1.compileRealPulsar", "p1.h0", "p1.nativeCoordinator"]
    if [record["id"] for record in records] != expected_ids:
        raise ValueError("supporting records do not contain the three P1 checks in order")
    return records


def main() -> int:
    args = arguments()
    try:
        context = load_json(args.context)
        cleanup = load_json(args.cleanup)
        source = context["source"]
        accepted_package_digest, accepted_files = package_digest(args.delay_root)
        package = {"packageSha256": accepted_package_digest, "files": accepted_files}
        if source["acceptedPackageSha256"] != accepted_package_digest:
            raise ValueError("context accepted package digest is stale")
        matrix = read_records(args.records)
        supporting = read_supporting(args.supporting)
        statuses = [entry["status"] for entry in matrix]
        if any(entry["skipped"] for entry in matrix):
            status = "FAIL"
        elif any(value == "EXECUTED_FAIL" for value in statuses):
            status = "FAIL"
        elif any(entry["status"] != "PASS" for entry in supporting):
            status = "FAIL"
        elif cleanup["status"] != "PASS":
            status = "FAIL"
        elif any(value == "NOT_COVERED" for value in statuses):
            status = "BLOCKED"
        else:
            status = "PASS"
        receipt = {
            "receiptSchema": "nereus-delay.disposable-local-certification-receipt",
            "receiptSchemaGeneration": 3,
            "classification": "DISPOSABLE_LOCAL",
            "status": status,
            "authority": False,
            "gateC": False,
            "shadow": False,
            "enabled": False,
            "startedAt": context["startedAt"],
            "finishedAt": args.finished_at,
            "source": source,
            "acceptedPackage": package,
            "environment": context["environment"],
            "supportingChecks": supporting,
            "matrix": matrix,
            "cleanup": cleanup,
            "boundaries": [
                "This is a disposable local certification receipt only; it is not authority.",
                "It does not create a deployment assessment scope and cannot satisfy Gate C.",
                "It cannot promote SHADOW or ENABLED; a persistent staging run must execute the real Gate C path.",
                "Native Pulsar immediate delivery for Exclusive and Failover is recorded as an expected PASS.",
                "No cross-subscription not-before claim is inferred from native behavior.",
            ],
            "report": {
                "kind": "DISPOSABLE_LOCAL_CERTIFICATION_RECEIPT",
                "path": str(args.output.resolve()),
            },
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except (KeyError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"cannot build disposable-local receipt: {exc}", file=sys.stderr)
        return 1
    print(f"receipt={args.output.resolve()}")
    print(f"status={status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
