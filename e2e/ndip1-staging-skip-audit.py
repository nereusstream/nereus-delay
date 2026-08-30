#!/usr/bin/env python3
"""Classify the 41 conditional real-service tests without treating skips as PASS."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET


EXPECTED = 41
EXPECTED_FAULT_SCHEMA = "nereus-delay.ndip1-expected-fault"
LOCAL_STORAGE_TEST_CLASS = "com.nereusstream.delay.store.LocalStorageDurableChaosTest"
LOCAL_STORAGE_TEST_NAME = "localStorageFailureSurvivesFreshProcessRecovery()"


def iter_cases(root: Path):
    for path in sorted(root.glob("TEST-*.xml")):
        tree = ET.parse(path)
        for case in tree.getroot().iter("testcase"):
            classname = case.attrib.get("classname", "")
            name = case.attrib.get("name", "")
            skipped = case.find("skipped")
            failure = case.find("failure")
            error = case.find("error")
            if skipped is not None:
                state = "SKIPPED"
                reason = skipped.attrib.get("message", "") or (skipped.text or "").strip()
            elif failure is not None or error is not None:
                state = "FAILED"
                node = failure if failure is not None else error
                reason = node.attrib.get("message", "") if node is not None else ""
            else:
                state = "PASS"
                reason = ""
            yield (classname, name), {"status": state, "reason": reason}


def read_cases(root: Path) -> dict[tuple[str, str], dict[str, str]]:
    """Read one result directory for baseline compatibility."""
    cases: dict[tuple[str, str], dict[str, str]] = {}
    for key, value in iter_cases(root):
        cases[key] = value
    return cases


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_expected_fault(root: Path) -> dict[str, object] | None:
    marker_path = root / "expected-fault.json"
    if not marker_path.is_file():
        return None
    try:
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read expected fault marker {marker_path}: {exc}") from exc
    if not isinstance(marker, dict):
        raise ValueError(f"expected fault marker is not an object: {marker_path}")

    required_strings = {
        "schema": EXPECTED_FAULT_SCHEMA,
        "classification": "EXPECTED_TERMINATION",
        "cell": "disaster-host-fault",
        "signal": "SIGKILL",
        "testClass": LOCAL_STORAGE_TEST_CLASS,
        "testName": LOCAL_STORAGE_TEST_NAME,
    }
    for field, expected in required_strings.items():
        if marker.get(field) != expected:
            raise ValueError(f"expected fault marker field {field} is not {expected}: {marker_path}")
    if marker.get("schemaGeneration") != 1 or marker.get("signalNumber") != 9:
        raise ValueError(f"expected fault marker generation or signal is invalid: {marker_path}")

    result_dir_value = marker.get("resultDir")
    kill_receipt_value = marker.get("killReceipt")
    kill_receipt_sha256 = marker.get("killReceiptSha256")
    recovery_run_value = marker.get("recoveryRunJson")
    recovery_run_sha256 = marker.get("recoveryRunJsonSha256")
    if not all(isinstance(value, str) and value for value in (
        result_dir_value,
        kill_receipt_value,
        kill_receipt_sha256,
        recovery_run_value,
        recovery_run_sha256,
    )):
        raise ValueError(f"expected fault marker paths or digests are missing: {marker_path}")

    result_dir = Path(result_dir_value).resolve()
    if result_dir != root.resolve():
        raise ValueError(f"expected fault marker resultDir does not bind this result directory: {marker_path}")
    kill_receipt = Path(kill_receipt_value).resolve()
    if not kill_receipt.is_file() or sha256_file(kill_receipt) != kill_receipt_sha256:
        raise ValueError(f"expected fault kill receipt is missing or has the wrong digest: {marker_path}")
    try:
        kill = json.loads(kill_receipt.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read expected fault kill receipt {kill_receipt}: {exc}") from exc
    if not isinstance(kill, dict) or not (
        kill.get("schema") == "nereus-delay.ndip1-local-storage-kill"
        and kill.get("cell") == "disaster-host-fault"
        and kill.get("signal") == "SIGKILL"
        and kill.get("signalNumber") == 9
        and kill.get("exactTarget") is True
        and isinstance(kill.get("targetPid"), int)
    ):
        raise ValueError(f"expected fault kill receipt is not an exact SIGKILL proof: {kill_receipt}")

    recovery_run = Path(recovery_run_value).resolve()
    if not recovery_run.is_file() or sha256_file(recovery_run) != recovery_run_sha256:
        raise ValueError(f"expected fault recovery run is missing or has the wrong digest: {marker_path}")
    try:
        recovery = json.loads(recovery_run.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read expected fault recovery run {recovery_run}: {exc}") from exc
    if not isinstance(recovery, dict) or not (
        recovery.get("schema") == "nereus-delay.ndip1-test-run"
        and recovery.get("exitCode") == 0
    ):
        raise ValueError(f"expected fault recovery run did not pass: {recovery_run}")
    return marker


def collect_cases(directories: list[Path]) -> dict[tuple[str, str], list[dict[str, object]]]:
    """Collect every observation; never let a later result mask an earlier one."""
    observed: dict[tuple[str, str], list[dict[str, object]]] = {}
    for directory in directories:
        try:
            expected_fault = read_expected_fault(directory)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
        for key, value in iter_cases(directory):
            observation: dict[str, object] = {"directory": str(directory), **value}
            if expected_fault is not None and key == (
                expected_fault["testClass"],
                expected_fault["testName"],
            ):
                observation["expectedFault"] = True
            observed.setdefault(key, []).append(observation)
    return observed


def aggregate_status(observations: list[dict[str, object]]) -> str:
    effective = [observation for observation in observations if not observation.get("expectedFault", False)]
    if not effective:
        return "NOT_EXECUTED"
    statuses = {str(observation["status"]) for observation in effective}
    if "FAILED" in statuses:
        return "FAILED"
    if "SKIPPED" in statuses:
        return "SKIPPED"
    return "PASS"


def aggregate_reason(observations: list[dict[str, object]]) -> str:
    reasons = []
    for observation in observations:
        detail = str(observation["status"])
        if observation.get("expectedFault", False):
            detail = f"EXPECTED_TERMINATION (raw {detail})"
        if observation["reason"]:
            detail += f": {observation['reason']}"
        reasons.append(f"{observation['directory']}: {detail}")
    return "; ".join(reasons)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--result-dir", type=Path, action="append", default=[])
    args = parser.parse_args()

    baseline = read_cases(args.baseline)
    selected = {key: value for key, value in baseline.items() if value["status"] == "SKIPPED"}
    if len(selected) != EXPECTED:
        raise SystemExit(f"expected {EXPECTED} baseline conditional skips, found {len(selected)}")

    observed = collect_cases(args.result_dir)

    rows = []
    for (classname, name), initial in sorted(selected.items()):
        observations = observed.get((classname, name), [])
        status = aggregate_status(observations)
        rows.append(
            {
                "classname": classname,
                "name": name,
                "baselineStatus": "CONDITIONAL_SKIP",
                "baselineReason": initial["reason"],
                "applicability": "REQUIRED_STAGING",
                "naReason": None,
                "status": status,
                "resultReason": aggregate_reason(observations),
                "observedRuns": len(observations),
                "observedStatuses": [observation["status"] for observation in observations],
                "effectiveRuns": sum(not observation.get("expectedFault", False) for observation in observations),
                "expectedFaultRuns": sum(observation.get("expectedFault", False) for observation in observations),
            }
        )

    payload = {
        "schema": "nereus-delay.ndip1-staging-skip-audit",
        "schemaGeneration": 1,
        "expectedConditionalSkips": EXPECTED,
        "classificationRule": "Every baseline conditional real-service skip is REQUIRED_STAGING; missing dependencies or an unexecuted case cannot promote Gate C.",
        "rows": rows,
        "counts": {
            "pass": sum(row["status"] == "PASS" for row in rows),
            "failed": sum(row["status"] == "FAILED" for row in rows),
            "skipped": sum(row["status"] == "SKIPPED" for row in rows),
            "notExecuted": sum(row["status"] == "NOT_EXECUTED" for row in rows),
        },
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    counts = payload["counts"]
    return 0 if counts["pass"] == EXPECTED else 2


if __name__ == "__main__":
    raise SystemExit(main())
