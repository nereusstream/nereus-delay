#!/usr/bin/env python3
"""Classify the 41 conditional real-service tests without treating skips as PASS."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import xml.etree.ElementTree as ET


EXPECTED = 41


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


def collect_cases(directories: list[Path]) -> dict[tuple[str, str], list[dict[str, str]]]:
    """Collect every observation; never let a later result mask an earlier one."""
    observed: dict[tuple[str, str], list[dict[str, str]]] = {}
    for directory in directories:
        for key, value in iter_cases(directory):
            observed.setdefault(key, []).append(
                {"directory": str(directory), **value}
            )
    return observed


def aggregate_status(observations: list[dict[str, str]]) -> str:
    if not observations:
        return "NOT_EXECUTED"
    statuses = {observation["status"] for observation in observations}
    if "FAILED" in statuses:
        return "FAILED"
    if "SKIPPED" in statuses:
        return "SKIPPED"
    return "PASS"


def aggregate_reason(observations: list[dict[str, str]]) -> str:
    reasons = []
    for observation in observations:
        detail = observation["status"]
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
