#!/usr/bin/env python3
"""Validate the bounded, persistent NDIP-1 SHADOW observation evidence."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET


def fail(message: str) -> None:
    raise SystemExit(message)


def normalized_case_name(name: str) -> str:
    """Normalize the no-argument JUnit suffix emitted by Gradle XML reports."""
    return name[:-2] if name.endswith("()") else name


def read_json(path: Path) -> dict:
    if not path.is_file() or path.is_symlink():
        fail(f"missing regular evidence file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        fail(f"invalid JSON evidence {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"evidence is not a JSON object: {path}")
    return value


def result_cases(directory: Path) -> dict[tuple[str, str], str]:
    cases: dict[tuple[str, str], str] = {}
    for path in sorted(directory.glob("TEST-*.xml")):
        try:
            root = ET.parse(path).getroot()
        except (OSError, ET.ParseError) as exc:
            fail(f"invalid JUnit XML {path}: {exc}")
        for case in root.iter("testcase"):
            key = (case.attrib.get("classname", ""), normalized_case_name(case.attrib.get("name", "")))
            if case.find("failure") is not None or case.find("error") is not None:
                state = "FAILED"
            elif case.find("skipped") is not None:
                state = "SKIPPED"
            else:
                state = "PASS"
            cases[key] = state
    return cases


def require_tests(run_dir: Path, label: str, names: set[str]) -> None:
    directory = run_dir / "results" / label
    cases = result_cases(directory)
    for name in names:
        matches = [(key, state) for key, state in cases.items() if key[1] == name]
        if len(matches) != 1 or matches[0][1] != "PASS":
            fail(f"SHADOW JUnit case is not exactly PASS: {label}/{name}: {matches}")


def verify_policy(policy_dir: Path, trusted_public_key: Path, expected_key_generation: int) -> None:
    trusted_key = trusted_public_key.read_bytes()
    prior_version = 0
    prior_generation = 0
    policy_scope: str | None = None
    for phase in ("shadow-initial", "shadow-candidate-add", "shadow-candidate-cancel"):
        envelope_path = policy_dir / f"{phase}.signed.json"
        envelope = read_json(envelope_path)
        if envelope.get("evidenceSchema") != "nereus-delay.persistent-staging-evidence":
            fail(f"SHADOW policy is not a signed evidence envelope: {envelope_path}")
        try:
            key = base64.b64decode(envelope["publicKeyDerBase64"], validate=True)
            payload = base64.b64decode(envelope["payloadBase64"], validate=True)
        except (KeyError, ValueError, TypeError) as exc:
            fail(f"SHADOW policy envelope contains invalid base64: {envelope_path}: {exc}")
        if key != trusted_key or envelope.get("keyGeneration") != expected_key_generation:
            fail(f"SHADOW policy is not bound to the external trust root: {envelope_path}")
        if hashlib.sha256(payload).hexdigest() != envelope.get("payloadSha256"):
            fail(f"SHADOW policy payload digest mismatch: {envelope_path}")
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            fail(f"SHADOW policy payload is not JSON: {envelope_path}: {exc}")
        if (value.get("policySchema") != "nereus-delay.handoff-policy-publication"
                or value.get("policyStatus") != "SHADOW"):
            fail(f"SHADOW policy has the wrong status: {envelope_path}")
        version = int(value.get("policyOxiaVersion", 0))
        generation = int(value.get("policyGeneration", 0))
        scope = value.get("policyScopeDigest")
        if version <= prior_version or generation <= prior_generation:
            fail(f"SHADOW policy CAS version/generation did not advance: {envelope_path}")
        if not isinstance(scope, str) or not re.fullmatch(r"[0-9a-f]{64}", scope):
            fail(f"SHADOW policy has no canonical scope: {envelope_path}")
        if policy_scope is None:
            policy_scope = scope
        elif scope != policy_scope:
            fail(f"SHADOW policy scope changed across current-head updates: {envelope_path}")
        readback = policy_dir / f"{phase}-readback.log"
        readback_text = readback.read_text(encoding="utf-8")
        for marker in (f"policyMode=SHADOW", f"policyOxiaVersion={version}", f"policyGeneration={generation}"):
            if marker not in readback_text:
                fail(f"real Oxia current-head readback is incomplete: {readback}: {marker}")
        prior_version = version
        prior_generation = generation


def state_has_unresolved(path: Path) -> bool:
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8")
    if re.search(r'"(?:attempt_state|state)"\s*:\s*"(?:PUBLISHING|UNCERTAIN)"', text):
        return True
    if re.search(r'"unresolved(?:Publishing|Uncertain)"\s*:\s*true', text):
        return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shadow-dir", type=Path, required=True)
    parser.add_argument("--policy-dir", type=Path, required=True)
    parser.add_argument("--trusted-public-key", type=Path, required=True)
    parser.add_argument("--issuer-key-generation", type=int, required=True)
    parser.add_argument("--gate-c-log", type=Path, required=True)
    parser.add_argument("--observation-seconds", type=int, required=True)
    args = parser.parse_args()

    if args.observation_seconds < 10:
        fail("SHADOW observation window is shorter than 10 seconds")
    run_dir = args.shadow_dir.parent
    verify_policy(args.policy_dir, args.trusted_public_key, args.issuer_key_generation)
    require_tests(
        run_dir,
        "shadow-policy-controls",
        {
            "shadowAndFifoNeverBecomeNativeCandidates",
            "crossingCandidateBoundaryRequiresAFreshSampleAndDoesNotSchedulePastWork",
            "crossingPolicyLeaseBoundaryRequiresAFreshSample",
            "signedSnapshotHeadAndOxiaRevisionRoundTrip",
            "casRejectsAStaleOxiaRevisionWithoutReplacingTheCurrentHead",
            "retryPolicyCatalogRequiresExactSourceVisibleHistory",
        },
    )
    require_tests(
        run_dir,
        "shadow-state-rebuild",
        {
            "openAndConservativeFinalSurviveReopen",
            "separateInstancesRereadTheLatestMerge",
            "hardFilterRejectsOverCapacityInsteadOfChoosingLeastOverfullWorker",
        },
    )
    require_tests(
        run_dir,
        "gate-c-shadow-oxia-restart",
        {"signedRouteProviderRecoversAfterRealOxiaRestart"},
    )
    require_tests(
        run_dir,
        "shadow-minio-outage",
        {"realMinioTimeoutAfterCommitResolvesByExactReadback"},
    )

    ownership = read_json(args.shadow_dir / "chaos/shadow-worker-ownership/ownership-transfer.json")
    for field in ("freshProcess", "ownershipTransfer", "ordinaryManagedPath"):
        if ownership.get(field) is not True:
            fail(f"SHADOW Worker ownership evidence is incomplete: {field}")
    for state_name in ("before-process-crash.json", "after-fresh-process.json"):
        if state_has_unresolved(args.shadow_dir / "chaos/shadow-worker-ownership/state" / state_name):
            fail(f"SHADOW Worker state contains unresolved PUBLISHING/UNCERTAIN: {state_name}")

    broker_ready = args.shadow_dir / "broker-restart/ready.json"
    if not broker_ready.is_file() or not broker_ready.read_text(encoding="utf-8").strip():
        fail("SHADOW broker restart has no real readiness readback")
    if not args.gate_c_log.is_file() or "Pulsar Worker vertical smoke passed" not in args.gate_c_log.read_text(
        encoding="utf-8"
    ):
        fail("SHADOW is missing the real managed Worker baseline evidence")

    forbidden = ("Pulsar native coordinator typed-evidence smoke passed", "native physical send")
    for path in args.shadow_dir.rglob("*.log"):
        text = path.read_text(encoding="utf-8")
        if any(marker in text for marker in forbidden):
            fail(f"SHADOW evidence contains a native physical send marker: {path}")
    for path in (args.shadow_dir / "chaos/shadow-worker-ownership/state").glob("*.json"):
        if state_has_unresolved(path):
            fail(f"SHADOW state contains unresolved state: {path}")

    run_files = [
        run_dir / "results/gate-c-shadow-oxia-restart/run.json",
        run_dir / "results/shadow-minio-outage/run.json",
    ]
    for path in run_files:
        if read_json(path).get("exitCode") != 0:
            fail(f"SHADOW real dependency run did not exit zero: {path}")

    print(json.dumps({
        "schema": "nereus-delay.ndip1-shadow-validation",
        "status": "PASS",
        "observationSeconds": args.observation_seconds,
        "nativeAdmission": 0,
        "nativeSend": 0,
        "handedOff": 0,
        "unresolvedPublishing": False,
        "unresolvedUncertain": False,
        "attemptJournalLeak": False,
        "generationIncarnationMix": False,
        "realOxiaRestart": True,
        "realMinioOutage": True,
        "realWorkerOwnershipTransfer": True,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
