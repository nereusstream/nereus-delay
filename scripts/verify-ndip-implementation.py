#!/usr/bin/env python3
"""Verify an NDIP implementation closure and its scoped source authority."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parent.parent
RECEIPT_SCHEMA = "nereus-delay.ndip.implementation-receipt"
RECEIPT_SCHEMA_GENERATION = 1
PACKAGE_DOMAIN = b"nereus-delay-ndip-package\0"
PACKAGE_DOMAIN_LABEL = r"nereus-delay-ndip-package\0"
RUNTIME_DOMAIN = b"nereus-delay-runtime-source-authority\0"
RUNTIME_DOMAIN_LABEL = r"nereus-delay-runtime-source-authority\0"
TOOLING_DOMAIN = b"nereus-delay-certification-tooling\0"
TOOLING_DOMAIN_LABEL = r"nereus-delay-certification-tooling\0"
HEX_256 = re.compile(r"[0-9a-f]{64}\Z")
GIT_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")

NORMATIVE_PATHS = (
    "docs/ndip/NDIP-1/01-调查与决策记录.md",
    "docs/ndip/NDIP-1/02-NDIP-1-Pulsar-Native-Delivery.md",
    "docs/ndip/NDIP-1/03-实施计划.md",
    "docs/ndip/NDIP-1/04-代码级目标设计.md",
)
RUNTIME_PREFIXES = (
    "gradle/wrapper/",
    "src/main/",
    "src/real-cross/",
    "src/real-kafka/",
    "src/real-pulsar/",
)
RUNTIME_EXACT_PATHS = (
    "build.gradle",
    "gradle.properties",
    "gradlew",
    "gradlew.bat",
    "settings.gradle",
)
IGNORED_FILESYSTEM_METADATA = {".DS_Store"}
TOOLING_PREFIXES = ("e2e/",)
TOOLING_EXACT_PATHS = (
    "scripts/build-disposable-local-certification.py",
    "scripts/test_verify_disposable_local_certification.py",
    "scripts/verify-disposable-local-certification.py",
)
BUILD_GRADLE_PROJECTION_RANGES = (
    (
        b"def runningArtifactIdentityDir =",
        b"tasks.named('processResources').configure {",
        b"<ndip-running-source-identity-governance>\n",
    ),
    (
        b"def designDocumentPaths = [",
        b"tasks.register('checkProjectVersionMarkers', Exec) {",
        b"<ndip-documentation-governance>\n",
    ),
)


class VerificationError(ValueError):
    """Raised when an implementation receipt or authority binding is invalid."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify a closed NDIP implementation receipt. Runtime-source equivalence "
            "uses a scoped digest, not repository HEAD equality."
        )
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        default=ROOT / "docs/ndip/NDIP-1/implementation-receipt.json",
    )
    parser.add_argument(
        "--historical-only",
        action="store_true",
        help="verify the closed historical receipt without requiring current runtime equivalence",
    )
    parser.add_argument(
        "--resolve-running-source-commit",
        action="store_true",
        help="print only the certified commit when the current runtime scope is equivalent",
    )
    parser.add_argument("--evidence-run-dir", type=Path)
    parser.add_argument("--trusted-public-key", type=Path)
    parser.add_argument("--disposable-receipt", type=Path)
    return parser.parse_args()


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise VerificationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json(path: Path, location: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise VerificationError(f"cannot read {location}: {exc}") from exc
    if raw.startswith(b"\xef\xbb\xbf"):
        raise VerificationError(f"{location} must not contain a UTF-8 BOM")
    if b"\r" in raw:
        raise VerificationError(f"{location} must use LF line endings")
    if not raw.endswith(b"\n") or raw.endswith(b"\n\n"):
        raise VerificationError(f"{location} must end with exactly one LF")
    try:
        value = json.loads(raw.decode("utf-8", errors="strict"), object_pairs_hook=reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VerificationError(f"{location} is not strict UTF-8 JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise VerificationError(f"{location} root must be an object")
    return value


def closed(value: Any, keys: set[str], location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise VerificationError(f"{location} must be an object")
    actual = set(value)
    if actual != keys:
        raise VerificationError(
            f"{location} keys are not closed: "
            f"missing={sorted(keys - actual)}, extra={sorted(actual - keys)}"
        )
    return value


def string(value: Any, location: str) -> str:
    if not isinstance(value, str) or not value:
        raise VerificationError(f"{location} must be a non-empty string")
    return value


def commit(value: Any, location: str) -> str:
    result = string(value, location)
    if GIT_COMMIT.fullmatch(result) is None:
        raise VerificationError(f"{location} must be a lowercase 40-hex commit")
    return result


def digest(value: Any, location: str) -> str:
    result = string(value, location)
    if HEX_256.fullmatch(result) is None:
        raise VerificationError(f"{location} must be a lowercase SHA-256")
    return result


def iso_date(value: Any, location: str) -> str:
    result = string(value, location)
    if ISO_DATE.fullmatch(result) is None:
        raise VerificationError(f"{location} must be YYYY-MM-DD")
    try:
        date.fromisoformat(result)
    except ValueError as exc:
        raise VerificationError(f"{location} is not a calendar date") from exc
    return result


def positive_int(value: Any, location: str) -> int:
    if type(value) is not int or value <= 0:
        raise VerificationError(f"{location} must be a positive integer")
    return value


def sha256_file(path: Path, location: str) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise VerificationError(f"cannot hash {location}: {exc}") from exc


def git_bytes(commit_id: str, path: str) -> bytes:
    process = subprocess.run(
        ["git", "show", f"{commit_id}:{path}"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if process.returncode != 0:
        error = process.stderr.decode("utf-8", errors="replace").strip()
        raise VerificationError(f"cannot read {path} at {commit_id}: {error}")
    return process.stdout


def git_paths(commit_id: str) -> tuple[str, ...]:
    commit(commit_id, "source commit")
    process = subprocess.run(
        ["git", "ls-tree", "-r", "-z", "--name-only", commit_id],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if process.returncode != 0:
        error = process.stderr.decode("utf-8", errors="replace").strip()
        raise VerificationError(f"cannot enumerate source commit {commit_id}: {error}")
    return tuple(
        item.decode("utf-8", errors="strict")
        for item in process.stdout.split(b"\0")
        if item
    )


def selected(path: str, prefixes: tuple[str, ...], exact: tuple[str, ...]) -> bool:
    return path in exact or any(path.startswith(prefix) for prefix in prefixes)


def project_source(path: str, data: bytes) -> bytes:
    if path == "gradlew.bat":
        return data.replace(b"\r\n", b"\n")
    if path != "build.gradle":
        return data
    projected = data
    for start, end, marker in BUILD_GRADLE_PROJECTION_RANGES:
        if projected.count(start) != 1 or projected.count(end) != 1:
            raise VerificationError(
                "build.gradle governance projection markers must occur exactly once"
            )
        start_index = projected.find(start)
        end_index = projected.find(end)
        if start_index < 0 or end_index < 0 or end_index <= start_index:
            raise VerificationError("build.gradle does not contain the closed governance projection range")
        projected = projected[:start_index] + marker + projected[end_index:]
    return projected


def canonical_source_digest(
    paths: tuple[str, ...], read: Callable[[str], bytes], domain: bytes
) -> tuple[str, int]:
    ordered = tuple(sorted(paths, key=lambda item: item.encode("utf-8")))
    if len(set(ordered)) != len(ordered):
        raise VerificationError("source authority contains duplicate paths")
    material = bytearray(domain)
    for path in ordered:
        path_bytes = path.encode("utf-8")
        file_digest = hashlib.sha256(project_source(path, read(path))).digest()
        material.extend(len(path_bytes).to_bytes(4, byteorder="big", signed=False))
        material.extend(path_bytes)
        material.extend(file_digest)
    return hashlib.sha256(material).hexdigest(), len(ordered)


def source_from_git(
    commit_id: str,
    prefixes: tuple[str, ...],
    exact: tuple[str, ...],
    domain: bytes,
) -> tuple[str, int, tuple[str, ...]]:
    paths = tuple(
        sorted(
            (path for path in git_paths(commit_id) if selected(path, prefixes, exact)),
            key=lambda item: item.encode("utf-8"),
        )
    )
    result, count = canonical_source_digest(paths, lambda path: git_bytes(commit_id, path), domain)
    return result, count, paths


def current_paths(prefixes: tuple[str, ...], exact: tuple[str, ...]) -> tuple[str, ...]:
    paths: set[str] = set()
    for path_text in exact:
        path = ROOT / path_text
        if not path.is_file() or path.is_symlink():
            raise VerificationError(f"runtime source path must be a regular file: {path_text}")
        paths.add(path_text)
    for prefix in prefixes:
        base = ROOT / prefix.rstrip("/")
        if not base.is_dir() or base.is_symlink():
            raise VerificationError(f"runtime source root must be a directory: {prefix}")
        for path in base.rglob("*"):
            if path.is_symlink():
                raise VerificationError(f"runtime source must not contain symlinks: {path}")
            if path.name in IGNORED_FILESYSTEM_METADATA:
                continue
            if path.is_file():
                paths.add(path.relative_to(ROOT).as_posix())
    return tuple(sorted(paths, key=lambda item: item.encode("utf-8")))


def source_from_current(
    prefixes: tuple[str, ...], exact: tuple[str, ...], domain: bytes
) -> tuple[str, int, tuple[str, ...]]:
    paths = current_paths(prefixes, exact)
    result, count = canonical_source_digest(paths, lambda path: (ROOT / path).read_bytes(), domain)
    return result, count, paths


def calculate_package(read: Callable[[str], bytes]) -> tuple[str, list[dict[str, str]]]:
    material = bytearray(PACKAGE_DOMAIN)
    files: list[dict[str, str]] = []
    for path in NORMATIVE_PATHS:
        data = read(path)
        if data.startswith(b"\xef\xbb\xbf") or b"\r" in data:
            raise VerificationError(f"normative file encoding is not canonical: {path}")
        if not data.endswith(b"\n") or data.endswith(b"\n\n"):
            raise VerificationError(f"normative file must end with exactly one LF: {path}")
        data.decode("utf-8", errors="strict")
        path_bytes = path.encode("utf-8")
        file_digest = hashlib.sha256(data).digest()
        material.extend(len(path_bytes).to_bytes(4, byteorder="big", signed=False))
        material.extend(path_bytes)
        material.extend(file_digest)
        files.append({"path": path, "sha256": file_digest.hex()})
    return hashlib.sha256(material).hexdigest(), files


def verify_package(value: Any) -> str:
    package = closed(
        value,
        {"digestAlgorithm", "digestDomain", "pathBase", "files", "digest"},
        "implementedNormativePackage",
    )
    if package["digestAlgorithm"] != "SHA-256":
        raise VerificationError("implementedNormativePackage.digestAlgorithm must be SHA-256")
    if package["digestDomain"] != PACKAGE_DOMAIN_LABEL:
        raise VerificationError("implementedNormativePackage.digestDomain is invalid")
    if package["pathBase"] != "repository-root":
        raise VerificationError("implementedNormativePackage.pathBase must be repository-root")
    actual_digest, actual_files = calculate_package(lambda path: (ROOT / path).read_bytes())
    if package["files"] != actual_files:
        raise VerificationError("implemented normative file digests do not match the checkout")
    if package["digest"] != actual_digest:
        raise VerificationError("implemented normative package digest does not match the checkout")
    return actual_digest


def verify_source_authority(value: Any, current_required: bool) -> tuple[str, str, int]:
    authority = closed(
        value,
        {"digestAlgorithm", "digestDomain", "scope", "certifiedCommit", "fileCount", "digest"},
        "runtimeSourceAuthority",
    )
    if authority["digestAlgorithm"] != "SHA-256":
        raise VerificationError("runtimeSourceAuthority.digestAlgorithm must be SHA-256")
    if authority["digestDomain"] != RUNTIME_DOMAIN_LABEL:
        raise VerificationError("runtimeSourceAuthority.digestDomain is invalid")
    if authority["scope"] != "NDIP_RUNTIME_SOURCE":
        raise VerificationError("runtimeSourceAuthority.scope is invalid")
    certified_commit = commit(authority["certifiedCommit"], "runtimeSourceAuthority.certifiedCommit")
    expected_digest = digest(authority["digest"], "runtimeSourceAuthority.digest")
    expected_count = positive_int(authority["fileCount"], "runtimeSourceAuthority.fileCount")
    historical_digest, historical_count, historical_paths = source_from_git(
        certified_commit, RUNTIME_PREFIXES, RUNTIME_EXACT_PATHS, RUNTIME_DOMAIN
    )
    if (historical_digest, historical_count) != (expected_digest, expected_count):
        raise VerificationError("certified runtime source does not match the implementation receipt")
    if current_required:
        current_digest, current_count, current_source_paths = source_from_current(
            RUNTIME_PREFIXES, RUNTIME_EXACT_PATHS, RUNTIME_DOMAIN
        )
        if current_source_paths != historical_paths:
            missing = sorted(set(historical_paths) - set(current_source_paths))
            extra = sorted(set(current_source_paths) - set(historical_paths))
            raise VerificationError(
                f"current runtime source path set differs: missing={missing}, extra={extra}"
            )
        if (current_digest, current_count) != (expected_digest, expected_count):
            raise VerificationError("current runtime source digest differs from the certified source")
    return certified_commit, expected_digest, expected_count


def verify_tooling_history(value: Any, certified_commit: str) -> tuple[str, int]:
    tooling = closed(
        value,
        {"digestAlgorithm", "digestDomain", "scope", "certifiedCommit", "fileCount", "digest"},
        "certificationToolingHistory",
    )
    if tooling["digestAlgorithm"] != "SHA-256" or tooling["digestDomain"] != TOOLING_DOMAIN_LABEL:
        raise VerificationError("certificationToolingHistory digest contract is invalid")
    if tooling["scope"] != "NDIP_CERTIFICATION_TOOLING":
        raise VerificationError("certificationToolingHistory.scope is invalid")
    if tooling["certifiedCommit"] != certified_commit:
        raise VerificationError("certification tooling and runtime source commits differ")
    expected_digest = digest(tooling["digest"], "certificationToolingHistory.digest")
    expected_count = positive_int(tooling["fileCount"], "certificationToolingHistory.fileCount")
    actual_digest, actual_count, _ = source_from_git(
        certified_commit, TOOLING_PREFIXES, TOOLING_EXACT_PATHS, TOOLING_DOMAIN
    )
    if (actual_digest, actual_count) != (expected_digest, expected_count):
        raise VerificationError("historical certification tooling digest mismatch")
    return expected_digest, expected_count


def verify_closure(value: Any) -> dict[str, Any]:
    closure = closed(
        value,
        {
            "implementation",
            "lifecycle",
            "deployment",
            "performanceAndScale",
            "productionRollout",
            "productionAuthority",
            "deferredScopes",
        },
        "closure",
    )
    expected = {
        "implementation": "CLOSED",
        "lifecycle": "CLOSED",
        "deployment": "SEPARATE_FUTURE_WORK",
        "performanceAndScale": "SEPARATE_FUTURE_WORK",
        "productionRollout": "SEPARATE_FUTURE_WORK",
        "productionAuthority": False,
        "deferredScopes": [
            "target-environment deployment",
            "performance and scale scenarios",
            "production rollout",
        ],
    }
    if closure != expected or closure["productionAuthority"] is not False:
        raise VerificationError("implementation closure boundaries are not closed")
    return closure


def verify_accepted_package(
    package_dir: Path, receipt_path: Path, source_commit: str | None = None
) -> None:
    command = [
        sys.executable,
        "-B",
        str(ROOT / "scripts/verify-ndip-package.py"),
        "--package-dir",
        str(package_dir),
        "--receipt",
        str(receipt_path),
        "--require-accepted",
    ]
    if source_commit is not None:
        command.extend(("--source-commit", source_commit))
    process = subprocess.run(
        command,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if process.returncode != 0:
        error = process.stderr.decode("utf-8", errors="replace").strip()
        raise VerificationError(f"Accepted package verifier failed: {error}")


def verify_receipt(receipt: dict[str, Any], current_required: bool) -> dict[str, Any]:
    closed(
        receipt,
        {
            "receiptSchema",
            "receiptSchemaGeneration",
            "proposalId",
            "receiptStatus",
            "authority",
            "preparedAt",
            "governanceAuthority",
            "acceptedPackageHistory",
            "implementedNormativePackage",
            "runtimeSourceAuthority",
            "certificationToolingHistory",
            "certificationEvidence",
            "decision",
            "closure",
        },
        "receipt",
    )
    if receipt["receiptSchema"] != RECEIPT_SCHEMA:
        raise VerificationError("unknown implementation receipt schema")
    if (
        type(receipt["receiptSchemaGeneration"]) is not int
        or receipt["receiptSchemaGeneration"] != RECEIPT_SCHEMA_GENERATION
    ):
        raise VerificationError("unknown implementation receipt schema generation")
    if receipt["proposalId"] != "NDIP-1" or receipt["receiptStatus"] != "IMPLEMENTED":
        raise VerificationError("implementation receipt must close NDIP-1 as IMPLEMENTED")
    if receipt["authority"] is not True:
        raise VerificationError("implementation lifecycle authority must be true")
    iso_date(receipt["preparedAt"], "preparedAt")

    governance = closed(
        receipt["governanceAuthority"],
        {"proposalId", "requiredStatus", "acceptanceReceipt", "acceptanceReceiptSha256"},
        "governanceAuthority",
    )
    if governance["proposalId"] != "NDIP-2" or governance["requiredStatus"] != "ACCEPTED":
        raise VerificationError("NDIP-2 Accepted authority is required for this lifecycle transition")
    governance_path = ROOT / string(governance["acceptanceReceipt"], "governanceAuthority.acceptanceReceipt")
    if sha256_file(governance_path, "NDIP-2 acceptance receipt") != digest(
        governance["acceptanceReceiptSha256"], "governanceAuthority.acceptanceReceiptSha256"
    ):
        raise VerificationError("NDIP-2 acceptance receipt digest mismatch")
    verify_accepted_package(governance_path.parent, governance_path)

    history = closed(
        receipt["acceptedPackageHistory"],
        {"receiptPath", "receiptSha256", "transitionCommit", "normativePackageDigest"},
        "acceptedPackageHistory",
    )
    history_path_text = string(history["receiptPath"], "acceptedPackageHistory.receiptPath")
    history_path = ROOT / history_path_text
    history_sha = digest(history["receiptSha256"], "acceptedPackageHistory.receiptSha256")
    if sha256_file(history_path, "NDIP-1 acceptance receipt") != history_sha:
        raise VerificationError("preserved NDIP-1 acceptance receipt digest mismatch")
    transition_commit = commit(history["transitionCommit"], "acceptedPackageHistory.transitionCommit")
    if hashlib.sha256(git_bytes(transition_commit, history_path_text)).hexdigest() != history_sha:
        raise VerificationError("NDIP-1 acceptance receipt is not preserved at its transition commit")
    verify_accepted_package(history_path.parent, history_path, transition_commit)
    accepted_digest, _ = calculate_package(lambda path: git_bytes(transition_commit, path))
    if accepted_digest != digest(history["normativePackageDigest"], "acceptedPackageHistory.normativePackageDigest"):
        raise VerificationError("historical Accepted normative package digest mismatch")

    implemented_digest = verify_package(receipt["implementedNormativePackage"])
    certified_commit, runtime_digest, runtime_count = verify_source_authority(
        receipt["runtimeSourceAuthority"], current_required
    )
    tooling_digest, tooling_count = verify_tooling_history(
        receipt["certificationToolingHistory"], certified_commit
    )

    evidence = closed(
        receipt["certificationEvidence"],
        {"disposableLocal", "persistentStaging", "productionAuthority"},
        "certificationEvidence",
    )
    if evidence["productionAuthority"] is not False:
        raise VerificationError("implementation certification must not grant production authority")
    disposable = closed(
        evidence["disposableLocal"],
        {
            "classification",
            "runId",
            "candidateCommit",
            "acceptedPackageDigest",
            "p1SourceLock",
            "receiptSha256",
            "stagingVerifierLogSha256",
            "matrixPassed",
            "matrixTotal",
            "supportingChecksPassed",
            "supportingChecksTotal",
            "status",
            "authority",
        },
        "certificationEvidence.disposableLocal",
    )
    if disposable["classification"] != "DISPOSABLE_LOCAL" or disposable["status"] != "PASS":
        raise VerificationError("disposable certification is not a closed PASS")
    if disposable["candidateCommit"] != certified_commit:
        raise VerificationError("disposable certification candidate differs from certified source")
    if disposable["acceptedPackageDigest"] != accepted_digest:
        raise VerificationError("disposable certification Accepted package digest mismatch")
    if (
        positive_int(disposable["matrixPassed"], "certificationEvidence.disposableLocal.matrixPassed") != 24
        or positive_int(disposable["matrixTotal"], "certificationEvidence.disposableLocal.matrixTotal") != 24
    ):
        raise VerificationError("disposable certification must bind 24/24")
    if (
        positive_int(
            disposable["supportingChecksPassed"],
            "certificationEvidence.disposableLocal.supportingChecksPassed",
        )
        != 3
        or positive_int(
            disposable["supportingChecksTotal"],
            "certificationEvidence.disposableLocal.supportingChecksTotal",
        )
        != 3
    ):
        raise VerificationError("disposable certification must bind 3/3 supporting checks")
    if disposable["authority"] is not False:
        raise VerificationError("disposable certification must remain non-authoritative")
    digest(disposable["receiptSha256"], "certificationEvidence.disposableLocal.receiptSha256")
    digest(
        disposable["stagingVerifierLogSha256"],
        "certificationEvidence.disposableLocal.stagingVerifierLogSha256",
    )
    p1_source_lock = commit(disposable["p1SourceLock"], "certificationEvidence.disposableLocal.p1SourceLock")

    staging = closed(
        evidence["persistentStaging"],
        {
            "classification",
            "environmentId",
            "runId",
            "candidateCommit",
            "acceptedPackageDigest",
            "p1SourceLock",
            "finalSummarySha256",
            "signedValidationSha256",
            "signedValidationPayloadSha256",
            "trustedPublicKeySha256",
            "gateC",
            "manifestReadback",
            "shadow",
            "autoFastCanary",
            "managedHandoffCanary",
            "attemptJournalStartupReplayRecords",
            "finalPolicy",
            "activeLeaseCount",
            "activeSendCount",
            "status",
            "productionAuthority",
        },
        "certificationEvidence.persistentStaging",
    )
    if staging["classification"] != "STAGING" or staging["status"] != "PASS":
        raise VerificationError("persistent staging certification is not a closed PASS")
    if staging["candidateCommit"] != certified_commit:
        raise VerificationError("persistent certification candidate differs from certified source")
    if staging["acceptedPackageDigest"] != accepted_digest or staging["p1SourceLock"] != p1_source_lock:
        raise VerificationError("persistent certification source/package lock differs")
    for field in (
        "finalSummarySha256",
        "signedValidationSha256",
        "signedValidationPayloadSha256",
        "trustedPublicKeySha256",
    ):
        digest(staging[field], f"certificationEvidence.persistentStaging.{field}")
    if staging["gateC"] != "41/41" or staging["manifestReadback"] != "13/13":
        raise VerificationError("persistent certification Gate C or Manifest readback is incomplete")
    if staging["shadow"] != "PASS_0_0_0":
        raise VerificationError("persistent certification SHADOW is incomplete")
    if staging["autoFastCanary"] != "PASS_1_1_0" or staging["managedHandoffCanary"] != "PASS_1_1_1":
        raise VerificationError("persistent certification canary evidence is incomplete")
    if (
        positive_int(
            staging["attemptJournalStartupReplayRecords"],
            "certificationEvidence.persistentStaging.attemptJournalStartupReplayRecords",
        )
        != 3
    ):
        raise VerificationError("persistent certification does not bind three Journal replay records")
    if (
        staging["finalPolicy"] != "DISABLED"
        or type(staging["activeLeaseCount"]) is not int
        or staging["activeLeaseCount"] != 0
        or type(staging["activeSendCount"]) is not int
        or staging["activeSendCount"] != 0
    ):
        raise VerificationError("persistent certification did not close in a safe DISABLED state")
    if staging["productionAuthority"] is not False:
        raise VerificationError("staging certification must remain productionAuthority=false")

    decision_value = closed(
        receipt["decision"],
        {"status", "implementedBy", "implementedAt", "decisionReference"},
        "decision",
    )
    if decision_value["status"] != "IMPLEMENTED":
        raise VerificationError("implementation decision status must be IMPLEMENTED")
    string(decision_value["implementedBy"], "decision.implementedBy")
    iso_date(decision_value["implementedAt"], "decision.implementedAt")
    string(decision_value["decisionReference"], "decision.decisionReference")

    verify_closure(receipt["closure"])

    return {
        "acceptedPackageDigest": accepted_digest,
        "implementedPackageDigest": implemented_digest,
        "certifiedCommit": certified_commit,
        "runtimeSourceDigest": runtime_digest,
        "runtimeSourceFileCount": runtime_count,
        "toolingDigest": tooling_digest,
        "toolingFileCount": tooling_count,
        "p1SourceLock": p1_source_lock,
        "stagingRunId": staging["runId"],
    }


def verify_external_evidence(
    receipt: dict[str, Any],
    run_dir: Path,
    trusted_public_key: Path,
    disposable_path: Path | None,
) -> None:
    evidence = receipt["certificationEvidence"]
    staging = evidence["persistentStaging"]
    disposable_expected = evidence["disposableLocal"]
    disposable_log = run_dir / "logs/disposable-receipt-verifier.log"
    if (
        sha256_file(disposable_log, "retained disposable verifier log")
        != disposable_expected["stagingVerifierLogSha256"]
    ):
        raise VerificationError("retained disposable verifier log digest mismatch")
    try:
        lines = disposable_log.read_text(encoding="utf-8", errors="strict").splitlines()
        parsed_lines = dict(line.split("=", 1) for line in lines)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise VerificationError(f"retained disposable verifier log is invalid: {exc}") from exc
    expected_lines = {
        "receipt_status": "PASS",
        "classification": "DISPOSABLE_LOCAL",
        "authority": "false",
        "gateC": "false",
        "shadow": "false",
        "enabled": "false",
        "matrix_cells": "24",
        "matrix_executed_pass": "24",
        "matrix_not_covered": "0",
        "cleanup": "verified",
    }
    if len(lines) != len(parsed_lines) or parsed_lines != expected_lines:
        raise VerificationError("retained disposable verifier log is not a closed PASS")

    if disposable_path is not None:
        if sha256_file(disposable_path, "disposable certification receipt") != disposable_expected["receiptSha256"]:
            raise VerificationError("external disposable certification receipt digest mismatch")
        disposable = load_json(disposable_path, "disposable certification receipt")
        if disposable.get("status") != "PASS" or disposable.get("authority") is not False:
            raise VerificationError("external disposable certification is not a non-authoritative PASS")
        matrix = disposable.get("matrix")
        supporting = disposable.get("supportingChecks")
        if (
            not isinstance(matrix, list)
            or len(matrix) != 24
            or any(
                not isinstance(item, dict) or item.get("status") != "EXECUTED_PASS"
                for item in matrix
            )
        ):
            raise VerificationError("external disposable matrix is not 24/24 EXECUTED_PASS")
        if (
            not isinstance(supporting, list)
            or len(supporting) != 3
            or any(
                not isinstance(item, dict) or item.get("status") != "PASS"
                for item in supporting
            )
        ):
            raise VerificationError("external disposable supporting checks are not 3/3 PASS")
        source = disposable.get("source")
        if not isinstance(source, dict) or source.get("delayCommit") != disposable_expected["candidateCommit"]:
            raise VerificationError("external disposable source binding mismatch")

    final_summary = run_dir / "final-summary.json"
    signed_validation = run_dir / "authority/persistent-certification-validation.signed.json"
    if sha256_file(final_summary, "persistent final summary") != staging["finalSummarySha256"]:
        raise VerificationError("external persistent final summary digest mismatch")
    if sha256_file(signed_validation, "signed persistent validation") != staging["signedValidationSha256"]:
        raise VerificationError("external signed persistent validation digest mismatch")
    if sha256_file(trusted_public_key, "trusted public key") != staging["trustedPublicKeySha256"]:
        raise VerificationError("external trusted public key digest mismatch")
    signed_validation_value = load_json(
        signed_validation, "signed persistent validation"
    )
    if signed_validation_value.get("payloadSha256") != staging["signedValidationPayloadSha256"]:
        raise VerificationError("external signed persistent validation payload digest mismatch")

    command = [
        sys.executable,
        str(ROOT / "e2e/validate-ndip1-persistent-certification.py"),
        "--run-dir",
        str(run_dir),
        "--trusted-public-key",
        str(trusted_public_key),
        "--expected-candidate",
        staging["candidateCommit"],
        "--expected-package-digest",
        staging["acceptedPackageDigest"],
        "--expected-p1-source-lock",
        staging["p1SourceLock"],
    ]
    process = subprocess.run(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if process.returncode != 0:
        error = process.stderr.decode("utf-8", errors="replace").strip()
        raise VerificationError(f"persistent independent validator failed: {error}")
    try:
        result = json.loads(process.stdout.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VerificationError(f"persistent validator output is not JSON: {exc}") from exc
    required = {
        "status": "PASS",
        "candidateCommit": staging["candidateCommit"],
        "ndipPackageDigest": staging["acceptedPackageDigest"],
        "p1SourceLock": staging["p1SourceLock"],
        "applicableChecks": 41,
        "resourceReadbackCount": 13,
        "nativeAdmission": 2,
        "nativeSend": 2,
        "handedOff": 1,
        "attemptJournalStartupReplayRecords": 3,
        "finalPolicy": "DISABLED",
        "activeLeaseCount": 0,
        "activeSendCount": 0,
        "productionAuthority": False,
        "trustedPublicKeySha256": staging["trustedPublicKeySha256"],
    }
    for key, expected in required.items():
        actual = result.get(key)
        if actual != expected or type(actual) is not type(expected):
            raise VerificationError(f"persistent validator result mismatch: {key}")


def main() -> int:
    args = parse_args()
    try:
        receipt = load_json(args.receipt.resolve(), "implementation receipt")
        result = verify_receipt(receipt, current_required=not args.historical_only)
        persistent_args = (args.evidence_run_dir, args.trusted_public_key)
        if any(value is not None for value in persistent_args):
            if any(value is None for value in persistent_args):
                raise VerificationError(
                    "--evidence-run-dir and --trusted-public-key must be supplied together"
                )
            verify_external_evidence(
                receipt,
                args.evidence_run_dir.resolve(),
                args.trusted_public_key.resolve(),
                (
                    args.disposable_receipt.resolve()
                    if args.disposable_receipt is not None
                    else None
                ),
            )
        elif args.disposable_receipt is not None:
            raise VerificationError(
                "--disposable-receipt requires --evidence-run-dir and --trusted-public-key"
            )
    except (OSError, UnicodeDecodeError, VerificationError) as exc:
        if args.resolve_running_source_commit:
            return 1
        print(f"NDIP implementation verification failed: {exc}", file=sys.stderr)
        return 1

    if args.resolve_running_source_commit:
        print(result["certifiedCommit"])
        return 0
    print("proposal=NDIP-1")
    print("receipt_status=IMPLEMENTED")
    print("lifecycle_authority=true")
    print("implementation=CLOSED")
    print("lifecycle=CLOSED")
    print(f"certified_commit={result['certifiedCommit']}")
    print(f"runtime_source_sha256={result['runtimeSourceDigest']}")
    print(f"runtime_source_files={result['runtimeSourceFileCount']}")
    print(f"implemented_package_sha256={result['implementedPackageDigest']}")
    print(f"staging_run={result['stagingRunId']}")
    print("deployment=SEPARATE_FUTURE_WORK")
    print("performance_and_scale=SEPARATE_FUTURE_WORK")
    print("production_rollout=SEPARATE_FUTURE_WORK")
    print("production_authority=false")
    if args.evidence_run_dir is not None:
        print("external_disposable_verifier_log=PASS")
        print(
            "external_disposable_receipt="
            + ("PASS" if args.disposable_receipt is not None else "NOT_RETAINED_HASH_BOUND")
        )
        print("external_persistent_chain=PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
