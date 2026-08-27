#!/usr/bin/env python3
"""Verify one closed Nereus Delay improvement proposal package receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any


PACKAGE_DOMAIN = b"nereus-delay-ndip-package\0"
PACKAGE_DOMAIN_LABEL = r"nereus-delay-ndip-package\0"
RECEIPT_SCHEMA = "nereus-delay.ndip.acceptance-receipt"
RECEIPT_SCHEMA_GENERATION = 2
HEX_256 = re.compile(r"[0-9a-f]{64}\Z")
GIT_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")

EXPECTED_PACKAGES = {
    "NDIP-1": (
        "docs/ndip/NDIP-1/01-调查与决策记录.md",
        "docs/ndip/NDIP-1/02-NDIP-1-Pulsar-Native-Delivery.md",
        "docs/ndip/NDIP-1/03-实施计划.md",
        "docs/ndip/NDIP-1/04-代码级目标设计.md",
    )
}


class VerificationError(ValueError):
    """Raised when a package or receipt is not canonical."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify exact NDIP normative bytes against a candidate or accepted "
            "receipt. Candidate verification never grants authority."
        )
    )
    parser.add_argument("--package-dir", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument(
        "--require-accepted",
        action="store_true",
        help="fail unless the receipt is a complete ACCEPTED receipt",
    )
    return parser.parse_args()


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise VerificationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_receipt(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise VerificationError(f"cannot read receipt {path}: {exc}") from exc
    if raw.startswith(b"\xef\xbb\xbf"):
        raise VerificationError("receipt must not contain a UTF-8 BOM")
    if b"\r" in raw:
        raise VerificationError("receipt must use LF line endings")
    if not raw.endswith(b"\n") or raw.endswith(b"\n\n"):
        raise VerificationError("receipt must end with exactly one LF")
    try:
        text = raw.decode("utf-8", errors="strict")
        value = json.loads(text, object_pairs_hook=reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VerificationError(f"receipt is not strict UTF-8 JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise VerificationError("receipt root must be an object")
    return value


def require_closed_object(
    value: Any, expected_keys: set[str], location: str
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise VerificationError(f"{location} must be an object")
    actual_keys = set(value)
    if actual_keys != expected_keys:
        missing = sorted(expected_keys - actual_keys)
        extra = sorted(actual_keys - expected_keys)
        raise VerificationError(
            f"{location} keys are not closed: missing={missing}, extra={extra}"
        )
    return value


def require_string(value: Any, location: str) -> str:
    if not isinstance(value, str) or not value:
        raise VerificationError(f"{location} must be a non-empty string")
    return value


def require_commit(value: Any, location: str) -> str:
    commit = require_string(value, location)
    if GIT_COMMIT.fullmatch(commit) is None:
        raise VerificationError(f"{location} must be a lowercase 40-hex commit")
    return commit


def require_date(value: Any, location: str) -> str:
    date_text = require_string(value, location)
    if ISO_DATE.fullmatch(date_text) is None:
        raise VerificationError(f"{location} must be YYYY-MM-DD")
    try:
        date.fromisoformat(date_text)
    except ValueError as exc:
        raise VerificationError(f"{location} is not a calendar date") from exc
    return date_text


def repository_root() -> Path:
    return Path(__file__).resolve().parent.parent


def repository_relative(path: Path, root: Path, location: str) -> str:
    try:
        return path.resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise VerificationError(f"{location} must be inside repository root") from exc


def validate_receipt_shape(
    receipt: dict[str, Any], package_dir: Path, receipt_path: Path, root: Path
) -> tuple[str, tuple[str, ...], str]:
    require_closed_object(
        receipt,
        {
            "receiptSchema",
            "receiptSchemaGeneration",
            "proposalId",
            "receiptStatus",
            "authority",
            "preparedAt",
            "governanceBridge",
            "normativePackage",
            "reviewBaseline",
            "decision",
            "authorization",
        },
        "receipt",
    )
    if receipt["receiptSchema"] != RECEIPT_SCHEMA:
        raise VerificationError("unknown receiptSchema")
    if (
        type(receipt["receiptSchemaGeneration"]) is not int
        or receipt["receiptSchemaGeneration"] != RECEIPT_SCHEMA_GENERATION
    ):
        raise VerificationError("unknown receiptSchemaGeneration")

    proposal_id = require_string(receipt["proposalId"], "proposalId")
    expected_paths = EXPECTED_PACKAGES.get(proposal_id)
    if expected_paths is None:
        raise VerificationError(f"proposal is not registered by verifier: {proposal_id}")
    expected_dir = Path(expected_paths[0]).parent.as_posix()
    if repository_relative(package_dir, root, "package-dir") != expected_dir:
        raise VerificationError(
            f"package-dir does not match registered proposal {proposal_id}"
        )

    require_date(receipt["preparedAt"], "preparedAt")

    status = receipt["receiptStatus"]
    if status not in {"CANDIDATE", "ACCEPTED"}:
        raise VerificationError("receiptStatus must be CANDIDATE or ACCEPTED")
    expected_receipt_name = (
        "acceptance-receipt.candidate.json"
        if status == "CANDIDATE"
        else "acceptance-receipt.json"
    )
    if receipt_path.parent != package_dir or receipt_path.name != expected_receipt_name:
        raise VerificationError(
            f"{status} receipt must be {expected_dir}/{expected_receipt_name}"
        )

    bridge = require_closed_object(
        receipt["governanceBridge"],
        {"proposalId", "requiredStatus", "observedStatus"},
        "governanceBridge",
    )
    if bridge != {
        "proposalId": "NDP-0002",
        "requiredStatus": "ACCEPTED",
        "observedStatus": "DRAFT" if status == "CANDIDATE" else "ACCEPTED",
    }:
        raise VerificationError("governanceBridge does not match receipt status")

    normative = require_closed_object(
        receipt["normativePackage"],
        {
            "digestAlgorithm",
            "digestDomain",
            "pathBase",
            "files",
            "digest",
        },
        "normativePackage",
    )
    if normative["digestAlgorithm"] != "SHA-256":
        raise VerificationError("normativePackage.digestAlgorithm must be SHA-256")
    if normative["digestDomain"] != PACKAGE_DOMAIN_LABEL:
        raise VerificationError("normativePackage.digestDomain is invalid")
    if normative["pathBase"] != "repository-root":
        raise VerificationError("normativePackage.pathBase must be repository-root")
    package_digest = require_string(normative["digest"], "normativePackage.digest")
    if HEX_256.fullmatch(package_digest) is None:
        raise VerificationError("normativePackage.digest must be lowercase SHA-256")

    files = normative["files"]
    if not isinstance(files, list):
        raise VerificationError("normativePackage.files must be a list")
    paths: list[str] = []
    for index, entry_value in enumerate(files):
        entry = require_closed_object(
            entry_value, {"path", "sha256"}, f"normativePackage.files[{index}]"
        )
        path = require_string(entry["path"], f"normativePackage.files[{index}].path")
        sha256 = require_string(
            entry["sha256"], f"normativePackage.files[{index}].sha256"
        )
        if HEX_256.fullmatch(sha256) is None:
            raise VerificationError(
                f"normativePackage.files[{index}].sha256 must be lowercase SHA-256"
            )
        paths.append(path)
    canonical_paths = tuple(sorted(paths, key=lambda item: item.encode("utf-8")))
    if tuple(paths) != canonical_paths:
        raise VerificationError("normativePackage.files are not in unsigned UTF-8 order")
    if canonical_paths != expected_paths:
        raise VerificationError(
            f"normativePackage.files do not match registered paths for {proposal_id}"
        )

    baseline = require_closed_object(
        receipt["reviewBaseline"],
        {
            "mainCommit",
            "designBaselineCommit",
            "h0ImplementationCommit",
            "h0DocumentationCommit",
            "p1SourceLockCommit",
        },
        "reviewBaseline",
    )
    for key, value in baseline.items():
        require_commit(value, f"reviewBaseline.{key}")

    decision = require_closed_object(
        receipt["decision"],
        {"status", "acceptedBy", "acceptedAt", "decisionReference"},
        "decision",
    )
    authorization = require_closed_object(
        receipt["authorization"],
        {
            "gateB",
            "implementationAuthorized",
            "localDisposableTestingAuthorized",
            "gateCRequiredBeforeShadow",
            "gateCRequiredBeforeEnabled",
        },
        "authorization",
    )
    if authorization["gateCRequiredBeforeShadow"] is not True:
        raise VerificationError(
            "authorization.gateCRequiredBeforeShadow must be true"
        )
    if authorization["gateCRequiredBeforeEnabled"] is not True:
        raise VerificationError(
            "authorization.gateCRequiredBeforeEnabled must be true"
        )

    if status == "CANDIDATE":
        if receipt["authority"] is not False:
            raise VerificationError("candidate authority must be false")
        if decision != {
            "status": "PENDING",
            "acceptedBy": None,
            "acceptedAt": None,
            "decisionReference": None,
        }:
            raise VerificationError("candidate decision must remain pending")
        if authorization["gateB"] != "PENDING":
            raise VerificationError("candidate Gate B must remain pending")
        if authorization["implementationAuthorized"] is not False:
            raise VerificationError(
                "candidate must not authorize H1 through H6 implementation"
            )
        if authorization["localDisposableTestingAuthorized"] is not False:
            raise VerificationError(
                "candidate must not authorize local disposable testing"
            )
    else:
        if receipt["authority"] is not True:
            raise VerificationError("accepted receipt authority must be true")
        if decision["status"] != "ACCEPTED":
            raise VerificationError("accepted receipt decision must be ACCEPTED")
        require_string(decision["acceptedBy"], "decision.acceptedBy")
        require_date(decision["acceptedAt"], "decision.acceptedAt")
        require_string(decision["decisionReference"], "decision.decisionReference")
        if authorization["gateB"] != "PASS":
            raise VerificationError("accepted receipt Gate B must be PASS")
        if authorization["implementationAuthorized"] is not True:
            raise VerificationError(
                "accepted Gate B must authorize H1 through H6 implementation"
            )
        if authorization["localDisposableTestingAuthorized"] is not True:
            raise VerificationError(
                "accepted Gate B must authorize local disposable testing"
            )

    return proposal_id, expected_paths, package_digest


def verify_repository_status(receipt_status: str, root: Path) -> None:
    expected_status = "Draft" if receipt_status == "CANDIDATE" else "Accepted"
    checks = {
        "docs/proposals/0002-register-ndip-governance.md": (
            f"- Status: {expected_status}"
        ),
        "docs/ndip/NDIP-1/02-NDIP-1-Pulsar-Native-Delivery.md": (
            f"- Status: {expected_status}"
        ),
        "docs/ndip/NDIP-1/03-实施计划.md": f"- 当前状态：`{expected_status}`",
        "docs/ndip/NDIP-1/04-代码级目标设计.md": f"- 提案状态：`{expected_status}`",
    }
    for path_text, marker in checks.items():
        try:
            text = (root / path_text).read_text(encoding="utf-8", errors="strict")
        except (OSError, UnicodeDecodeError) as exc:
            raise VerificationError(
                f"cannot verify proposal status in {path_text}: {exc}"
            ) from exc
        if marker not in text:
            raise VerificationError(
                f"proposal status marker is missing from {path_text}: {marker}"
            )


def calculate_package(
    paths: tuple[str, ...], root: Path
) -> tuple[str, list[tuple[str, str]]]:
    material = bytearray(PACKAGE_DOMAIN)
    file_digests: list[tuple[str, str]] = []
    for path_text in paths:
        path = root / path_text
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise VerificationError(f"cannot read normative file {path_text}: {exc}") from exc
        if data.startswith(b"\xef\xbb\xbf"):
            raise VerificationError(f"normative file has UTF-8 BOM: {path_text}")
        if b"\r" in data:
            raise VerificationError(f"normative file must use LF: {path_text}")
        if not data.endswith(b"\n") or data.endswith(b"\n\n"):
            raise VerificationError(
                f"normative file must end with exactly one LF: {path_text}"
            )
        try:
            data.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise VerificationError(
                f"normative file is not strict UTF-8: {path_text}: {exc}"
            ) from exc
        path_bytes = path_text.encode("utf-8")
        file_digest_bytes = hashlib.sha256(data).digest()
        material.extend(len(path_bytes).to_bytes(4, byteorder="big", signed=False))
        material.extend(path_bytes)
        material.extend(file_digest_bytes)
        file_digests.append((path_text, file_digest_bytes.hex()))
    return hashlib.sha256(material).hexdigest(), file_digests


def verify_file_digests(
    receipt: dict[str, Any], actual: list[tuple[str, str]]
) -> None:
    expected_files = receipt["normativePackage"]["files"]
    for index, (path, digest) in enumerate(actual):
        expected = expected_files[index]
        if expected["path"] != path or expected["sha256"] != digest:
            raise VerificationError(
                f"normative file digest mismatch: {path}: "
                f"expected={expected['sha256']} actual={digest}"
            )


def verify_required_status(receipt_status: str, require_accepted: bool) -> None:
    if require_accepted and receipt_status != "ACCEPTED":
        raise VerificationError(
            "receipt is only a candidate; explicit Accepted authority is required"
        )


def main() -> int:
    args = parse_args()
    root = repository_root()
    try:
        package_dir = args.package_dir.resolve()
        receipt_path = args.receipt.resolve()
        receipt = load_receipt(receipt_path)
        proposal_id, paths, expected_package_digest = validate_receipt_shape(
            receipt, package_dir, receipt_path, root
        )
        verify_repository_status(receipt["receiptStatus"], root)
        actual_package_digest, file_digests = calculate_package(paths, root)
        verify_file_digests(receipt, file_digests)
        if actual_package_digest != expected_package_digest:
            raise VerificationError(
                "normative package digest mismatch: "
                f"expected={expected_package_digest} actual={actual_package_digest}"
            )
        verify_required_status(receipt["receiptStatus"], args.require_accepted)
    except VerificationError as exc:
        print(f"NDIP package verification failed: {exc}", file=sys.stderr)
        return 1

    print(f"proposal={proposal_id}")
    print(f"receipt_status={receipt['receiptStatus']}")
    print(f"authority={str(receipt['authority']).lower()}")
    for path, digest in file_digests:
        print(f"file_sha256 {digest} {path}")
    print(f"package_sha256={actual_package_digest}")
    if receipt["receiptStatus"] == "CANDIDATE":
        print("gate_b=PENDING (candidate integrity only; no acceptance authority)")
        print("implementation=BLOCKED")
        print("local_disposable_testing=BLOCKED")
    else:
        print("gate_b=PASS")
        print("implementation=AUTHORIZED")
        print("local_disposable_testing=AUTHORIZED_WITH_EXACT_ATTESTATION")
        print("gate_c=PENDING_DEPLOYMENT")
        print("shadow=BLOCKED_BY_GATE_C")
        print("enabled=BLOCKED_BY_GATE_C_AND_SHADOW_REQUIREMENTS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
