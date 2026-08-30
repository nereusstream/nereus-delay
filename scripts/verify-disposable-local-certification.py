#!/usr/bin/env python3
"""Verify a source-bound, non-authoritative disposable local receipt."""

from __future__ import annotations

import argparse
import datetime as datetime_module
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


RECEIPT_SCHEMA = "nereus-delay.disposable-local-certification-receipt"
ATTESTATION_SCHEMA = "nereus-delay.disposable-local-attestation-r1"
EXPECTED_P1_COMMIT = "0a2536484cd3932801a98dc88ff112b2df88a1c7"
HEX_256 = re.compile(r"[0-9a-f]{64}\Z")
COMMIT = re.compile(r"[0-9a-f]{40}\Z")
NATIVE_CELLS = (
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
)
REQUIRED_CELLS = NATIVE_CELLS + (
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
GIT_COMMIT_FIELDS = {
    "delayCommit",
    "p1Commit",
    "p1ExpectedCommit",
    "oxiaCommit",
}


class VerificationError(ValueError):
    """Raised when a receipt or its bound evidence is invalid."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", required=True, type=Path)
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
        value = json.loads(
            raw.decode("utf-8", errors="strict"), object_pairs_hook=reject_duplicate_keys
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VerificationError(f"{location} is not strict UTF-8 JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise VerificationError(f"{location} root must be an object")
    return value


def closed_object(value: Any, keys: set[str], location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise VerificationError(f"{location} must be an object")
    actual = set(value)
    if actual != keys:
        raise VerificationError(
            f"{location} keys are not closed: missing={sorted(keys - actual)}, "
            f"extra={sorted(actual - keys)}"
        )
    return value


def non_empty_string(value: Any, location: str) -> str:
    if not isinstance(value, str) or not value:
        raise VerificationError(f"{location} must be a non-empty string")
    return value


def absolute_path(value: Any, location: str) -> Path:
    path = Path(non_empty_string(value, location))
    if not path.is_absolute():
        raise VerificationError(f"{location} must be absolute")
    return path


def digest(value: Any, location: str) -> str:
    result = non_empty_string(value, location)
    if HEX_256.fullmatch(result) is None:
        raise VerificationError(f"{location} must be lowercase SHA-256")
    return result


def commit(value: Any, location: str) -> str:
    result = non_empty_string(value, location)
    if COMMIT.fullmatch(result) is None:
        raise VerificationError(f"{location} must be a lowercase 40-hex commit")
    return result


def sha256_file(path: Path, location: str) -> str:
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise VerificationError(f"cannot read {location}: {exc}") from exc
    return hashlib.sha256(data).hexdigest()


def git_value(checkout: Path, *arguments: str, location: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(checkout), *arguments],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise VerificationError(f"cannot inspect {location}: {exc}") from exc
    return result.stdout.strip()


def package_digest(root: Path) -> tuple[str, list[tuple[str, str]]]:
    paths = (
        "docs/ndip/NDIP-1/01-调查与决策记录.md",
        "docs/ndip/NDIP-1/02-NDIP-1-Pulsar-Native-Delivery.md",
        "docs/ndip/NDIP-1/03-实施计划.md",
        "docs/ndip/NDIP-1/04-代码级目标设计.md",
    )
    material = bytearray(b"nereus-delay-ndip-package\0")
    file_digests: list[tuple[str, str]] = []
    for path_text in paths:
        path = root / path_text
        try:
            data = path.read_bytes()
            data.decode("utf-8", errors="strict")
        except (OSError, UnicodeDecodeError) as exc:
            raise VerificationError(f"cannot read normative file {path_text}: {exc}") from exc
        if data.startswith(b"\xef\xbb\xbf") or b"\r" in data:
            raise VerificationError(f"normative file encoding is not canonical: {path_text}")
        if not data.endswith(b"\n") or data.endswith(b"\n\n"):
            raise VerificationError(f"normative file must end with one LF: {path_text}")
        path_bytes = path_text.encode("utf-8")
        file_digest = hashlib.sha256(data).digest()
        material.extend(len(path_bytes).to_bytes(4, byteorder="big"))
        material.extend(path_bytes)
        material.extend(file_digest)
        file_digests.append((path_text, file_digest.hex()))
    return hashlib.sha256(material).hexdigest(), file_digests


def parse_timestamp(value: Any, location: str) -> datetime_module.datetime:
    text = non_empty_string(value, location)
    try:
        parsed = datetime_module.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise VerificationError(f"{location} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise VerificationError(f"{location} must include a timezone")
    return parsed


def verify_source(receipt: dict[str, Any], root: Path) -> None:
    expected_keys = {
        "delayCheckout",
        "delayCommit",
        "p1Checkout",
        "p1Commit",
        "p1ExpectedCommit",
        "p1Branch",
        "oxiaCheckout",
        "oxiaCommit",
        "acceptedPackageDir",
        "acceptedReceiptPath",
        "acceptedPackageSha256",
        "p1DistributionPath",
        "p1DistributionSha256",
        "p1ClientArtifacts",
        "composeFiles",
        "composeConfigPath",
        "composeConfigSha256",
        "attestationPath",
        "attestationSha256",
    }
    if receipt["receiptSchemaGeneration"] >= 3:
        expected_keys.update(
            {
                "oxiaCliPath",
                "oxiaCliSha256",
                "oxiaCliBuildInfoPath",
                "oxiaCliBuildInfoSha256",
            }
        )
    source = closed_object(
        receipt["source"],
        expected_keys,
        "source",
    )
    for key in GIT_COMMIT_FIELDS:
        commit(source[key], f"source.{key}")
    if source["p1ExpectedCommit"] != EXPECTED_P1_COMMIT:
        raise VerificationError("source.p1ExpectedCommit is not the locked P1 commit")
    for key in (
        "delayCheckout",
        "p1Checkout",
        "oxiaCheckout",
        "acceptedPackageDir",
        "acceptedReceiptPath",
        "p1DistributionPath",
        "composeConfigPath",
        "attestationPath",
    ):
        absolute_path(source[key], f"source.{key}")
    if receipt["receiptSchemaGeneration"] >= 3:
        oxia_cli_path = absolute_path(source["oxiaCliPath"], "source.oxiaCliPath")
        oxia_cli_build_info_path = absolute_path(
            source["oxiaCliBuildInfoPath"], "source.oxiaCliBuildInfoPath"
        )
        if sha256_file(oxia_cli_path, "Oxia CLI") != digest(
            source["oxiaCliSha256"], "source.oxiaCliSha256"
        ):
            raise VerificationError("Oxia CLI SHA-256 mismatch")
        if sha256_file(oxia_cli_build_info_path, "Oxia CLI build info") != digest(
            source["oxiaCliBuildInfoSha256"], "source.oxiaCliBuildInfoSha256"
        ):
            raise VerificationError("Oxia CLI build-info SHA-256 mismatch")
        try:
            build_info = oxia_cli_build_info_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise VerificationError(f"cannot read Oxia CLI build info: {exc}") from exc
        if (
            f"vcs.revision={source['oxiaCommit']}" not in build_info
            or "vcs.modified=false" not in build_info
        ):
            raise VerificationError("Oxia CLI build info is not clean or source-locked")
    delay_checkout = absolute_path(source["delayCheckout"], "source.delayCheckout")
    p1_checkout = absolute_path(source["p1Checkout"], "source.p1Checkout")
    oxia_checkout = absolute_path(source["oxiaCheckout"], "source.oxiaCheckout")
    if delay_checkout != root:
        raise VerificationError("source.delayCheckout must be the verifier repository")
    if git_value(delay_checkout, "rev-parse", "HEAD", location="Delay HEAD") != source["delayCommit"]:
        raise VerificationError("Delay HEAD does not match source.delayCommit")
    if git_value(p1_checkout, "rev-parse", "HEAD", location="P1 HEAD") != source["p1Commit"]:
        raise VerificationError("P1 HEAD does not match source.p1Commit")
    if git_value(oxia_checkout, "rev-parse", "HEAD", location="Oxia HEAD") != source["oxiaCommit"]:
        raise VerificationError("Oxia HEAD does not match source.oxiaCommit")
    for checkout, location in (
        (delay_checkout, "Delay checkout"),
        (p1_checkout, "P1 checkout"),
        (oxia_checkout, "Oxia checkout"),
    ):
        if git_value(checkout, "status", "--porcelain", location=location) != "":
            raise VerificationError(f"{location} is not clean")
    if sha256_file(Path(source["p1DistributionPath"]), "P1 distribution") != source["p1DistributionSha256"]:
        raise VerificationError("P1 distribution SHA-256 mismatch")
    digest(source["p1DistributionSha256"], "source.p1DistributionSha256")
    digest(source["acceptedPackageSha256"], "source.acceptedPackageSha256")
    digest(source["composeConfigSha256"], "source.composeConfigSha256")
    digest(source["attestationSha256"], "source.attestationSha256")
    client_artifacts = source["p1ClientArtifacts"]
    if not isinstance(client_artifacts, list) or not client_artifacts:
        raise VerificationError("source.p1ClientArtifacts must be a non-empty list")
    for index, value in enumerate(client_artifacts):
        entry = closed_object(value, {"path", "sha256"}, f"source.p1ClientArtifacts[{index}]")
        path = absolute_path(entry["path"], f"source.p1ClientArtifacts[{index}].path")
        digest(entry["sha256"], f"source.p1ClientArtifacts[{index}].sha256")
        if sha256_file(path, f"P1 client artifact {path}") != entry["sha256"]:
            raise VerificationError(f"P1 client artifact SHA-256 mismatch: {path}")
    compose_files = source["composeFiles"]
    if not isinstance(compose_files, list) or not compose_files:
        raise VerificationError("source.composeFiles must be a non-empty list")
    for index, value in enumerate(compose_files):
        entry = closed_object(value, {"path", "sha256"}, f"source.composeFiles[{index}]")
        path = absolute_path(entry["path"], f"source.composeFiles[{index}].path")
        digest(entry["sha256"], f"source.composeFiles[{index}].sha256")
        if sha256_file(path, f"compose file {path}") != entry["sha256"]:
            raise VerificationError(f"compose file SHA-256 mismatch: {path}")
    compose_config = Path(source["composeConfigPath"])
    if sha256_file(compose_config, "compose config snapshot") != source["composeConfigSha256"]:
        raise VerificationError("compose config snapshot SHA-256 mismatch")
    attestation_path = Path(source["attestationPath"])
    if sha256_file(attestation_path, "attestation") != source["attestationSha256"]:
        raise VerificationError("attestation SHA-256 mismatch")
    attestation = load_json(attestation_path, "attestation")
    verify_attestation(attestation, source)


def verify_attestation(attestation: dict[str, Any], source: dict[str, Any]) -> None:
    expected_keys = {
        "schema",
        "classification",
        "authority",
        "gateC",
        "shadow",
        "enabled",
        "delayCommit",
        "p1Commit",
        "oxiaCommit",
        "acceptedPackageSha256",
        "composeProject",
        "resourcePrefix",
        "resources",
        "createdAt",
    }
    closed_object(attestation, expected_keys, "attestation")
    if attestation["schema"] != ATTESTATION_SCHEMA:
        raise VerificationError("attestation schema is not the disposable-local schema")
    if attestation["classification"] != "DISPOSABLE_LOCAL":
        raise VerificationError("attestation classification is not DISPOSABLE_LOCAL")
    for key in ("authority", "gateC", "shadow", "enabled"):
        if attestation[key] is not False:
            raise VerificationError(f"attestation.{key} must be false")
    for key in ("delayCommit", "p1Commit", "oxiaCommit"):
        commit(attestation[key], f"attestation.{key}")
        if attestation[key] != source[key]:
            raise VerificationError(f"attestation.{key} is not source-bound")
    digest(attestation["acceptedPackageSha256"], "attestation.acceptedPackageSha256")
    if attestation["acceptedPackageSha256"] != source["acceptedPackageSha256"]:
        raise VerificationError("attestation package digest is not source-bound")
    non_empty_string(attestation["composeProject"], "attestation.composeProject")
    non_empty_string(attestation["resourcePrefix"], "attestation.resourcePrefix")
    resources = closed_object(
        attestation["resources"], {"containers", "volumes", "networks", "topics"}, "attestation.resources"
    )
    for key in resources:
        if not isinstance(resources[key], list) or any(not isinstance(item, str) or not item for item in resources[key]):
            raise VerificationError(f"attestation.resources.{key} must be a list of strings")
    parse_timestamp(attestation["createdAt"], "attestation.createdAt")


def verify_accepted_package(receipt: dict[str, Any], root: Path) -> None:
    package = closed_object(receipt["acceptedPackage"], {"packageSha256", "files"}, "acceptedPackage")
    expected_digest = digest(package["packageSha256"], "acceptedPackage.packageSha256")
    actual_digest, actual_files = package_digest(root)
    if actual_digest != expected_digest:
        raise VerificationError("accepted package digest mismatch")
    files = package["files"]
    if not isinstance(files, list) or len(files) != len(actual_files):
        raise VerificationError("acceptedPackage.files has the wrong length")
    for index, (path, actual) in enumerate(actual_files):
        entry = closed_object(files[index], {"path", "sha256"}, f"acceptedPackage.files[{index}]")
        if entry["path"] != path or entry["sha256"] != actual:
            raise VerificationError(f"accepted package file digest mismatch: {path}")
    if receipt["source"]["acceptedPackageSha256"] != actual_digest:
        raise VerificationError("source.acceptedPackageSha256 does not match the accepted package")


def verify_environment(receipt: dict[str, Any]) -> None:
    environment = closed_object(
        receipt["environment"],
        {
            "attestation",
            "composeProject",
            "resourcePrefix",
            "p1Image",
            "minioImage",
            "minioRepoDigest",
            "ports",
            "topics",
            "workers",
            "resources",
            "oxiaBootstrap",
        },
        "environment",
    )
    if environment["attestation"] != "DISPOSABLE_LOCAL":
        raise VerificationError("environment.attestation is not DISPOSABLE_LOCAL")
    for key in ("composeProject", "resourcePrefix", "p1Image", "minioImage"):
        non_empty_string(environment[key], f"environment.{key}")
    digest(environment["minioRepoDigest"], "environment.minioRepoDigest")
    ports = closed_object(
        environment["ports"],
        {"broker1", "web1", "broker2", "web2", "oxiaData1", "oxiaData2", "oxiaData3", "minio"},
        "environment.ports",
    )
    for key, value in ports.items():
        if type(value) is not int or not 1 <= value <= 65535:
            raise VerificationError(f"environment.ports.{key} must be a TCP port")
    topics = closed_object(environment["topics"], {"command", "evidence", "business"}, "environment.topics")
    for key in topics:
        non_empty_string(topics[key], f"environment.topics.{key}")
    if environment["workers"] != ["worker-a", "worker-b"]:
        raise VerificationError("environment.workers must record both worker identities")
    resources = closed_object(
        environment["resources"], {"containers", "volumes", "networks", "topics"}, "environment.resources"
    )
    for key in resources:
        if not isinstance(resources[key], list) or any(not isinstance(item, str) or not item for item in resources[key]):
            raise VerificationError(f"environment.resources.{key} must be a list of strings")
    bootstrap = closed_object(
        environment["oxiaBootstrap"],
        {"status", "command", "logPath", "resultSha256", "namespace", "dataServers"},
        "environment.oxiaBootstrap",
    )
    if bootstrap["status"] != "PASS":
        raise VerificationError("environment.oxiaBootstrap.status must be PASS")
    non_empty_string(bootstrap["command"], "environment.oxiaBootstrap.command")
    bootstrap_log = absolute_path(bootstrap["logPath"], "environment.oxiaBootstrap.logPath")
    if not bootstrap_log.is_file():
        raise VerificationError("environment.oxiaBootstrap.logPath is missing")
    if sha256_file(bootstrap_log, "environment.oxiaBootstrap.logPath") != digest(
        bootstrap["resultSha256"], "environment.oxiaBootstrap.resultSha256"
    ):
        raise VerificationError("environment.oxiaBootstrap.resultSha256 does not match its log")
    namespace = closed_object(
        bootstrap["namespace"],
        {"name", "initialShards", "replicationFactor", "notifications"},
        "environment.oxiaBootstrap.namespace",
    )
    if namespace != {
        "name": "default",
        "initialShards": 1,
        "replicationFactor": 3,
        "notifications": True,
    }:
        raise VerificationError("environment.oxiaBootstrap.namespace is not the expected disposable namespace")
    data_servers = bootstrap["dataServers"]
    if not isinstance(data_servers, list) or len(data_servers) != 3:
        raise VerificationError("environment.oxiaBootstrap.dataServers must contain three servers")
    expected_ports = [ports["oxiaData1"], ports["oxiaData2"], ports["oxiaData3"]]
    for index, value in enumerate(data_servers, 1):
        server = closed_object(
            value,
            {"name", "public", "internal", "state"},
            f"environment.oxiaBootstrap.dataServers[{index - 1}]",
        )
        expected = {
            "name": f"data-server-{index}",
            "public": f"127.0.0.1:{expected_ports[index - 1]}",
            "internal": f"data-server-{index}:6649",
            "state": "DATA_SERVER_STATE_RUNNING",
        }
        if server != expected:
            raise VerificationError(
                f"environment.oxiaBootstrap.dataServers[{index - 1}] is not the exact registered server"
            )


def verify_matrix(receipt: dict[str, Any]) -> None:
    matrix = receipt["matrix"]
    if not isinstance(matrix, list) or len(matrix) != len(REQUIRED_CELLS):
        raise VerificationError("matrix must contain exactly the registered required cells")
    observed_ids: list[str] = []
    for index, value in enumerate(matrix):
        expected_keys = {
            "id",
            "category",
            "expected",
            "status",
            "skipped",
            "command",
            "logPath",
            "evidencePath",
            "resultSha256",
            "reason",
        }
        if receipt["receiptSchemaGeneration"] >= 3:
            expected_keys.add("logSha256")
        entry = closed_object(
            value,
            expected_keys,
            f"matrix[{index}]",
        )
        cell_id = non_empty_string(entry["id"], f"matrix[{index}].id")
        observed_ids.append(cell_id)
        if cell_id != REQUIRED_CELLS[index]:
            raise VerificationError(f"matrix order or cell id mismatch at index {index}: {cell_id}")
        non_empty_string(entry["category"], f"matrix[{index}].category")
        non_empty_string(entry["expected"], f"matrix[{index}].expected")
        if entry["status"] not in {"EXECUTED_PASS", "EXECUTED_FAIL", "NOT_COVERED"}:
            raise VerificationError(f"invalid matrix status: {cell_id}")
        if type(entry["skipped"]) is not bool or entry["skipped"]:
            raise VerificationError(f"matrix cell has skipped evidence: {cell_id}")
        non_empty_string(entry["command"], f"matrix[{index}].command")
        log_path = absolute_path(entry["logPath"], f"matrix[{index}].logPath")
        evidence_path = absolute_path(entry["evidencePath"], f"matrix[{index}].evidencePath")
        if not log_path.is_file() or not evidence_path.is_file():
            raise VerificationError(f"matrix evidence path is missing: {cell_id}")
        if receipt["receiptSchemaGeneration"] >= 3 and sha256_file(
            log_path, f"matrix log {cell_id}"
        ) != digest(entry["logSha256"], f"matrix[{index}].logSha256"):
            raise VerificationError(f"matrix log SHA-256 mismatch: {cell_id}")
        if sha256_file(evidence_path, f"matrix evidence {cell_id}") != digest(entry["resultSha256"], f"matrix[{index}].resultSha256"):
            raise VerificationError(f"matrix evidence SHA-256 mismatch: {cell_id}")
        if entry["status"] == "NOT_COVERED":
            non_empty_string(entry["reason"], f"matrix[{index}].reason")


def verify_cleanup(receipt: dict[str, Any]) -> None:
    cleanup = closed_object(
        receipt["cleanup"],
        {
            "status",
            "composeProjectAbsent",
            "containersRemaining",
            "volumesRemaining",
            "networksRemaining",
            "imagesRemaining",
            "topicsRemaining",
            "processesRemaining",
            "temporaryCredentialsRemaining",
            "method",
        },
        "cleanup",
    )
    if cleanup["status"] not in {"PASS", "FAIL"}:
        raise VerificationError("cleanup.status must be PASS or FAIL")
    if cleanup["composeProjectAbsent"] is not True:
        raise VerificationError("compose project was not proven absent")
    for key in (
        "containersRemaining",
        "volumesRemaining",
        "networksRemaining",
        "imagesRemaining",
        "topicsRemaining",
        "processesRemaining",
        "temporaryCredentialsRemaining",
    ):
        value = cleanup[key]
        if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
            raise VerificationError(f"cleanup.{key} must be a list of strings")
        if value:
            raise VerificationError(f"cleanup.{key} is not empty")
    non_empty_string(cleanup["method"], "cleanup.method")


def verify_receipt(receipt: dict[str, Any], receipt_path: Path, root: Path) -> None:
    closed_object(
        receipt,
        {
            "receiptSchema",
            "receiptSchemaGeneration",
            "classification",
            "status",
            "authority",
            "gateC",
            "shadow",
            "enabled",
            "startedAt",
            "finishedAt",
            "source",
            "acceptedPackage",
            "environment",
            "supportingChecks",
            "matrix",
            "cleanup",
            "boundaries",
            "report",
        },
        "receipt",
    )
    if receipt["receiptSchema"] != RECEIPT_SCHEMA or receipt["receiptSchemaGeneration"] not in {1, 2, 3}:
        raise VerificationError("unknown disposable-local receipt schema")
    if receipt["classification"] != "DISPOSABLE_LOCAL":
        raise VerificationError("receipt classification is not DISPOSABLE_LOCAL")
    if receipt["status"] not in {"PASS", "FAIL", "BLOCKED"}:
        raise VerificationError("receipt status is invalid")
    for key in ("authority", "gateC", "shadow", "enabled"):
        if receipt[key] is not False:
            raise VerificationError(f"receipt.{key} must be false")
    started = parse_timestamp(receipt["startedAt"], "receipt.startedAt")
    finished = parse_timestamp(receipt["finishedAt"], "receipt.finishedAt")
    if finished < started:
        raise VerificationError("receipt finishedAt precedes startedAt")
    report = closed_object(receipt["report"], {"kind", "path"}, "report")
    if report["kind"] != "DISPOSABLE_LOCAL_CERTIFICATION_RECEIPT":
        raise VerificationError("report kind is not local disposable certification")
    if Path(report["path"]).resolve() != receipt_path.resolve():
        raise VerificationError("report path does not point to this receipt")
    if not isinstance(receipt["boundaries"], list) or not receipt["boundaries"]:
        raise VerificationError("receipt.boundaries must be a non-empty list")
    if any(not isinstance(item, str) or not item for item in receipt["boundaries"]):
        raise VerificationError("receipt.boundaries must contain strings")
    verify_source(receipt, root)
    verify_accepted_package(receipt, root)
    verify_environment(receipt)
    verify_supporting_checks(receipt)
    verify_matrix(receipt)
    verify_cleanup(receipt)
    statuses = [entry["status"] for entry in receipt["matrix"]]
    if receipt["status"] == "PASS" and (
        any(status != "EXECUTED_PASS" for status in statuses)
        or any(entry["status"] != "PASS" for entry in receipt["supportingChecks"])
        or receipt["cleanup"]["status"] != "PASS"
    ):
        raise VerificationError("PASS receipt contains a non-pass cell or cleanup result")
    if receipt["status"] == "BLOCKED" and all(status == "EXECUTED_PASS" for status in statuses):
        raise VerificationError("BLOCKED receipt has no blocked or failed cell")


def verify_supporting_checks(receipt: dict[str, Any]) -> None:
    checks = receipt["supportingChecks"]
    expected_ids = ["p1.compileRealPulsar", "p1.h0"]
    if receipt["receiptSchemaGeneration"] >= 2:
        expected_ids.append("p1.nativeCoordinator")
    if not isinstance(checks, list) or len(checks) != len(expected_ids):
        raise VerificationError(
            f"supportingChecks must contain the {len(expected_ids)} schema-bound P1 checks"
        )
    for index, value in enumerate(checks):
        expected_keys = {"id", "command", "status", "logPath"}
        if receipt["receiptSchemaGeneration"] >= 3:
            expected_keys.add("logSha256")
        entry = closed_object(
            value,
            expected_keys,
            f"supportingChecks[{index}]",
        )
        if entry["id"] != expected_ids[index]:
            raise VerificationError(f"supporting check id mismatch at index {index}")
        non_empty_string(entry["command"], f"supportingChecks[{index}].command")
        if entry["status"] not in {"PASS", "FAIL"}:
            raise VerificationError(f"invalid supporting check status: {entry['id']}")
        log_path = absolute_path(entry["logPath"], f"supportingChecks[{index}].logPath")
        if not log_path.is_file():
            raise VerificationError(f"supporting check log is missing: {entry['id']}")
        if receipt["receiptSchemaGeneration"] >= 3 and sha256_file(
            log_path, f"supporting check log {entry['id']}"
        ) != digest(entry["logSha256"], f"supportingChecks[{index}].logSha256"):
            raise VerificationError(f"supporting check log SHA-256 mismatch: {entry['id']}")


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parent.parent
    receipt_path = args.receipt.resolve()
    try:
        receipt = load_json(receipt_path, "receipt")
        verify_receipt(receipt, receipt_path, root)
    except VerificationError as exc:
        print(f"disposable-local certification verification failed: {exc}", file=sys.stderr)
        return 1
    print(f"receipt_status={receipt['status']}")
    print("classification=DISPOSABLE_LOCAL")
    print("authority=false")
    print("gateC=false")
    print("shadow=false")
    print("enabled=false")
    print(f"matrix_cells={len(receipt['matrix'])}")
    print(
        "matrix_executed_pass="
        + str(sum(entry["status"] == "EXECUTED_PASS" for entry in receipt["matrix"]))
    )
    print(
        "matrix_not_covered="
        + str(sum(entry["status"] == "NOT_COVERED" for entry in receipt["matrix"]))
    )
    print("cleanup=verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
