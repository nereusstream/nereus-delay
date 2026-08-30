#!/usr/bin/env python3
"""Independently validate one closed NDIP-1 persistent staging certification chain."""

from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Any


ENVELOPE_SCHEMA = "nereus-delay.persistent-staging-evidence"
SIGNATURE_DOMAIN = b"nereus-delay-persistent-staging-evidence\0"
MANIFEST_DIGEST_DOMAIN = b"nereus-delay-data-reset-manifest\0"
MANIFEST_SIGNATURE_DOMAIN = b"nereus-delay-data-reset-manifest-signature\0"
HEX_32 = re.compile(r"^[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
RESOURCE_KINDS = {
    "COMMAND_TOPIC",
    "SYSTEM_TOPIC",
    "ROCKSDB_STORE",
    "CHECKPOINT_CATALOG",
    "PROFILE_OXIA_STATE",
    "RUNTIME_POLICY_STATE",
    "PAYLOAD_RESERVATION_OBJECT_STATE",
    "PULSAR_ATTEMPT_JOURNAL",
    "EVIDENCE_TOPIC_CURSOR",
    "QUERY_DEDUPE_STATE",
    "OBLIGATION_INDEX",
    "RESOURCE_INCARNATION_REGISTRY",
    "WORKER_REGISTRY",
}


class VerificationError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def canonical_p1_source_lock_digest(source_lock_commit: str) -> str:
    require(COMMIT.fullmatch(source_lock_commit) is not None, "expected P1 source lock is not a commit")
    return hashlib.sha256(f"nereus/delay-resource-guard@{source_lock_commit}".encode("ascii")).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def read_bytes(path: Path, label: str) -> bytes:
    require(path.is_file() and not path.is_symlink(), f"{label} is not a regular non-symlink file: {path}")
    return path.read_bytes()


def read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(read_bytes(path, label).decode("utf-8"))
    except (UnicodeDecodeError, ValueError, OSError) as exc:
        raise VerificationError(f"{label} is not a JSON object: {path}: {exc}") from exc
    require(isinstance(value, dict), f"{label} is not a JSON object: {path}")
    return value


def within_run(path_value: str, run_dir: Path, label: str) -> Path:
    path = Path(path_value).expanduser().resolve(strict=True)
    require(path == run_dir or run_dir in path.parents, f"{label} escapes the immutable run: {path}")
    require(path.is_file() and not path.is_symlink(), f"{label} is not a regular file: {path}")
    return path


def decode_base64(value: Any, label: str) -> bytes:
    require(isinstance(value, str) and value, f"{label} is missing")
    try:
        return base64.b64decode(value, validate=True)
    except ValueError as exc:
        raise VerificationError(f"{label} is not canonical base64") from exc


@dataclass(frozen=True)
class VerifiedEnvelope:
    path: Path
    envelope_sha256: str
    payload_bytes: bytes
    payload: dict[str, Any]
    key_generation: int


def verify_ed25519(public_key: bytes, message: bytes, signature: bytes) -> None:
    with tempfile.TemporaryDirectory(prefix="ndip1-envelope-verify-") as directory:
        root = Path(directory)
        key_path = root / "public.der"
        message_path = root / "message.bin"
        signature_path = root / "signature.bin"
        key_path.write_bytes(public_key)
        message_path.write_bytes(message)
        signature_path.write_bytes(signature)
        result = subprocess.run(
            [
                "openssl",
                "pkeyutl",
                "-verify",
                "-pubin",
                "-inkey",
                str(key_path),
                "-keyform",
                "DER",
                "-rawin",
                "-in",
                str(message_path),
                "-sigfile",
                str(signature_path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    require(result.returncode == 0, "Ed25519 envelope signature verification failed")


def verify_envelope(path: Path, trusted_public_key: bytes, expected_key_generation: int = 1) -> VerifiedEnvelope:
    encoded = read_bytes(path, "signed envelope")
    envelope = read_json(path, "signed envelope")
    require(envelope.get("evidenceSchema") == ENVELOPE_SCHEMA, f"unknown evidence schema: {path}")
    require(envelope.get("evidenceSchemaGeneration") == 1, f"unknown evidence generation: {path}")
    key_generation = envelope.get("keyGeneration")
    require(key_generation == expected_key_generation, f"issuer key generation mismatch: {path}")
    public_key = decode_base64(envelope.get("publicKeyDerBase64"), "publicKeyDerBase64")
    require(public_key == trusted_public_key, f"envelope is signed by another trust root: {path}")
    payload = decode_base64(envelope.get("payloadBase64"), "payloadBase64")
    payload_digest = sha256_bytes(payload)
    require(payload_digest == envelope.get("payloadSha256"), f"payload digest mismatch: {path}")
    signature = decode_base64(envelope.get("signatureBase64"), "signatureBase64")
    signature_input = SIGNATURE_DOMAIN + int(key_generation).to_bytes(4, "big") + hashlib.sha256(payload).digest()
    verify_ed25519(public_key, signature_input, signature)
    try:
        payload_json = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise VerificationError(f"signed payload is not JSON: {path}") from exc
    require(isinstance(payload_json, dict), f"signed payload is not a JSON object: {path}")
    return VerifiedEnvelope(path, sha256_bytes(encoded), payload, payload_json, key_generation)


def require_path_digest(
    value: dict[str, Any], path_field: str, digest_field: str, run_dir: Path, label: str
) -> Path:
    path = within_run(str(value.get(path_field, "")), run_dir, label)
    require(sha256_bytes(read_bytes(path, label)) == value.get(digest_field), f"{label} digest mismatch")
    return path


def require_evidence_ref(value: dict[str, Any], name: str, run_dir: Path, label: str) -> Path:
    reference = value.get(name)
    require(isinstance(reference, dict) and set(reference) == {"path", "sha256"},
            f"{label} is not a closed path/digest reference")
    path = within_run(str(reference.get("path", "")), run_dir, label)
    require(sha256_bytes(read_bytes(path, label)) == reference.get("sha256"), f"{label} digest mismatch")
    return path


def verify_rollback_topic_stats(value: dict[str, Any], run_dir: Path) -> None:
    evidence = value.get("evidence")
    require(
        isinstance(evidence, dict)
        and set(evidence) == {"nativeTopicStats", "nativeTopicStatsHttpStatus"},
        "rollback topic evidence is not closed",
    )
    stats_path = require_evidence_ref(evidence, "nativeTopicStats", run_dir, "rollback native topic stats")
    http_status = evidence.get("nativeTopicStatsHttpStatus")
    require(http_status in (200, 404), "rollback native topic stats has an invalid HTTP status")
    if http_status == 404:
        require(value.get("activeSendCount") == 0, "an absent rollback topic retained an active send")
        return
    stats = read_json(stats_path, "rollback native topic stats")
    publishers = stats.get("publishers")
    require(isinstance(publishers, (list, dict)), "rollback native topic stats lacks publishers")
    require(len(publishers) == value.get("activeSendCount") == 0, "rollback topic retained an active publisher")


def resource_set(scope: dict[str, Any], label: str) -> set[tuple[str, str]]:
    resources = scope.get("resources")
    require(isinstance(resources, list) and len(resources) == len(RESOURCE_KINDS),
            f"{label} is not a closed 13-resource scope")
    result: set[tuple[str, str]] = set()
    kinds: set[str] = set()
    for row in resources:
        require(isinstance(row, dict), f"{label} contains a non-object resource")
        kind = row.get("kind")
        identity = row.get("identity")
        require(kind in RESOURCE_KINDS and isinstance(identity, str) and identity,
                f"{label} contains an invalid resource")
        require(kind not in kinds and (kind, identity) not in result, f"{label} contains a duplicate resource")
        kinds.add(kind)
        result.add((kind, identity))
    require(kinds == RESOURCE_KINDS, f"{label} does not contain exactly one resource of every kind")
    return result


def read_varint(value: bytes, offset: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while offset < len(value) and shift < 70:
        byte = value[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            return result, offset
        shift += 7
    raise VerificationError("DataResetManifest contains an invalid varint")


def parse_proto_fields(value: bytes) -> list[tuple[int, int, int, int, bytes | int]]:
    fields: list[tuple[int, int, int, int, bytes | int]] = []
    offset = 0
    while offset < len(value):
        start = offset
        tag, offset = read_varint(value, offset)
        number = tag >> 3
        wire = tag & 7
        require(number > 0 and wire in (0, 2), "DataResetManifest contains an unsupported field")
        if wire == 0:
            field_value, offset = read_varint(value, offset)
        else:
            length, offset = read_varint(value, offset)
            require(length >= 0 and offset + length <= len(value), "DataResetManifest field exceeds input")
            field_value = value[offset:offset + length]
            offset += length
        fields.append((number, wire, start, offset, field_value))
    return fields


def verify_manifest_signature(path: Path, trusted_public_key: bytes, expected_digest: str) -> None:
    encoded = read_bytes(path, "DataResetManifest")
    fields = parse_proto_fields(encoded)
    require(len(fields) >= 14 and fields[-2][0:2] == (13, 2) and fields[-1][0:2] == (14, 2),
            "DataResetManifest digest/signature fields are missing")
    require([field[0] for field in fields[:5]] == [1, 2, 3, 4, 5],
            "DataResetManifest required prefix is not canonical")
    numbers = [field[0] for field in fields]
    cursor = 5
    require(cursor < len(numbers) and numbers[cursor] == 6, "DataResetManifest has no resource")
    while cursor < len(numbers) and numbers[cursor] == 6:
        cursor += 1
    require(numbers[cursor:cursor + 2] == [7, 8], "DataResetManifest evidence/obligation fields differ")
    cursor += 2
    require(cursor < len(numbers) and numbers[cursor] == 9, "DataResetManifest has no Worker capability")
    while cursor < len(numbers) and numbers[cursor] == 9:
        cursor += 1
    require(numbers[cursor:] == [10, 11, 12, 13, 14], "DataResetManifest trailing fields are not closed")
    require(fields[0][4] == 1, "DataResetManifest schema generation is not current")
    key_fields = [field for field in fields[:-2] if field[0] == 12 and field[1] == 0]
    require(len(key_fields) == 1 and isinstance(key_fields[0][4], int),
            "DataResetManifest issuer key generation is missing")
    key_generation = int(key_fields[0][4])
    require(key_generation == 1, "DataResetManifest issuer key generation differs")
    digest = fields[-2][4]
    signature = fields[-1][4]
    require(isinstance(digest, bytes) and len(digest) == 32 and isinstance(signature, bytes) and len(signature) == 64,
            "DataResetManifest digest/signature length is invalid")
    canonical_fields = encoded[:fields[-2][2]]
    computed = hashlib.sha256(MANIFEST_DIGEST_DOMAIN + canonical_fields).digest()
    require(computed == digest and computed.hex() == expected_digest, "DataResetManifest logical digest mismatch")
    verify_ed25519(
        trusted_public_key,
        MANIFEST_SIGNATURE_DOMAIN + digest + key_generation.to_bytes(4, "big"),
        signature,
    )


def require_candidate(value: dict[str, Any], candidate: str, label: str) -> None:
    require(value.get("candidateCommit") == candidate, f"{label} candidate mismatch")


def verify_policy_chain(
    run_dir: Path, trusted_public_key: bytes, candidate: str
) -> tuple[dict[str, VerifiedEnvelope], str]:
    phases = (
        ("shadow-initial", "SHADOW"),
        ("shadow-candidate-add", "SHADOW"),
        ("shadow-candidate-cancel", "SHADOW"),
        ("enabled", "ENABLED"),
        ("disabled", "DISABLED"),
    )
    result: dict[str, VerifiedEnvelope] = {}
    prior_version = 0
    prior_generation = 0
    scope: str | None = None
    for phase, mode in phases:
        path = run_dir / "authority" / "policy" / f"{phase}.signed.json"
        verified = verify_envelope(path, trusted_public_key)
        value = verified.payload
        require(value.get("policySchema") == "nereus-delay.handoff-policy-publication", f"bad policy: {phase}")
        require(value.get("policyStatus") == mode, f"wrong policy mode: {phase}")
        require_candidate(value, candidate, f"policy {phase}")
        version = int(value.get("policyOxiaVersion", 0))
        generation = int(value.get("policyGeneration", 0))
        require(version > prior_version and generation > prior_generation, f"policy head did not advance: {phase}")
        phase_scope = value.get("policyScopeDigest")
        require(isinstance(phase_scope, str) and HEX_32.fullmatch(phase_scope) is not None, f"bad policy scope: {phase}")
        if scope is None:
            scope = phase_scope
        require(phase_scope == scope, f"policy scope changed: {phase}")
        readback = read_bytes(
            run_dir / "authority" / "policy" / f"{phase}-readback.log", f"policy readback {phase}"
        ).decode("utf-8")
        for marker in (f"policyMode={mode}", f"policyOxiaVersion={version}", f"policyGeneration={generation}"):
            require(marker in readback, f"current-head readback lacks {marker}: {phase}")
        prior_version = version
        prior_generation = generation
        result[phase] = verified
    assert scope is not None
    enabled = result["enabled"].payload
    disabled = result["disabled"].payload
    require(int(enabled.get("effectiveLeadMs", 0)) > 0, "ENABLED policy has no positive lead")
    require(int(enabled.get("allowedPathBits", 0)) & 1 == 1, "ENABLED policy excludes Managed Handoff")
    require(int(disabled.get("effectiveLeadMs", -1)) == 0, "DISABLED policy retains a lead")
    require(int(disabled.get("allowedPathBits", -1)) == 0, "DISABLED policy retains an allowed path")
    require(
        int(disabled.get("effectiveDisabledAfterEpochMs", 0)) == int(enabled.get("validUntilEpochMs", -1)),
        "DISABLED boundary differs from the frozen ENABLED lease",
    )
    return result, scope


def verify_managed_handoff_evidence(
    path: Path,
    policy_scope_digest: str,
    policy_snapshot_digest: str,
    artifact_set_digest: str,
    p1_source_lock_digest: str,
) -> dict[str, Any]:
    value = read_json(path, "Managed Handoff evidence")
    require(value.get("schema") == "nereus-delay.managed-handoff-canary-evidence", "bad Managed Handoff schema")
    require(value.get("schemaGeneration") == 2, "bad Managed Handoff evidence generation")
    require(value.get("verdict") == "PASS", "Managed Handoff did not PASS")
    require(value.get("productionPath") is True and value.get("productionAuthority") is False,
            "Managed Handoff path/authority boundary is wrong")
    require(
        (value.get("nativeAdmission"), value.get("nativeSend"), value.get("handedOff")) == (1, 1, 1),
        "Managed Handoff counters are not 1/1/1",
    )
    action_at = int(value.get("actionAtEpochMs", -1))
    persisted_at = int(value.get("brokerPersistenceTimeEpochMs", -1))
    deliver_at = int(value.get("deliverAtEpochMs", -1))
    require(action_at <= persisted_at < deliver_at, "Managed Handoff did not persist in [actionAt, deliverAt)")
    require(value.get("deliveryContract") == "PULSAR_NATIVE_DELIVERY", "wrong Managed Handoff contract")
    require(value.get("policyScopeDigest") == policy_scope_digest, "Managed policy scope mismatch")
    require(value.get("policySnapshotDigest") == policy_snapshot_digest, "Managed policy snapshot mismatch")
    require(value.get("artifactSetDigest") == artifact_set_digest, "Managed artifact set mismatch")
    require(value.get("p1SourceLock") == p1_source_lock_digest, "Managed P1 source lock digest mismatch")
    require(value.get("destinationResponseLossResolved") is True, "destination response loss was not resolved")
    require(value.get("attemptJournalResponseLossRecoveries") == 3, "Attempt Journal recovery count is not exact")
    require(value.get("attemptJournalStartupReplayRecords") == 3
            and value.get("attemptJournalStartupReplayVerified") is True,
            "Attempt Journal startup reconstruction was not verified")
    journal = value.get("journal")
    require(isinstance(journal, list), "Managed Attempt Journal is absent")
    require([row.get("kind") for row in journal] == ["MAPPED", "OWNERSHIP_STARTED", "PUBLISHED"],
            "Managed Attempt Journal ordering is incomplete")
    sequence = value.get("sequenceId")
    require(all(row.get("sequenceId") == sequence for row in journal), "Managed sequenceId changed across Journal records")
    for field in (
        "publishAttemptId",
        "policyScopeDigest",
        "preparedPublishHash",
        "recordTemplateHash",
        "preparedRecordHash",
        "sendCommandSha256",
        "authenticatedResponseCommandSha256",
        "artifactSetDigest",
        "schedulePositionSha256",
        "admissionPositionSha256",
        "outcomePositionSha256",
    ):
        require(isinstance(value.get(field), str) and HEX_32.fullmatch(value[field]) is not None,
                f"Managed evidence has no canonical {field}")
    return value


def verify_native_evidence(
    path: Path,
    candidate: str,
    environment_id: str,
    policy_snapshot_digest: str,
    artifact_set_digest: str,
    p1_source_lock_digest: str,
    expected_topic: str,
) -> dict[str, Any]:
    value = read_json(path, "AUTO_FAST native evidence")
    require(value.get("schema") == "nereus-delay.persistent-native-canary"
            and value.get("schemaGeneration") == 2, "bad AUTO_FAST evidence schema")
    require_candidate(value, candidate, "AUTO_FAST evidence")
    require(value.get("environmentId") == environment_id, "AUTO_FAST environment mismatch")
    require(value.get("productionPath") is True and value.get("productionAuthority") is False,
            "AUTO_FAST path/authority boundary is wrong")
    require(value.get("verdict") == "PASS", "AUTO_FAST did not PASS")
    require((value.get("nativeAdmission"), value.get("nativeSend"), value.get("handedOff")) == (1, 1, 0),
            "AUTO_FAST counters are not 1/1/0")
    require(value.get("targetPhysicalTopic") == expected_topic, "AUTO_FAST target differs from the canary")
    require(value.get("policySnapshotDigest") == policy_snapshot_digest, "AUTO_FAST policy snapshot mismatch")
    require(value.get("artifactSetDigest") == artifact_set_digest, "AUTO_FAST artifact set mismatch")
    require(value.get("p1SourceLock") == p1_source_lock_digest, "AUTO_FAST P1 source lock digest mismatch")
    persisted_at = int(value.get("brokerPersistenceTimeEpochMs", -1))
    deliver_at = int(value.get("deliverAtEpochMs", -1))
    require(0 <= persisted_at < deliver_at, "AUTO_FAST was not persisted before deliverAt")
    require(isinstance(value.get("sequenceId"), int) and value["sequenceId"] >= 0,
            "AUTO_FAST sequenceId is invalid")
    for field in (
        "policyScopeDigest",
        "policySnapshotDigest",
        "preparedRecordHash",
        "sendCommandSha256",
        "authenticatedResponseCommandSha256",
        "artifactSetDigest",
    ):
        require(isinstance(value.get(field), str) and HEX_32.fullmatch(value[field]) is not None,
                f"AUTO_FAST evidence has no canonical {field}")
    return value


def validate(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = args.run_dir.expanduser().resolve(strict=True)
    require(run_dir.is_dir() and not run_dir.is_symlink(), "run directory is not a regular directory")
    candidate = args.expected_candidate
    require(COMMIT.fullmatch(candidate) is not None, "expected candidate is not a commit")
    p1_source_lock_digest = canonical_p1_source_lock_digest(args.expected_p1_source_lock)
    trusted_public_key = read_bytes(args.trusted_public_key.expanduser().resolve(strict=True), "trusted public key")

    gate = verify_envelope(run_dir / "authority/gate-c-receipt.signed.json", trusted_public_key)
    gate_value = gate.payload
    require(gate_value.get("gateCSchema") == "nereus-delay.gate-c" and gate_value.get("gateCStatus") == "PASS",
            "Gate C is not PASS")
    require(gate_value.get("gateCSchemaGeneration") == 1 and gate_value.get("productionAuthority") is False,
            "Gate C schema/authority boundary is wrong")
    require_candidate(gate_value, candidate, "Gate C")
    environment_id = gate_value.get("environmentId")
    require(isinstance(environment_id, str) and environment_id, "Gate C environment is missing")
    require(gate_value.get("environmentClassification") == "STAGING", "Gate C is not scoped to STAGING")
    require(gate_value.get("ndipPackageDigest") == args.expected_package_digest, "Gate C package digest mismatch")
    require(gate_value.get("p1SourceLock") == args.expected_p1_source_lock, "Gate C P1 lock mismatch")
    require(gate_value.get("applicableChecks") == 41 and gate_value.get("passedChecks") == 41,
            "Gate C is not 41/41")
    for field in ("startupAssignmentGate", "noOldGeneration", "noUnresolvedPublishing", "noUnresolvedUncertain", "freshness"):
        require(gate_value.get(field) is True, f"Gate C lacks {field}")

    disposition_path = require_path_digest(
        gate_value, "dataDispositionPath", "dataDispositionSha256", run_dir, "data disposition envelope"
    )
    disposition = verify_envelope(disposition_path, trusted_public_key).payload
    require(disposition.get("schema") == "nereus-delay.ndip1-staging-data-disposition"
            and disposition.get("schemaGeneration") == 1, "data disposition schema is wrong")
    require_candidate(disposition, candidate, "data disposition")
    require(disposition.get("environmentId") == environment_id
            and disposition.get("environmentClassification") == "STAGING",
            "data disposition scope differs from Gate C")
    require(disposition.get("externalUserDataPresent") is False, "external user data was not ruled out")
    require(disposition.get("existingResourcesAreInternalStagingOnly") is True,
            "existing resources were not classified as internal staging")
    require(disposition.get("replacementDisposition") == "REINCARNATE", "data disposition is not reincarnate")
    require(disposition.get("operatorAuthorization") == "EXPLICIT_ENVIRONMENT_INPUT", "operator did not opt in")
    require(disposition.get("destructiveOperationsAuthorized") is False
            and disposition.get("productionAuthority") is False, "data disposition grants destructive authority")
    require(isinstance(disposition.get("operator"), str)
            and re.fullmatch(r"[A-Za-z0-9._@-]+", disposition["operator"]) is not None,
            "data disposition operator is not a closed identifier")
    expected_resolution = "RESET" if disposition.get("decision") == "RESET_INTERNAL_ONLY" else None
    if disposition.get("decision") == "CREATE_NEW_INTERNAL_ONLY":
        expected_resolution = "RESET"
    require(gate_value.get("resolution") == expected_resolution, "Gate C resolution differs from data decision")
    candidate_scope_path = require_path_digest(
        disposition, "candidateScope", "candidateScopeSha256", run_dir, "candidate scope"
    )
    candidate_scope_document = read_json(candidate_scope_path, "candidate scope")
    candidate_scope = candidate_scope_document.get("scope")
    require(isinstance(candidate_scope, dict), "candidate scope document has no scope")
    require(candidate_scope.get("environmentId") == environment_id
            and candidate_scope.get("environmentClassification") == "STAGING",
            "candidate scope differs from Gate C")
    candidate_resources = resource_set(candidate_scope, "candidate scope")

    assessment_path = require_path_digest(
        gate_value, "assessmentEnvelopePath", "assessmentEnvelopeSha256", run_dir, "assessment envelope"
    )
    assessment = verify_envelope(assessment_path, trusted_public_key)
    assessment_receipt_path = require_path_digest(
        gate_value, "assessmentReceiptPath", "assessmentReceiptSha256", run_dir, "assessment receipt"
    )
    require(read_bytes(assessment_receipt_path, "assessment receipt") == assessment.payload_bytes,
            "assessment sidecar differs from signed payload")
    assessment_value = assessment.payload
    require(assessment_value.get("outcome") in ("PASS_DIRECT_REPLACE", "PASS_RETAIN"), "assessment is not decision-ready")
    require(assessment_value.get("sourceBaselineCommit") == candidate, "assessment source commit mismatch")
    require(assessment_value.get("ndipPackageDigest") == args.expected_package_digest, "assessment package mismatch")
    assessment_scope = assessment_value.get("scope")
    require(isinstance(assessment_scope, dict), "assessment scope is missing")
    require(assessment_scope.get("environmentId") == environment_id
            and assessment_scope.get("environmentClassification") == "STAGING",
            "assessment scope differs from Gate C")
    assessment_resources = resource_set(assessment_scope, "assessment scope")
    expected_assessment_outcome = "PASS_DIRECT_REPLACE" if gate_value.get("resolution") == "RESET" else "PASS_RETAIN"
    require(assessment_value.get("outcome") == expected_assessment_outcome,
            "assessment outcome differs from Gate C resolution")

    manifest_path = require_path_digest(gate_value, "manifestPath", "manifestSha256", run_dir, "DataResetManifest")
    require(HEX_32.fullmatch(str(gate_value.get("manifestDigest", ""))) is not None, "manifest logical digest is invalid")
    require(
        decode_base64(gate_value.get("manifestPublicKeyDerBase64"), "manifestPublicKeyDerBase64") == trusted_public_key,
        "manifest trust root differs",
    )
    verify_manifest_signature(
        manifest_path, trusted_public_key, str(gate_value.get("manifestDigest", ""))
    )
    manifest_readback_path = require_path_digest(
        gate_value, "manifestReadbackPath", "manifestReadbackSha256", run_dir, "manifest readback"
    )
    manifest_readback = read_json(manifest_readback_path, "manifest readback")
    require(manifest_readback.get("schema") == "nereus-delay.ndip1-manifest-operation-readback"
            and manifest_readback.get("schemaGeneration") == 1, "manifest readback schema is wrong")
    require_candidate(manifest_readback, candidate, "manifest readback")
    require(manifest_readback.get("environmentId") == environment_id, "manifest readback environment mismatch")
    readback_scope_path = require_path_digest(
        manifest_readback, "scope", "scopeSha256", run_dir, "manifest candidate scope"
    )
    require(readback_scope_path == candidate_scope_path, "manifest readback names another candidate scope")
    intent_path = require_path_digest(
        manifest_readback, "intent", "intentSha256", run_dir, "manifest operation intent"
    )
    intent = read_json(intent_path, "manifest operation intent")
    require(intent.get("exactScope") is True and intent.get("destructiveOperations") == [],
            "manifest operation intent is destructive or open-scoped")
    resources_path = require_path_digest(
        manifest_readback, "resourceReadback", "resourceReadbackSha256", run_dir, "manifest resource readback"
    )
    resources = json.loads(read_bytes(resources_path, "manifest resource readback").decode("utf-8"))
    require(isinstance(resources, list) and len(resources) == 13, "manifest does not have 13 resource readbacks")
    require(all(isinstance(row, dict) for row in resources), "manifest resource readback contains a non-object")
    readback_resources = {(row.get("kind"), row.get("identity")) for row in resources if isinstance(row, dict)}
    require(readback_resources == candidate_resources, "manifest readback differs from the candidate scope")
    require(all(row.get("status") == "PASS" for row in resources), "manifest resource readback is not all PASS")
    for row in resources:
        require_path_digest(row, "evidence", "evidenceSha256", run_dir, "manifest resource evidence")
    require(manifest_readback.get("destructiveOperations") == [], "unexpected destructive operation")
    operations = manifest_readback.get("operations")
    require(isinstance(operations, dict)
            and operations.get("resourceReadbackCount") == 13
            and all(value is True or (key == "resourceReadbackCount" and value == 13)
                    for key, value in operations.items()), "manifest operation readback is incomplete")

    g0_path = require_path_digest(gate_value, "g0SnapshotPath", "g0SnapshotSha256", run_dir, "G0 snapshot")
    g0 = read_json(g0_path, "G0 snapshot")
    require(g0.get("schema") == "nereus-delay.ndip1-g0-data-reset-snapshot"
            and g0.get("schemaGeneration") == 1, "G0 schema is wrong")
    require_candidate(g0, candidate, "G0")
    require(g0.get("environmentId") == environment_id and g0.get("classification") == "STAGING",
            "G0 scope differs from Gate C")
    require(g0.get("acceptedPackageDigest") == args.expected_package_digest
            and g0.get("p1SourceLock") == args.expected_p1_source_lock, "G0 source locks differ")
    require(g0.get("unresolvedPublishingOrUncertain") is False, "G0 has unresolved obligations")
    assessed = g0.get("assessedDeployment")
    require(isinstance(assessed, dict)
            and assessed.get("dataDispositionEnvelope") == str(disposition_path)
            and assessed.get("dataDispositionEnvelopeSha256") == sha256_bytes(read_bytes(disposition_path, "data disposition")),
            "G0 does not bind the signed data disposition")
    observations_path = require_path_digest(
        gate_value, "g0ObservationsPath", "g0ObservationsSha256", run_dir, "G0 observations"
    )
    observations = json.loads(read_bytes(observations_path, "G0 observations").decode("utf-8"))
    require(isinstance(observations, list) and len(observations) == 13, "G0 does not cover 13 resources")
    inventory = assessment_value.get("inventory")
    require(isinstance(inventory, dict), "assessment inventory is missing")
    signed_observations = inventory.get("resourceObservations")
    require(observations == signed_observations, "G0 observations differ from the signed assessment")
    require({(row.get("kind"), row.get("identity")) for row in observations} == assessment_resources,
            "G0 observations differ from the assessment scope")
    require(all(row.get("externalRetention") == "NONE" and row.get("replacementDisposition") == "REINCARNATE"
                for row in observations), "G0 disposition is not closed")

    skip_path = require_path_digest(
        gate_value, "skipAuditPath", "skipAuditSha256", run_dir, "Gate C skip audit"
    )
    skip_audit = read_json(skip_path, "Gate C skip audit")
    require(skip_audit.get("schema") == "nereus-delay.ndip1-staging-skip-audit"
            and skip_audit.get("schemaGeneration") == 1
            and skip_audit.get("expectedConditionalSkips") == 41, "Gate C skip audit schema is wrong")
    require(skip_audit.get("counts") == {"pass": 41, "failed": 0, "skipped": 0, "notExecuted": 0},
            "Gate C skip audit counts are not 41/0/0/0")
    skip_rows = skip_audit.get("rows")
    require(isinstance(skip_rows, list) and len(skip_rows) == 41
            and all(row.get("status") == "PASS" and row.get("applicability") == "REQUIRED_STAGING"
                    and int(row.get("effectiveRuns", 0)) >= 1 for row in skip_rows),
            "Gate C skip audit contains an unexecuted/non-passing case")

    shadow = verify_envelope(run_dir / "authority/shadow-receipt.signed.json", trusted_public_key)
    shadow_value = shadow.payload
    require(shadow_value.get("shadowSchema") == "nereus-delay.shadow-certification"
            and shadow_value.get("shadowStatus") == "PASS", "SHADOW is not PASS")
    require_candidate(shadow_value, candidate, "SHADOW")
    require(shadow_value.get("environmentId") == environment_id
            and shadow_value.get("evidence", {}).get("productionAuthority") is False,
            "SHADOW scope/authority boundary is wrong")
    require(shadow_value.get("gateCEnvelopeSha256") == gate.envelope_sha256, "SHADOW does not bind Gate C")
    require((shadow_value.get("nativeAdmission"), shadow_value.get("nativeSend"), shadow_value.get("handedOff"))
            == (0, 0, 0), "SHADOW native counters are not 0/0/0")
    require(int(shadow_value.get("observationSeconds", 0)) >= 10, "SHADOW observation is too short")
    for field in ("unresolvedPublishing", "unresolvedUncertain", "attemptJournalLeak", "generationIncarnationMix"):
        require(shadow_value.get(field) is False, f"SHADOW reports {field}")

    policies, policy_scope = verify_policy_chain(run_dir, trusted_public_key, candidate)
    require(shadow_value.get("shadowPolicyEnvelopeSha256") == policies["shadow-candidate-cancel"].envelope_sha256,
            "SHADOW receipt does not bind the final SHADOW policy")

    canary = verify_envelope(run_dir / "authority/enabled-canary-receipt.signed.json", trusted_public_key)
    canary_value = canary.payload
    require(canary_value.get("canarySchema") == "nereus-delay.enabled-canary"
            and canary_value.get("canaryStatus") == "PASS", "ENABLED canary is not PASS")
    require(canary_value.get("canarySchemaGeneration") == 2, "ENABLED canary schema generation is wrong")
    require_candidate(canary_value, candidate, "ENABLED canary")
    require(canary_value.get("environmentId") == environment_id, "ENABLED canary environment mismatch")
    require(canary_value.get("gateCEnvelopeSha256") == gate.envelope_sha256, "canary does not bind Gate C")
    require(canary_value.get("shadowEnvelopeSha256") == shadow.envelope_sha256, "canary does not bind SHADOW")
    require(canary_value.get("enabledPolicyEnvelopeSha256") == policies["enabled"].envelope_sha256,
            "canary does not bind ENABLED policy")
    require((canary_value.get("nativeAdmission"), canary_value.get("nativeSend"), canary_value.get("handedOff"))
            == (2, 2, 1), "combined canary counters are not 2/2/1")
    require(canary_value.get("maxRecords") == 2, "combined canary is not bounded to two records")
    require(canary_value.get("branches") == {
        "autoFast": {"nativeAdmission": 1, "nativeSend": 1, "handedOff": 0},
        "managedHandoff": {"nativeAdmission": 1, "nativeSend": 1, "handedOff": 1},
    }, "canary branch accounting is not closed")
    canary_evidence = canary_value.get("evidence")
    require(isinstance(canary_evidence, dict) and canary_evidence.get("productionAuthority") is False,
            "canary evidence authority boundary is wrong")
    evidence_paths = {
        name: require_evidence_ref(canary_evidence, name, run_dir, label)
        for name, label in (
            ("nativeCanary", "AUTO_FAST native evidence"),
            ("managedHandoff", "Managed Handoff evidence"),
            ("managedHandoffLog", "Managed Handoff log"),
            ("log", "AUTO_FAST canary log"),
            ("stats", "native topic stats"),
            ("internalStats", "native topic internal stats"),
            ("brokerFailoverBefore", "Broker failover before evidence"),
            ("brokerFailoverAfter", "Broker failover after evidence"),
        )
    }
    enabled_policy = policies["enabled"].payload
    verify_native_evidence(
        evidence_paths["nativeCanary"],
        candidate,
        environment_id,
        str(enabled_policy.get("policySnapshotDigest")),
        str(enabled_policy.get("artifactSetDigest")),
        p1_source_lock_digest,
        str(canary_value.get("topic")),
    )
    verify_managed_handoff_evidence(
        evidence_paths["managedHandoff"],
        str(enabled_policy.get("policyScopeDigest")),
        str(enabled_policy.get("policySnapshotDigest")),
        str(enabled_policy.get("artifactSetDigest")),
        p1_source_lock_digest,
    )
    managed_log = read_bytes(evidence_paths["managedHandoffLog"], "Managed Handoff log").decode("utf-8")
    for marker in (
        "Pulsar Worker source-applied physical publish passed",
        "Pulsar Worker destination response-loss smoke passed",
        "Pulsar Worker Attempt Journal response-loss smoke passed",
    ):
        require(marker in managed_log, f"Managed Handoff log lacks {marker}")
    native_log = read_bytes(evidence_paths["log"], "AUTO_FAST canary log").decode("utf-8")
    require(native_log.count("Pulsar native coordinator typed-evidence smoke passed") == 1
            and "deliverAt=" in native_log and "sequence=" in native_log,
            "AUTO_FAST log does not prove one exact typed-evidence send")

    rollback = verify_envelope(run_dir / "authority/rollback-receipt.signed.json", trusted_public_key)
    rollback_value = rollback.payload
    require(
        rollback_value.get("schema") == "nereus-delay.ndip1-rollback"
        and rollback_value.get("schemaGeneration") == 2,
        "rollback schema is not current",
    )
    require(rollback_value.get("status") == "PASS" and rollback_value.get("environmentReturnedToDisabled") is True,
            "rollback is not PASS/DISABLED")
    require_candidate(rollback_value, candidate, "rollback")
    require(rollback_value.get("environmentId") == environment_id
            and rollback_value.get("productionAuthority") is False, "rollback scope/authority boundary is wrong")
    require(rollback_value.get("finalPolicyEnvelopeSha256") == policies["disabled"].envelope_sha256,
            "rollback does not bind DISABLED policy")
    require(rollback_value.get("activeLeaseCount") == 0 and rollback_value.get("activeSendCount") == 0,
            "rollback retained an active lease/send")
    require(rollback_value.get("activeNativeProcessCount") == 0
            and rollback_value.get("activeWorkerProcessCount") == 0, "rollback retained a native/Worker process")
    verify_rollback_topic_stats(rollback_value, run_dir)
    final_state = read_json(run_dir / "authority/final-state.json", "final state")
    require(final_state.get("status") == "DISABLED", "final state is not DISABLED")
    require_candidate(final_state, candidate, "final state")
    require(final_state.get("environmentId") == environment_id
            and final_state.get("productionAuthority") is False, "final state scope/authority boundary is wrong")
    require(final_state.get("disabledPolicyEnvelopeSha256") == policies["disabled"].envelope_sha256,
            "final state does not bind DISABLED policy")
    require(final_state.get("rollbackReceiptSha256") == rollback.envelope_sha256,
            "final state does not bind rollback")
    require(final_state.get("activeLeaseCount") == 0 and final_state.get("activeSendCount") == 0,
            "final state retained active work")
    require(final_state.get("activeNativeProcessCount") == 0
            and final_state.get("activeWorkerProcessCount") == 0, "final state retained a native/Worker process")

    return {
        "schema": "nereus-delay.ndip1-persistent-certification-validation",
        "schemaGeneration": 1,
        "status": "PASS",
        "candidateCommit": candidate,
        "ndipPackageDigest": args.expected_package_digest,
        "p1SourceLock": args.expected_p1_source_lock,
        "trustedPublicKeySha256": sha256_bytes(trusted_public_key),
        "gateCEnvelopeSha256": gate.envelope_sha256,
        "shadowEnvelopeSha256": shadow.envelope_sha256,
        "canaryEnvelopeSha256": canary.envelope_sha256,
        "disabledPolicyEnvelopeSha256": policies["disabled"].envelope_sha256,
        "rollbackEnvelopeSha256": rollback.envelope_sha256,
        "policyScopeDigest": policy_scope,
        "resourceReadbackCount": 13,
        "applicableChecks": 41,
        "manifestSignatureVerified": True,
        "rawCanaryEvidenceDigestsVerified": 8,
        "rawRollbackEvidenceDigestsVerified": 1,
        "attemptJournalStartupReplayRecords": 3,
        "nativeAdmission": 2,
        "nativeSend": 2,
        "handedOff": 1,
        "finalPolicy": "DISABLED",
        "activeLeaseCount": 0,
        "activeSendCount": 0,
        "productionAuthority": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--trusted-public-key", type=Path, required=True)
    parser.add_argument("--expected-candidate", required=True)
    parser.add_argument("--expected-package-digest", required=True)
    parser.add_argument("--expected-p1-source-lock", required=True)
    args = parser.parse_args()
    try:
        result = validate(args)
    except (VerificationError, OSError, subprocess.SubprocessError, TypeError, ValueError) as exc:
        print(f"NDIP-1 persistent certification verification failed: {exc}")
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
