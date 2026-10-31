#!/usr/bin/env python3
"""Check NDP-0001 project naming, keeping registered format bytes unchanged."""

import os
import re
import subprocess
import sys
from pathlib import Path


MARKER = re.compile(
    r"(?<![A-Za-z0-9])[vV][12](?![A-Za-z0-9])|"
    r"\b[vV][12][A-Z_]|[A-Za-z0-9_]V[12](?=[A-Z_]|\b)"
)
PROJECT = re.compile(
    r"(?i)(?:nereus[ _-]*delay|project|项目|整体设计|整体版本|design|runbook|protocol[ _-]*registry)"
    r"[\s_:/()（）：=.\-]*(?:(?:version|版本)[\s_:/()（）：=.\-]*)?[vV][1-9][0-9]*(?![0-9])"
)
PATH_MARKER = re.compile(
    r"(?<![A-Za-z0-9])[vV][1-9][0-9]*(?![A-Za-z0-9])|"
    r"\b[vV][1-9][0-9]*[A-Z_]|[A-Za-z0-9_]V[1-9][0-9]*(?=[A-Z_]|\b)"
)
SELF_FILES = {
    "scripts/check-project-version-markers.sh",
    "scripts/check-project-version-markers.py",
    "scripts/test-check-project-version-markers.py",
}
# These paths name registered Target Admission bodies or the RFC 9562 UUIDv7
# algorithm, rather than a project release line.
FORMAT_PATHS = {
    "docs/ndip/NDIP-3/evidence/e5-target-publish-admission-body-v3-focused.log",
    "docs/ndip/NDIP-3/evidence/e5-materialized-admission-v4-focused.log",
    "src/main/java/com/nereusstream/delay/semantic/LogicalUuidV7Generator.java",
    "src/main/java/com/nereusstream/delay/semantic/SecureLogicalUuidV7Generator.java",
}

# Exact domain bytes, not project versions. The Registry's mTLS and Target
# migration/SLO/evidence entries own these preimages. The local-only
# custody, checkpoint-operation and key-match domains retain their existing
# Bytes.utf8/sha256 spellings; none of these permits an arbitrary -v1 token.
DOMAINS = set("""
nd-local-mtls-v1
nereus-delay-local-mtls-offline-pkix-v1
nereus-delay-catalog-root-current-policy-v1
nereus-delay-catalog-root-current-policy-signature-v1
nereus-delay-evidence-marker-conditional-response-v1
nereus-delay-evidence-marker-local-dispatch-v1
nereus-delay-evidence-object-lifecycle-history-v1
nereus-delay-evidence-object-lifecycle-history-signature-v1
nereus-delay-f2-checkpoint-operation-v1
nereus-delay-live-migration-prepared-v1
nereus-delay-migration-archive-coverage-v1
nereus-delay-migration-archive-custody-v1
nereus-delay-migration-archive-entry-v1
nereus-delay-migration-archive-packet-v1
nereus-delay-migration-archive-range-v1
nereus-delay-migration-archive-state-v1
nereus-delay-migration-logical-first-archive-v1
nereus-delay-migration-logical-first-native72-v1
nereus-delay-migration-logical-first-native72-v2
nereus-delay-migration-prepared-c-manifest-v1
nereus-delay-migration-root-issuer-authorization-v1
nereus-delay-migration-root-issuer-authorization-v2
nereus-delay-migration-root-issuer-authorization-signature-v1
nereus-delay-migration-root-issuer-authorization-signature-v2
nereus-delay-payload-summary-candidate-v1
nereus-delay-payload-summary-initialize-v1
nereus-delay-payload-summary-origin-v2
nereus-delay-scope-original-owned-backing-v1
nereus-delay-scope-original-producer-admin-domain-v1
nereus-delay-target-checkpoint-install-intent-v1
nereus-delay-target-journal-record-v2
nereus-delay-target-migration-activation-attempt-v1
nereus-delay-target-migration-activation-attempt-v2
nereus-delay-target-migration-assignment-adoption-v1
nereus-delay-target-migration-control-v1
nereus-delay-target-migration-custody-guard-v1
nereus-delay-target-migration-origin-v1
nereus-delay-target-migration-plan-v1
nereus-delay-target-migration-prefix-record-v1
nereus-delay-target-migration-reservation-v2
nereus-delay-target-migration-retire-intent-v1
nereus-delay-target-migration-retirement-inventory-v1
nereus-delay-target-migration-retirement-root-v1
nereus-delay-target-migration-root-tuple-authorization-v1
nereus-delay-target-migration-root-v1
nereus-delay-target-migration-root-v2
nereus-delay-target-migration-schedule-binding-v2
nereus-delay-target-migration-shadow-progress-v1
nereus-delay-target-root-tuple-authorization-v1
nereus-delay-target-root-tuple-authorization-signature-v1
nereus-k1-guarded-send-custody-v1
nereus-p1-source-native-custody-v1
NEREUS_DELAY_ROOT_RECOVERY_KEY_MATCH_V1
NEREUS_DELAY_ROOT_RETIREMENT_KEY_MATCH_V1
""".split())

# These classes own distinct canonical schemas registered by their type name or
# Native/META tag in PROTOCOL-REGISTRY. Only revision/bound/domain members are
# allowed, never a new V1-prefixed class/package/environment/script name.
SCHEMA_TYPES = set("""
TargetClaimRecord TargetMigrationActivateRequest TargetMigrationActivationAttempt
TargetMigrationArchiveRow TargetMigrationArchiveState TargetMigrationControlBody
TargetMigrationRoot TargetMigrationRootIssuerAuthorization
TargetPayloadSummaryInitializationRequest TargetProtocolActivatePayload
TargetProtocolActivationBody TargetProtocolActivationRecord TargetQuotaPayloadOwner
TargetScopeControlObligation TargetScopeControlObligationBody
TargetScopeControlObligationRequest TargetSloCustodyRecord TargetSloFinalEvidence
TargetSloObservationControlBody TargetSloObservationControlRequest
TargetSloObservationEventBody TargetSloObservationEventRecord
TargetSloObservationGeneration TargetSloObservationPolicy TargetSloScopeNativePlan
""".split())
SCHEMA_MEMBER = re.compile(
    r"(?:V[12]_(?:SCHEMA_VERSION|DOMAIN|SIGN|(?:MAX_)?(?:CANONICAL|INPUT|PREPARED|SOURCE|ROOT|VECTOR|MARKER)_BYTES|MAX_BYTES)"
    r"|MAX_V1_(?:CANONICAL|INPUT)_BYTES|ACTIVATE_V2_MAX_(?:CANONICAL|INPUT)_BYTES)\Z"
)
FORMAT_SELECTORS = {
    "TargetClaimRecord": {"nativeV2"},
    "TargetProtocolReaderSet": {"issuerV2"},
    "TargetSourceOperationEnvelope": {"payloadV2", "PAYLOAD_V1", "ARCHIVE_HEAD_V2", "ARCHIVE_ROW_V1"},
    "TargetRecordAccounting": {"PAYLOAD_V1", "ARCHIVE_HEAD_V2", "ARCHIVE_ROW_V1"},
}
EVIDENCE_RULES = set("""
CONDITIONAL_ORIGINAL_VALUE_WRITER_V1 NATIVE_ONLY_RESERVATION_QUERY_V1
NATIVE_OR_AUTHENTICATED_COPY_EXPORT_V1 ROOT_NATIVE_CHECKPOINT_REFERENCED_READER_V1
SOURCE_CONFIRMATION_FROM_ACTUAL_MARKER_OBSERVATION_V1
""".split())

# Registered format families, including their customary short prose names.
# This qualifies *bare revision references*, not identifiers or repository paths.
# Generic words such as "schema" or "Native" alone do not qualify a paragraph.
FORMAT_NAMES = set("""
Manifest Claim Admission Outcome Journal CommandDedupeRecord PUBLISH_ADMISSION
PUBLISH_OUTCOME SystemMutation SLO SloObservation Final Start outbox sampleId
Fence RecoveryIntent Binding ProtocolCapabilityDeclaration Catalog
TargetCheckpointInstallIntent TargetRecoveryCredentialAuthorization
TargetRecoveryProofRef TargetRecoveryProof TargetPulsarAttemptJournalRecordCodec
TargetJournalNegativeSettlement TargetKafkaReceiptBinding TargetKafkaReceiptMapping
TargetKafkaReceiptSlot TargetSloObservationSample TargetSloObservationOutbox
PulsarSourceReactivationReceipt Capability CapabilityDeclaration PayloadOwner
OxiaSyncTargetKafkaReceiptProtectionCatalog TargetSloControlScopeAuthority
""".split()) | SCHEMA_TYPES
FORMAT = re.compile(
    r"(?<![A-Za-z0-9_])(?:" + "|".join(sorted(FORMAT_NAMES, key=len, reverse=True))
    + r"|checkpoint (?:semantic snapshot|install provenance)|authority[- ]record|"
    r"assignment (?:CAS|record)|System Mutation|[Pp]ayload (?:[Oo]wner|summary)|"
    r"Native(?:32|36|53|54|59|60|61|65|69|71|72)(?:v[0-9]+)?|"
    r"legacy-v1 (?:Source|prefix)|summary (?:initialization|初始化)|outboxDigest32|TRUSTED_OBSERVATION|"
    r"range observation|evidence encoder|Scope (?:registry|Native)|Target RECOVERY|CAS record|"
    r"entry ?V2[^\n]*operation ?keys?)"
    r"(?![A-Za-z0-9_])", re.I
)
PROSE_PREFIXES = FORMAT_NAMES | set("""
v1 v2 V1 V2 schema Native Target ordinary original unchanged old legacy explicit
closed canonical conservative bounded complete exact prior previous existing
and or at of the retains preserves to from both independent Record record
image images fields bytes only preserves shared merge kind type5 settlement
entry Entry Authority authority issuer policy originalV1
Mutation snapshot provenance internal nine-field Existing Legacy merged
unchanged- persisted type-5 incoming legacy- missing observation
Native32 codec reencoding historical weakening no protected v1- v1-to-
Ref proof post-A private body reject
authority-record migration-root- migration-activation-attempt-
a authenticated enable finite live generic imported grants qualified
Reconstructs retroactive
""".split())
PROSE_PREFIXES = {word.lower() for word in PROSE_PREFIXES}
FORMAT_OWNERS = set("""
TargetPayloadSummaryInitializationMaintenance TargetPayloadSummaryLegacyPrefix
ProtocolTuple TargetPayloadObligationSummaryBootstrap TargetPublishAdmissionStore
TargetPublishOutcomeStore TargetSourceAccounting TargetCheckpointRootVerifier
TargetScopeNativeUsageAudit LegacyCheckpointStateInventory TargetRecordAccounting
PersistentSloObservationCollector PulsarClientArtifactAttemptJournal
PulsarAttemptJournalTest TargetSloCustodyReceipt
""".split()) | FORMAT_NAMES
FORMAT_DETAIL = re.compile(
    r"(?i)bytes|fields?|schema|image|canonical|codec|encoding|digest|domain|preimage|"
    r"bounds?|caps?|prefix|permit|writer|reader|summary|settlement|payer|merge|"
    r"envelope|record|tuple|proof|outbox|sample|BAD|TRUSTED_OBSERVATION|Native[0-9]+|"
    r"wire|delivery|formats?|source.work|pointfind|successor|commit|event|retained|"
    r"noFuture|ref|first|Source|live|晚帧"
)
EXTERNAL = re.compile(
    r"(?<![A-Za-z0-9_/-])transaction-v2(?![A-Za-z0-9_-])|"
    r"(?<![A-Za-z0-9_/-])/admin/v2(?=/|\b)|"
    r"pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/|"
    r"(?<![A-Za-z0-9_/-])stateDiagram-v2(?![A-Za-z0-9_-])|"
    r"(?<![A-Za-z0-9_/-])nereus/delay-(?:client-cleanup|guarded-producer|resource-guard)-v1(?![A-Za-z0-9_-])|"
    r"(?:\.\./)?oxia-client-java-worktrees/nereus-delay-client-cleanup-v1(?![A-Za-z0-9_-])|"
    r"go: downloading [A-Za-z0-9./_-]+ v[0-9]+\.[0-9]+\.[A-Za-z0-9.+_-]+|"
    r"(?:github.com/(?:cockroachdb/pebble|emirpasic/gods)|k8s.io/(?:api|client-go)|gopkg.in/yaml)"
    r"[A-Za-z0-9./_-]*|"
    r"Cluster ID was `[A-Za-z0-9_-]{22}`|"
    r"consumerName=[A-Za-z0-9]{5}(?=[,}])"
)
URL_API = re.compile(r"https?://[^/\s\"`'<>]+(/admin/v2)(?=/|\b)")


def covered(match, spans):
    return any(start <= match.start() and match.end() <= end for start, end in spans)


def violations(path, text):
    """Return individual (offset, marker) failures, never waive a whole line."""
    allowed = [(m.start(), m.end()) for m in EXTERNAL.finditer(text)]
    # The URL supplies API context, but only its API revision is exempted.
    allowed.extend(m.span(1) for m in URL_API.finditer(text))
    for token in re.finditer(r"[A-Za-z][A-Za-z0-9_-]*", text):
        if token.group() in DOMAINS and (text[max(0, token.start()-1):token.start()] in {'"', "'", "`"}
                or path == "docs/PROTOCOL-REGISTRY.md" and text[token.end():].startswith(r"\0")):
            allowed.append(token.span())
    if path == "docs/PROTOCOL-REGISTRY.md":
        # Registry describes these as suffixes of the two full domains above.
        allowed.extend(m.span() for m in re.finditer(
            r"`(?:migration-root-v2|migration-activation-attempt-v2)`", text))

    # A recognized protocol elsewhere on the line cannot excuse a project title.
    project = list(PROJECT.finditer(text))
    failures = [(m.start(), m.group()) for m in project]
    project_spans = [m.span() for m in project]
    owner = Path(path).stem
    for marker in MARKER.finditer(text):
        if covered(marker, project_spans) or covered(marker, allowed):
            continue
        before = re.search(r"[A-Za-z0-9_]*\Z", text[max(0, marker.start() - 512):marker.start()]).group()
        after = re.match(r"[A-Za-z0-9_]*", text[marker.end():]).group()
        token = before + marker.group() + after
        start = marker.start() - len(before)
        if token not in {"v1", "v2", "V1", "V2"}:
            qualifier = re.search(r"([A-Za-z][A-Za-z0-9]*)(?:\s*\.\s*Control)?\s*\.\s*\Z", text[max(0, start - 200):start])
            schema = qualifier.group(1) if qualifier else owner
            if schema == "Control":
                schema = owner
            if SCHEMA_MEMBER.fullmatch(token) and schema in SCHEMA_TYPES:
                continue
            if token in FORMAT_SELECTORS.get(owner, set()):
                continue
            if token in EVIDENCE_RULES and (owner == "TargetEvidenceObjectLifecycleAuthority"
                    or path == "docs/PROTOCOL-REGISTRY.md"
                    or re.search(r"TargetEvidenceObjectLifecycleAuthority\s*\.\s*Rule\s*\.\s*\Z",
                                 text[max(0, start - 200):start])):
                continue
            if path == "docs/PROTOCOL-REGISTRY.md" and token == "V1_MAX_" and text[start:].startswith("V1_MAX_*"):
                continue
            if path == "docs/PROTOCOL-REGISTRY.md" and token == "originalV1SampleId" and text[start+len(token):].startswith("[32]"):
                # Native59's original, unchanged schema1 sample identity key.
                continue
            if token in {"entryV2", "EntryV2", "v1PrefixPermit"} and re.search(r"scopeId|summary|legacy-v1 Source", text[max(0, start-350):start+350]):
                continue
            # NV is the registered Native ValueEnvelope kind namespace (NV1..72).
            if token in {"NV1", "NV2"}:
                continue
            failures.append((marker.start(), token))
            continue

        # Paths/compound labels are names, not a prose format revision. Local
        # registered settlement and finite-prefix labels are listed explicitly.
        compound = re.search(r"[A-Za-z0-9_./-]*\Z", text[max(0, marker.start() - 512):marker.start()]).group() + marker.group()
        compound += re.match(r"[A-Za-z0-9_./-]*", text[marker.end():]).group()
        if compound.rstrip("/") in {"settlement-v2", "legacy-v1-prefix", "v1-prefix"}:
            continue
        if re.search(r"\.(?:md|java|sh|json)\b|(?:^|/)docs/|[-_]PROTOCOL|[-_]RUNBOOK", compound, re.I):
            failures.append((marker.start(), compound))
            continue
        # Slash/hyphen prose joins may combine registered field/format names,
        # but cannot turn a package, script or arbitrary new label into prose.
        parts = re.split(r"[-/.]", compound)
        prose_parts = {"", "v1", "v2", "v3", "v4", "v5", "type1", "type5", "algorithm1",
                       "sampleId", "binding", "activation", "unchanged", "legacy", "only", "to",
                       "format", "format2", "Claim", "reserve", "Attempt", "Outcome", "assignment",
                       "Admission", "producer", "Lane", "canonical", "first", "POS", "hash", "Source", "mapping"}
        if any(part.lower() not in {p.lower() for p in prose_parts} and not part.isdigit() for part in parts):
            if not (path == "docs/PROTOCOL-REGISTRY.md" and re.fullmatch(r"reads[0-9]+v[12]/v[12]", compound)):
                failures.append((marker.start(), compound))
                continue

        # A bounded paragraph supplies the referent for "old v1 bytes", "v2
        # fields", etc. It must name a registered format, not merely say schema.
        paragraph = text.rfind("\n\n", 0, marker.start())
        left = max(paragraph + 2 if paragraph >= 0 else 0, marker.start() - 600)
        right = text.find("\n\n", marker.end())
        right = min(right if right >= 0 else len(text), marker.end() + 600)
        context = text[left:right]
        line_start = text.rfind("\n", 0, marker.start()) + 1
        line_end = text.find("\n", marker.end())
        line = text[line_start:line_end if line_end >= 0 else len(text)]
        if re.match(r"^#{1,6} ", line) and not FORMAT.search(line):
            failures.append((marker.start(), marker.group()))
            continue
        prefix = re.search(r"([A-Za-z][A-Za-z0-9_-]*)[ `]*\Z", text[left:marker.start()])
        if prefix and prefix.group(1).lower() not in PROSE_PREFIXES:
            failures.append((marker.start(), marker.group()))
            continue
        headings = list(re.finditer(r"(?m)^#{1,6} .*$", text[:marker.start()]))
        heading = headings[-1].group() if headings else ""
        local = text[max(left, marker.start()-80):min(right, marker.end()+100)]
        if (FORMAT.search(context) or FORMAT.search(heading) or owner in FORMAT_OWNERS) and (
                FORMAT.search(local) or FORMAT_DETAIL.search(local)):
            continue
        failures.append((marker.start(), marker.group()))
    return sorted(failures)


def check(root):
    failed = False
    # Path names remain checked even when their contents are gitignored. These
    # are physical repository-owned names, unlike textual external SDK locks.
    for directory, subdirectories, filenames in os.walk(root):
        if Path(directory) == root:
            subdirectories[:] = [name for name in subdirectories if name not in {".git", "build"}]
        for name in subdirectories + filenames:
            path = (Path(directory) / name).relative_to(root).as_posix()
            if PATH_MARKER.search(path) and path not in FORMAT_PATHS:
                print(f"{path}: project-wide numbered marker in repository path", file=sys.stderr)
                failed = True
    listing = subprocess.run(
        ["rg", "--files", "--hidden", "--null", "--glob", "!.git/**", "--glob", "!build/**", "."],
        cwd=root, capture_output=True,
    )
    if listing.returncode not in {0, 1}:
        listing.check_returncode()
    files = listing.stdout.split(b"\0")
    for filename in sorted(filter(None, files)):
        path = filename.decode().removeprefix("./")
        if path in SELF_FILES:
            continue
        data = (root / path).read_bytes()
        if b"\0" in data:
            continue
        text = data.decode("utf-8", errors="replace")
        for offset, marker in violations(path, text):
            line = text.count("\n", 0, offset) + 1
            print(f"{path}:{line}: forbidden project version marker: {marker}", file=sys.stderr)
            failed = True
    return int(failed)


if __name__ == "__main__":
    sys.exit(check(Path(sys.argv[1]).resolve()))
