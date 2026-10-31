package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Generation8 keeps original intent state0 separate from state1, with distinct deletion and retained-marker evidence.
 */
public record TargetEvidenceDeleteConfirmedBody(
        ShardId shard,
        long retryUntil,
        ResourceDeleteConfirmedBody.RetireIntentRef intent,
        ResourceDeleteConfirmedBody.DeleteOutcome outcome,
        ResourceDeleteConfirmedBody.ExternalDeleteEvidence evidence,
        TrustedUtcIntervalEvidence confirmedAt,
        TargetExternalEvidenceObjectResource resource,
        TargetEvidenceRetirementContext context,
        long expectedRetireStateVersion,
        TargetEvidenceRetiredMarkerEvidence markerEvidence) {
    private static final int MAX_LEGACY_EVIDENCE_BYTES = 4096;
    public static final int MAX_DELETE_EVIDENCE_BYTES =
            Math.max(MAX_LEGACY_EVIDENCE_BYTES, TargetEvidenceRetiredMarkerEvidence.MAX_BYTES);
    public static final int MAX_TIME_BYTES = TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES;

    public TargetEvidenceDeleteConfirmedBody(
            ShardId shard,
            long retryUntil,
            ResourceDeleteConfirmedBody.RetireIntentRef intent,
            ResourceDeleteConfirmedBody.DeleteOutcome outcome,
            ResourceDeleteConfirmedBody.ExternalDeleteEvidence evidence,
            TrustedUtcIntervalEvidence confirmedAt,
            TargetExternalEvidenceObjectResource resource,
            TargetEvidenceRetirementContext context,
            long expectedRetireStateVersion) {
        this(
                shard,
                retryUntil,
                intent,
                outcome,
                evidence,
                confirmedAt,
                resource,
                context,
                expectedRetireStateVersion,
                null);
    }

    public static TargetEvidenceDeleteConfirmedBody markerRetained(
            ShardId shard,
            long retryUntil,
            ResourceDeleteConfirmedBody.RetireIntentRef intent,
            TargetEvidenceRetiredMarkerEvidence evidence,
            TrustedUtcIntervalEvidence confirmedAt,
            TargetExternalEvidenceObjectResource resource,
            TargetEvidenceRetirementContext context) {
        return new TargetEvidenceDeleteConfirmedBody(
                shard,
                retryUntil,
                intent,
                ResourceDeleteConfirmedBody.DeleteOutcome.PAYLOAD_RETIRED_MARKER_RETAINED,
                null,
                confirmedAt,
                resource,
                context,
                1,
                evidence);
    }

    public TargetEvidenceDeleteConfirmedBody {
        Objects.requireNonNull(shard);
        Objects.requireNonNull(intent);
        Objects.requireNonNull(outcome);
        Objects.requireNonNull(confirmedAt);
        Objects.requireNonNull(resource);
        Objects.requireNonNull(context).requireResource(resource);
        if (retryUntil < 0
                || expectedRetireStateVersion != 1
                || intent.expectedResourceStateVersion() != 0
                || !shard.equals(resource.shard())
                || !Arrays.equals(
                        intent.resourceIdentityHash(), resource.exactIdentity().identityHash())) {
            throw new IllegalArgumentException("invalid Target state1 confirmation/intent binding");
        }
        if (confirmedAt.canonicalBytes().length > MAX_TIME_BYTES) {
            throw new IllegalArgumentException("Target confirmation time exceeds finite bound");
        }
        if (outcome == ResourceDeleteConfirmedBody.DeleteOutcome.PAYLOAD_RETIRED_MARKER_RETAINED) {
            if (evidence != null) {
                throw new IllegalArgumentException("marker confirmation cannot carry legacy deletion evidence");
            }
            Objects.requireNonNull(markerEvidence).marker().requireBindings(resource, context, intent);
            confirmedAt.requireEarliestAtLeast(markerEvidence.observedAt().latestEpochMs());
        } else {
            Objects.requireNonNull(evidence);
            if (markerEvidence != null
                    || evidence.outcome() != outcome
                    || !Arrays.equals(intent.resourceIdentityHash(), evidence.resourceIdentityHash())) {
                throw new IllegalArgumentException("Target legacy confirmation changed evidence branch/identity");
            }
            confirmedAt.requireEarliestAtLeast(evidence.observedAt().latestEpochMs());
            if (evidence.observedAt().canonicalBytes().length > MAX_TIME_BYTES
                    || evidence.canonicalBytes().length > MAX_LEGACY_EVIDENCE_BYTES
                    || evidence.observedImmutableVersion().length
                            > TargetExternalEvidenceObjectResource.MAX_VERSION_BYTES) {
                throw new IllegalArgumentException("Target deletion evidence/time exceeds finite bound");
            }
            if (evidence.observedEtag().length != 0) {
                throw new IllegalArgumentException("Catalog object has no etag identity");
            }
            ResourceRetireIntentBody.validateExternalDeleteIdentity(
                    ResourceKind.TARGET_EXTERNAL_EVIDENCE_OBJECT,
                    resource.exactIdentity().canonicalBytes(),
                    evidence.observedImmutableVersion(),
                    evidence.observedEtag(),
                    outcome);
        }
    }

    public boolean markerRetained() {
        return outcome == ResourceDeleteConfirmedBody.DeleteOutcome.PAYLOAD_RETIRED_MARKER_RETAINED;
    }

    public ProtocolTuple requiredTuple() {
        return ProtocolTuple.targetEvidenceRetirement();
    }

    public byte[] logicalOperationIdentity() {
        return intent.mutationId();
    }

    public byte[] canonicalBytes() {
        final var encoded = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.RESOURCE_DELETE_CONFIRMED.wireValue());
            CanonicalProtobuf.int64(out, 3, retryUntil);
            CanonicalProtobuf.uint32(out, 9, TargetEvidenceRetireIntentBody.BODY_VERSION);
            CanonicalProtobuf.bytes(out, 10, intent.canonicalBytes());
            CanonicalProtobuf.uint32(out, 11, outcome.wireValue());
            CanonicalProtobuf.bytes(
                    out, 12, markerRetained() ? markerEvidence.canonicalBytes() : evidence.canonicalBytes());
            CanonicalProtobuf.bytes(out, 13, confirmedAt.canonicalBytes());
            CanonicalProtobuf.bytes(out, 14, resource.exactIdentity().canonicalBytes());
            CanonicalProtobuf.bytes(out, 15, context.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 16, expectedRetireStateVersion);
        });
        if (encoded.length > TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target confirmation body exceeds finite ceiling");
        }
        return encoded;
    }

    public static TargetEvidenceDeleteConfirmedBody decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded, TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES, 11, false, "Target confirmation body");
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 9, 10, 11, 12, 13, 14, 15, 16}, "Target confirmation body");
        if (QueryCodecSupport.uint(f.get(1), 2) != SystemMutationType.RESOURCE_DELETE_CONFIRMED.wireValue()
                || QueryCodecSupport.uint(f.get(3), 9) != TargetEvidenceRetireIntentBody.BODY_VERSION) {
            throw new IllegalArgumentException("unknown Target confirmation operation/version");
        }
        final var outcome = ResourceDeleteConfirmedBody.DeleteOutcome.fromWire(QueryCodecSupport.uint(f.get(5), 11));
        final boolean marker = outcome == ResourceDeleteConfirmedBody.DeleteOutcome.PAYLOAD_RETIRED_MARKER_RETAINED;
        final byte[] evidenceBytes = QueryCodecSupport.nested(f.get(6), 12);
        final var value = new TargetEvidenceDeleteConfirmedBody(
                TargetEvidenceRetireIntentBody.decodeShard(QueryCodecSupport.nested(f.get(0), 1)),
                QueryCodecSupport.uint(f.get(2), 3),
                decodeIntent(QueryCodecSupport.nested(f.get(4), 10)),
                outcome,
                marker ? null : decodeLegacyEvidence(evidenceBytes),
                decodeTime(QueryCodecSupport.nested(f.get(7), 13)),
                TargetEvidenceRetireIntentBody.decodeResource(QueryCodecSupport.nested(f.get(8), 14)),
                TargetEvidenceRetirementContext.decode(QueryCodecSupport.nested(f.get(9), 15)),
                QueryCodecSupport.uint64Bits(f.get(10), 16),
                marker ? TargetEvidenceRetiredMarkerEvidence.decode(evidenceBytes) : null);
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target confirmation body");
        return value;
    }

    static ResourceDeleteConfirmedBody.RetireIntentRef decodeIntent(byte[] encoded) {
        TargetCompatibilityCodec.read(encoded, 128, 4, false, "Target original retire reference");
        return ResourceDeleteConfirmedBody.RetireIntentRef.decode(encoded);
    }

    static void decodeEvidence(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded, MAX_DELETE_EVIDENCE_BYTES, 10, false, "Target provider evidence");
        if (!f.isEmpty() && f.getFirst().number() == 1 && f.getFirst().wireType() == 0) {
            TargetEvidenceRetiredMarkerEvidence.decode(encoded);
            return;
        }
        decodeLegacyEvidence(encoded);
    }

    private static ResourceDeleteConfirmedBody.ExternalDeleteEvidence decodeLegacyEvidence(byte[] encoded) {
        final var f =
                TargetCompatibilityCodec.read(encoded, MAX_LEGACY_EVIDENCE_BYTES, 7, false, "Target delete evidence");
        if (f.size() < 5 || f.getLast().number() != 7) {
            throw new IllegalArgumentException("incomplete Target delete evidence");
        }
        decodeTime(QueryCodecSupport.nested(f.getLast(), 7));
        return ResourceDeleteConfirmedBody.ExternalDeleteEvidence.decode(encoded);
    }

    static TrustedUtcIntervalEvidence decodeTime(byte[] encoded) {
        TargetCompatibilityCodec.read(encoded, MAX_TIME_BYTES, 10, false, "Target delete time evidence");
        return TrustedUtcIntervalEvidence.decode(encoded);
    }
}
