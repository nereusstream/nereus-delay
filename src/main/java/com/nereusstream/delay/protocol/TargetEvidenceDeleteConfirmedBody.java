package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Generation8 keeps the original intent's expectedVersion0 separate from current retiring state1. */
public record TargetEvidenceDeleteConfirmedBody(ShardId shard, long retryUntil,
        ResourceDeleteConfirmedBody.RetireIntentRef intent, ResourceDeleteConfirmedBody.DeleteOutcome outcome,
        ResourceDeleteConfirmedBody.ExternalDeleteEvidence evidence, TrustedUtcIntervalEvidence confirmedAt,
        TargetExternalEvidenceObjectResource resource, TargetEvidenceRetirementContext context, long expectedRetireStateVersion) {
    public static final int MAX_DELETE_EVIDENCE_BYTES = 4096;
    public static final int MAX_TIME_BYTES = TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES;
    public TargetEvidenceDeleteConfirmedBody {
        Objects.requireNonNull(shard); Objects.requireNonNull(intent); Objects.requireNonNull(outcome);
        Objects.requireNonNull(evidence); Objects.requireNonNull(confirmedAt); Objects.requireNonNull(resource);
        Objects.requireNonNull(context).requireResource(resource);
        if (retryUntil < 0 || expectedRetireStateVersion != 1 || intent.expectedResourceStateVersion() != 0
                || !shard.equals(resource.shard()) || evidence.outcome() != outcome
                || !Arrays.equals(intent.resourceIdentityHash(), resource.exactIdentity().identityHash())
                || !Arrays.equals(intent.resourceIdentityHash(), evidence.resourceIdentityHash()))
            throw new IllegalArgumentException("invalid Target state1 confirmation/intent binding");
        confirmedAt.requireEarliestAtLeast(evidence.observedAt().latestEpochMs());
        if (confirmedAt.canonicalBytes().length > MAX_TIME_BYTES || evidence.observedAt().canonicalBytes().length > MAX_TIME_BYTES
                || evidence.canonicalBytes().length > MAX_DELETE_EVIDENCE_BYTES
                || evidence.observedImmutableVersion().length > TargetExternalEvidenceObjectResource.MAX_VERSION_BYTES)
            throw new IllegalArgumentException("Target deletion evidence/time exceeds finite bound");
        if (evidence.observedEtag().length != 0) throw new IllegalArgumentException("Catalog object has no etag identity");
        ResourceRetireIntentBody.validateExternalDeleteIdentity(ResourceKind.TARGET_EXTERNAL_EVIDENCE_OBJECT,
                resource.exactIdentity().canonicalBytes(), evidence.observedImmutableVersion(), evidence.observedEtag(), outcome);
    }
    public ProtocolTuple requiredTuple() { return ProtocolTuple.targetEvidenceRetirement(); }
    public byte[] logicalOperationIdentity() { return intent.mutationId(); }
    public byte[] canonicalBytes() {
        final var encoded = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.RESOURCE_DELETE_CONFIRMED.wireValue());
            CanonicalProtobuf.int64(out, 3, retryUntil); CanonicalProtobuf.uint32(out, 9, TargetEvidenceRetireIntentBody.BODY_VERSION);
            CanonicalProtobuf.bytes(out, 10, intent.canonicalBytes()); CanonicalProtobuf.uint32(out, 11, outcome.wireValue());
            CanonicalProtobuf.bytes(out, 12, evidence.canonicalBytes()); CanonicalProtobuf.bytes(out, 13, confirmedAt.canonicalBytes());
            CanonicalProtobuf.bytes(out, 14, resource.exactIdentity().canonicalBytes());
            CanonicalProtobuf.bytes(out, 15, context.canonicalBytes()); CanonicalProtobuf.uint64Bits(out, 16, expectedRetireStateVersion);
        });
        if (encoded.length > TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES)
            throw new IllegalArgumentException("Target confirmation body exceeds finite ceiling");
        return encoded;
    }
    public static TargetEvidenceDeleteConfirmedBody decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES, 11,
                false, "Target confirmation body");
        QueryCodecSupport.requireNumbers(f, new int[]{1,2,3,9,10,11,12,13,14,15,16}, "Target confirmation body");
        if (QueryCodecSupport.uint(f.get(1), 2) != SystemMutationType.RESOURCE_DELETE_CONFIRMED.wireValue()
                || QueryCodecSupport.uint(f.get(3), 9) != TargetEvidenceRetireIntentBody.BODY_VERSION)
            throw new IllegalArgumentException("unknown Target confirmation operation/version");
        final var value = new TargetEvidenceDeleteConfirmedBody(TargetEvidenceRetireIntentBody.decodeShard(QueryCodecSupport.nested(f.get(0), 1)),
                QueryCodecSupport.uint(f.get(2), 3), decodeIntent(QueryCodecSupport.nested(f.get(4), 10)),
                ResourceDeleteConfirmedBody.DeleteOutcome.fromWire(QueryCodecSupport.uint(f.get(5), 11)),
                decodeEvidence(QueryCodecSupport.nested(f.get(6), 12)), decodeTime(QueryCodecSupport.nested(f.get(7), 13)),
                TargetEvidenceRetireIntentBody.decodeResource(QueryCodecSupport.nested(f.get(8), 14)),
                TargetEvidenceRetirementContext.decode(QueryCodecSupport.nested(f.get(9), 15)), QueryCodecSupport.uint64Bits(f.get(10), 16));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target confirmation body"); return value;
    }
    static ResourceDeleteConfirmedBody.RetireIntentRef decodeIntent(byte[] encoded) {
        TargetCompatibilityCodec.read(encoded, 128, 4, false, "Target original retire reference");
        return ResourceDeleteConfirmedBody.RetireIntentRef.decode(encoded);
    }
    static ResourceDeleteConfirmedBody.ExternalDeleteEvidence decodeEvidence(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_DELETE_EVIDENCE_BYTES, 7, false, "Target delete evidence");
        if (f.size() < 5 || f.getLast().number() != 7)
            throw new IllegalArgumentException("incomplete Target delete evidence");
        decodeTime(QueryCodecSupport.nested(f.getLast(), 7));
        return ResourceDeleteConfirmedBody.ExternalDeleteEvidence.decode(encoded);
    }
    static TrustedUtcIntervalEvidence decodeTime(byte[] encoded) {
        TargetCompatibilityCodec.read(encoded, MAX_TIME_BYTES, 10, false, "Target delete time evidence");
        return TrustedUtcIntervalEvidence.decode(encoded);
    }
}
