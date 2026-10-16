package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Independently admitted Target generation8; parsing alone grants no Native retirement permission. */
public record TargetEvidenceRetireIntentBody(ShardId shard, long retryUntil,
        TargetExternalEvidenceObjectResource resource, long expectedResourceStateVersion,
        ResourceRetireIntentBody.ProtectionSet protections, TargetEvidenceRetirementContext context) {
    public static final int BODY_VERSION = 8;
    public static final int MAX_CANONICAL_BYTES = 64 * 1024 * 1024;
    public static final int MAX_PROTECTION_BYTES = 16 * 1024 * 1024;
    public static final int MAX_PROTECTIONS = 4096;
    public TargetEvidenceRetireIntentBody {
        Objects.requireNonNull(shard); Objects.requireNonNull(resource); Objects.requireNonNull(protections);
        Objects.requireNonNull(context).requireResource(resource);
        if (retryUntil < 0 || expectedResourceStateVersion != 0 || !shard.equals(resource.shard())
                || protections.canonicalBytes().length > MAX_PROTECTION_BYTES || protections.references().size() > MAX_PROTECTIONS)
            throw new IllegalArgumentException("invalid Target retiring state0 request");
        for (var protection : protections.references()) {
            if (protection.minimumSourcePosition().length != 0
                    && !shard.equals(TargetSourcePosition.decode(protection.minimumSourcePosition()).shardId()))
                throw new IllegalArgumentException("retirement protection belongs to another Source Shard");
        }
    }
    public ProtocolTuple requiredTuple() { return ProtocolTuple.targetEvidenceRetirement(); }
    public byte[] logicalOperationIdentity() {
        return SystemMutation.computeResourceRetireLogicalIdentity(ResourceKind.TARGET_EXTERNAL_EVIDENCE_OBJECT,
                resource.exactIdentity().identityHash(), expectedResourceStateVersion);
    }
    public byte[] canonicalBytes() {
        final var encoded = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.RESOURCE_RETIRE_INTENT.wireValue());
            CanonicalProtobuf.int64(out, 3, retryUntil); CanonicalProtobuf.uint32(out, 9, BODY_VERSION);
            CanonicalProtobuf.uint32(out, 10, ResourceKind.TARGET_EXTERNAL_EVIDENCE_OBJECT.wireValue());
            CanonicalProtobuf.bytes(out, 11, resource.exactIdentity().canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 12, expectedResourceStateVersion);
            CanonicalProtobuf.bytes(out, 13, protections.canonicalBytes()); CanonicalProtobuf.bytes(out, 14, context.canonicalBytes());
        });
        if (encoded.length > MAX_CANONICAL_BYTES) throw new IllegalArgumentException("Target retire body exceeds finite ceiling");
        return encoded;
    }
    public static TargetEvidenceRetireIntentBody decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "Target retire body");
        QueryCodecSupport.requireNumbers(f, new int[]{1,2,3,9,10,11,12,13,14}, "Target retire body");
        if (QueryCodecSupport.uint(f.get(1), 2) != SystemMutationType.RESOURCE_RETIRE_INTENT.wireValue()
                || QueryCodecSupport.uint(f.get(3), 9) != BODY_VERSION
                || QueryCodecSupport.uint(f.get(4), 10) != ResourceKind.TARGET_EXTERNAL_EVIDENCE_OBJECT.wireValue())
            throw new IllegalArgumentException("unknown Target retire operation/version/kind");
        final var value = new TargetEvidenceRetireIntentBody(decodeShard(QueryCodecSupport.nested(f.get(0), 1)),
                QueryCodecSupport.uint(f.get(2), 3), decodeResource(QueryCodecSupport.nested(f.get(5), 11)),
                QueryCodecSupport.uint64Bits(f.get(6), 12), decodeProtections(QueryCodecSupport.nested(f.get(7), 13)),
                TargetEvidenceRetirementContext.decode(QueryCodecSupport.nested(f.get(8), 14)));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target retire body"); return value;
    }
    static TargetExternalEvidenceObjectResource decodeResource(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES + 16,
                1, false, "Target resource oneof");
        QueryCodecSupport.requireNumbers(f, new int[]{8}, "Target resource oneof");
        return TargetExternalEvidenceObjectResource.decode(QueryCodecSupport.nested(f.get(0), 8));
    }
    static ShardId decodeShard(byte[] encoded) {
        TargetCompatibilityCodec.read(encoded, 24, 2, false, "Target retirement subject");
        return ShardSubject.decode(encoded).shardId();
    }
    static ResourceRetireIntentBody.ProtectionSet decodeProtections(byte[] encoded) {
        TargetCompatibilityCodec.read(encoded, MAX_PROTECTION_BYTES, MAX_PROTECTIONS + 1, true, "Target retirement protections");
        return ResourceRetireIntentBody.ProtectionSet.decodeCanonical(encoded);
    }
}
