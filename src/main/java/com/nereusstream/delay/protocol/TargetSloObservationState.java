package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Current ordered certification head; stopped generations and samples remain separate retained records. */
public record TargetSloObservationState(TargetQuotaScope scope, byte[] lineage, long generation, boolean active,
        byte[] controlIdentity, byte[] controlDigest, TargetQuotaMutation mutation) {
    public static final int VALUE_TYPE = 57;
    public static final int MAX_CANONICAL_BYTES = TargetQuotaScope.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 192;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-observation-state\0");

    public TargetSloObservationState {
        Objects.requireNonNull(scope); Objects.requireNonNull(mutation);
        lineage = TargetCompatibilityCodec.assigned(lineage, 16, "Target SLO state lineage");
        controlIdentity = TargetCompatibilityCodec.assigned(controlIdentity, 32, "Target SLO current Control identity");
        controlDigest = TargetCompatibilityCodec.assigned(controlDigest, 32, "Target SLO generation Control digest");
        mutation.requireSourceApplied();
        if (scope.target() != null || generation == 0 || !scope.shard().equals(mutation.source().shardId())) throw new IllegalArgumentException("invalid Target SLO current Source scope");
    }
    public static TargetSloObservationState from(TargetSloObservationGeneration generation) {
        final var policy = generation.policy();
        return new TargetSloObservationState(policy.scope(), policy.lineage(), generation.generation(), generation.active(),
                generation.control().body().logicalIdentity(), generation.controlDigest(), generation.control().appliedAt());
    }
    @Override public byte[] lineage() { return Bytes.copy(lineage); }
    @Override public byte[] controlIdentity() { return Bytes.copy(controlIdentity); }
    @Override public byte[] controlDigest() { return Bytes.copy(controlDigest); }
    public byte[] key() { return key(scope); }
    public static byte[] key(TargetQuotaScope scope) {
        if (scope.target() != null) throw new IllegalArgumentException("Target SLO state requires Source scope");
        return Bytes.concat(new byte[]{TargetKeyCodec.SLO_STATE_TAG, TargetKeyCodec.KEY_FORMAT}, scope.keySuffix());
    }
    public void requireGeneration(TargetSloObservationGeneration original) {
        if (!Arrays.equals(canonicalBytes(), from(original).canonicalBytes())) throw new IllegalStateException("Target SLO state lost exact original Control head");
    }
    private byte[] fields() { return CanonicalProtobuf.message(out -> {
        CanonicalProtobuf.uint32(out, 1, 1);
        CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
        CanonicalProtobuf.bytes(out, 3, lineage);
        CanonicalProtobuf.uint64Bits(out, 4, generation);
        CanonicalProtobuf.uint32(out, 5, active ? 1 : 2);
        CanonicalProtobuf.bytes(out, 6, controlIdentity);
        CanonicalProtobuf.bytes(out, 7, controlDigest);
        CanonicalProtobuf.bytes(out, 8, mutation.canonicalBytes());
    }); }
    public byte[] digest() { return Bytes.sha256(DOMAIN, fields()); }
    public byte[] canonicalBytes() { return Bytes.concat(fields(), CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 9, digest()))); }
    public static TargetSloObservationState decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "Target SLO current state");
        QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, "Target SLO current state");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) throw new IllegalArgumentException("unknown Target SLO state schema");
        final boolean active = switch (QueryCodecSupport.uint32(f.get(4), 5)) {
            case 1 -> true; case 2 -> false; default -> throw new IllegalArgumentException("unknown Target SLO state phase");
        };
        final var result = new TargetSloObservationState(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 16), QueryCodecSupport.uint64Bits(f.get(3), 4), active,
                QueryCodecSupport.fixed(f.get(5), 6, 32), QueryCodecSupport.fixed(f.get(6), 7, 32), TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(7), 8)));
        if (!Arrays.equals(result.digest(), QueryCodecSupport.fixed(f.get(8), 9, 32))) throw new IllegalArgumentException("Target SLO state digest mismatch");
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target SLO current state");
        return result;
    }
    public static TargetSloObservationState decodeForStore(byte[] key, byte[] encoded, TargetQuotaScope scope, byte[] lineage) {
        final var result = decode(encoded);
        if (!scope.equals(result.scope) || !Arrays.equals(lineage, result.lineage) || !Arrays.equals(key, result.key())) throw new IllegalArgumentException("Target SLO state changed key/scope/lineage");
        return result;
    }
}
