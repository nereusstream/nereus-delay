package com.nereusstream.delay.protocol;

import java.util.Arrays;

/** Exact Target Native schema/source-lock/environment authority; it does not activate a runtime. */
public final class TargetNativeArtifactSet {
    public static final int VERSION = 1;
    public static final int SNAPSHOT_VERSION = 2;
    public static final int TARGET_STORE_FORMAT = 2;
    public static final int MAX_CANONICAL_BYTES = 2 + 34 + 34 + 11 + 2 + 2 + 2 + 34;
    private static final byte[] HASH_DOMAIN = Bytes.utf8("nereus-delay-target-native-artifacts\0");
    private final byte[] schemaBundleHash;
    private final byte[] sourceLock;
    private final long environmentResetGeneration;
    private final byte[] digest;

    public TargetNativeArtifactSet(
            final byte[] schemaBundleHash, final byte[] sourceLock, final long environmentResetGeneration) {
        this.schemaBundleHash = TargetCompatibilityCodec.assigned(schemaBundleHash, 32, "schemaBundleHash");
        this.sourceLock = TargetCompatibilityCodec.assigned(sourceLock, 32, "sourceLock");
        PulsarSourceLock.requireExact(this.sourceLock);
        if (environmentResetGeneration == 0) {
            throw new IllegalArgumentException("Target Native environment reset generation is unassigned");
        }
        this.environmentResetGeneration = environmentResetGeneration;
        digest = Bytes.sha256(HASH_DOMAIN, fields());
    }

    public byte[] schemaBundleHash() {
        return Bytes.copy(schemaBundleHash);
    }

    public byte[] sourceLock() {
        return Bytes.copy(sourceLock);
    }

    public long environmentResetGeneration() {
        return environmentResetGeneration;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, schemaBundleHash);
            CanonicalProtobuf.bytes(out, 3, sourceLock);
            CanonicalProtobuf.uint64Bits(out, 4, environmentResetGeneration);
            CanonicalProtobuf.uint32(out, 5, TargetNativePolicyScope.VERSION);
            CanonicalProtobuf.uint32(out, 6, SNAPSHOT_VERSION);
            CanonicalProtobuf.uint32(out, 7, TARGET_STORE_FORMAT);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 8, digest);
        });
    }

    public static TargetNativeArtifactSet decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 8, false, "Target Native artifacts");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8}, "Target Native artifacts");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION
                || QueryCodecSupport.uint32(fields.get(4), 5) != TargetNativePolicyScope.VERSION
                || QueryCodecSupport.uint32(fields.get(5), 6) != SNAPSHOT_VERSION
                || QueryCodecSupport.uint32(fields.get(6), 7) != TARGET_STORE_FORMAT) {
            throw new IllegalArgumentException("unknown Target Native artifact generation");
        }
        final var value = new TargetNativeArtifactSet(
                QueryCodecSupport.fixed(fields.get(1), 2, 32),
                QueryCodecSupport.fixed(fields.get(2), 3, 32),
                QueryCodecSupport.uint64Bits(fields.get(3), 4));
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(fields.get(7), 8, 32))) {
            throw new IllegalArgumentException("Target Native artifact digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target Native artifacts");
        return value;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetNativeArtifactSet that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
