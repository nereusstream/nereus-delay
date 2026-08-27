package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable reference to the exact signed policy head observed by a Claim. */
public final class HandoffPolicyHeadRef {
    public static final int HASH_LENGTH = 32;

    private final byte[] scopeDigest;
    private final long generation;
    private final byte[] snapshotDigest;
    private final long oxiaVersion;

    public HandoffPolicyHeadRef(
            final byte[] scopeDigest, final long generation, final byte[] snapshotDigest, final long oxiaVersion) {
        this.scopeDigest = fixed(scopeDigest, "scopeDigest");
        if (generation == 0) {
            throw new IllegalArgumentException("policy head generation must be non-zero");
        }
        if (oxiaVersion < 0) {
            throw new IllegalArgumentException("oxiaVersion must be non-negative");
        }
        this.generation = generation;
        this.snapshotDigest = fixed(snapshotDigest, "snapshotDigest");
        this.oxiaVersion = oxiaVersion;
    }

    public byte[] scopeDigest() {
        return Bytes.copy(scopeDigest);
    }

    public long generation() {
        return generation;
    }

    public byte[] snapshotDigest() {
        return Bytes.copy(snapshotDigest);
    }

    public long oxiaVersion() {
        return oxiaVersion;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, scopeDigest);
            CanonicalProtobuf.uint64Bits(output, 2, generation);
            CanonicalProtobuf.bytes(output, 3, snapshotDigest);
            CanonicalProtobuf.uint64Bits(output, 4, oxiaVersion);
        });
    }

    public static HandoffPolicyHeadRef decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "HandoffPolicyHeadRef");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "HandoffPolicyHeadRef");
        final HandoffPolicyHeadRef result = new HandoffPolicyHeadRef(
                QueryCodecSupport.fixed(fields.get(0), 1, HASH_LENGTH),
                QueryCodecSupport.uint64Bits(fields.get(1), 2),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                QueryCodecSupport.uint64Bits(fields.get(3), 4));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "HandoffPolicyHeadRef");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof HandoffPolicyHeadRef that
                && generation == that.generation
                && oxiaVersion == that.oxiaVersion
                && Arrays.equals(scopeDigest, that.scopeDigest)
                && Arrays.equals(snapshotDigest, that.snapshotDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(scopeDigest), generation, Arrays.hashCode(snapshotDigest), oxiaVersion);
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }
}
