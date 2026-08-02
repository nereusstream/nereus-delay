package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Immutable reference to the source-pinned retry policy semantic version. */
public final class RetryPolicyRefV1 {
    public static final int HASH_LENGTH = 32;

    private final byte[] policyId;
    private final long version;
    private final byte[] semanticHash;

    public RetryPolicyRefV1(final byte[] policyId, final long version, final byte[] semanticHash) {
        Objects.requireNonNull(policyId, "policyId");
        if (policyId.length == 0) {
            throw new IllegalArgumentException("policyId must not be empty");
        }
        if (version <= 0) {
            throw new IllegalArgumentException("retry policy version must be positive");
        }
        Bytes.requireLength(semanticHash, HASH_LENGTH, "retryPolicySemanticHash");
        this.policyId = Bytes.copy(policyId);
        this.version = version;
        this.semanticHash = Bytes.copy(semanticHash);
    }

    public byte[] policyId() {
        return Bytes.copy(policyId);
    }

    public long version() {
        return version;
    }

    public byte[] semanticHash() {
        return Bytes.copy(semanticHash);
    }

    /** Returns whether this reference is the exact immutable semantic value. */
    public boolean matches(final RetryPolicySemanticV1 semantic) {
        Objects.requireNonNull(semantic, "semantic");
        return version == semantic.version() && Arrays.equals(policyId, semantic.policyId())
                && Bytes.constantTimeEquals(semanticHash, semantic.semanticHash());
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, policyId);
            CanonicalProtobuf.uint64(output, 2, version);
            CanonicalProtobuf.bytes(output, 3, semanticHash);
        });
    }

    public static RetryPolicyRefV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "RetryPolicyRefV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3}, "RetryPolicyRefV1");
        final RetryPolicyRefV1 result = new RetryPolicyRefV1(
                QueryCodecSupport.bytes(fields.get(0), 1),
                QueryCodecSupport.uint(fields.get(1), 2),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RetryPolicyRefV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof RetryPolicyRefV1 that && version == that.version
                && Arrays.equals(policyId, that.policyId) && Arrays.equals(semanticHash, that.semanticHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(policyId), version, Arrays.hashCode(semanticHash));
    }
}
