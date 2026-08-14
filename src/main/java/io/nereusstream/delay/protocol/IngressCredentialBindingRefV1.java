package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Safe, immutable reference to the credential binding authorized by a Route. */
public final class IngressCredentialBindingRefV1 {
    private static final int HASH_LENGTH = 32;

    private final byte[] bindingId;
    private final long generation;
    private final byte[] bindingDigest;
    private final byte[] resolvedCredentialFingerprintDigest;
    private final byte[] authorizationScopeDigest;

    public IngressCredentialBindingRefV1(final byte[] bindingId, final long generation,
                                         final byte[] bindingDigest,
                                         final byte[] resolvedCredentialFingerprintDigest,
                                         final byte[] authorizationScopeDigest) {
        this.bindingId = nonZero(bindingId, "bindingId");
        if (generation == 0) {
            throw new IllegalArgumentException("credential binding generation must be nonzero");
        }
        this.generation = generation;
        this.bindingDigest = nonZero(bindingDigest, "bindingDigest");
        this.resolvedCredentialFingerprintDigest = nonZero(resolvedCredentialFingerprintDigest,
                "resolvedCredentialFingerprintDigest");
        this.authorizationScopeDigest = nonZero(authorizationScopeDigest, "authorizationScopeDigest");
    }

    public byte[] bindingId() {
        return Bytes.copy(bindingId);
    }

    public long generation() {
        return generation;
    }

    public byte[] bindingDigest() {
        return Bytes.copy(bindingDigest);
    }

    public byte[] resolvedCredentialFingerprintDigest() {
        return Bytes.copy(resolvedCredentialFingerprintDigest);
    }

    public byte[] authorizationScopeDigest() {
        return Bytes.copy(authorizationScopeDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, bindingId);
            CanonicalProtobuf.uint64Bits(output, 2, generation);
            CanonicalProtobuf.bytes(output, 3, bindingDigest);
            CanonicalProtobuf.bytes(output, 4, resolvedCredentialFingerprintDigest);
            CanonicalProtobuf.bytes(output, 5, authorizationScopeDigest);
        });
    }

    public static IngressCredentialBindingRefV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "IngressCredentialBindingRefV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5}, "IngressCredentialBindingRefV1");
        final IngressCredentialBindingRefV1 result = new IngressCredentialBindingRefV1(
                QueryCodecSupport.fixed(fields.get(0), 1, HASH_LENGTH),
                QueryCodecSupport.uint64Bits(fields.get(1), 2),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(4), 5, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "IngressCredentialBindingRefV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof IngressCredentialBindingRefV1 that && generation == that.generation
                && Arrays.equals(bindingId, that.bindingId) && Arrays.equals(bindingDigest, that.bindingDigest)
                && Arrays.equals(resolvedCredentialFingerprintDigest, that.resolvedCredentialFingerprintDigest)
                && Arrays.equals(authorizationScopeDigest, that.authorizationScopeDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(bindingId), generation, Arrays.hashCode(bindingDigest),
                Arrays.hashCode(resolvedCredentialFingerprintDigest), Arrays.hashCode(authorizationScopeDigest));
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        for (byte item : value) {
            if (item != 0) {
                return Bytes.copy(value);
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
