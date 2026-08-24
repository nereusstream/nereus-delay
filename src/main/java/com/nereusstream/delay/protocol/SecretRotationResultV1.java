package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Public-safe result for a credential rotation control operation. */
public final class SecretRotationResultV1 {
    private final ProfileRefV1 profile;
    private final long secretGeneration;
    private final byte[] secretReferenceDigest;
    private final byte[] credentialBindingDigest;
    private final long bindingHeadRevision;
    private final byte[] bindingHeadDigest;

    public SecretRotationResultV1(
            final ProfileRefV1 profile,
            final long secretGeneration,
            final byte[] secretReferenceDigest,
            final byte[] credentialBindingDigest,
            final long bindingHeadRevision,
            final byte[] bindingHeadDigest) {
        this.profile = Objects.requireNonNull(profile, "profile");
        if (profile.profileKind() != ProfileKindV1.DESTINATION && profile.profileKind() != ProfileKindV1.OBJECT_STORE) {
            throw new IllegalArgumentException("secret rotation requires a destination or object-store Profile");
        }
        if (secretGeneration == 0 || bindingHeadRevision == 0) {
            throw new IllegalArgumentException("secret generation and binding revision must be non-zero");
        }
        this.secretGeneration = secretGeneration;
        this.secretReferenceDigest = fixed(secretReferenceDigest, "secretReferenceDigest");
        this.credentialBindingDigest = fixed(credentialBindingDigest, "credentialBindingDigest");
        this.bindingHeadRevision = bindingHeadRevision;
        this.bindingHeadDigest = fixed(bindingHeadDigest, "bindingHeadDigest");
    }

    public ProfileRefV1 profile() {
        return profile;
    }

    public long secretGeneration() {
        return secretGeneration;
    }

    public byte[] secretReferenceDigest() {
        return Bytes.copy(secretReferenceDigest);
    }

    public byte[] credentialBindingDigest() {
        return Bytes.copy(credentialBindingDigest);
    }

    public long bindingHeadRevision() {
        return bindingHeadRevision;
    }

    public byte[] bindingHeadDigest() {
        return Bytes.copy(bindingHeadDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 2, secretGeneration);
            CanonicalProtobuf.bytes(output, 3, secretReferenceDigest);
            CanonicalProtobuf.bytes(output, 4, credentialBindingDigest);
            CanonicalProtobuf.uint64Bits(output, 5, bindingHeadRevision);
            CanonicalProtobuf.bytes(output, 6, bindingHeadDigest);
        });
    }

    public static SecretRotationResultV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "SecretRotationResultV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6}, "SecretRotationResultV1");
        final SecretRotationResultV1 result = new SecretRotationResultV1(
                ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.uint(fields.get(1), 2),
                QueryCodecSupport.fixed(fields.get(2), 3, 32),
                QueryCodecSupport.fixed(fields.get(3), 4, 32),
                QueryCodecSupport.uint64Bits(fields.get(4), 5),
                QueryCodecSupport.fixed(fields.get(5), 6, 32));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "SecretRotationResultV1");
        return result;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, 32, name);
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SecretRotationResultV1 that
                && secretGeneration == that.secretGeneration
                && bindingHeadRevision == that.bindingHeadRevision
                && profile.equals(that.profile)
                && Arrays.equals(secretReferenceDigest, that.secretReferenceDigest)
                && Arrays.equals(credentialBindingDigest, that.credentialBindingDigest)
                && Arrays.equals(bindingHeadDigest, that.bindingHeadDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                profile,
                secretGeneration,
                Arrays.hashCode(secretReferenceDigest),
                Arrays.hashCode(credentialBindingDigest),
                bindingHeadRevision,
                Arrays.hashCode(bindingHeadDigest));
    }
}
