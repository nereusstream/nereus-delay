package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Mutable-pointer projection for the current immutable credential binding generation. */
public final class CredentialBindingHead {
    public static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-credential-binding-head\0");

    private final ProfileRef profile;
    private final long secretGeneration;
    private final byte[] bindingDigest;
    private final long headRevision;
    private final byte[] headDigest;

    public CredentialBindingHead(
            final ProfileRef profile,
            final long secretGeneration,
            final byte[] bindingDigest,
            final long headRevision,
            final byte[] headDigest) {
        this.profile = requireBindableProfile(profile);
        this.secretGeneration = nonZero(secretGeneration, "secretGeneration");
        this.bindingDigest = fixed(bindingDigest, "bindingDigest");
        this.headRevision = nonZero(headRevision, "headRevision");
        this.headDigest = fixed(headDigest, "headDigest");
        if (!Bytes.constantTimeEquals(this.headDigest, digestForFields())) {
            throw new IllegalArgumentException("CredentialBindingHead digest mismatch");
        }
    }

    public static CredentialBindingHead create(
            final ProfileRef profile,
            final long secretGeneration,
            final byte[] bindingDigest,
            final long headRevision) {
        final byte[] digest = digestForFields(profile, secretGeneration, bindingDigest, headRevision);
        return new CredentialBindingHead(profile, secretGeneration, bindingDigest, headRevision, digest);
    }

    public static CredentialBindingHead forBinding(final CredentialBinding binding, final long headRevision) {
        Objects.requireNonNull(binding, "binding");
        return create(binding.profile(), binding.secretGeneration(), binding.bindingDigest(), headRevision);
    }

    public static CredentialBindingHead decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "CredentialBindingHead");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5}, "CredentialBindingHead");
        final CredentialBindingHead result = new CredentialBindingHead(
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                nonZero(QueryCodecSupport.uint(fields.get(1), 2), "secretGeneration"),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                nonZero(QueryCodecSupport.uint64Bits(fields.get(3), 4), "headRevision"),
                QueryCodecSupport.fixed(fields.get(4), 5, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CredentialBindingHead");
        return result;
    }

    public ProfileRef profile() {
        return profile;
    }

    public long secretGeneration() {
        return secretGeneration;
    }

    public byte[] bindingDigest() {
        return Bytes.copy(bindingDigest);
    }

    public long headRevision() {
        return headRevision;
    }

    public byte[] headDigest() {
        return Bytes.copy(headDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 2, secretGeneration);
            CanonicalProtobuf.bytes(output, 3, bindingDigest);
            CanonicalProtobuf.uint64Bits(output, 4, headRevision);
            CanonicalProtobuf.bytes(output, 5, headDigest);
        });
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CredentialBindingHead that
                && secretGeneration == that.secretGeneration
                && headRevision == that.headRevision
                && profile.equals(that.profile)
                && Arrays.equals(bindingDigest, that.bindingDigest)
                && Arrays.equals(headDigest, that.headDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                profile, secretGeneration, Arrays.hashCode(bindingDigest), headRevision, Arrays.hashCode(headDigest));
    }

    private byte[] digestForFields() {
        return digestForFields(profile, secretGeneration, bindingDigest, headRevision);
    }

    private static byte[] digestForFields(
            final ProfileRef profile,
            final long secretGeneration,
            final byte[] bindingDigest,
            final long headRevision) {
        final byte[] fields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 2, secretGeneration);
            CanonicalProtobuf.bytes(output, 3, bindingDigest);
            CanonicalProtobuf.uint64Bits(output, 4, headRevision);
        });
        return Bytes.sha256(DIGEST_DOMAIN, fields);
    }

    private static ProfileRef requireBindableProfile(final ProfileRef value) {
        final ProfileRef profile = Objects.requireNonNull(value, "profile");
        if (profile.profileKind() != ProfileKind.DESTINATION && profile.profileKind() != ProfileKind.OBJECT_STORE) {
            throw new IllegalArgumentException("credential binding profile must be DESTINATION or OBJECT_STORE");
        }
        return profile;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static long nonZero(final long value, final String name) {
        if (value == 0) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return value;
    }
}
