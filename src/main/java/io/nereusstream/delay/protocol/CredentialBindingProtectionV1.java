package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Monotonic protection projection for one immutable credential binding generation. */
public final class CredentialBindingProtectionV1 {
    public static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-credential-binding-protection-v1\0");

    private final ProfileRefV1 profile;
    private final long secretGeneration;
    private final byte[] bindingDigest;
    private final long managedChannelProtectionUntilEpochMs;
    private final long objectStoreLeaseProtectionUntilEpochMs;
    private final long nativeCapabilityProtectionUntilEpochMs;
    private final long uploadHandleProtectionUntilEpochMs;
    private final long protectionRevision;
    private final byte[] protectionDigest;

    public CredentialBindingProtectionV1(final ProfileRefV1 profile, final long secretGeneration,
                                         final byte[] bindingDigest,
                                         final long managedChannelProtectionUntilEpochMs,
                                         final long objectStoreLeaseProtectionUntilEpochMs,
                                         final long nativeCapabilityProtectionUntilEpochMs,
                                         final long uploadHandleProtectionUntilEpochMs,
                                         final long protectionRevision, final byte[] protectionDigest) {
        this.profile = requireBindableProfile(profile);
        this.secretGeneration = positive(secretGeneration, "secretGeneration");
        this.bindingDigest = fixed(bindingDigest, "bindingDigest");
        this.managedChannelProtectionUntilEpochMs = nonNegative(managedChannelProtectionUntilEpochMs,
                "managedChannelProtectionUntilEpochMs");
        this.objectStoreLeaseProtectionUntilEpochMs = nonNegative(objectStoreLeaseProtectionUntilEpochMs,
                "objectStoreLeaseProtectionUntilEpochMs");
        this.nativeCapabilityProtectionUntilEpochMs = nonNegative(nativeCapabilityProtectionUntilEpochMs,
                "nativeCapabilityProtectionUntilEpochMs");
        this.uploadHandleProtectionUntilEpochMs = nonNegative(uploadHandleProtectionUntilEpochMs,
                "uploadHandleProtectionUntilEpochMs");
        this.protectionRevision = positive(protectionRevision, "protectionRevision");
        this.protectionDigest = fixed(protectionDigest, "protectionDigest");
        if (!Bytes.constantTimeEquals(this.protectionDigest, digestForFields())) {
            throw new IllegalArgumentException("CredentialBindingProtectionV1 digest mismatch");
        }
    }

    public static CredentialBindingProtectionV1 create(final ProfileRefV1 profile, final long secretGeneration,
                                                      final byte[] bindingDigest,
                                                      final long managedChannelProtectionUntilEpochMs,
                                                      final long objectStoreLeaseProtectionUntilEpochMs,
                                                      final long nativeCapabilityProtectionUntilEpochMs,
                                                      final long uploadHandleProtectionUntilEpochMs,
                                                      final long protectionRevision) {
        final byte[] digest = digestForFields(profile, secretGeneration, bindingDigest,
                managedChannelProtectionUntilEpochMs, objectStoreLeaseProtectionUntilEpochMs,
                nativeCapabilityProtectionUntilEpochMs, uploadHandleProtectionUntilEpochMs, protectionRevision);
        return new CredentialBindingProtectionV1(profile, secretGeneration, bindingDigest,
                managedChannelProtectionUntilEpochMs, objectStoreLeaseProtectionUntilEpochMs,
                nativeCapabilityProtectionUntilEpochMs, uploadHandleProtectionUntilEpochMs, protectionRevision,
                digest);
    }

    public static CredentialBindingProtectionV1 forBinding(final CredentialBindingV1 binding,
                                                           final long managedChannelProtectionUntilEpochMs,
                                                           final long objectStoreLeaseProtectionUntilEpochMs,
                                                           final long nativeCapabilityProtectionUntilEpochMs,
                                                           final long uploadHandleProtectionUntilEpochMs,
                                                           final long protectionRevision) {
        Objects.requireNonNull(binding, "binding");
        return create(binding.profile(), binding.secretGeneration(), binding.bindingDigest(),
                managedChannelProtectionUntilEpochMs, objectStoreLeaseProtectionUntilEpochMs,
                nativeCapabilityProtectionUntilEpochMs, uploadHandleProtectionUntilEpochMs, protectionRevision);
    }

    public static CredentialBindingProtectionV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "CredentialBindingProtectionV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9},
                "CredentialBindingProtectionV1");
        final CredentialBindingProtectionV1 result = new CredentialBindingProtectionV1(
                ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                positive(QueryCodecSupport.uint(fields.get(1), 2), "secretGeneration"),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                nonNegative(QueryCodecSupport.uint(fields.get(3), 4), "managedChannelProtectionUntilEpochMs"),
                nonNegative(QueryCodecSupport.uint(fields.get(4), 5), "objectStoreLeaseProtectionUntilEpochMs"),
                nonNegative(QueryCodecSupport.uint(fields.get(5), 6), "nativeCapabilityProtectionUntilEpochMs"),
                nonNegative(QueryCodecSupport.uint(fields.get(6), 7), "uploadHandleProtectionUntilEpochMs"),
                positive(QueryCodecSupport.uint(fields.get(7), 8), "protectionRevision"),
                QueryCodecSupport.fixed(fields.get(8), 9, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CredentialBindingProtectionV1");
        return result;
    }

    public ProfileRefV1 profile() {
        return profile;
    }

    public long secretGeneration() {
        return secretGeneration;
    }

    public byte[] bindingDigest() {
        return Bytes.copy(bindingDigest);
    }

    public long managedChannelProtectionUntilEpochMs() {
        return managedChannelProtectionUntilEpochMs;
    }

    public long objectStoreLeaseProtectionUntilEpochMs() {
        return objectStoreLeaseProtectionUntilEpochMs;
    }

    public long nativeCapabilityProtectionUntilEpochMs() {
        return nativeCapabilityProtectionUntilEpochMs;
    }

    public long uploadHandleProtectionUntilEpochMs() {
        return uploadHandleProtectionUntilEpochMs;
    }

    public long protectionRevision() {
        return protectionRevision;
    }

    public byte[] protectionDigest() {
        return Bytes.copy(protectionDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.uint64(output, 2, secretGeneration);
            CanonicalProtobuf.bytes(output, 3, bindingDigest);
            CanonicalProtobuf.int64(output, 4, managedChannelProtectionUntilEpochMs);
            CanonicalProtobuf.int64(output, 5, objectStoreLeaseProtectionUntilEpochMs);
            CanonicalProtobuf.int64(output, 6, nativeCapabilityProtectionUntilEpochMs);
            CanonicalProtobuf.int64(output, 7, uploadHandleProtectionUntilEpochMs);
            CanonicalProtobuf.uint64(output, 8, protectionRevision);
            CanonicalProtobuf.bytes(output, 9, protectionDigest);
        });
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CredentialBindingProtectionV1 that
                && secretGeneration == that.secretGeneration
                && managedChannelProtectionUntilEpochMs == that.managedChannelProtectionUntilEpochMs
                && objectStoreLeaseProtectionUntilEpochMs == that.objectStoreLeaseProtectionUntilEpochMs
                && nativeCapabilityProtectionUntilEpochMs == that.nativeCapabilityProtectionUntilEpochMs
                && uploadHandleProtectionUntilEpochMs == that.uploadHandleProtectionUntilEpochMs
                && protectionRevision == that.protectionRevision
                && profile.equals(that.profile)
                && Arrays.equals(bindingDigest, that.bindingDigest)
                && Arrays.equals(protectionDigest, that.protectionDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profile, secretGeneration, Arrays.hashCode(bindingDigest),
                managedChannelProtectionUntilEpochMs, objectStoreLeaseProtectionUntilEpochMs,
                nativeCapabilityProtectionUntilEpochMs, uploadHandleProtectionUntilEpochMs, protectionRevision,
                Arrays.hashCode(protectionDigest));
    }

    private byte[] digestForFields() {
        return digestForFields(profile, secretGeneration, bindingDigest, managedChannelProtectionUntilEpochMs,
                objectStoreLeaseProtectionUntilEpochMs, nativeCapabilityProtectionUntilEpochMs,
                uploadHandleProtectionUntilEpochMs, protectionRevision);
    }

    private static byte[] digestForFields(final ProfileRefV1 profile, final long secretGeneration,
                                          final byte[] bindingDigest,
                                          final long managedChannelProtectionUntilEpochMs,
                                          final long objectStoreLeaseProtectionUntilEpochMs,
                                          final long nativeCapabilityProtectionUntilEpochMs,
                                          final long uploadHandleProtectionUntilEpochMs,
                                          final long protectionRevision) {
        final byte[] fields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.uint64(output, 2, secretGeneration);
            CanonicalProtobuf.bytes(output, 3, bindingDigest);
            CanonicalProtobuf.int64(output, 4, managedChannelProtectionUntilEpochMs);
            CanonicalProtobuf.int64(output, 5, objectStoreLeaseProtectionUntilEpochMs);
            CanonicalProtobuf.int64(output, 6, nativeCapabilityProtectionUntilEpochMs);
            CanonicalProtobuf.int64(output, 7, uploadHandleProtectionUntilEpochMs);
            CanonicalProtobuf.uint64(output, 8, protectionRevision);
        });
        return Bytes.sha256(DIGEST_DOMAIN, fields);
    }

    private static ProfileRefV1 requireBindableProfile(final ProfileRefV1 value) {
        final ProfileRefV1 profile = Objects.requireNonNull(value, "profile");
        if (profile.profileKind() != ProfileKindV1.DESTINATION
                && profile.profileKind() != ProfileKindV1.OBJECT_STORE) {
            throw new IllegalArgumentException("credential binding profile must be DESTINATION or OBJECT_STORE");
        }
        return profile;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static long positive(final long value, final String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long nonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }
}
