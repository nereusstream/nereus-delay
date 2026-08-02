package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical bounded credential-use lease shared by channel/provider identities. */
public final class CredentialUseLeaseV1 {
    public static final int HASH_LENGTH = 32;
    private static final int VERSION = 1;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-credential-use-lease-v1\0");
    private static final byte[] DESTINATION_CHANNEL_SCOPE_DOMAIN =
            Bytes.utf8("nereus-delay-credential-holder-destination-channel-v1\0");

    private final ProfileRefV1 profile;
    private final CredentialUseKindV1 kind;
    private final byte[] holderScopeDigest;
    private final long secretGeneration;
    private final byte[] credentialBindingDigest;
    private final byte[] resolvedCredentialFingerprintDigest;
    private final TrustedUtcIntervalEvidence issuedAt;
    private final long validUntilEpochMs;
    private final long protectionRevision;

    public CredentialUseLeaseV1(final ProfileRefV1 profile, final CredentialUseKindV1 kind,
                                final byte[] holderScopeDigest, final long secretGeneration,
                                final byte[] credentialBindingDigest,
                                final byte[] resolvedCredentialFingerprintDigest,
                                final TrustedUtcIntervalEvidence issuedAt, final long validUntilEpochMs,
                                final long protectionRevision) {
        this.profile = Objects.requireNonNull(profile, "profile");
        this.kind = Objects.requireNonNull(kind, "kind");
        if ((kind == CredentialUseKindV1.DESTINATION_CHANNEL
                && profile.profileKind() != ProfileKindV1.DESTINATION)
                || (kind == CredentialUseKindV1.OBJECT_STORE_ADAPTER
                && profile.profileKind() != ProfileKindV1.OBJECT_STORE)) {
            throw new IllegalArgumentException("credential lease kind/profile mismatch");
        }
        this.holderScopeDigest = fixed(holderScopeDigest, "holderScopeDigest");
        if (secretGeneration <= 0) {
            throw new IllegalArgumentException("secretGeneration must be positive");
        }
        this.secretGeneration = secretGeneration;
        this.credentialBindingDigest = fixed(credentialBindingDigest, "credentialBindingDigest");
        this.resolvedCredentialFingerprintDigest = fixed(resolvedCredentialFingerprintDigest,
                "resolvedCredentialFingerprintDigest");
        this.issuedAt = Objects.requireNonNull(issuedAt, "issuedAt");
        if (validUntilEpochMs <= issuedAt.latestEpochMs()) {
            throw new IllegalArgumentException("credential lease must outlive issuedAt.latest");
        }
        this.validUntilEpochMs = validUntilEpochMs;
        if (protectionRevision <= 0) {
            throw new IllegalArgumentException("protectionRevision must be positive");
        }
        this.protectionRevision = protectionRevision;
    }

    public ProfileRefV1 profile() {
        return profile;
    }

    public CredentialUseKindV1 kind() {
        return kind;
    }

    public byte[] holderScopeDigest() {
        return Bytes.copy(holderScopeDigest);
    }

    public long secretGeneration() {
        return secretGeneration;
    }

    public byte[] credentialBindingDigest() {
        return Bytes.copy(credentialBindingDigest);
    }

    public byte[] resolvedCredentialFingerprintDigest() {
        return Bytes.copy(resolvedCredentialFingerprintDigest);
    }

    public TrustedUtcIntervalEvidence issuedAt() {
        return issuedAt;
    }

    public long validUntilEpochMs() {
        return validUntilEpochMs;
    }

    public long protectionRevision() {
        return protectionRevision;
    }

    /** Checks the configured kind-specific TTL bound without inventing a default here. */
    public void requireTtlAtMost(final long maxTtlMs) {
        if (maxTtlMs < 0) {
            throw new IllegalArgumentException("maxTtlMs must be non-negative");
        }
        final long upper;
        try {
            upper = Math.addExact(issuedAt.earliestEpochMs(), maxTtlMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("credential lease TTL overflows epoch range", overflow);
        }
        if (validUntilEpochMs > upper) {
            throw new IllegalArgumentException("credential lease exceeds configured TTL");
        }
    }

    /** Verifies the lease against the exact immutable binding and resolved fingerprint. */
    public void requireBinding(final CredentialBindingV1 binding) {
        Objects.requireNonNull(binding, "binding");
        if (!profile.equals(binding.profile()) || secretGeneration != binding.secretGeneration()
                || !Arrays.equals(credentialBindingDigest, binding.bindingDigest())
                || !Arrays.equals(resolvedCredentialFingerprintDigest,
                binding.equivalenceAttestation().resolvedCredentialFingerprintDigest())) {
            throw new IllegalArgumentException("credential lease does not match binding");
        }
    }

    /**
     * Verifies the durable protection projection that authorizes this bounded
     * lease. The external CAS/reread remains outside this value codec.
     */
    public void requireProtectedBy(final CredentialBindingProtectionV1 protection) {
        Objects.requireNonNull(protection, "protection");
        if (!profile.equals(protection.profile()) || secretGeneration != protection.secretGeneration()
                || !Arrays.equals(credentialBindingDigest, protection.bindingDigest())
                || protectionRevision != protection.protectionRevision()) {
            throw new IllegalArgumentException("credential lease protection identity mismatch");
        }
        final long protectedUntil = kind == CredentialUseKindV1.DESTINATION_CHANNEL
                ? protection.managedChannelProtectionUntilEpochMs()
                : protection.objectStoreLeaseProtectionUntilEpochMs();
        if (protectedUntil < validUntilEpochMs) {
            throw new IllegalArgumentException("credential lease outlives its protection record");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, profile.canonicalBytes());
            CanonicalProtobuf.uint32(output, 3, kind.wireValue());
            CanonicalProtobuf.bytes(output, 4, holderScopeDigest);
            CanonicalProtobuf.uint64(output, 5, secretGeneration);
            CanonicalProtobuf.bytes(output, 6, credentialBindingDigest);
            CanonicalProtobuf.bytes(output, 7, resolvedCredentialFingerprintDigest);
            CanonicalProtobuf.bytes(output, 8, issuedAt.canonicalBytes());
            CanonicalProtobuf.int64(output, 9, validUntilEpochMs);
            CanonicalProtobuf.uint64(output, 10, protectionRevision);
            CanonicalProtobuf.bytes(output, 11, digest());
        });
    }

    public static CredentialUseLeaseV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "CredentialUseLeaseV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
                "CredentialUseLeaseV1");
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported CredentialUseLeaseV1 version");
        }
        final CredentialUseLeaseV1 result = new CredentialUseLeaseV1(
                ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                CredentialUseKindV1.fromWire(QueryCodecSupport.uint(fields.get(2), 3)),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH),
                positive(QueryCodecSupport.uint(fields.get(4), 5), "secretGeneration"),
                QueryCodecSupport.fixed(fields.get(5), 6, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(6), 7, HASH_LENGTH),
                TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(fields.get(7), 8)),
                nonNegative(QueryCodecSupport.uint(fields.get(8), 9), "validUntilEpochMs"),
                positive(QueryCodecSupport.uint(fields.get(9), 10), "protectionRevision"));
        if (!Arrays.equals(fields.get(10).rawValue(), result.digest())) {
            throw new IllegalArgumentException("CredentialUseLeaseV1 digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CredentialUseLeaseV1");
        return result;
    }

    public static byte[] destinationChannelHolderScope(final byte[] canonicalFieldsThrough13) {
        Objects.requireNonNull(canonicalFieldsThrough13, "canonicalFieldsThrough13");
        return Bytes.sha256(DESTINATION_CHANNEL_SCOPE_DOMAIN, canonicalFieldsThrough13);
    }

    private byte[] digest() {
        return Bytes.sha256(DIGEST_DOMAIN, canonicalFieldsThrough10());
    }

    private byte[] canonicalFieldsThrough10() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, profile.canonicalBytes());
            CanonicalProtobuf.uint32(output, 3, kind.wireValue());
            CanonicalProtobuf.bytes(output, 4, holderScopeDigest);
            CanonicalProtobuf.uint64(output, 5, secretGeneration);
            CanonicalProtobuf.bytes(output, 6, credentialBindingDigest);
            CanonicalProtobuf.bytes(output, 7, resolvedCredentialFingerprintDigest);
            CanonicalProtobuf.bytes(output, 8, issuedAt.canonicalBytes());
            CanonicalProtobuf.int64(output, 9, validUntilEpochMs);
            CanonicalProtobuf.uint64(output, 10, protectionRevision);
        });
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

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CredentialUseLeaseV1 that
                && Objects.equals(profile, that.profile)
                && kind == that.kind
                && Arrays.equals(holderScopeDigest, that.holderScopeDigest)
                && secretGeneration == that.secretGeneration
                && Arrays.equals(credentialBindingDigest, that.credentialBindingDigest)
                && Arrays.equals(resolvedCredentialFingerprintDigest, that.resolvedCredentialFingerprintDigest)
                && Objects.equals(issuedAt, that.issuedAt)
                && validUntilEpochMs == that.validUntilEpochMs
                && protectionRevision == that.protectionRevision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(profile, kind, Arrays.hashCode(holderScopeDigest), secretGeneration,
                Arrays.hashCode(credentialBindingDigest), Arrays.hashCode(resolvedCredentialFingerprintDigest),
                issuedAt, validUntilEpochMs, protectionRevision);
    }
}
