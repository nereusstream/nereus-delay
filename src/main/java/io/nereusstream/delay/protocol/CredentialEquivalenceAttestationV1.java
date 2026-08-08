package io.nereusstream.delay.protocol;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Signed proof that a candidate credential reference resolves to an immutable
 * provider version under the selected Profile's authorization scope.
 *
 * <p>The verifier trust set, provider lookup and maximum attestation age are
 * external authorities.  This class only closes the canonical value and its
 * domain-separated digest/signature projection.</p>
 */
public final class CredentialEquivalenceAttestationV1 {
    public static final int HASH_LENGTH = 32;
    public static final int SIGNATURE_LENGTH = 64;
    public static final int MAX_VERIFIER_ID_BYTES = 256;
    private static final byte[] DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-credential-equivalence-v1\0");
    private static final byte[] SIGNATURE_DOMAIN =
            Bytes.utf8("nereus-delay-credential-equivalence-signature-v1\0");

    private final ProfileRefV1 profile;
    private final long secretGeneration;
    private final byte[] secretReferenceSha256;
    private final byte[] authorizationScopeDigest;
    private final byte[] resolvedCredentialFingerprintDigest;
    private final int verifierVersion;
    private final byte[] verifierId;
    private final TrustedUtcIntervalEvidence verifiedAt;
    private final long notAfterEpochMs;
    private final byte[] verificationEvidenceSha256;
    private final byte[] attestationDigest;
    private final int signingKeyVersion;
    private final byte[] signature;

    public CredentialEquivalenceAttestationV1(final ProfileRefV1 profile, final long secretGeneration,
                                              final byte[] secretReferenceSha256,
                                              final byte[] authorizationScopeDigest,
                                              final byte[] resolvedCredentialFingerprintDigest,
                                              final int verifierVersion, final byte[] verifierId,
                                              final TrustedUtcIntervalEvidence verifiedAt,
                                              final long notAfterEpochMs,
                                              final byte[] verificationEvidenceSha256,
                                              final byte[] attestationDigest, final int signingKeyVersion,
                                              final byte[] signature) {
        this.profile = requireBindableProfile(profile);
        this.secretGeneration = nonZero(secretGeneration, "secretGeneration");
        this.secretReferenceSha256 = fixed(secretReferenceSha256, "secretReferenceSha256");
        this.authorizationScopeDigest = fixed(authorizationScopeDigest, "authorizationScopeDigest");
        this.resolvedCredentialFingerprintDigest = fixed(resolvedCredentialFingerprintDigest,
                "resolvedCredentialFingerprintDigest");
        if (verifierVersion == 0) {
            throw new IllegalArgumentException("verifierVersion must be a non-zero uint32");
        }
        this.verifierVersion = verifierVersion;
        this.verifierId = boundedNonEmpty(verifierId, MAX_VERIFIER_ID_BYTES, "verifierId");
        this.verifiedAt = Objects.requireNonNull(verifiedAt, "verifiedAt");
        if (notAfterEpochMs < 0 || notAfterEpochMs <= verifiedAt.latestEpochMs()) {
            throw new IllegalArgumentException("notAfterEpochMs must outlive verifiedAt.latest");
        }
        this.notAfterEpochMs = notAfterEpochMs;
        this.verificationEvidenceSha256 = fixed(verificationEvidenceSha256,
                "verificationEvidenceSha256");
        this.attestationDigest = fixed(attestationDigest, "attestationDigest");
        if (signingKeyVersion == 0) {
            throw new IllegalArgumentException("signingKeyVersion must be a non-zero uint32");
        }
        this.signingKeyVersion = signingKeyVersion;
        Bytes.requireLength(signature, SIGNATURE_LENGTH, "signature");
        this.signature = Bytes.copy(signature);
        if (!Bytes.constantTimeEquals(this.attestationDigest, digestForFields())) {
            throw new IllegalArgumentException("CredentialEquivalenceAttestationV1 digest mismatch");
        }
    }

    /** Creates and signs an attestation after resolving the external evidence. */
    public static CredentialEquivalenceAttestationV1 signed(final ProfileRefV1 profile,
                                                            final long secretGeneration,
                                                            final byte[] secretReferenceSha256,
                                                            final byte[] authorizationScopeDigest,
                                                            final byte[] resolvedCredentialFingerprintDigest,
                                                            final int verifierVersion,
                                                            final byte[] verifierId,
                                                            final TrustedUtcIntervalEvidence verifiedAt,
                                                            final long notAfterEpochMs,
                                                            final byte[] verificationEvidenceSha256,
                                                            final int signingKeyVersion,
                                                            final PrivateKey signingKey) {
        Objects.requireNonNull(signingKey, "signingKey");
        final byte[] digest = digestForFields(profile, secretGeneration, secretReferenceSha256,
                authorizationScopeDigest, resolvedCredentialFingerprintDigest, verifierVersion, verifierId,
                verifiedAt, notAfterEpochMs, verificationEvidenceSha256);
        final byte[] signature = sign(signatureDigest(digest, signingKeyVersion), signingKey);
        return new CredentialEquivalenceAttestationV1(profile, secretGeneration, secretReferenceSha256,
                authorizationScopeDigest, resolvedCredentialFingerprintDigest, verifierVersion, verifierId,
                verifiedAt, notAfterEpochMs, verificationEvidenceSha256, digest, signingKeyVersion, signature);
    }

    public static CredentialEquivalenceAttestationV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "CredentialEquivalenceAttestationV1");
        QueryCodecSupport.requireNumbers(fields,
                new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
                "CredentialEquivalenceAttestationV1");
        final ProfileRefV1 profile = ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1));
        final long generation = nonZero(QueryCodecSupport.uint(fields.get(1), 2), "secretGeneration");
        final byte[] referenceHash = QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH);
        final byte[] scope = QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH);
        final byte[] fingerprint = QueryCodecSupport.fixed(fields.get(4), 5, HASH_LENGTH);
        final int verifierVersion = nonZeroUint32(QueryCodecSupport.uint32Bits(fields.get(5), 6),
                "verifierVersion");
        final byte[] verifierId = QueryCodecSupport.bytes(fields.get(6), 7);
        final TrustedUtcIntervalEvidence verifiedAt = TrustedUtcIntervalEvidence.decode(
                QueryCodecSupport.nested(fields.get(7), 8));
        final long notAfter = QueryCodecSupport.uint(fields.get(8), 9);
        final byte[] evidence = QueryCodecSupport.fixed(fields.get(9), 10, HASH_LENGTH);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(10), 11, HASH_LENGTH);
        final int keyVersion = nonZeroUint32(QueryCodecSupport.uint32Bits(fields.get(11), 12),
                "signingKeyVersion");
        final byte[] signature = QueryCodecSupport.fixed(fields.get(12), 13, SIGNATURE_LENGTH);
        final CredentialEquivalenceAttestationV1 result = new CredentialEquivalenceAttestationV1(profile,
                generation, referenceHash, scope, fingerprint, verifierVersion, verifierId, verifiedAt,
                nonNegative(notAfter, "notAfterEpochMs"), evidence, digest, keyVersion, signature);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(),
                "CredentialEquivalenceAttestationV1");
        return result;
    }

    public ProfileRefV1 profile() {
        return profile;
    }

    public long secretGeneration() {
        return secretGeneration;
    }

    public byte[] secretReferenceSha256() {
        return Bytes.copy(secretReferenceSha256);
    }

    public byte[] authorizationScopeDigest() {
        return Bytes.copy(authorizationScopeDigest);
    }

    public byte[] resolvedCredentialFingerprintDigest() {
        return Bytes.copy(resolvedCredentialFingerprintDigest);
    }

    public int verifierVersion() {
        return verifierVersion;
    }

    public byte[] verifierId() {
        return Bytes.copy(verifierId);
    }

    public TrustedUtcIntervalEvidence verifiedAt() {
        return verifiedAt;
    }

    public long notAfterEpochMs() {
        return notAfterEpochMs;
    }

    public byte[] verificationEvidenceSha256() {
        return Bytes.copy(verificationEvidenceSha256);
    }

    public byte[] attestationDigest() {
        return Bytes.copy(attestationDigest);
    }

    public int signingKeyVersion() {
        return signingKeyVersion;
    }

    public byte[] signature() {
        return Bytes.copy(signature);
    }

    /** Checks the selected Profile's immutable credential scope projection. */
    public void requireAuthorizationScopeDigest(final byte[] expected) {
        if (!Bytes.constantTimeEquals(authorizationScopeDigest, fixed(expected, "expectedAuthorizationScopeDigest"))) {
            throw new IllegalArgumentException("credential authorization scope digest mismatch");
        }
    }

    /** Checks the candidate binding tuple represented by fields 1-3. */
    public void requireCandidate(final ProfileRefV1 expectedProfile, final long expectedGeneration,
                                 final byte[] expectedSecretReferenceSha256) {
        if (!profile.equals(expectedProfile) || secretGeneration != expectedGeneration
                || !Bytes.constantTimeEquals(secretReferenceSha256,
                fixed(expectedSecretReferenceSha256, "expectedSecretReferenceSha256"))) {
            throw new IllegalArgumentException("credential equivalence candidate mismatch");
        }
    }

    /** Applies the configured maximum proof age without inventing a default. */
    public void requireNotAfterAtMost(final long maxAgeMs) {
        if (maxAgeMs < 0) {
            throw new IllegalArgumentException("maxAgeMs must be non-negative");
        }
        final long upper;
        try {
            upper = Math.addExact(verifiedAt.earliestEpochMs(), maxAgeMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("attestation age overflows epoch range", overflow);
        }
        if (notAfterEpochMs > upper) {
            throw new IllegalArgumentException("credential attestation exceeds configured maximum age");
        }
    }

    public boolean verifySignature(final PublicKey verificationKey) {
        Objects.requireNonNull(verificationKey, "verificationKey");
        try {
            final Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(verificationKey);
            verifier.update(signatureDigest(attestationDigest, signingKeyVersion));
            return verifier.verify(signature);
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("Ed25519 verification is unavailable", exception);
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(this::writeFields);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CredentialEquivalenceAttestationV1 that
                && secretGeneration == that.secretGeneration
                && verifierVersion == that.verifierVersion
                && notAfterEpochMs == that.notAfterEpochMs
                && signingKeyVersion == that.signingKeyVersion
                && profile.equals(that.profile)
                && Arrays.equals(secretReferenceSha256, that.secretReferenceSha256)
                && Arrays.equals(authorizationScopeDigest, that.authorizationScopeDigest)
                && Arrays.equals(resolvedCredentialFingerprintDigest,
                that.resolvedCredentialFingerprintDigest)
                && Arrays.equals(verifierId, that.verifierId)
                && Arrays.equals(verifiedAt.canonicalBytes(), that.verifiedAt.canonicalBytes())
                && Arrays.equals(verificationEvidenceSha256, that.verificationEvidenceSha256)
                && Arrays.equals(attestationDigest, that.attestationDigest)
                && Arrays.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profile, secretGeneration, Arrays.hashCode(secretReferenceSha256),
                Arrays.hashCode(authorizationScopeDigest), Arrays.hashCode(resolvedCredentialFingerprintDigest),
                verifierVersion, Arrays.hashCode(verifierId), Arrays.hashCode(verifiedAt.canonicalBytes()),
                notAfterEpochMs, Arrays.hashCode(verificationEvidenceSha256), Arrays.hashCode(attestationDigest),
                signingKeyVersion, Arrays.hashCode(signature));
    }

    private void writeFields(final java.io.ByteArrayOutputStream output) {
        CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
        CanonicalProtobuf.uint64Bits(output, 2, secretGeneration);
        CanonicalProtobuf.bytes(output, 3, secretReferenceSha256);
        CanonicalProtobuf.bytes(output, 4, authorizationScopeDigest);
        CanonicalProtobuf.bytes(output, 5, resolvedCredentialFingerprintDigest);
        CanonicalProtobuf.uint32Bits(output, 6, verifierVersion);
        CanonicalProtobuf.bytes(output, 7, verifierId);
        CanonicalProtobuf.bytes(output, 8, verifiedAt.canonicalBytes());
        CanonicalProtobuf.int64(output, 9, notAfterEpochMs);
        CanonicalProtobuf.bytes(output, 10, verificationEvidenceSha256);
        CanonicalProtobuf.bytes(output, 11, attestationDigest);
        CanonicalProtobuf.uint32Bits(output, 12, signingKeyVersion);
        CanonicalProtobuf.bytes(output, 13, signature);
    }

    private byte[] digestForFields() {
        return digestForFields(profile, secretGeneration, secretReferenceSha256, authorizationScopeDigest,
                resolvedCredentialFingerprintDigest, verifierVersion, verifierId, verifiedAt, notAfterEpochMs,
                verificationEvidenceSha256);
    }

    private static byte[] digestForFields(final ProfileRefV1 profile, final long secretGeneration,
                                          final byte[] secretReferenceSha256,
                                          final byte[] authorizationScopeDigest,
                                          final byte[] resolvedCredentialFingerprintDigest,
                                          final int verifierVersion, final byte[] verifierId,
                                          final TrustedUtcIntervalEvidence verifiedAt,
                                          final long notAfterEpochMs,
                                          final byte[] verificationEvidenceSha256) {
        final byte[] fields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 2, secretGeneration);
            CanonicalProtobuf.bytes(output, 3, secretReferenceSha256);
            CanonicalProtobuf.bytes(output, 4, authorizationScopeDigest);
            CanonicalProtobuf.bytes(output, 5, resolvedCredentialFingerprintDigest);
            CanonicalProtobuf.uint32Bits(output, 6, verifierVersion);
            CanonicalProtobuf.bytes(output, 7, verifierId);
            CanonicalProtobuf.bytes(output, 8, verifiedAt.canonicalBytes());
            CanonicalProtobuf.int64(output, 9, notAfterEpochMs);
            CanonicalProtobuf.bytes(output, 10, verificationEvidenceSha256);
        });
        return Bytes.sha256(DIGEST_DOMAIN, fields);
    }

    private static byte[] signatureDigest(final byte[] attestationDigest, final int signingKeyVersion) {
        return Bytes.sha256(SIGNATURE_DOMAIN, attestationDigest, Bytes.u32beBits(signingKeyVersion));
    }

    private static byte[] sign(final byte[] digest, final PrivateKey signingKey) {
        try {
            final Signature signer = Signature.getInstance("Ed25519");
            signer.initSign(signingKey);
            signer.update(digest);
            return signer.sign();
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("Ed25519 signing is unavailable", exception);
        }
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

    private static byte[] boundedNonEmpty(final byte[] value, final int maximum, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0 || value.length > maximum) {
            throw new IllegalArgumentException(name + " must be non-empty and at most " + maximum + " bytes");
        }
        return Bytes.copy(value);
    }

    private static long nonZero(final long value, final String name) {
        if (value == 0) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return value;
    }

    private static int nonZeroUint32(final int value, final String name) {
        if (value == 0) {
            throw new IllegalArgumentException(name + " must be a non-zero uint32");
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
