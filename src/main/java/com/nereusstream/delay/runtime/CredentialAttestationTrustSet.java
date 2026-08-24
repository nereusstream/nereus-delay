package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CredentialBindingV1;
import com.nereusstream.delay.protocol.CredentialEquivalenceAttestationV1;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Immutable verifier-key authority for credential equivalence attestations.
 *
 * <p>The trust set selects an exact verifier-version, verifier-id and signing
 * key-version tuple, checks the attestation's verification window against the
 * retained key window, and verifies the Ed25519 signature.  It intentionally
 * does not resolve private material or authorize a control actor.</p>
 */
public final class CredentialAttestationTrustSet {
    private static final int MAX_KEYS = 1024;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-credential-attestation-trust-set-v1\0");

    private final List<VerifierKey> keys;
    private final byte[] semanticDigest;

    public CredentialAttestationTrustSet(final List<VerifierKey> keys) {
        Objects.requireNonNull(keys, "keys");
        if (keys.isEmpty() || keys.size() > MAX_KEYS) {
            throw new IllegalArgumentException("credential attestation trust set size is outside the V1 bound");
        }
        final List<VerifierKey> ordered = new ArrayList<>(keys.size());
        for (VerifierKey key : keys) {
            ordered.add(Objects.requireNonNull(key, "trust-set verifier key"));
        }
        ordered.sort(VerifierKey.ORDER);
        for (int index = 1; index < ordered.size(); index++) {
            if (ordered.get(index - 1).sameIdentity(ordered.get(index))) {
                throw new IllegalArgumentException("credential attestation trust-set key identity is duplicated");
            }
        }
        this.keys = Collections.unmodifiableList(ordered);
        this.semanticDigest = computeSemanticDigest();
    }

    /** Builds a single-key trust set for a configured verifier deployment. */
    public static CredentialAttestationTrustSet single(
            final int verifierVersion,
            final byte[] verifierId,
            final int signingKeyVersion,
            final PublicKey publicKey,
            final long verifyNotBeforeEpochMs,
            final long verifyNotAfterEpochMs) {
        return new CredentialAttestationTrustSet(List.of(new VerifierKey(
                verifierVersion,
                verifierId,
                signingKeyVersion,
                publicKey,
                verifyNotBeforeEpochMs,
                verifyNotAfterEpochMs)));
    }

    public List<VerifierKey> keys() {
        return keys;
    }

    /** Stable digest for evidence/configuration identity; no private key material is included. */
    public byte[] semanticDigest() {
        return Bytes.copy(semanticDigest);
    }

    /** Verifies one exact Profile binding against this immutable trust set. */
    public void verify(final ProfileSemanticEnvelopeV1 profile, final CredentialBindingV1 binding) {
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(binding, "binding");
        if (!profile.ref().equals(binding.profile())) {
            throw new IllegalArgumentException("credential attestation Profile differs");
        }
        final CredentialEquivalenceAttestationV1 attestation = binding.equivalenceAttestation();
        final VerifierKey key = keys.stream()
                .filter(candidate -> candidate.matches(attestation))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "credential attestation verifier is not in the configured trust set"));
        if (attestation.verifiedAt().earliestEpochMs() < key.verifyNotBeforeEpochMs()
                || attestation.notAfterEpochMs() > key.verifyNotAfterEpochMs()) {
            throw new IllegalArgumentException("credential attestation falls outside the verifier key window");
        }
        if (!attestation.verifySignature(key.publicKey())) {
            throw new IllegalArgumentException("credential attestation signature is not trusted");
        }
    }

    private byte[] computeSemanticDigest() {
        final byte[] canonicalKeys = CanonicalProtobuf.message(output -> {
            for (VerifierKey key : keys) {
                CanonicalProtobuf.bytes(output, 1, key.canonicalBytes());
            }
        });
        return Bytes.sha256(DIGEST_DOMAIN, canonicalKeys);
    }

    /** One immutable public verifier entry retained for attestation validation. */
    public static final class VerifierKey {
        private static final int MAX_VERIFIER_ID_BYTES = 256;
        private static final int ED25519_X509_LENGTH = 44;
        private static final byte[] ED25519_X509_PREFIX = Bytes.hexToBytes("302a300506032b6570032100");

        private static final Comparator<VerifierKey> ORDER = (left, right) -> {
            int result = Integer.compareUnsigned(left.verifierVersion, right.verifierVersion);
            if (result != 0) {
                return result;
            }
            result = Arrays.compareUnsigned(left.verifierId, right.verifierId);
            if (result != 0) {
                return result;
            }
            return Integer.compareUnsigned(left.signingKeyVersion, right.signingKeyVersion);
        };

        private final int verifierVersion;
        private final byte[] verifierId;
        private final int signingKeyVersion;
        private final PublicKey publicKey;
        private final byte[] publicKeyEncoded;
        private final long verifyNotBeforeEpochMs;
        private final long verifyNotAfterEpochMs;

        public VerifierKey(
                final int verifierVersion,
                final byte[] verifierId,
                final int signingKeyVersion,
                final PublicKey publicKey,
                final long verifyNotBeforeEpochMs,
                final long verifyNotAfterEpochMs) {
            if (verifierVersion == 0 || signingKeyVersion == 0) {
                throw new IllegalArgumentException("credential verifier versions must be non-zero");
            }
            Objects.requireNonNull(verifierId, "verifierId");
            if (verifierId.length == 0 || verifierId.length > MAX_VERIFIER_ID_BYTES) {
                throw new IllegalArgumentException("verifierId is outside the V1 bound");
            }
            if (verifyNotBeforeEpochMs < 0 || verifyNotAfterEpochMs <= verifyNotBeforeEpochMs) {
                throw new IllegalArgumentException("invalid credential verifier key window");
            }
            this.publicKey = Objects.requireNonNull(publicKey, "publicKey");
            if (!"Ed25519".equals(publicKey.getAlgorithm()) && !"EdDSA".equals(publicKey.getAlgorithm())) {
                throw new IllegalArgumentException("credential verifier key must use Ed25519");
            }
            final byte[] encoded = Objects.requireNonNull(publicKey.getEncoded(), "publicKey encoding");
            if (encoded.length != ED25519_X509_LENGTH
                    || !Arrays.equals(ED25519_X509_PREFIX, Arrays.copyOf(encoded, ED25519_X509_PREFIX.length))) {
                throw new IllegalArgumentException("credential verifier key is not canonical Ed25519");
            }
            this.verifierVersion = verifierVersion;
            this.verifierId = Bytes.copy(verifierId);
            this.signingKeyVersion = signingKeyVersion;
            this.publicKeyEncoded = Bytes.copy(encoded);
            this.verifyNotBeforeEpochMs = verifyNotBeforeEpochMs;
            this.verifyNotAfterEpochMs = verifyNotAfterEpochMs;
        }

        public int verifierVersion() {
            return verifierVersion;
        }

        public byte[] verifierId() {
            return Bytes.copy(verifierId);
        }

        public int signingKeyVersion() {
            return signingKeyVersion;
        }

        public PublicKey publicKey() {
            return publicKey;
        }

        public long verifyNotBeforeEpochMs() {
            return verifyNotBeforeEpochMs;
        }

        public long verifyNotAfterEpochMs() {
            return verifyNotAfterEpochMs;
        }

        private boolean matches(final CredentialEquivalenceAttestationV1 attestation) {
            return verifierVersion == attestation.verifierVersion()
                    && signingKeyVersion == attestation.signingKeyVersion()
                    && Bytes.constantTimeEquals(verifierId, attestation.verifierId());
        }

        private boolean sameIdentity(final VerifierKey other) {
            return verifierVersion == other.verifierVersion
                    && signingKeyVersion == other.signingKeyVersion
                    && Bytes.constantTimeEquals(verifierId, other.verifierId);
        }

        private byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32Bits(output, 1, verifierVersion);
                CanonicalProtobuf.bytes(output, 2, verifierId);
                CanonicalProtobuf.uint32Bits(output, 3, signingKeyVersion);
                CanonicalProtobuf.bytes(output, 4, publicKeyEncoded);
                CanonicalProtobuf.int64(output, 5, verifyNotBeforeEpochMs);
                CanonicalProtobuf.int64(output, 6, verifyNotAfterEpochMs);
            });
        }
    }
}
