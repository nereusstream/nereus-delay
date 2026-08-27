package com.nereusstream.delay.protocol;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Signed, self-contained bounded lease for a native handoff decision. */
public final class HandoffPolicySnapshot {
    public static final int SCHEMA_GENERATION = 1;
    public static final int HASH_LENGTH = 32;
    public static final int SIGNATURE_LENGTH = 64;
    private static final String HASH_DOMAIN = "nereus-delay-handoff-policy-snapshot\0";
    private static final String SIGNATURE_DOMAIN = "nereus-delay-handoff-policy-snapshot-signature\0";

    private final byte[] policyScopeDigest;
    private final long generation;
    private final HandoffPolicyMode mode;
    private final long effectiveLeadMs;
    private final long validFromEpochMs;
    private final long validUntilEpochMs;
    private final int allowedPathBits;
    private final TrustedUtcIntervalEvidence issuedAt;
    private final int issuerKeyGeneration;
    private final byte[] artifactGenerationSetDigest;
    private final byte[] snapshotDigest;
    private final byte[] signature;

    private HandoffPolicySnapshot(
            final byte[] policyScopeDigest,
            final long generation,
            final HandoffPolicyMode mode,
            final long effectiveLeadMs,
            final long validFromEpochMs,
            final long validUntilEpochMs,
            final int allowedPathBits,
            final TrustedUtcIntervalEvidence issuedAt,
            final int issuerKeyGeneration,
            final byte[] artifactGenerationSetDigest,
            final byte[] snapshotDigest,
            final byte[] signature) {
        this.policyScopeDigest = fixed(policyScopeDigest, "policyScopeDigest");
        if (generation == 0) {
            throw new IllegalArgumentException("policy generation must be non-zero");
        }
        this.generation = generation;
        this.mode = Objects.requireNonNull(mode, "mode");
        if (effectiveLeadMs < 0
                || validFromEpochMs < 0
                || validUntilEpochMs <= validFromEpochMs
                || issuerKeyGeneration <= 0) {
            throw new IllegalArgumentException("invalid handoff policy lease bounds");
        }
        this.effectiveLeadMs = effectiveLeadMs;
        this.validFromEpochMs = validFromEpochMs;
        this.validUntilEpochMs = validUntilEpochMs;
        HandoffPath.requireValid(allowedPathBits);
        if ((mode == HandoffPolicyMode.DISABLED && (effectiveLeadMs != 0 || allowedPathBits != 0))
                || (mode == HandoffPolicyMode.ENABLED && (effectiveLeadMs == 0 || allowedPathBits == 0))) {
            throw new IllegalArgumentException("handoff policy mode and permissions disagree");
        }
        this.allowedPathBits = allowedPathBits;
        this.issuedAt = Objects.requireNonNull(issuedAt, "issuedAt");
        if (issuedAt.latestEpochMs() > validFromEpochMs || issuedAt.latestEpochMs() >= validUntilEpochMs) {
            throw new IllegalArgumentException("policy issuance evidence is outside the lease window");
        }
        this.issuerKeyGeneration = issuerKeyGeneration;
        this.artifactGenerationSetDigest = fixed(artifactGenerationSetDigest, "artifactGenerationSetDigest");
        this.snapshotDigest = fixed(snapshotDigest, "snapshotDigest");
        Bytes.requireLength(signature, SIGNATURE_LENGTH, "signature");
        this.signature = Bytes.copy(signature);
    }

    public static HandoffPolicySnapshot create(
            final byte[] policyScopeDigest,
            final long generation,
            final HandoffPolicyMode mode,
            final long effectiveLeadMs,
            final long validFromEpochMs,
            final long validUntilEpochMs,
            final int allowedPathBits,
            final TrustedUtcIntervalEvidence issuedAt,
            final int issuerKeyGeneration,
            final byte[] artifactGenerationSetDigest,
            final PrivateKey issuerKey) {
        final byte[] fields = canonicalFields(
                policyScopeDigest,
                generation,
                mode,
                effectiveLeadMs,
                validFromEpochMs,
                validUntilEpochMs,
                allowedPathBits,
                issuedAt,
                issuerKeyGeneration,
                artifactGenerationSetDigest);
        final byte[] digest = Bytes.sha256(Bytes.utf8(HASH_DOMAIN), fields);
        final byte[] signature = sign(digest, issuerKeyGeneration, issuerKey);
        return new HandoffPolicySnapshot(
                policyScopeDigest,
                generation,
                mode,
                effectiveLeadMs,
                validFromEpochMs,
                validUntilEpochMs,
                allowedPathBits,
                issuedAt,
                issuerKeyGeneration,
                artifactGenerationSetDigest,
                digest,
                signature);
    }

    public static HandoffPolicySnapshot decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "HandoffPolicySnapshot");
        QueryCodecSupport.requireNumbers(
                fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, "HandoffPolicySnapshot");
        if (QueryCodecSupport.uint(fields.get(0), 1) != SCHEMA_GENERATION) {
            throw new IllegalArgumentException("unsupported HandoffPolicySnapshot generation");
        }
        final byte[] scope = QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH);
        final long generation = QueryCodecSupport.uint64Bits(fields.get(2), 3);
        final HandoffPolicyMode mode = HandoffPolicyMode.fromWire(QueryCodecSupport.uint(fields.get(3), 4));
        final long lead = QueryCodecSupport.uint(fields.get(4), 5);
        final long validFrom = QueryCodecSupport.uint(fields.get(5), 6);
        final long validUntil = QueryCodecSupport.uint(fields.get(6), 7);
        final int pathBits = QueryCodecSupport.uint32Bits(fields.get(7), 8);
        final TrustedUtcIntervalEvidence issuedAt =
                TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(fields.get(8), 9));
        final int keyGeneration = QueryCodecSupport.uint32(fields.get(9), 10);
        final byte[] artifactDigest = QueryCodecSupport.fixed(fields.get(10), 11, HASH_LENGTH);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(11), 12, HASH_LENGTH);
        final byte[] signature = QueryCodecSupport.fixed(fields.get(12), 13, SIGNATURE_LENGTH);
        final byte[] expectedDigest = Bytes.sha256(
                Bytes.utf8(HASH_DOMAIN),
                canonicalFields(
                        scope,
                        generation,
                        mode,
                        lead,
                        validFrom,
                        validUntil,
                        pathBits,
                        issuedAt,
                        keyGeneration,
                        artifactDigest));
        if (!Bytes.constantTimeEquals(digest, expectedDigest)) {
            throw new IllegalArgumentException("HandoffPolicySnapshot digest mismatch");
        }
        final HandoffPolicySnapshot result = new HandoffPolicySnapshot(
                scope,
                generation,
                mode,
                lead,
                validFrom,
                validUntil,
                pathBits,
                issuedAt,
                keyGeneration,
                artifactDigest,
                digest,
                signature);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "HandoffPolicySnapshot");
        return result;
    }

    public byte[] policyScopeDigest() {
        return Bytes.copy(policyScopeDigest);
    }

    public long generation() {
        return generation;
    }

    public HandoffPolicyMode mode() {
        return mode;
    }

    public long effectiveLeadMs() {
        return effectiveLeadMs;
    }

    public long validFromEpochMs() {
        return validFromEpochMs;
    }

    public long validUntilEpochMs() {
        return validUntilEpochMs;
    }

    public int allowedPathBits() {
        return allowedPathBits;
    }

    public TrustedUtcIntervalEvidence issuedAt() {
        return issuedAt;
    }

    public int issuerKeyGeneration() {
        return issuerKeyGeneration;
    }

    public byte[] artifactGenerationSetDigest() {
        return Bytes.copy(artifactGenerationSetDigest);
    }

    public byte[] snapshotDigest() {
        return Bytes.copy(snapshotDigest);
    }

    public byte[] signature() {
        return Bytes.copy(signature);
    }

    public HandoffPolicyHeadRef headRef() {
        // A snapshot alone has no Oxia revision. Keep the zero-valued helper
        // for compatibility with pre-authority callers; Claim/READY paths
        // must use headRef(oxiaVersion) so the exact CAS publication is
        // retained in durable identity.
        return headRef(0);
    }

    /** Returns a durable head reference bound to the observed Oxia revision. */
    public HandoffPolicyHeadRef headRef(final long oxiaVersion) {
        return new HandoffPolicyHeadRef(policyScopeDigest, generation, snapshotDigest, oxiaVersion);
    }

    public boolean allows(final int path) {
        return mode != HandoffPolicyMode.DISABLED && HandoffPath.includes(allowedPathBits, path);
    }

    /** Requires a trusted interval to prove that this lease is currently active. */
    public void requireActiveAt(final TrustedUtcIntervalEvidence trustedTime) {
        Objects.requireNonNull(trustedTime, "trustedTime");
        if (trustedTime.latestEpochMs() < validFromEpochMs || trustedTime.earliestEpochMs() >= validUntilEpochMs) {
            throw new IllegalArgumentException("handoff policy snapshot is not active at trusted time");
        }
    }

    /** Enforces the immutable maximum lead without silently clamping it. */
    public void requireLeadAtMost(final long maximumLeadMs) {
        if (maximumLeadMs < 0 || effectiveLeadMs > maximumLeadMs) {
            throw new IllegalArgumentException("handoff policy lead exceeds the Destination Profile bound");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, SCHEMA_GENERATION);
            CanonicalProtobuf.bytes(output, 2, policyScopeDigest);
            CanonicalProtobuf.uint64Bits(output, 3, generation);
            CanonicalProtobuf.uint32(output, 4, mode.wireValue());
            CanonicalProtobuf.uint64(output, 5, effectiveLeadMs);
            CanonicalProtobuf.int64(output, 6, validFromEpochMs);
            CanonicalProtobuf.int64(output, 7, validUntilEpochMs);
            CanonicalProtobuf.uint32Bits(output, 8, allowedPathBits);
            CanonicalProtobuf.bytes(output, 9, issuedAt.canonicalBytes());
            CanonicalProtobuf.uint32(output, 10, issuerKeyGeneration);
            CanonicalProtobuf.bytes(output, 11, artifactGenerationSetDigest);
            CanonicalProtobuf.bytes(output, 12, snapshotDigest);
            CanonicalProtobuf.bytes(output, 13, signature);
        });
    }

    public boolean verifySignature(final PublicKey issuerKey) {
        Objects.requireNonNull(issuerKey, "issuerKey");
        try {
            final Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(issuerKey);
            verifier.update(signatureInput(snapshotDigest, issuerKeyGeneration));
            return verifier.verify(signature);
        } catch (GeneralSecurityException error) {
            throw new IllegalArgumentException("cannot verify HandoffPolicySnapshot signature", error);
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof HandoffPolicySnapshot that
                && generation == that.generation
                && mode == that.mode
                && effectiveLeadMs == that.effectiveLeadMs
                && validFromEpochMs == that.validFromEpochMs
                && validUntilEpochMs == that.validUntilEpochMs
                && allowedPathBits == that.allowedPathBits
                && issuerKeyGeneration == that.issuerKeyGeneration
                && issuedAt.equals(that.issuedAt)
                && Arrays.equals(policyScopeDigest, that.policyScopeDigest)
                && Arrays.equals(artifactGenerationSetDigest, that.artifactGenerationSetDigest)
                && Arrays.equals(snapshotDigest, that.snapshotDigest)
                && Arrays.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                Arrays.hashCode(policyScopeDigest),
                generation,
                mode,
                effectiveLeadMs,
                validFromEpochMs,
                validUntilEpochMs,
                allowedPathBits,
                issuedAt,
                issuerKeyGeneration,
                Arrays.hashCode(artifactGenerationSetDigest),
                Arrays.hashCode(snapshotDigest),
                Arrays.hashCode(signature));
    }

    private static byte[] canonicalFields(
            final byte[] scope,
            final long generation,
            final HandoffPolicyMode mode,
            final long lead,
            final long validFrom,
            final long validUntil,
            final int pathBits,
            final TrustedUtcIntervalEvidence issuedAt,
            final int keyGeneration,
            final byte[] artifactDigest) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, SCHEMA_GENERATION);
            CanonicalProtobuf.bytes(output, 2, scope);
            CanonicalProtobuf.uint64Bits(output, 3, generation);
            CanonicalProtobuf.uint32(output, 4, mode.wireValue());
            CanonicalProtobuf.uint64(output, 5, lead);
            CanonicalProtobuf.int64(output, 6, validFrom);
            CanonicalProtobuf.int64(output, 7, validUntil);
            CanonicalProtobuf.uint32Bits(output, 8, pathBits);
            CanonicalProtobuf.bytes(output, 9, issuedAt.canonicalBytes());
            CanonicalProtobuf.uint32(output, 10, keyGeneration);
            CanonicalProtobuf.bytes(output, 11, artifactDigest);
        });
    }

    private static byte[] signatureInput(final byte[] digest, final int keyGeneration) {
        return Bytes.concat(Bytes.utf8(SIGNATURE_DOMAIN), digest, Bytes.u32be(keyGeneration));
    }

    private static byte[] sign(final byte[] digest, final int keyGeneration, final PrivateKey issuerKey) {
        Objects.requireNonNull(issuerKey, "issuerKey");
        try {
            final Signature signer = Signature.getInstance("Ed25519");
            signer.initSign(issuerKey);
            signer.update(signatureInput(digest, keyGeneration));
            final byte[] signature = signer.sign();
            Bytes.requireLength(signature, SIGNATURE_LENGTH, "signature");
            return signature;
        } catch (GeneralSecurityException error) {
            throw new IllegalArgumentException("cannot sign HandoffPolicySnapshot", error);
        }
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }
}
