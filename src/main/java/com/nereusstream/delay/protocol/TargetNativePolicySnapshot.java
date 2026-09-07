package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.Objects;

/** Signed, self-contained bounded lease for a native handoff decision. */
public final class TargetNativePolicySnapshot {
    public static final int SCHEMA_GENERATION = TargetNativeArtifactSet.SNAPSHOT_VERSION;
    public static final int VALUE_TYPE = 25;
    public static final int MAX_CANONICAL_BYTES =
            2 + 34 + 11 + 2 + 10 + 10 + 10 + 2 + 3 + TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES + 6 + 34 + 34 + 66;
    public static final int HASH_LENGTH = 32;
    public static final int SIGNATURE_LENGTH = 64;
    private static final String HASH_DOMAIN = "nereus-delay-target-native-policy-snapshot\0";
    private static final String SIGNATURE_DOMAIN = "nereus-delay-target-native-policy-snapshot-signature\0";

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

    private TargetNativePolicySnapshot(
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
                || issuerKeyGeneration == 0) {
            throw new IllegalArgumentException("invalid handoff policy lease bounds");
        }
        this.effectiveLeadMs = effectiveLeadMs;
        this.validFromEpochMs = validFromEpochMs;
        this.validUntilEpochMs = validUntilEpochMs;
        if (allowedPathBits != 0 && allowedPathBits != HandoffPath.MANAGED_HANDOFF) {
            throw new IllegalArgumentException("Target Native snapshot cannot grant AUTO_FAST or unknown paths");
        }
        if ((mode == HandoffPolicyMode.DISABLED && (effectiveLeadMs != 0 || allowedPathBits != 0))
                || (mode != HandoffPolicyMode.DISABLED
                        && (effectiveLeadMs == 0 || allowedPathBits != HandoffPath.MANAGED_HANDOFF))) {
            throw new IllegalArgumentException("handoff policy mode and permissions disagree");
        }
        this.allowedPathBits = allowedPathBits;
        this.issuedAt = Objects.requireNonNull(issuedAt, "issuedAt");
        requireBoundedTime(this.issuedAt);
        if (issuedAt.latestEpochMs() > validFromEpochMs || issuedAt.latestEpochMs() >= validUntilEpochMs) {
            throw new IllegalArgumentException("policy issuance evidence is outside the lease window");
        }
        this.issuerKeyGeneration = issuerKeyGeneration;
        this.artifactGenerationSetDigest = fixed(artifactGenerationSetDigest, "artifactGenerationSetDigest");
        this.snapshotDigest = fixed(snapshotDigest, "snapshotDigest");
        Bytes.requireLength(signature, SIGNATURE_LENGTH, "signature");
        this.signature = Bytes.copy(signature);
    }

    public static TargetNativePolicySnapshot create(
            final TargetNativePolicyScope scope,
            final long generation,
            final HandoffPolicyMode mode,
            final long effectiveLeadMs,
            final long validFromEpochMs,
            final long validUntilEpochMs,
            final int allowedPathBits,
            final TrustedUtcIntervalEvidence issuedAt,
            final int issuerKeyGeneration,
            final PrivateKey issuerKey) {
        Objects.requireNonNull(scope, "scope");
        if (effectiveLeadMs > scope.fixedLeadCapMs()) {
            throw new IllegalArgumentException("Target Native lead exceeds the fixed queue cap");
        }
        final byte[] policyScopeDigest = scope.digest();
        final byte[] artifactGenerationSetDigest = scope.artifacts().digest();
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
        return new TargetNativePolicySnapshot(
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

    public static TargetNativePolicySnapshot decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 13, false, "TargetNativePolicySnapshot");
        QueryCodecSupport.requireNumbers(
                fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, "TargetNativePolicySnapshot");
        if (QueryCodecSupport.uint(fields.get(0), 1) != SCHEMA_GENERATION) {
            throw new IllegalArgumentException("unsupported TargetNativePolicySnapshot generation");
        }
        final byte[] scope = QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH);
        final long generation = QueryCodecSupport.uint64Bits(fields.get(2), 3);
        final HandoffPolicyMode mode = HandoffPolicyMode.fromWire(QueryCodecSupport.uint(fields.get(3), 4));
        final long lead = QueryCodecSupport.uint(fields.get(4), 5);
        final long validFrom = QueryCodecSupport.uint(fields.get(5), 6);
        final long validUntil = QueryCodecSupport.uint(fields.get(6), 7);
        final int pathBits = QueryCodecSupport.uint32Bits(fields.get(7), 8);
        TargetCompatibilityCodec.read(
                QueryCodecSupport.bytes(fields.get(8), 9),
                TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,
                10,
                false,
                "Target Native issued time");
        final TrustedUtcIntervalEvidence issuedAt =
                TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(fields.get(8), 9));
        final int keyGeneration = QueryCodecSupport.uint32Bits(fields.get(9), 10);
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
            throw new IllegalArgumentException("TargetNativePolicySnapshot digest mismatch");
        }
        final TargetNativePolicySnapshot result = new TargetNativePolicySnapshot(
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
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetNativePolicySnapshot");
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

    public HandoffPolicyHeadRef headRef(final long publicationRevision) {
        if (publicationRevision <= 0) {
            throw new IllegalArgumentException("Target Native head ref requires a positive publication revision");
        }
        return new HandoffPolicyHeadRef(policyScopeDigest, generation, snapshotDigest, publicationRevision);
    }

    public byte[] encodedKey() {
        return TargetKeyCodec.nativePolicySnapshot(snapshotDigest);
    }

    public void requireScope(final TargetNativePolicyScope scope) {
        if (!Arrays.equals(policyScopeDigest, scope.digest())
                || !Arrays.equals(artifactGenerationSetDigest, scope.artifacts().digest())
                || effectiveLeadMs > scope.fixedLeadCapMs()
                || (allowedPathBits & ~scope.allowedPathBits()) != 0) {
            throw new IllegalArgumentException("Target Native snapshot scope, artifacts, cap or path mismatch");
        }
    }

    public static TargetNativePolicySnapshot decodeForStore(
            final byte[] key, final byte[] encoded, final TargetNativePolicyScope scope, final ShardId shard) {
        final var snapshot = decode(encoded);
        snapshot.requireScope(scope);
        if (!Arrays.equals(key, snapshot.encodedKey()) || !shard.equals(scope.sourceShard())) {
            throw new IllegalArgumentException("Target Native snapshot key/Shard mismatch");
        }
        return snapshot;
    }

    public static void requireBoundedTime(final TrustedUtcIntervalEvidence time) {
        if (time.sourceId().length > TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES
                || time.canonicalBytes().length > TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES) {
            throw new IllegalArgumentException("Target Native time evidence exceeds its bound");
        }
    }

    public boolean allows(final int path) {
        return mode != HandoffPolicyMode.DISABLED && HandoffPath.includes(allowedPathBits, path);
    }

    /** Requires a trusted interval to prove that this lease is currently active. */
    public void requireActiveAt(final TrustedUtcIntervalEvidence trustedTime) {
        Objects.requireNonNull(trustedTime, "trustedTime");
        if (trustedTime.earliestEpochMs() < validFromEpochMs || trustedTime.latestEpochMs() >= validUntilEpochMs) {
            throw new IllegalArgumentException("trusted interval is not fully contained by the handoff policy lease");
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
            CanonicalProtobuf.uint32Bits(output, 10, issuerKeyGeneration);
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
            throw new IllegalArgumentException("cannot verify TargetNativePolicySnapshot signature", error);
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetNativePolicySnapshot that
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
            CanonicalProtobuf.uint32Bits(output, 10, keyGeneration);
            CanonicalProtobuf.bytes(output, 11, artifactDigest);
        });
    }

    private static byte[] signatureInput(final byte[] digest, final int keyGeneration) {
        return Bytes.concat(Bytes.utf8(SIGNATURE_DOMAIN), digest, Bytes.u32beBits(keyGeneration));
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
            throw new IllegalArgumentException("cannot sign TargetNativePolicySnapshot", error);
        }
    }

    private static byte[] fixed(final byte[] value, final String name) {
        return TargetCompatibilityCodec.assigned(value, HASH_LENGTH, name);
    }
}
