package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.Objects;

/**
 * Immutable external evidence bundle required before a REAPING provider call.
 * The two opaque hashes identify certified old-owner and provider-horizon
 * attestations; this value does not issue either attestation.
 */
public record CheckpointReapingQuiescenceProof(
        byte[] pendingIntentDigest,
        TrustedUtcIntervalEvidence reapingEvidence,
        TrustedUtcIntervalEvidence observedAt,
        TrustedUtcIntervalEvidence oldOwnerGuardClosedAt,
        TrustedUtcIntervalEvidence providerOwnershipClosedAt,
        long requestQuiescenceHorizonMs,
        long maximumProviderOwnershipLifetimeMs,
        long maximumTrustedUtcIntervalWidthMs,
        byte[] oldOwnerGuardEvidenceDigest,
        byte[] providerOwnershipEvidenceDigest) {
    private static final int HASH_LENGTH = 32;

    public CheckpointReapingQuiescenceProof {
        Bytes.requireLength(pendingIntentDigest, HASH_LENGTH, "pendingIntentDigest");
        reapingEvidence = Objects.requireNonNull(reapingEvidence, "reapingEvidence");
        observedAt = Objects.requireNonNull(observedAt, "observedAt");
        oldOwnerGuardClosedAt = Objects.requireNonNull(oldOwnerGuardClosedAt, "oldOwnerGuardClosedAt");
        providerOwnershipClosedAt = Objects.requireNonNull(providerOwnershipClosedAt, "providerOwnershipClosedAt");
        if (requestQuiescenceHorizonMs <= 0
                || maximumProviderOwnershipLifetimeMs < 0
                || maximumTrustedUtcIntervalWidthMs < 0) {
            throw new IllegalArgumentException("invalid checkpoint reaping quiescence bounds");
        }
        long minimumHorizon;
        try {
            minimumHorizon = Math.addExact(maximumProviderOwnershipLifetimeMs, maximumTrustedUtcIntervalWidthMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("checkpoint reaping quiescence horizon overflow", overflow);
        }
        if (requestQuiescenceHorizonMs < minimumHorizon) {
            throw new IllegalArgumentException(
                    "checkpoint reaping horizon is shorter than provider ownership plus clock width");
        }
        reapingEvidence.requireWidthAtMost(maximumTrustedUtcIntervalWidthMs);
        observedAt.requireWidthAtMost(maximumTrustedUtcIntervalWidthMs);
        oldOwnerGuardClosedAt.requireWidthAtMost(maximumTrustedUtcIntervalWidthMs);
        providerOwnershipClosedAt.requireWidthAtMost(maximumTrustedUtcIntervalWidthMs);
        requireNonZeroDigest(oldOwnerGuardEvidenceDigest, "oldOwnerGuardEvidenceDigest");
        requireNonZeroDigest(providerOwnershipEvidenceDigest, "providerOwnershipEvidenceDigest");
        pendingIntentDigest = Bytes.copy(pendingIntentDigest);
        oldOwnerGuardEvidenceDigest = Bytes.copy(oldOwnerGuardEvidenceDigest);
        providerOwnershipEvidenceDigest = Bytes.copy(providerOwnershipEvidenceDigest);
    }

    @Override
    public byte[] pendingIntentDigest() {
        return Bytes.copy(pendingIntentDigest);
    }

    @Override
    public byte[] oldOwnerGuardEvidenceDigest() {
        return Bytes.copy(oldOwnerGuardEvidenceDigest);
    }

    @Override
    public byte[] providerOwnershipEvidenceDigest() {
        return Bytes.copy(providerOwnershipEvidenceDigest);
    }

    private static void requireNonZeroDigest(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        for (byte element : value) {
            if (element != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
