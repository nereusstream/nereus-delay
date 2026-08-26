package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.ProfileRef;
import java.util.Arrays;
import java.util.Objects;

/** Exact zero-I/O input for the Registry PrepareLargeSchedule command. */
public final class LargeSchedulePreparation {
    private final CanonicalScheduleIntent intentWithoutPayload;
    private final long expectedPayloadLength;
    private final byte[] payloadSha256;
    private final long reservationTtlMs;
    private final PayloadProofTrustSetRef trustSet;
    private final ProfileRef objectStoreProfile;

    public LargeSchedulePreparation(
            final CanonicalScheduleIntent intentWithoutPayload,
            final long expectedPayloadLength,
            final byte[] payloadSha256,
            final long reservationTtlMs,
            final PayloadProofTrustSetRef trustSet,
            final ProfileRef objectStoreProfile) {
        this.intentWithoutPayload = Objects.requireNonNull(intentWithoutPayload, "intentWithoutPayload");
        if (intentWithoutPayload.hasPayloadBranch()) {
            throw new IllegalArgumentException("large preparation intent must not contain a payload branch");
        }
        if (expectedPayloadLength < 0 || reservationTtlMs <= 0) {
            throw new IllegalArgumentException("invalid large preparation bounds");
        }
        this.expectedPayloadLength = expectedPayloadLength;
        com.nereusstream.delay.protocol.Bytes.requireLength(payloadSha256, 32, "payloadSha256");
        this.payloadSha256 = com.nereusstream.delay.protocol.Bytes.copy(payloadSha256);
        this.reservationTtlMs = reservationTtlMs;
        this.trustSet = Objects.requireNonNull(trustSet, "trustSet");
        this.objectStoreProfile = Objects.requireNonNull(objectStoreProfile, "objectStoreProfile");
    }

    public CanonicalScheduleIntent intentWithoutPayload() {
        return intentWithoutPayload;
    }

    public long expectedPayloadLength() {
        return expectedPayloadLength;
    }

    public byte[] payloadSha256() {
        return com.nereusstream.delay.protocol.Bytes.copy(payloadSha256);
    }

    public long reservationTtlMs() {
        return reservationTtlMs;
    }

    public PayloadProofTrustSetRef trustSet() {
        return trustSet;
    }

    public ProfileRef objectStoreProfile() {
        return objectStoreProfile;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LargeSchedulePreparation that
                && intentWithoutPayload.equals(that.intentWithoutPayload)
                && expectedPayloadLength == that.expectedPayloadLength
                && Arrays.equals(payloadSha256, that.payloadSha256)
                && reservationTtlMs == that.reservationTtlMs
                && trustSet.equals(that.trustSet)
                && objectStoreProfile.equals(that.objectStoreProfile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                intentWithoutPayload,
                expectedPayloadLength,
                Arrays.hashCode(payloadSha256),
                reservationTtlMs,
                trustSet,
                objectStoreProfile);
    }
}
