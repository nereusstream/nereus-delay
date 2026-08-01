package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Schedule metadata used by the reserve/upload/commit large-payload path.
 * The payload bytes themselves never enter this command body.
 */
public record LargeScheduleIntent(
        DestinationLaneId laneId,
        long deliverAtEpochMs,
        long expireAtEpochMs,
        OrderingMode orderingMode,
        long expectedPayloadLength,
        byte[] payloadSha256,
        long reservationTtlMs,
        long payloadProofTrustSetVersion) {
    public LargeScheduleIntent {
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(orderingMode, "orderingMode");
        Bytes.requireLength(payloadSha256, 32, "payloadSha256");
        if (deliverAtEpochMs < 0 || expireAtEpochMs < deliverAtEpochMs || expectedPayloadLength < 0
                || reservationTtlMs <= 0 || payloadProofTrustSetVersion <= 0) {
            throw new IllegalArgumentException("invalid large schedule intent");
        }
        payloadSha256 = Bytes.copy(payloadSha256);
    }

    @Override
    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    /** Fixed versioned binary projection used by the current embedded codec. */
    public byte[] canonicalBytes() {
        return ByteBuffer.allocate(4 + 8 + 8 + DestinationLaneId.LENGTH + 1 + 8 + 32 + 8 + 8)
                .putInt(1)
                .putLong(deliverAtEpochMs)
                .putLong(expireAtEpochMs)
                .put(laneId.bytes())
                .put((byte) orderingMode.wireValue())
                .putLong(expectedPayloadLength)
                .put(payloadSha256)
                .putLong(reservationTtlMs)
                .putLong(payloadProofTrustSetVersion)
                .array();
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof LargeScheduleIntent that)) {
            return false;
        }
        return laneId.equals(that.laneId) && deliverAtEpochMs == that.deliverAtEpochMs
                && expireAtEpochMs == that.expireAtEpochMs && orderingMode == that.orderingMode
                && expectedPayloadLength == that.expectedPayloadLength
                && Arrays.equals(payloadSha256, that.payloadSha256)
                && reservationTtlMs == that.reservationTtlMs
                && payloadProofTrustSetVersion == that.payloadProofTrustSetVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(laneId, deliverAtEpochMs, expireAtEpochMs, orderingMode, expectedPayloadLength,
                Arrays.hashCode(payloadSha256), reservationTtlMs, payloadProofTrustSetVersion);
    }
}
