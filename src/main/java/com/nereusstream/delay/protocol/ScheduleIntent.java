package com.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Compact Schedule intent for the embedded direct API.
 *
 * <p>Managed transport uses {@link CanonicalScheduleIntent}. Callers choose the embedded or managed API explicitly;
 * this type is not a fallback decoder or a numbered project revision.</p>
 */
public record ScheduleIntent(
        DestinationLaneId laneId,
        long deliverAtEpochMs,
        long expireAtEpochMs,
        OrderingMode orderingMode,
        byte[] payload) {
    public ScheduleIntent {
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(orderingMode, "orderingMode");
        Objects.requireNonNull(payload, "payload");
        if (deliverAtEpochMs < 0 || expireAtEpochMs < deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid delivery window");
        }
        if (payload.length > 0xffff_ffffL) {
            throw new IllegalArgumentException("payload is too large");
        }
        payload = Bytes.copy(payload);
    }

    @Override
    public byte[] payload() {
        return Bytes.copy(payload);
    }

    public byte[] canonicalBytes() {
        final ByteBuffer result = ByteBuffer.allocate(4 + 8 + 8 + DestinationLaneId.LENGTH + 1 + 4 + payload.length);
        result.putInt(1);
        result.putLong(deliverAtEpochMs);
        result.putLong(expireAtEpochMs);
        result.put(laneId.bytes());
        result.put((byte) orderingMode.wireValue());
        result.putInt(payload.length).put(payload);
        return result.array();
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ScheduleIntent that)) {
            return false;
        }
        return laneId.equals(that.laneId)
                && deliverAtEpochMs == that.deliverAtEpochMs
                && expireAtEpochMs == that.expireAtEpochMs
                && orderingMode == that.orderingMode
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(laneId, deliverAtEpochMs, expireAtEpochMs, orderingMode, Arrays.hashCode(payload));
    }
}
