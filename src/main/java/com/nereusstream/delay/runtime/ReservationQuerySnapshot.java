package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import java.util.Objects;

/** Bounded local projection of one large-payload reservation. */
public record ReservationQuerySnapshot(
        byte[] reservationId,
        DelayMessageId delayMessageId,
        long stateVersion,
        PayloadReservationStatus status,
        long reservationExpiryEpochMs,
        PayloadAvailability payloadAvailability) {
    public ReservationQuerySnapshot {
        Bytes.requireLength(reservationId, 32, "reservationId");
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(payloadAvailability, "payloadAvailability");
        if (stateVersion <= 0 || reservationExpiryEpochMs < 0) {
            throw new IllegalArgumentException("invalid reservation query snapshot");
        }
        if ((status == PayloadReservationStatus.RESERVED && payloadAvailability != PayloadAvailability.UPLOAD_PENDING)
                || (status == PayloadReservationStatus.COMMITTED
                        && payloadAvailability != PayloadAvailability.OBJECT_RETAINED)
                || ((status == PayloadReservationStatus.ABANDONED || status == PayloadReservationStatus.EXPIRED)
                        && payloadAvailability != PayloadAvailability.NOT_APPLICABLE)) {
            throw new IllegalArgumentException("reservation payload availability does not match status");
        }
        reservationId = Bytes.copy(reservationId);
    }

    @Override
    public byte[] reservationId() {
        return Bytes.copy(reservationId);
    }
}
