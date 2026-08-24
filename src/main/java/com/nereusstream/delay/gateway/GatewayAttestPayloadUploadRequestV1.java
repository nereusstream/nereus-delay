package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import com.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import java.util.Objects;

/** Transport-neutral Gateway payload-attestation request. */
public record GatewayAttestPayloadUploadRequestV1(
        PayloadReservationReceiptV1 reservation, OpaquePayloadUploadHandleV1 handle) {
    public GatewayAttestPayloadUploadRequestV1 {
        reservation = Objects.requireNonNull(reservation, "reservation");
        handle = Objects.requireNonNull(handle, "handle");
    }

    /** Canonical request fields 1..N used by payload audit hashing. */
    public byte[] canonicalBodyBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, reservation.payload());
            CanonicalProtobuf.bytes(output, 2, handle.canonicalBytes());
        });
    }
}
