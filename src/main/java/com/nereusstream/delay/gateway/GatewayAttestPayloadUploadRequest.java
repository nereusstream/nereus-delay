package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import java.util.Objects;

/** Transport-neutral Gateway payload-attestation request. */
public record GatewayAttestPayloadUploadRequest(
        PayloadReservationReceipt reservation, OpaquePayloadUploadHandle handle) {
    public GatewayAttestPayloadUploadRequest {
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
