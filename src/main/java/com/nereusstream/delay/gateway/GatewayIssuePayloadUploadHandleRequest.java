package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.UploadHandleKind;
import java.util.Objects;

/** Transport-neutral Gateway upload-handle request. */
public record GatewayIssuePayloadUploadHandleRequest(PayloadReservationReceipt reservation, UploadHandleKind kind) {
    public GatewayIssuePayloadUploadHandleRequest {
        reservation = Objects.requireNonNull(reservation, "reservation");
        kind = Objects.requireNonNull(kind, "kind");
    }

    /** Canonical request fields 1..N used by payload audit hashing. */
    public byte[] canonicalBodyBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, reservation.payload());
            CanonicalProtobuf.uint32(output, 2, kind.wireValue());
        });
    }
}
