package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.UploadHandleKindV1;

import java.util.Objects;

/** Transport-neutral Gateway upload-handle request. */
public record GatewayIssuePayloadUploadHandleRequestV1(PayloadReservationReceiptV1 reservation,
                                                       UploadHandleKindV1 kind) {
    public GatewayIssuePayloadUploadHandleRequestV1 {
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
