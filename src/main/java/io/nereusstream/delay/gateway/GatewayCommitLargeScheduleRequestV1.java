package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;

import java.util.Objects;

/** Transport-neutral Gateway CommitLargeSchedule request. */
public record GatewayCommitLargeScheduleRequestV1(
        byte[] idempotencyKey,
        PayloadReservationReceiptV1 reservation,
        PayloadCommitProofV1 proof,
        long retryUntilEpochMs) {
    public GatewayCommitLargeScheduleRequestV1 {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        if (idempotencyKey.length < 16 || idempotencyKey.length > 64) {
            throw new IllegalArgumentException("idempotencyKey must be 16..64 bytes");
        }
        idempotencyKey = Bytes.copy(idempotencyKey);
        reservation = Objects.requireNonNull(reservation, "reservation");
        proof = Objects.requireNonNull(proof, "proof");
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntilEpochMs must be non-negative");
        }
    }

    @Override
    public byte[] idempotencyKey() {
        return Bytes.copy(idempotencyKey);
    }

    /** Canonical request fields 2..N used by Gateway bodyHash. */
    public byte[] canonicalBodyBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 2, reservation.payload());
            CanonicalProtobuf.bytes(output, 3, proof.canonicalBytes());
            CanonicalProtobuf.int64(output, 4, retryUntilEpochMs);
        });
    }
}
