package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import java.util.Objects;

/** Transport-neutral Gateway CommitLargeSchedule request. */
public record GatewayCommitLargeScheduleRequest(
        byte[] idempotencyKey,
        PayloadReservationReceipt reservation,
        CanonicalPayloadCommitProof proof,
        long retryUntilEpochMs) {
    public GatewayCommitLargeScheduleRequest {
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
