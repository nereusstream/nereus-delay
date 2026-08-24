package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.MessagePreconditionV1;
import java.util.Objects;

/** Transport-neutral Gateway Reschedule request with an idempotency key. */
public record GatewayRescheduleRequestV1(
        byte[] idempotencyKey,
        DelayMessageId delayMessageId,
        MessagePreconditionV1 messagePrecondition,
        long deliverAtEpochMs,
        long expireAtEpochMs,
        long retryUntilEpochMs) {
    public GatewayRescheduleRequestV1 {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        if (idempotencyKey.length < 16 || idempotencyKey.length > 64) {
            throw new IllegalArgumentException("idempotencyKey must be 16..64 bytes");
        }
        idempotencyKey = Bytes.copy(idempotencyKey);
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        Objects.requireNonNull(messagePrecondition, "messagePrecondition");
        if (deliverAtEpochMs < 0
                || expireAtEpochMs < 0
                || retryUntilEpochMs < 0
                || expireAtEpochMs < deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid Gateway reschedule timing");
        }
    }

    @Override
    public byte[] idempotencyKey() {
        return Bytes.copy(idempotencyKey);
    }

    /** Canonical request fields 2..N used by Gateway bodyHash. */
    public byte[] canonicalBodyBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 2, delayMessageId.bytes());
            CanonicalProtobuf.bytes(output, 3, messagePrecondition.canonicalBytes());
            CanonicalProtobuf.int64(output, 4, deliverAtEpochMs);
            CanonicalProtobuf.int64(output, 5, expireAtEpochMs);
            CanonicalProtobuf.int64(output, 6, retryUntilEpochMs);
        });
    }
}
