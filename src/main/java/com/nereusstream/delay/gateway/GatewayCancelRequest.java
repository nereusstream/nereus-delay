package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.MessagePrecondition;
import java.util.Objects;

/** Transport-neutral Gateway Cancel request with an idempotency key. */
public record GatewayCancelRequest(
        byte[] idempotencyKey,
        DelayMessageId delayMessageId,
        MessagePrecondition messagePrecondition,
        long retryUntilEpochMs) {
    public GatewayCancelRequest {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        if (idempotencyKey.length < 16 || idempotencyKey.length > 64) {
            throw new IllegalArgumentException("idempotencyKey must be 16..64 bytes");
        }
        idempotencyKey = Bytes.copy(idempotencyKey);
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        Objects.requireNonNull(messagePrecondition, "messagePrecondition");
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
            CanonicalProtobuf.bytes(output, 2, delayMessageId.bytes());
            CanonicalProtobuf.bytes(output, 3, messagePrecondition.canonicalBytes());
            CanonicalProtobuf.int64(output, 4, retryUntilEpochMs);
        });
    }
}
