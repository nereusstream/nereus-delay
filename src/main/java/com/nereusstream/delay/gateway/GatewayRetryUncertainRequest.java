package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import java.util.Objects;

/** Exact Gateway RetryUncertain request fields from Registry §6.5. */
public record GatewayRetryUncertainRequest(
        byte[] originalIdempotencyKey,
        PhysicalEnqueueAttemptId expectedPriorPhysicalAttemptId,
        PhysicalEnqueueAttemptId retryRequestId) {
    public GatewayRetryUncertainRequest {
        Objects.requireNonNull(originalIdempotencyKey, "originalIdempotencyKey");
        if (originalIdempotencyKey.length < 16 || originalIdempotencyKey.length > 64) {
            throw new IllegalArgumentException("originalIdempotencyKey must be 16..64 bytes");
        }
        originalIdempotencyKey = Bytes.copy(originalIdempotencyKey);
        expectedPriorPhysicalAttemptId =
                Objects.requireNonNull(expectedPriorPhysicalAttemptId, "expectedPriorPhysicalAttemptId");
        retryRequestId = Objects.requireNonNull(retryRequestId, "retryRequestId");
    }

    @Override
    public byte[] originalIdempotencyKey() {
        return Bytes.copy(originalIdempotencyKey);
    }
}
