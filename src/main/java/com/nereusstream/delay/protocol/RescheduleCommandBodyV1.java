package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry-shaped {@code RescheduleV1} client body. */
public final class RescheduleCommandBodyV1 {
    private static final int COMMAND_TYPE = 5;

    private final DelayMessageId delayMessageId;
    private final long retryUntilEpochMs;
    private final MessagePreconditionV1 precondition;
    private final long newDeliverAtEpochMs;
    private final long newExpireAtEpochMs;

    public RescheduleCommandBodyV1(
            final DelayMessageId delayMessageId,
            final long retryUntilEpochMs,
            final MessagePreconditionV1 precondition,
            final long newDeliverAtEpochMs,
            final long newExpireAtEpochMs) {
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (retryUntilEpochMs < 0 || newDeliverAtEpochMs < 0 || newExpireAtEpochMs < newDeliverAtEpochMs) {
            throw new IllegalArgumentException("invalid RescheduleV1 timing");
        }
        this.retryUntilEpochMs = retryUntilEpochMs;
        this.precondition = Objects.requireNonNull(precondition, "precondition");
        this.newDeliverAtEpochMs = newDeliverAtEpochMs;
        this.newExpireAtEpochMs = newExpireAtEpochMs;
    }

    public DelayMessageId delayMessageId() {
        return delayMessageId;
    }

    public long retryUntilEpochMs() {
        return retryUntilEpochMs;
    }

    public MessagePreconditionV1 precondition() {
        return precondition;
    }

    public long newDeliverAtEpochMs() {
        return newDeliverAtEpochMs;
    }

    public long newExpireAtEpochMs() {
        return newExpireAtEpochMs;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, delayMessageId.bytes());
            CanonicalProtobuf.uint32(output, 2, COMMAND_TYPE);
            CanonicalProtobuf.int64(output, 3, retryUntilEpochMs);
            CanonicalProtobuf.bytes(output, 10, precondition.canonicalBytes());
            CanonicalProtobuf.int64(output, 11, newDeliverAtEpochMs);
            CanonicalProtobuf.int64(output, 12, newExpireAtEpochMs);
        });
    }

    public static RescheduleCommandBodyV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "RescheduleCommandBodyV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 10, 11, 12}, "RescheduleCommandBodyV1");
        if (QueryCodecSupport.uint(fields.get(1), 2) != COMMAND_TYPE) {
            throw new IllegalArgumentException("RescheduleCommandBodyV1 has the wrong command type");
        }
        final RescheduleCommandBodyV1 result = new RescheduleCommandBodyV1(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(0), 1, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint(fields.get(2), 3),
                MessagePreconditionV1.decode(QueryCodecSupport.nested(fields.get(3), 10)),
                QueryCodecSupport.uint(fields.get(4), 11),
                QueryCodecSupport.uint(fields.get(5), 12));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RescheduleCommandBodyV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof RescheduleCommandBodyV1 that
                && retryUntilEpochMs == that.retryUntilEpochMs
                && newDeliverAtEpochMs == that.newDeliverAtEpochMs
                && newExpireAtEpochMs == that.newExpireAtEpochMs
                && delayMessageId.equals(that.delayMessageId)
                && precondition.equals(that.precondition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delayMessageId, retryUntilEpochMs, precondition, newDeliverAtEpochMs, newExpireAtEpochMs);
    }
}
