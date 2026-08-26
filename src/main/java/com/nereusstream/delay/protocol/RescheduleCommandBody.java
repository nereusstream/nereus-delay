package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry-shaped {@code Reschedule} client body. */
public final class RescheduleCommandBody {
    private static final int COMMAND_TYPE = 5;

    private final DelayMessageId delayMessageId;
    private final long retryUntilEpochMs;
    private final MessagePrecondition precondition;
    private final long newDeliverAtEpochMs;
    private final long newExpireAtEpochMs;

    public RescheduleCommandBody(
            final DelayMessageId delayMessageId,
            final long retryUntilEpochMs,
            final MessagePrecondition precondition,
            final long newDeliverAtEpochMs,
            final long newExpireAtEpochMs) {
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (retryUntilEpochMs < 0 || newDeliverAtEpochMs < 0 || newExpireAtEpochMs < newDeliverAtEpochMs) {
            throw new IllegalArgumentException("invalid Reschedule timing");
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

    public MessagePrecondition precondition() {
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

    public static RescheduleCommandBody decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "RescheduleCommandBody");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 10, 11, 12}, "RescheduleCommandBody");
        if (QueryCodecSupport.uint(fields.get(1), 2) != COMMAND_TYPE) {
            throw new IllegalArgumentException("RescheduleCommandBody has the wrong command type");
        }
        final RescheduleCommandBody result = new RescheduleCommandBody(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(0), 1, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint(fields.get(2), 3),
                MessagePrecondition.decode(QueryCodecSupport.nested(fields.get(3), 10)),
                QueryCodecSupport.uint(fields.get(4), 11),
                QueryCodecSupport.uint(fields.get(5), 12));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RescheduleCommandBody");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof RescheduleCommandBody that
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
