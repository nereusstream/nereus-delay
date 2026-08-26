package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry-shaped {@code Cancel} client body. */
public final class CancelCommandBody {
    private static final int COMMAND_TYPE = 4;

    private final DelayMessageId delayMessageId;
    private final long retryUntilEpochMs;
    private final MessagePrecondition precondition;

    public CancelCommandBody(
            final DelayMessageId delayMessageId, final long retryUntilEpochMs, final MessagePrecondition precondition) {
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntil must be non-negative");
        }
        this.retryUntilEpochMs = retryUntilEpochMs;
        this.precondition = Objects.requireNonNull(precondition, "precondition");
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

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, delayMessageId.bytes());
            CanonicalProtobuf.uint32(output, 2, COMMAND_TYPE);
            CanonicalProtobuf.int64(output, 3, retryUntilEpochMs);
            CanonicalProtobuf.bytes(output, 10, precondition.canonicalBytes());
        });
    }

    public static CancelCommandBody decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "CancelCommandBody");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 10}, "CancelCommandBody");
        if (QueryCodecSupport.uint(fields.get(1), 2) != COMMAND_TYPE) {
            throw new IllegalArgumentException("CancelCommandBody has the wrong command type");
        }
        final CancelCommandBody result = new CancelCommandBody(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(0), 1, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint(fields.get(2), 3),
                MessagePrecondition.decode(QueryCodecSupport.nested(fields.get(3), 10)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CancelCommandBody");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CancelCommandBody that
                && retryUntilEpochMs == that.retryUntilEpochMs
                && delayMessageId.equals(that.delayMessageId)
                && precondition.equals(that.precondition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delayMessageId, retryUntilEpochMs, precondition);
    }
}
