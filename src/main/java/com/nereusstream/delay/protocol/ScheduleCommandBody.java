package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry-shaped {@code Schedule} client body, including its common fields. */
public final class ScheduleCommandBody {
    private static final int COMMAND_TYPE = 1;

    private final DelayMessageId delayMessageId;
    private final long retryUntilEpochMs;
    private final CanonicalScheduleIntent intent;

    public ScheduleCommandBody(
            final DelayMessageId delayMessageId, final long retryUntilEpochMs, final CanonicalScheduleIntent intent) {
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntil must be non-negative");
        }
        this.retryUntilEpochMs = retryUntilEpochMs;
        this.intent = Objects.requireNonNull(intent, "intent");
        if (!intent.hasPayloadBranch()) {
            throw new IllegalArgumentException("Schedule requires a payload branch");
        }
    }

    public DelayMessageId delayMessageId() {
        return delayMessageId;
    }

    public long retryUntilEpochMs() {
        return retryUntilEpochMs;
    }

    public CanonicalScheduleIntent intent() {
        return intent;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, delayMessageId.bytes());
            CanonicalProtobuf.uint32(output, 2, COMMAND_TYPE);
            CanonicalProtobuf.int64(output, 3, retryUntilEpochMs);
            CanonicalProtobuf.bytes(output, 10, intent.canonicalBytes());
        });
    }

    public static ScheduleCommandBody decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ScheduleCommandBody");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 10}, "ScheduleCommandBody");
        if (QueryCodecSupport.uint(fields.get(1), 2) != COMMAND_TYPE) {
            throw new IllegalArgumentException("ScheduleCommandBody has the wrong command type");
        }
        final ScheduleCommandBody result = new ScheduleCommandBody(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(0), 1, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint(fields.get(2), 3),
                CanonicalScheduleIntent.decode(QueryCodecSupport.nested(fields.get(3), 10)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ScheduleCommandBody");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ScheduleCommandBody that
                && retryUntilEpochMs == that.retryUntilEpochMs
                && delayMessageId.equals(that.delayMessageId)
                && intent.equals(that.intent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delayMessageId, retryUntilEpochMs, intent);
    }
}
