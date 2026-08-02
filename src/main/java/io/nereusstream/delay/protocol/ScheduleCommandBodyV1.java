package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry-shaped {@code ScheduleV1} client body, including its common fields. */
public final class ScheduleCommandBodyV1 {
    private static final int COMMAND_TYPE = 1;

    private final DelayMessageId delayMessageId;
    private final long retryUntilEpochMs;
    private final ScheduleIntentV1 intent;

    public ScheduleCommandBodyV1(final DelayMessageId delayMessageId, final long retryUntilEpochMs,
                                 final ScheduleIntentV1 intent) {
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntil must be non-negative");
        }
        this.retryUntilEpochMs = retryUntilEpochMs;
        this.intent = Objects.requireNonNull(intent, "intent");
        if (!intent.hasPayloadBranch()) {
            throw new IllegalArgumentException("ScheduleV1 requires a payload branch");
        }
    }

    public DelayMessageId delayMessageId() {
        return delayMessageId;
    }

    public long retryUntilEpochMs() {
        return retryUntilEpochMs;
    }

    public ScheduleIntentV1 intent() {
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

    public static ScheduleCommandBodyV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "ScheduleCommandBodyV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 10}, "ScheduleCommandBodyV1");
        if (QueryCodecSupport.uint(fields.get(1), 2) != COMMAND_TYPE) {
            throw new IllegalArgumentException("ScheduleCommandBodyV1 has the wrong command type");
        }
        final ScheduleCommandBodyV1 result = new ScheduleCommandBodyV1(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(0), 1, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint(fields.get(2), 3),
                ScheduleIntentV1.decode(QueryCodecSupport.nested(fields.get(3), 10)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ScheduleCommandBodyV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ScheduleCommandBodyV1 that && retryUntilEpochMs == that.retryUntilEpochMs
                && delayMessageId.equals(that.delayMessageId) && intent.equals(that.intent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delayMessageId, retryUntilEpochMs, intent);
    }
}
