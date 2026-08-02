package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Registry-shaped {@code PrepareLargeScheduleV1} client body. */
public final class PrepareLargeScheduleBodyV1 {
    private static final int COMMAND_TYPE = 2;
    private static final int HASH_LENGTH = 32;

    private final DelayMessageId delayMessageId;
    private final long retryUntilEpochMs;
    private final ScheduleIntentV1 intentWithoutPayload;
    private final long expectedPayloadLength;
    private final byte[] payloadSha256;
    private final long reservationTtlMs;
    private final PayloadProofTrustSetRefV1 trustSet;

    public PrepareLargeScheduleBodyV1(final DelayMessageId delayMessageId, final long retryUntilEpochMs,
                                      final ScheduleIntentV1 intentWithoutPayload,
                                      final long expectedPayloadLength, final byte[] payloadSha256,
                                      final long reservationTtlMs, final PayloadProofTrustSetRefV1 trustSet) {
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (retryUntilEpochMs < 0 || expectedPayloadLength < 0 || reservationTtlMs <= 0) {
            throw new IllegalArgumentException("invalid PrepareLargeSchedule timing/length");
        }
        this.retryUntilEpochMs = retryUntilEpochMs;
        this.intentWithoutPayload = Objects.requireNonNull(intentWithoutPayload, "intentWithoutPayload");
        if (intentWithoutPayload.hasPayloadBranch()) {
            throw new IllegalArgumentException("PrepareLargeSchedule must not select a payload branch");
        }
        this.expectedPayloadLength = expectedPayloadLength;
        Bytes.requireLength(payloadSha256, HASH_LENGTH, "payloadSha256");
        this.payloadSha256 = Bytes.copy(payloadSha256);
        this.reservationTtlMs = reservationTtlMs;
        this.trustSet = Objects.requireNonNull(trustSet, "trustSet");
    }

    public DelayMessageId delayMessageId() {
        return delayMessageId;
    }

    public long retryUntilEpochMs() {
        return retryUntilEpochMs;
    }

    public ScheduleIntentV1 intentWithoutPayload() {
        return intentWithoutPayload;
    }

    public long expectedPayloadLength() {
        return expectedPayloadLength;
    }

    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    public long reservationTtlMs() {
        return reservationTtlMs;
    }

    public PayloadProofTrustSetRefV1 trustSet() {
        return trustSet;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, delayMessageId.bytes());
            CanonicalProtobuf.uint32(output, 2, COMMAND_TYPE);
            CanonicalProtobuf.int64(output, 3, retryUntilEpochMs);
            CanonicalProtobuf.bytes(output, 10, intentWithoutPayload.canonicalBytes());
            CanonicalProtobuf.uint64(output, 11, expectedPayloadLength);
            CanonicalProtobuf.bytes(output, 12, payloadSha256);
            CanonicalProtobuf.uint64(output, 13, reservationTtlMs);
            CanonicalProtobuf.bytes(output, 14, trustSet.canonicalBytes());
        });
    }

    public static PrepareLargeScheduleBodyV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "PrepareLargeScheduleBodyV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 10, 11, 12, 13, 14},
                "PrepareLargeScheduleBodyV1");
        if (QueryCodecSupport.uint(fields.get(1), 2) != COMMAND_TYPE) {
            throw new IllegalArgumentException("PrepareLargeScheduleBodyV1 has the wrong command type");
        }
        final PrepareLargeScheduleBodyV1 result = new PrepareLargeScheduleBodyV1(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(0), 1, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint(fields.get(2), 3),
                ScheduleIntentV1.decode(QueryCodecSupport.nested(fields.get(3), 10)),
                QueryCodecSupport.uint(fields.get(4), 11), QueryCodecSupport.fixed(fields.get(5), 12, HASH_LENGTH),
                QueryCodecSupport.uint(fields.get(6), 13),
                PayloadProofTrustSetRefV1.decode(QueryCodecSupport.nested(fields.get(7), 14)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PrepareLargeScheduleBodyV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PrepareLargeScheduleBodyV1 that && retryUntilEpochMs == that.retryUntilEpochMs
                && expectedPayloadLength == that.expectedPayloadLength && reservationTtlMs == that.reservationTtlMs
                && delayMessageId.equals(that.delayMessageId) && intentWithoutPayload.equals(that.intentWithoutPayload)
                && Arrays.equals(payloadSha256, that.payloadSha256) && trustSet.equals(that.trustSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delayMessageId, retryUntilEpochMs, intentWithoutPayload, expectedPayloadLength,
                Arrays.hashCode(payloadSha256), reservationTtlMs, trustSet);
    }
}
