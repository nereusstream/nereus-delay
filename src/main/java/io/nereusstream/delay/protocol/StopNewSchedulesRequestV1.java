package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 operation branch that closes admission of new schedules. */
public final class StopNewSchedulesRequestV1 implements ControlOperationRequestBranchV1 {
    private final ControlReasonV1 reason;

    public StopNewSchedulesRequestV1(final ControlReasonV1 reason) {
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ControlReasonV1 reason() {
        return reason;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, reason.canonicalBytes()));
    }

    public static StopNewSchedulesRequestV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "StopNewSchedulesRequestV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1}, "StopNewSchedulesRequestV1");
        final StopNewSchedulesRequestV1 result = new StopNewSchedulesRequestV1(
                ControlReasonV1.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "StopNewSchedulesRequestV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof StopNewSchedulesRequestV1 that && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason);
    }
}
