package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 operation branch that closes admission of new schedules. */
public final class StopNewSchedulesRequest implements ControlOperationRequestBranch {
    private final ControlReason reason;

    public StopNewSchedulesRequest(final ControlReason reason) {
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ControlReason reason() {
        return reason;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, reason.canonicalBytes()));
    }

    public static StopNewSchedulesRequest decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "StopNewSchedulesRequest");
        QueryCodecSupport.requireNumbers(fields, new int[] {1}, "StopNewSchedulesRequest");
        final StopNewSchedulesRequest result =
                new StopNewSchedulesRequest(ControlReason.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "StopNewSchedulesRequest");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof StopNewSchedulesRequest that && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason);
    }
}
