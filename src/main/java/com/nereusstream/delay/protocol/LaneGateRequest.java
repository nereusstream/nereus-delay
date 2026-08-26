package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 shared branch for pause/resume Destination Lane gates. */
public final class LaneGateRequest implements ControlOperationRequestBranch {
    private final ControlReason reason;

    public LaneGateRequest(final ControlReason reason) {
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ControlReason reason() {
        return reason;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, reason.canonicalBytes()));
    }

    public static LaneGateRequest decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "LaneGateRequest");
        QueryCodecSupport.requireNumbers(fields, new int[] {1}, "LaneGateRequest");
        final LaneGateRequest result =
                new LaneGateRequest(ControlReason.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LaneGateRequest");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LaneGateRequest that && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason);
    }
}
