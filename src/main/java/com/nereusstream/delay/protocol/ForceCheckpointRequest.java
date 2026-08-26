package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch for an operator-requested checkpoint. */
public final class ForceCheckpointRequest implements ControlOperationRequestBranch {
    private final ControlReason reason;

    public ForceCheckpointRequest(final ControlReason reason) {
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ControlReason reason() {
        return reason;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, reason.canonicalBytes()));
    }

    public static ForceCheckpointRequest decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ForceCheckpointRequest");
        QueryCodecSupport.requireNumbers(fields, new int[] {1}, "ForceCheckpointRequest");
        final ForceCheckpointRequest result =
                new ForceCheckpointRequest(ControlReason.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ForceCheckpointRequest");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ForceCheckpointRequest that && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason);
    }
}
