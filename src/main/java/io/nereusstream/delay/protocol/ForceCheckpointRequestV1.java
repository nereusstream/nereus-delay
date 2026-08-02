package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch for an operator-requested checkpoint. */
public final class ForceCheckpointRequestV1 implements ControlOperationRequestBranchV1 {
    private final ControlReasonV1 reason;

    public ForceCheckpointRequestV1(final ControlReasonV1 reason) {
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ControlReasonV1 reason() {
        return reason;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, reason.canonicalBytes()));
    }

    public static ForceCheckpointRequestV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "ForceCheckpointRequestV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1}, "ForceCheckpointRequestV1");
        final ForceCheckpointRequestV1 result = new ForceCheckpointRequestV1(
                ControlReasonV1.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ForceCheckpointRequestV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ForceCheckpointRequestV1 that && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason);
    }
}
