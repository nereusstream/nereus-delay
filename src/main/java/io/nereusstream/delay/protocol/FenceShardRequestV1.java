package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch for fencing a shard for maintenance. */
public final class FenceShardRequestV1 implements ControlOperationRequestBranchV1 {
    private final ControlReasonV1 reason;

    public FenceShardRequestV1(final ControlReasonV1 reason) {
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ControlReasonV1 reason() {
        return reason;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, reason.canonicalBytes()));
    }

    public static FenceShardRequestV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "FenceShardRequestV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1}, "FenceShardRequestV1");
        final FenceShardRequestV1 result = new FenceShardRequestV1(
                ControlReasonV1.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "FenceShardRequestV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof FenceShardRequestV1 that && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason);
    }
}
