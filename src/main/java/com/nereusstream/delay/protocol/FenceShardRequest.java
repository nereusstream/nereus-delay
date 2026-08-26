package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch for fencing a shard for maintenance. */
public final class FenceShardRequest implements ControlOperationRequestBranch {
    private final ControlReason reason;

    public FenceShardRequest(final ControlReason reason) {
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ControlReason reason() {
        return reason;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, reason.canonicalBytes()));
    }

    public static FenceShardRequest decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "FenceShardRequest");
        QueryCodecSupport.requireNumbers(fields, new int[] {1}, "FenceShardRequest");
        final FenceShardRequest result =
                new FenceShardRequest(ControlReason.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "FenceShardRequest");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof FenceShardRequest that && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason);
    }
}
