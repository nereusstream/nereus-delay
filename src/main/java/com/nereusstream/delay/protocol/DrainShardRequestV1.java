package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch for bounded shard drain. */
public final class DrainShardRequestV1 implements ControlOperationRequestBranchV1 {
    private final ControlReasonV1 reason;
    private final long maxDrainWaitMs;
    private final boolean requestFinalCheckpoint;

    public DrainShardRequestV1(
            final ControlReasonV1 reason, final long maxDrainWaitMs, final boolean requestFinalCheckpoint) {
        this.reason = Objects.requireNonNull(reason, "reason");
        if (maxDrainWaitMs < 0) {
            throw new IllegalArgumentException("maxDrainWaitMs must be non-negative");
        }
        this.maxDrainWaitMs = maxDrainWaitMs;
        this.requestFinalCheckpoint = requestFinalCheckpoint;
    }

    public ControlReasonV1 reason() {
        return reason;
    }

    public long maxDrainWaitMs() {
        return maxDrainWaitMs;
    }

    public boolean requestFinalCheckpoint() {
        return requestFinalCheckpoint;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, reason.canonicalBytes());
            CanonicalProtobuf.uint64(output, 2, maxDrainWaitMs);
            CanonicalProtobuf.uint32(output, 3, requestFinalCheckpoint ? 1 : 0);
        });
    }

    public static DrainShardRequestV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "DrainShardRequestV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "DrainShardRequestV1");
        final DrainShardRequestV1 result = new DrainShardRequestV1(
                ControlReasonV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.uint(fields.get(1), 2),
                QueryCodecSupport.bool(fields.get(2), 3));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "DrainShardRequestV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof DrainShardRequestV1 that
                && maxDrainWaitMs == that.maxDrainWaitMs
                && requestFinalCheckpoint == that.requestFinalCheckpoint
                && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason, maxDrainWaitMs, requestFinalCheckpoint);
    }
}
