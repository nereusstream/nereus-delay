package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Exact lane identity and optimistic control-version precondition. */
public final class LaneControlTarget {
    public static final int LANE_ID_LENGTH = DestinationLaneId.LENGTH;
    public static final int INCARNATION_LENGTH = RouteIncarnation.LENGTH;

    private final byte[] laneId;
    private final byte[] laneIncarnation;
    private final long expectedLaneControlVersion;

    public LaneControlTarget(final byte[] laneId, final byte[] laneIncarnation, final long expectedLaneControlVersion) {
        Bytes.requireLength(laneId, LANE_ID_LENGTH, "laneId");
        Bytes.requireLength(laneIncarnation, INCARNATION_LENGTH, "laneIncarnation");
        if (expectedLaneControlVersion <= 0) {
            throw new IllegalArgumentException("expectedLaneControlVersion must be positive");
        }
        this.laneId = Bytes.copy(laneId);
        this.laneIncarnation = Bytes.copy(laneIncarnation);
        this.expectedLaneControlVersion = expectedLaneControlVersion;
    }

    public byte[] laneId() {
        return Bytes.copy(laneId);
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public long expectedLaneControlVersion() {
        return expectedLaneControlVersion;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, laneId);
            CanonicalProtobuf.bytes(output, 2, laneIncarnation);
            CanonicalProtobuf.uint64(output, 3, expectedLaneControlVersion);
        });
    }

    public static LaneControlTarget decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "LaneControlTarget");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "LaneControlTarget");
        final LaneControlTarget result = new LaneControlTarget(
                QueryCodecSupport.fixed(fields.get(0), 1, LANE_ID_LENGTH),
                QueryCodecSupport.fixed(fields.get(1), 2, INCARNATION_LENGTH),
                QueryCodecSupport.uint(fields.get(2), 3));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LaneControlTarget");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LaneControlTarget that
                && expectedLaneControlVersion == that.expectedLaneControlVersion
                && Arrays.equals(laneId, that.laneId)
                && Arrays.equals(laneIncarnation, that.laneIncarnation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(laneId), Arrays.hashCode(laneIncarnation), expectedLaneControlVersion);
    }
}
