package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Public-safe result for one Lane control target. */
public final class LaneControlResultV1 {
    private final DestinationLaneId laneId;
    private final byte[] laneIncarnation;
    private final long laneControlVersion;
    private final LaneAdmissionGateV1 admissionGate;
    private final long outstandingAttempts;
    private final StableCode stableCode;

    public LaneControlResultV1(final DestinationLaneId laneId, final byte[] laneIncarnation,
                               final long laneControlVersion, final LaneAdmissionGateV1 admissionGate,
                               final long outstandingAttempts, final StableCode stableCode) {
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        this.laneIncarnation = fixed(laneIncarnation, 16, "laneIncarnation");
        if (laneControlVersion <= 0 || outstandingAttempts < 0) {
            throw new IllegalArgumentException("invalid Lane control counters");
        }
        this.laneControlVersion = laneControlVersion;
        this.admissionGate = Objects.requireNonNull(admissionGate, "admissionGate");
        this.outstandingAttempts = outstandingAttempts;
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
    }

    public DestinationLaneId laneId() {
        return laneId;
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public long laneControlVersion() {
        return laneControlVersion;
    }

    public LaneAdmissionGateV1 admissionGate() {
        return admissionGate;
    }

    public long outstandingAttempts() {
        return outstandingAttempts;
    }

    public StableCode stableCode() {
        return stableCode;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, laneId.bytes());
            CanonicalProtobuf.bytes(output, 2, laneIncarnation);
            CanonicalProtobuf.uint64(output, 3, laneControlVersion);
            CanonicalProtobuf.uint32(output, 4, admissionGate.wireValue());
            CanonicalProtobuf.uint64(output, 5, outstandingAttempts);
            CanonicalProtobuf.uint32(output, 6, stableCode.wireValue());
        });
    }

    public static LaneControlResultV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "LaneControlResultV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6}, "LaneControlResultV1");
        final LaneControlResultV1 result = new LaneControlResultV1(
                new DestinationLaneId(QueryCodecSupport.fixed(fields.get(0), 1, DestinationLaneId.LENGTH)),
                QueryCodecSupport.fixed(fields.get(1), 2, 16), QueryCodecSupport.uint(fields.get(2), 3),
                LaneAdmissionGateV1.fromWire(QueryCodecSupport.uint(fields.get(3), 4)),
                QueryCodecSupport.uint(fields.get(4), 5), StableCode.fromWire(QueryCodecSupport.uint32(fields.get(5), 6)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LaneControlResultV1");
        return result;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LaneControlResultV1 that && laneControlVersion == that.laneControlVersion
                && outstandingAttempts == that.outstandingAttempts && laneId.equals(that.laneId)
                && Arrays.equals(laneIncarnation, that.laneIncarnation) && admissionGate == that.admissionGate
                && stableCode == that.stableCode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(laneId, Arrays.hashCode(laneIncarnation), laneControlVersion, admissionGate,
                outstandingAttempts, stableCode);
    }
}
