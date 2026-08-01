package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical per-Lane charge projection used by the quota usage map. */
public final class LaneQuotaUsageEntryV1 {
    private static final int HASH_LENGTH = 32;
    private static final int INCARNATION_LENGTH = 16;
    private static final byte[] DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-lane-quota-usage-entry-v1\0");

    private final DestinationLaneId laneId;
    private final byte[] laneIncarnation;
    private final PublishAdmissionBody.ChargeVector usage;
    private final long usageRevision;
    private final byte[] entryDigest;

    public LaneQuotaUsageEntryV1(final DestinationLaneId laneId, final byte[] laneIncarnation,
                                 final PublishAdmissionBody.ChargeVector usage, final long usageRevision) {
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "laneIncarnation");
        this.usage = Objects.requireNonNull(usage, "usage");
        if (usageRevision <= 0) {
            throw new IllegalArgumentException("usageRevision must be positive");
        }
        this.usageRevision = usageRevision;
        this.entryDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToFour());
    }

    public DestinationLaneId laneId() {
        return laneId;
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public PublishAdmissionBody.ChargeVector usage() {
        return usage;
    }

    public long usageRevision() {
        return usageRevision;
    }

    public byte[] entryDigest() {
        return Bytes.copy(entryDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToFour());
            CanonicalProtobuf.bytes(output, 5, entryDigest);
        });
    }

    public static LaneQuotaUsageEntryV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "LaneQuotaUsageEntryV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5}, "LaneQuotaUsageEntryV1");
        final byte[] entryDigest = QueryCodecSupport.fixed(fields.get(4), 5, HASH_LENGTH);
        final LaneQuotaUsageEntryV1 result = new LaneQuotaUsageEntryV1(
                new DestinationLaneId(QueryCodecSupport.fixed(fields.get(0), 1, DestinationLaneId.LENGTH)),
                QueryCodecSupport.fixed(fields.get(1), 2, INCARNATION_LENGTH),
                PublishAdmissionBody.ChargeVector.decodeCanonical(QueryCodecSupport.nested(fields.get(2), 3)),
                QueryCodecSupport.uint(fields.get(3), 4));
        if (!Bytes.constantTimeEquals(entryDigest, result.entryDigest)) {
            throw new IllegalArgumentException("LaneQuotaUsageEntryV1 digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LaneQuotaUsageEntryV1");
        return result;
    }

    private byte[] fieldsOneToFour() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, laneId.bytes());
            CanonicalProtobuf.bytes(output, 2, laneIncarnation);
            CanonicalProtobuf.bytes(output, 3, usage.canonicalBytes());
            CanonicalProtobuf.uint64(output, 4, usageRevision);
        });
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LaneQuotaUsageEntryV1 that && usageRevision == that.usageRevision
                && laneId.equals(that.laneId) && Arrays.equals(laneIncarnation, that.laneIncarnation)
                && usage.equals(that.usage) && Arrays.equals(entryDigest, that.entryDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(laneId, Arrays.hashCode(laneIncarnation), usage, usageRevision,
                Arrays.hashCode(entryDigest));
    }
}
