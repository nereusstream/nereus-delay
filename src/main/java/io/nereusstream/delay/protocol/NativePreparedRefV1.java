package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Native AUTO_FAST identity projection; it is not a managed Message locator. */
public final class NativePreparedRefV1 {
    public static final int HASH_LENGTH = 32;
    public static final int NATIVE_DELIVERY_ID_LENGTH = 32;

    private final byte[] nativeDeliveryId;
    private final byte[] submissionHash;
    private final ProfileRefV1 destination;
    private final PulsarBrokerResourceIdentityV1 target;
    private final int physicalPartition;
    private final byte[] capabilitySnapshotDigest;
    private final long capabilityExpiryEpochMs;
    private final byte[] preparedBytesSha256;

    public NativePreparedRefV1(final byte[] nativeDeliveryId, final byte[] submissionHash,
                               final ProfileRefV1 destination, final PulsarBrokerResourceIdentityV1 target,
                               final int physicalPartition, final byte[] capabilitySnapshotDigest,
                               final long capabilityExpiryEpochMs, final byte[] preparedBytesSha256) {
        requireNonZero(nativeDeliveryId, NATIVE_DELIVERY_ID_LENGTH, "nativeDeliveryId");
        Bytes.requireLength(submissionHash, HASH_LENGTH, "submissionHash");
        this.nativeDeliveryId = Bytes.copy(nativeDeliveryId);
        this.submissionHash = Bytes.copy(submissionHash);
        this.destination = Objects.requireNonNull(destination, "destination");
        if (destination.profileKind() != ProfileKindV1.DESTINATION) {
            throw new IllegalArgumentException("native prepared destination must be a DESTINATION profile");
        }
        this.target = Objects.requireNonNull(target, "target");
        if (physicalPartition < 0 || capabilityExpiryEpochMs < 0) {
            throw new IllegalArgumentException("invalid native prepared numbers");
        }
        Bytes.requireLength(capabilitySnapshotDigest, HASH_LENGTH, "capabilitySnapshotDigest");
        Bytes.requireLength(preparedBytesSha256, HASH_LENGTH, "preparedBytesSha256");
        this.physicalPartition = physicalPartition;
        this.capabilitySnapshotDigest = Bytes.copy(capabilitySnapshotDigest);
        this.capabilityExpiryEpochMs = capabilityExpiryEpochMs;
        this.preparedBytesSha256 = Bytes.copy(preparedBytesSha256);
    }

    public byte[] nativeDeliveryId() {
        return Bytes.copy(nativeDeliveryId);
    }

    public byte[] submissionHash() {
        return Bytes.copy(submissionHash);
    }

    public ProfileRefV1 destination() {
        return destination;
    }

    public PulsarBrokerResourceIdentityV1 target() {
        return target;
    }

    public int physicalPartition() {
        return physicalPartition;
    }

    public byte[] capabilitySnapshotDigest() {
        return Bytes.copy(capabilitySnapshotDigest);
    }

    public long capabilityExpiryEpochMs() {
        return capabilityExpiryEpochMs;
    }

    public byte[] preparedBytesSha256() {
        return Bytes.copy(preparedBytesSha256);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, nativeDeliveryId);
            CanonicalProtobuf.bytes(output, 2, submissionHash);
            CanonicalProtobuf.bytes(output, 3, destination.canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, target.canonicalBytes());
            CanonicalProtobuf.uint32(output, 5, physicalPartition);
            CanonicalProtobuf.bytes(output, 6, capabilitySnapshotDigest);
            CanonicalProtobuf.int64(output, 7, capabilityExpiryEpochMs);
            CanonicalProtobuf.bytes(output, 8, preparedBytesSha256);
        });
    }

    public static NativePreparedRefV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "NativePreparedRefV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8}, "NativePreparedRefV1");
        final NativePreparedRefV1 result = new NativePreparedRefV1(
                QueryCodecSupport.fixed(fields.get(0), 1, NATIVE_DELIVERY_ID_LENGTH),
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(2), 3)),
                PulsarBrokerResourceIdentityV1.decode(QueryCodecSupport.nested(fields.get(3), 4)),
                QueryCodecSupport.uint32(fields.get(4), 5),
                QueryCodecSupport.fixed(fields.get(5), 6, HASH_LENGTH),
                QueryCodecSupport.uint(fields.get(6), 7),
                QueryCodecSupport.fixed(fields.get(7), 8, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "NativePreparedRefV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof NativePreparedRefV1 that)) {
            return false;
        }
        return physicalPartition == that.physicalPartition
                && capabilityExpiryEpochMs == that.capabilityExpiryEpochMs
                && Arrays.equals(nativeDeliveryId, that.nativeDeliveryId)
                && Arrays.equals(submissionHash, that.submissionHash)
                && destination.equals(that.destination) && target.equals(that.target)
                && Arrays.equals(capabilitySnapshotDigest, that.capabilitySnapshotDigest)
                && Arrays.equals(preparedBytesSha256, that.preparedBytesSha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(nativeDeliveryId), Arrays.hashCode(submissionHash), destination, target,
                physicalPartition, Arrays.hashCode(capabilitySnapshotDigest), capabilityExpiryEpochMs,
                Arrays.hashCode(preparedBytesSha256));
    }

    private static void requireNonZero(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        for (byte item : value) {
            if (item != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
