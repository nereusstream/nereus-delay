package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Canonical Registry {@code OwnerIdentityV1}. */
public final class OwnerIdentityV1 {
    public static final int HASH_LENGTH = 32;

    private final byte[] deploymentId;
    private final byte[] workerRunId;
    private final long ownerEpoch;
    private final byte[] leaseFencingDigest;

    public OwnerIdentityV1(final byte[] deploymentId, final byte[] workerRunId, final long ownerEpoch,
                           final byte[] leaseFencingDigest) {
        this.deploymentId = nonEmpty(deploymentId, "deploymentId");
        this.workerRunId = nonEmpty(workerRunId, "workerRunId");
        if (ownerEpoch <= 0) {
            throw new IllegalArgumentException("ownerEpoch must be positive");
        }
        this.ownerEpoch = ownerEpoch;
        Bytes.requireLength(leaseFencingDigest, HASH_LENGTH, "leaseFencingDigest");
        this.leaseFencingDigest = Bytes.copy(leaseFencingDigest);
    }

    public byte[] deploymentId() {
        return Bytes.copy(deploymentId);
    }

    public byte[] workerRunId() {
        return Bytes.copy(workerRunId);
    }

    public long ownerEpoch() {
        return ownerEpoch;
    }

    public byte[] leaseFencingDigest() {
        return Bytes.copy(leaseFencingDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, deploymentId);
            CanonicalProtobuf.bytes(output, 2, workerRunId);
            CanonicalProtobuf.uint64(output, 3, ownerEpoch);
            CanonicalProtobuf.bytes(output, 4, leaseFencingDigest);
        });
    }

    public static OwnerIdentityV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "OwnerIdentityV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4}, "OwnerIdentityV1");
        final OwnerIdentityV1 result = new OwnerIdentityV1(
                QueryCodecSupport.bytes(fields.get(0), 1),
                QueryCodecSupport.bytes(fields.get(1), 2),
                QueryCodecSupport.uint(fields.get(2), 3),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "OwnerIdentityV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof OwnerIdentityV1 that && ownerEpoch == that.ownerEpoch
                && Arrays.equals(deploymentId, that.deploymentId)
                && Arrays.equals(workerRunId, that.workerRunId)
                && Arrays.equals(leaseFencingDigest, that.leaseFencingDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(deploymentId), Arrays.hashCode(workerRunId), ownerEpoch,
                Arrays.hashCode(leaseFencingDigest));
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }
}
