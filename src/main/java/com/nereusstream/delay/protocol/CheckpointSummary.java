package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Public-safe summary of one checkpoint catalog entry. */
public final class CheckpointSummary {
    private static final int CHECKPOINT_ID_LENGTH = 16;
    private static final int HASH_LENGTH = 32;

    private final byte[] checkpointId;
    private final byte[] manifestSha256;
    private final SourcePosition appliedSourcePosition;
    private final long catalogGeneration;
    private final boolean recoveryFloor;

    public CheckpointSummary(
            final byte[] checkpointId,
            final byte[] manifestSha256,
            final SourcePosition appliedSourcePosition,
            final long catalogGeneration,
            final boolean recoveryFloor) {
        this.checkpointId = nonZeroFixed(checkpointId, CHECKPOINT_ID_LENGTH, "checkpointId");
        this.manifestSha256 = fixed(manifestSha256, HASH_LENGTH, "manifestSha256");
        this.appliedSourcePosition = Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        if (catalogGeneration == 0) {
            throw new IllegalArgumentException("catalogGeneration must be nonzero");
        }
        this.catalogGeneration = catalogGeneration;
        this.recoveryFloor = recoveryFloor;
    }

    public byte[] checkpointId() {
        return Bytes.copy(checkpointId);
    }

    public byte[] manifestSha256() {
        return Bytes.copy(manifestSha256);
    }

    public SourcePosition appliedSourcePosition() {
        return appliedSourcePosition;
    }

    public long catalogGeneration() {
        return catalogGeneration;
    }

    public boolean recoveryFloor() {
        return recoveryFloor;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, checkpointId);
            CanonicalProtobuf.bytes(output, 2, manifestSha256);
            CanonicalProtobuf.bytes(output, 3, QueryCodecSupport.encodeSourcePosition(appliedSourcePosition));
            CanonicalProtobuf.uint64Bits(output, 4, catalogGeneration);
            CanonicalProtobuf.uint32(output, 5, recoveryFloor ? 1 : 0);
        });
    }

    public static CheckpointSummary decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "CheckpointSummary");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5}, "CheckpointSummary");
        final CheckpointSummary result = new CheckpointSummary(
                QueryCodecSupport.fixed(fields.get(0), 1, CHECKPOINT_ID_LENGTH),
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(2), 3)),
                QueryCodecSupport.uint64Bits(fields.get(3), 4),
                QueryCodecSupport.bool(fields.get(4), 5));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CheckpointSummary");
        return result;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static byte[] nonZeroFixed(final byte[] value, final int length, final String name) {
        final byte[] result = fixed(value, length, name);
        boolean nonZero = false;
        for (byte current : result) {
            nonZero |= current != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CheckpointSummary that
                && catalogGeneration == that.catalogGeneration
                && recoveryFloor == that.recoveryFloor
                && appliedSourcePosition.equals(that.appliedSourcePosition)
                && Arrays.equals(checkpointId, that.checkpointId)
                && Arrays.equals(manifestSha256, that.manifestSha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                Arrays.hashCode(checkpointId),
                Arrays.hashCode(manifestSha256),
                appliedSourcePosition,
                catalogGeneration,
                recoveryFloor);
    }
}
