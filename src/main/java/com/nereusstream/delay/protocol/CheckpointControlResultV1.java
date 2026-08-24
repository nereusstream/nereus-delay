package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Typed result for a force/checkpoint control target. */
public final class CheckpointControlResultV1 {
    private static final int ID_LENGTH = 16;
    private static final int HASH_LENGTH = 32;

    private final ShardSubjectV1 shard;
    private final byte[] checkpointId;
    private final byte[] manifestSha256;
    private final long catalogGeneration;

    public CheckpointControlResultV1(
            final ShardSubjectV1 shard,
            final byte[] checkpointId,
            final byte[] manifestSha256,
            final long catalogGeneration) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.checkpointId = nonZeroFixed(checkpointId, ID_LENGTH, "checkpointId");
        this.manifestSha256 = fixed(manifestSha256, HASH_LENGTH, "manifestSha256");
        if (catalogGeneration == 0) {
            throw new IllegalArgumentException("catalogGeneration must be nonzero");
        }
        this.catalogGeneration = catalogGeneration;
    }

    public ShardSubjectV1 shard() {
        return shard;
    }

    public byte[] checkpointId() {
        return Bytes.copy(checkpointId);
    }

    public byte[] manifestSha256() {
        return Bytes.copy(manifestSha256);
    }

    public long catalogGeneration() {
        return catalogGeneration;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, checkpointId);
            CanonicalProtobuf.bytes(output, 3, manifestSha256);
            CanonicalProtobuf.uint64Bits(output, 4, catalogGeneration);
        });
    }

    public static CheckpointControlResultV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "CheckpointControlResultV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "CheckpointControlResultV1");
        final CheckpointControlResultV1 result = new CheckpointControlResultV1(
                ShardSubjectV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, ID_LENGTH),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                QueryCodecSupport.uint64Bits(fields.get(3), 4));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CheckpointControlResultV1");
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
        return other instanceof CheckpointControlResultV1 that
                && catalogGeneration == that.catalogGeneration
                && shard.equals(that.shard)
                && Arrays.equals(checkpointId, that.checkpointId)
                && Arrays.equals(manifestSha256, that.manifestSha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shard, Arrays.hashCode(checkpointId), Arrays.hashCode(manifestSha256), catalogGeneration);
    }
}
