package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical public projection of one shard's checkpoint catalog. */
public final class CheckpointCatalogResultV1 {
    private static final int ID_LENGTH = 16;
    private static final int HASH_LENGTH = 32;

    private final ShardSubjectV1 shard;
    private final byte[] recoveryLineageId;
    private final byte[] floorCheckpointId;
    private final byte[] floorManifestSha256;
    private final long catalogGeneration;
    private final List<CheckpointSummaryV1> summaries;

    public CheckpointCatalogResultV1(final ShardSubjectV1 shard, final byte[] recoveryLineageId,
                                     final byte[] floorCheckpointId, final byte[] floorManifestSha256,
                                     final long catalogGeneration, final List<CheckpointSummaryV1> summaries) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.recoveryLineageId = nonZeroFixed(recoveryLineageId, ID_LENGTH, "recoveryLineageId");
        this.floorCheckpointId = nonZeroFixed(floorCheckpointId, ID_LENGTH, "floorCheckpointId");
        this.floorManifestSha256 = fixed(floorManifestSha256, HASH_LENGTH, "floorManifestSha256");
        if (catalogGeneration <= 0) {
            throw new IllegalArgumentException("catalogGeneration must be positive");
        }
        this.catalogGeneration = catalogGeneration;
        this.summaries = sortedUnique(summaries, shard);
        for (CheckpointSummaryV1 summary : this.summaries) {
            if (summary.recoveryFloor()
                    && (!Arrays.equals(summary.checkpointId(), this.floorCheckpointId)
                    || !Arrays.equals(summary.manifestSha256(), this.floorManifestSha256))) {
                throw new IllegalArgumentException("Recovery Floor summary does not match catalog Floor identity");
            }
        }
    }

    public ShardSubjectV1 shard() {
        return shard;
    }

    public byte[] recoveryLineageId() {
        return Bytes.copy(recoveryLineageId);
    }

    public byte[] floorCheckpointId() {
        return Bytes.copy(floorCheckpointId);
    }

    public byte[] floorManifestSha256() {
        return Bytes.copy(floorManifestSha256);
    }

    public long catalogGeneration() {
        return catalogGeneration;
    }

    public List<CheckpointSummaryV1> summaries() {
        return summaries;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, recoveryLineageId);
            CanonicalProtobuf.bytes(output, 3, floorCheckpointId);
            CanonicalProtobuf.bytes(output, 4, floorManifestSha256);
            CanonicalProtobuf.uint64(output, 5, catalogGeneration);
            for (CheckpointSummaryV1 summary : summaries) {
                CanonicalProtobuf.bytes(output, 6, summary.canonicalBytes());
            }
        });
    }

    public static CheckpointCatalogResultV1 decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 5) {
            throw new IllegalArgumentException("CheckpointCatalogResultV1 has too few fields");
        }
        if (fields.get(0).number() != 1 || fields.get(1).number() != 2 || fields.get(2).number() != 3
                || fields.get(3).number() != 4 || fields.get(4).number() != 5) {
            throw new IllegalArgumentException("CheckpointCatalogResultV1 has invalid required field order");
        }
        final List<CheckpointSummaryV1> summaries = new ArrayList<>();
        for (int index = 5; index < fields.size(); index++) {
            if (fields.get(index).number() != 6) {
                throw new IllegalArgumentException("CheckpointCatalogResultV1 summaries must use field 6");
            }
            summaries.add(CheckpointSummaryV1.decode(QueryCodecSupport.nested(fields.get(index), 6)));
        }
        final CheckpointCatalogResultV1 result = new CheckpointCatalogResultV1(
                ShardSubjectV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, ID_LENGTH),
                QueryCodecSupport.fixed(fields.get(2), 3, ID_LENGTH),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH),
                QueryCodecSupport.uint(fields.get(4), 5), summaries);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CheckpointCatalogResultV1");
        return result;
    }

    private static List<CheckpointSummaryV1> sortedUnique(final List<CheckpointSummaryV1> values,
                                                           final ShardSubjectV1 shard) {
        Objects.requireNonNull(values, "summaries");
        final List<CheckpointSummaryV1> result = new ArrayList<>(values.size());
        for (CheckpointSummaryV1 value : values) {
            Objects.requireNonNull(value, "checkpoint summary");
            if (!shard.shardId().equals(value.appliedSourcePosition().shardId())) {
                throw new IllegalArgumentException("checkpoint summary belongs to another shard");
            }
            if (!result.isEmpty()) {
                final CheckpointSummaryV1 previous = result.get(result.size() - 1);
                final int order = Long.compare(previous.catalogGeneration(), value.catalogGeneration()) != 0
                        ? Long.compare(previous.catalogGeneration(), value.catalogGeneration())
                        : Arrays.compareUnsigned(previous.checkpointId(), value.checkpointId());
                if (order >= 0) {
                    throw new IllegalArgumentException("checkpoint summaries must be sorted and unique");
                }
            }
            result.add(value);
        }
        int floorCount = 0;
        for (CheckpointSummaryV1 summary : result) {
            if (summary.recoveryFloor()) {
                floorCount++;
            }
        }
        if (floorCount > 1) {
            throw new IllegalArgumentException("checkpoint catalog must have at most one Recovery Floor summary");
        }
        return List.copyOf(result);
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
        return other instanceof CheckpointCatalogResultV1 that
                && catalogGeneration == that.catalogGeneration && shard.equals(that.shard)
                && Arrays.equals(recoveryLineageId, that.recoveryLineageId)
                && Arrays.equals(floorCheckpointId, that.floorCheckpointId)
                && Arrays.equals(floorManifestSha256, that.floorManifestSha256)
                && summaries.equals(that.summaries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shard, Arrays.hashCode(recoveryLineageId), Arrays.hashCode(floorCheckpointId),
                Arrays.hashCode(floorManifestSha256), catalogGeneration, summaries);
    }
}
