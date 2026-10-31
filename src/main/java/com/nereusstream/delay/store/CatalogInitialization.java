package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.Arrays;
import java.util.Objects;

/** Durable authenticated Catalog genesis facts; neither a published Manifest nor recovery/Floor authority. */
public record CatalogInitialization(
        CheckpointUploadIntent pending,
        byte[] dbIdentity,
        byte[] manifestSha256,
        SourcePosition source,
        long mutationSequence) {
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-catalog-initialization\0");
    private static final int MAX_BYTES = 1 << 20;

    public CatalogInitialization {
        Objects.requireNonNull(pending, "pending");
        if (pending.state() != CheckpointUploadState.PENDING_UPLOAD
                || pending.baseCatalogGeneration() != 1
                || pending.parentCheckpointId() != null) {
            throw new IllegalArgumentException("Catalog initialization requires a generation-1 genesis pending intent");
        }
        Bytes.requireLength(dbIdentity, 32, "dbIdentity");
        Bytes.requireLength(manifestSha256, 32, "manifestSha256");
        new StoreMetadata(2, pending.shard().shardId(), pending.sourceStoreIncarnation(), dbIdentity);
        dbIdentity = Bytes.copy(dbIdentity);
        manifestSha256 = Bytes.copy(manifestSha256);
        TargetSourcePosition.requireBounded(source);
        if (!pending.shard().shardId().equals(source.shardId()) || mutationSequence == 0) {
            throw new IllegalArgumentException("Catalog initialization lacks exact bounded Source/mutation identity");
        }
    }

    @Override
    public byte[] dbIdentity() {
        return Bytes.copy(dbIdentity);
    }

    @Override
    public byte[] manifestSha256() {
        return Bytes.copy(manifestSha256);
    }

    static CatalogInitialization from(CheckpointUploadIntent pending, CheckpointManifest manifest) {
        final var at = manifest.createdAt();
        final var time = new TrustedUtcIntervalEvidence(
                at.earliestEpochMs(),
                at.latestEpochMs(),
                TrustedUtcIntervalEvidence.Source.valueOf(at.source()),
                at.sourceId(),
                at.sourceConfigGeneration(),
                at.sampleSequence(),
                at.monotonicAnchorNs(),
                at.sourceEvidenceSha256(),
                at.sourceKeyVersion(),
                at.sourceSignature());
        if (!time.equals(pending.checkpointCreatedAt())
                || manifest.storeFormatVersion() != 2
                || manifest.lineageGeneration() != 0
                || manifest.parentCheckpoint() != null
                || !pending.shard().shardId().equals(manifest.shardId())
                || !Arrays.equals(pending.recoveryLineageId(), manifest.recoveryLineageId())
                || !Arrays.equals(pending.checkpointId(), manifest.checkpointId())
                || !Arrays.equals(
                        pending.owner().deploymentId(), manifest.createdBy().deploymentId())
                || !Arrays.equals(
                        pending.owner().workerRunId(), manifest.createdBy().workerRunId())
                || pending.owner().ownerEpoch() != manifest.createdBy().ownerEpoch()
                || !Arrays.equals(
                        pending.sourceStoreIncarnation(),
                        java.nio.ByteBuffer.allocate(16)
                                .putLong(manifest.sourceStoreIncarnation().getMostSignificantBits())
                                .putLong(manifest.sourceStoreIncarnation().getLeastSignificantBits())
                                .array())) {
            throw new IllegalArgumentException("Catalog initialization intent differs from its authenticated image");
        }
        return new CatalogInitialization(
                pending,
                manifest.dbIdentity(),
                manifest.manifestSha256(),
                manifest.appliedShardLogPosition(),
                manifest.shardMutationSequence());
    }

    void requireFirstManifest(CheckpointManifest manifest) {
        if (manifest.storeFormatVersion() != 2
                || !pending.shard().shardId().equals(manifest.shardId())
                || !Arrays.equals(pending.recoveryLineageId(), manifest.recoveryLineageId())
                || !Arrays.equals(dbIdentity, manifest.dbIdentity())
                || !source.sameSourceIdentity(manifest.appliedShardLogPosition())
                || manifest.appliedShardLogPosition().compareTo(source) < 0
                || Long.compareUnsigned(manifest.shardMutationSequence(), mutationSequence) < 0) {
            throw new IllegalArgumentException("first Target Manifest contradicts authenticated Catalog genesis");
        }
    }

    public byte[] canonicalBytes() {
        final byte[] fields = fields();
        if (fields.length > MAX_BYTES - 64) {
            throw new IllegalArgumentException("Catalog initialization exceeds its byte bound");
        }
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields);
            CanonicalProtobuf.bytes(out, 7, Bytes.sha256(DOMAIN, fields));
        });
    }

    public static CatalogInitialization decode(byte[] encoded) {
        if (encoded == null || encoded.length > MAX_BYTES) {
            throw new IllegalArgumentException("Catalog initialization exceeds its byte bound");
        }
        final var fields = QueryCodecSupport.read(encoded, "CatalogInitialization");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7}, "CatalogInitialization");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != 1) {
            throw new IllegalArgumentException("unsupported Catalog initialization version");
        }
        final var result = new CatalogInitialization(
                CheckpointUploadIntent.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.fixed(fields.get(2), 3, 32),
                QueryCodecSupport.fixed(fields.get(3), 4, 32),
                SourcePositionCodec.decode(QueryCodecSupport.bytes(fields.get(4), 5)),
                QueryCodecSupport.uint(fields.get(5), 6));
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("Catalog initialization digest/canonical bytes mismatch");
        }
        return result;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof CatalogInitialization initialization
                && Arrays.equals(canonicalBytes(), initialization.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, pending.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, dbIdentity);
            CanonicalProtobuf.bytes(out, 4, manifestSha256);
            CanonicalProtobuf.bytes(out, 5, source.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 6, mutationSequence);
        });
    }
}
