package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetMigrationPlan;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.runtime.TargetMigrationStore;
import com.nereusstream.delay.store.CheckpointManifest;
import com.nereusstream.delay.store.StoreMetadata;
import java.util.Arrays;
import java.util.Objects;

/** Durable metadata adoption of an already committed Source A; it grants no Source activation. */
public final class TargetMigrationAssignmentReceipt {
    private static final int MAX_BYTES = TargetMigrationPlan.MAX_CANONICAL_BYTES + 131_072;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-migration-assignment-adoption-v1\0");
    private final TargetMigrationPlan plan;
    private final TargetQuotaMutation activation;
    private final byte[] rootDigest, catalogNamespace, checkpointId, manifestDigest, previousDigest;
    private final long previousRevision;
    private final WorkerAssignment original;

    private TargetMigrationAssignmentReceipt(
            TargetMigrationPlan plan,
            TargetQuotaMutation activation,
            byte[] rootDigest,
            byte[] catalogNamespace,
            byte[] checkpointId,
            byte[] manifestDigest,
            long previousRevision,
            byte[] previousDigest,
            WorkerAssignment original) {
        this.plan = Objects.requireNonNull(plan);
        this.activation = Objects.requireNonNull(activation);
        activation.requireAfter(plan.baseline());
        this.rootDigest = fixed(rootDigest, 32);
        this.catalogNamespace = fixed(catalogNamespace, 32);
        this.checkpointId = fixed(checkpointId, 16);
        this.manifestDigest = fixed(manifestDigest, 32);
        this.previousDigest = fixed(previousDigest, 32);
        this.previousRevision = previousRevision;
        this.original = Objects.requireNonNull(original);
        if (previousRevision < 1
                || previousRevision == Long.MAX_VALUE
                || !original.routeBound()
                || !original.sourceAssignment().shardId().equals(plan.scope().shard())) {
            throw new IllegalArgumentException("migration adoption lost its original assignment/Shard");
        }
    }

    static TargetMigrationAssignmentReceipt from(
            WorkerAssignmentAuthority.Publication prior,
            TargetMigrationStore.Activated applied,
            CheckpointManifest manifest,
            byte[] catalogNamespace) {
        applied.requireCurrent();
        final var root = applied.migration();
        final var value = new TargetMigrationAssignmentReceipt(
                root.plan(),
                root.activatedAt(),
                root.digest(),
                catalogNamespace,
                manifest.checkpointId(),
                manifest.manifestSha256(),
                prior.revision(),
                prior.digest(),
                prior.assignment());
        value.requireManifest(manifest);
        value.requireApplied(applied);
        return value;
    }

    public TargetMigrationPlan plan() {
        return plan;
    }

    public TargetQuotaMutation activation() {
        return activation;
    }

    public byte[] catalogNamespace() {
        return Bytes.copy(catalogNamespace);
    }

    public byte[] checkpointId() {
        return Bytes.copy(checkpointId);
    }

    public byte[] manifestDigest() {
        return Bytes.copy(manifestDigest);
    }

    public long adoptedRevision() {
        return Math.addExact(previousRevision, 1);
    }

    void requirePublication(long revision, WorkerAssignment current) {
        if (revision < adoptedRevision()
                || !current.sourceAssignment().shardId().equals(plan.scope().shard())
                || !Arrays.equals(current.routeSnapshotDigest(), original.routeSnapshotDigest())) {
            throw new IllegalArgumentException("assignment lost its original migration binding");
        }
    }

    public void requireNative(StoreMetadata metadata, TargetMigrationRoot actual) {
        TargetMigrationStore.requireIdentity(metadata, plan);
        if (actual == null
                || !actual.activated()
                || !actual.plan().equals(plan)
                || !actual.activatedAt().equals(activation)
                || !Arrays.equals(rootDigest, actual.digest())) {
            throw new IllegalStateException("reopened Native Store differs from adopted exact Source A");
        }
    }

    public void requireApplied(TargetMigrationStore.Activated actual) {
        actual.requireCurrent();
        requireNative(actual.candidate().store().metadata(), actual.migration());
    }

    public void requireManifest(CheckpointManifest manifest) {
        final var uuid = manifest.sourceStoreIncarnation();
        final byte[] store = java.nio.ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
        if (manifest.storeFormatVersion() != plan.newFormat()
                || !manifest.shardId().equals(plan.scope().shard())
                || !Arrays.equals(store, plan.newStore())
                || !Arrays.equals(manifest.dbIdentity(), plan.newDbIdentity())
                || !Arrays.equals(manifest.recoveryLineageId(), plan.newLineage())
                || !Arrays.equals(checkpointId, manifest.checkpointId())
                || !Arrays.equals(manifestDigest, manifest.manifestSha256())
                || !manifest.appliedShardLogPosition().sameSourceIdentity(activation.source())
                || manifest.appliedShardLogPosition().compareTo(activation.source()) < 0
                || Long.compareUnsigned(manifest.shardMutationSequence(), activation.sequence()) < 0) {
            throw new IllegalStateException("adoption requires the exact formal post-A Target Manifest");
        }
    }

    public byte[] canonicalBytes() {
        final byte[] body = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, plan.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, activation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, rootDigest);
            CanonicalProtobuf.bytes(out, 5, catalogNamespace);
            CanonicalProtobuf.bytes(out, 6, checkpointId);
            CanonicalProtobuf.bytes(out, 7, manifestDigest);
            CanonicalProtobuf.uint64Bits(out, 8, previousRevision);
            CanonicalProtobuf.bytes(out, 9, previousDigest);
            CanonicalProtobuf.bytes(out, 10, original.canonicalBytes());
        });
        final var bytes = CanonicalProtobuf.message(out -> {
            out.writeBytes(body);
            CanonicalProtobuf.bytes(out, 11, Bytes.sha256(DOMAIN, body));
        });
        if (bytes.length > MAX_BYTES) {
            throw new IllegalArgumentException("migration adoption exceeds finite metadata bound");
        }
        return bytes;
    }

    public static TargetMigrationAssignmentReceipt decode(byte[] encoded) {
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_BYTES) {
            throw new IllegalArgumentException("migration adoption exceeds finite metadata bound");
        }
        final var f = QueryCodecSupport.read(encoded, "migration assignment adoption");
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, "migration assignment adoption");
        if (QueryCodecSupport.uint32(f.get(0), 1) != 1) {
            throw new IllegalArgumentException("unknown migration adoption version");
        }
        final var value = new TargetMigrationAssignmentReceipt(
                TargetMigrationPlan.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                QueryCodecSupport.fixed(f.get(3), 4, 32),
                QueryCodecSupport.fixed(f.get(4), 5, 32),
                QueryCodecSupport.fixed(f.get(5), 6, 16),
                QueryCodecSupport.fixed(f.get(6), 7, 32),
                QueryCodecSupport.uint64Bits(f.get(7), 8),
                QueryCodecSupport.fixed(f.get(8), 9, 32),
                WorkerAssignment.decode(QueryCodecSupport.bytes(f.get(9), 10)));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration assignment adoption");
        return value;
    }

    private static byte[] fixed(byte[] value, int length) {
        Bytes.requireLength(value, length, "migration adoption field");
        return Bytes.copy(value);
    }
}
