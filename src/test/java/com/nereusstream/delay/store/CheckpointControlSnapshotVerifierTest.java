package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CompatibleControlSnapshot;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointControlSnapshotVerifierTest {
    @TempDir
    Path tempDir;

    @Test
    void validatesPhysicalControlSnapshotAgainstManifestDigest() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("source"));
        final Path checkpoint = tempDir.resolve("checkpoint");
        final CompatibleControlSnapshot snapshot = controlSnapshot(shardId);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.recordControlSnapshot(snapshot);
            store.createCheckpoint(checkpoint, bytes(16, 1));
        }

        assertDoesNotThrow(
                () -> CheckpointControlSnapshotVerifier.validate(checkpoint, shardId, snapshot.snapshotDigest()));
        assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointControlSnapshotVerifier.validate(checkpoint, shardId, new byte[32]));
    }

    @Test
    void rejectsMissingControlSnapshotFromRecognizedRocksDbImage() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("missing-control-source"));
        final Path checkpoint = tempDir.resolve("missing-control-checkpoint");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.createCheckpoint(checkpoint, bytes(16, 9));
        }

        final IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointControlSnapshotVerifier.validate(checkpoint, shardId, new byte[32]));
        org.junit.jupiter.api.Assertions.assertTrue(failure.getMessage().contains("missing control snapshot"));
    }

    @Test
    void rejectsManifestWhenCheckpointStoreIdentityDrifts() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 9);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("identity-source"));
        final Path checkpoint = tempDir.resolve("identity-checkpoint");
        final CompatibleControlSnapshot snapshot = controlSnapshot(shardId);
        final KafkaSourcePosition appliedPosition =
                new KafkaSourcePosition(shardId, "cluster", java.util.UUID.randomUUID(), 0, null, 1_000);
        final CheckpointManifest manifest;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.recordControlSnapshot(snapshot);
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(3), appliedPosition.canonicalBytes());
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64beBits(0));
            });
            store.createCheckpoint(checkpoint, bytes(16, 10));
            manifest = manifestFor(checkpoint, shardId, store, bytes(16, 10), snapshot.snapshotDigest());
        }

        assertDoesNotThrow(() -> CheckpointControlSnapshotVerifier.validate(checkpoint, manifest));
        final CheckpointManifest wrongIdentity = new CheckpointManifest(
                manifest.checkpointId(),
                manifest.recoveryLineageId(),
                manifest.lineageGeneration(),
                manifest.parentCheckpoint(),
                manifest.restoredFromCheckpointId(),
                manifest.createdBy(),
                manifest.createdAt(),
                manifest.shardId(),
                bytes(32, 90),
                manifest.sourceStoreIncarnation(),
                manifest.storeFormatVersion(),
                manifest.shardMutationSequence(),
                manifest.appliedShardLogPosition(),
                manifest.controlStateDigest(),
                manifest.referencedSemanticVersionsDigest(),
                manifest.evidenceCursors(),
                manifest.files());
        final IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointControlSnapshotVerifier.validate(checkpoint, wrongIdentity));
        org.junit.jupiter.api.Assertions.assertTrue(failure.getMessage().contains("DB identity"));
    }

    private static CompatibleControlSnapshot controlSnapshot(final ShardId shardId) {
        return new CompatibleControlSnapshot(
                new ShardSubject(shardId),
                List.of(new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRef(bytes(32, 2), 1, bytes(32, 3), ProfileKind.DESTINATION)),
                new QuotaGrantRef(
                        bytes(32, 4),
                        1,
                        new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    private static CheckpointManifest manifestFor(
            final Path checkpoint,
            final ShardId shardId,
            final ShardStore store,
            final byte[] checkpointId,
            final byte[] controlStateDigest) {
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(checkpoint).stream()
                .map(file -> new CheckpointManifest.FileEntry(
                        file.name(),
                        file.length(),
                        file.checksum(),
                        Bytes.utf8("object/" + file.name()),
                        Bytes.utf8("version-1"),
                        null))
                .toList();
        final KafkaSourcePosition position = (KafkaSourcePosition) store.appliedShardLogPosition();
        return new CheckpointManifest(
                checkpointId,
                bytes(16, 11),
                0,
                null,
                null,
                new CheckpointManifest.CreatedBy(bytes(8, 12), bytes(8, 13), 1),
                new CheckpointManifest.CreatedAt(
                        900,
                        1_000,
                        "CERTIFIED_HOST_CLOCK",
                        bytes(8, 14),
                        1,
                        2,
                        3,
                        Bytes.sha256(Bytes.utf8("time")),
                        0,
                        null),
                shardId,
                store.metadata().dbIdentity(),
                store.metadata().storeIncarnationUuid(),
                1,
                store.shardMutationSequence(),
                position,
                controlStateDigest,
                bytes(32, 15),
                List.of(),
                files);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
