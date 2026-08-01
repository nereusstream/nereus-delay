package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RecoveryCatalogTest {
    @Test
    void publishesAncestryAdvancesFloorAndSelectsOnlyDescendants() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(1);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(2), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        assertEquals(1, catalog.publish(genesis, 0).catalogGeneration());

        final CheckpointManifest child = manifest(shard, topic, lineage, id16(3), 1, 2, 2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        assertEquals(2, catalog.publish(child, 1).catalogGeneration());
        final RecoveryFloor firstFloor = catalog.advanceFloor(genesis.checkpointId(), 2, id32(4));
        assertArrayEquals(genesis.checkpointId(), firstFloor.checkpointId());
        assertEquals(List.of(genesis, child), catalog.recoverySet(child.checkpointId()));

        final RecoveryFloor secondFloor = catalog.advanceFloor(child.checkpointId(), 3, id32(5));
        assertArrayEquals(child.checkpointId(), secondFloor.checkpointId());
        assertEquals(List.of(child), catalog.recoverySet(child.checkpointId()));
        catalog.validatePublishedRestoreCandidate(child);
        assertEquals(child, catalog.selectRecoveryCandidate(child.checkpointId()));
        assertEquals(4, catalog.publish(child, 4).catalogGeneration());
    }

    @Test
    void floorCoverageRequiresExactAncestryAndFloorCounters() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(40);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(41), 0, 10, 10, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(genesis, 0);
        final CheckpointManifest child = manifest(shard, topic, lineage, id16(42), 1, 11, 11,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        catalog.publish(child, 1);
        catalog.advanceFloor(genesis.checkpointId(), 2, id32(43));

        assertTrue(catalog.proveFloorCoverage(child.checkpointId(), 10, genesis.appliedShardLogPosition()).isPresent());
        assertFalse(catalog.proveFloorCoverage(child.checkpointId(), 11, genesis.appliedShardLogPosition()).isPresent());
        assertFalse(catalog.proveFloorCoverage(genesis.checkpointId(), 10,
                new KafkaSourcePosition(shard, "cluster", topic, 11, null, 1_011)).isPresent());

        catalog.advanceFloor(child.checkpointId(), 3, id32(44));
        final RecoveryCatalog.FloorCoverage coverage = catalog.proveFloorCoverage(child.checkpointId(), 11,
                child.appliedShardLogPosition()).orElseThrow();
        assertArrayEquals(child.checkpointId(), coverage.floor().checkpointId());
        assertEquals(List.of(child), coverage.ancestry());
    }

    @Test
    void floorCannotMoveAcrossAPublishedSibling() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final byte[] lineage = id16(30);
        final CheckpointManifest genesis = manifest(shard, UUID.randomUUID(), lineage, id16(31), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(genesis, 0);
        final CheckpointManifest first = manifest(shard, ((KafkaSourcePosition) genesis.appliedShardLogPosition())
                .nativeTopicUuid(), lineage, id16(32), 1, 2, 2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        final CheckpointManifest sibling = manifest(shard, ((KafkaSourcePosition) genesis.appliedShardLogPosition())
                .nativeTopicUuid(), lineage, id16(33), 1, 3, 3,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        catalog.publish(first, 1);
        catalog.publish(sibling, 2);
        catalog.advanceFloor(first.checkpointId(), 3, id32(34));
        assertThrows(IllegalStateException.class,
                () -> catalog.advanceFloor(sibling.checkpointId(), 4, id32(35)));
    }

    @Test
    void rejectsParentHashLineageAndSourceIdentityViolations() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final byte[] lineage = id16(10);
        final CheckpointManifest parent = manifest(shard, UUID.randomUUID(), lineage, id16(11), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(parent, 0);
        final CheckpointManifest wrongHash = manifest(shard, parent.appliedShardLogPosition() instanceof KafkaSourcePosition p
                ? p.nativeTopicUuid() : UUID.randomUUID(), lineage, id16(12), 1, 2, 2,
                new CheckpointManifest.ParentCheckpoint(parent.checkpointId(), Bytes.hex(id32(13))));
        assertThrows(IllegalArgumentException.class, () -> catalog.publish(wrongHash, 1));

        final CheckpointManifest wrongSource = manifest(shard, UUID.randomUUID(), lineage, id16(14), 1, 2, 2,
                new CheckpointManifest.ParentCheckpoint(parent.checkpointId(), Bytes.hex(parent.manifestSha256())));
        assertThrows(IllegalArgumentException.class, () -> catalog.publish(wrongSource, 1));
    }

    @Test
    void OxiaBoundaryDelegatesCasAndRejectsIdentityDrift() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), id16(50), id16(51), 0, 1, 1, null);
        final CheckpointManifest wrongPublication = manifest(shard, UUID.randomUUID(), id16(52), id16(53), 0,
                1, 1, null);
        final OxiaRecoveryCatalog authority = new OxiaRecoveryCatalog(new RecoveryCatalog());
        assertEquals(1, authority.publish(manifest, 0).catalogGeneration());
        assertEquals(manifest.canonicalJson(), authority.manifest(manifest.checkpointId()).orElseThrow()
                .canonicalJson());
        authority.validatePublishedRestoreCandidate(manifest);

        final OxiaRecoveryCatalog.CasBackend malformed = new OxiaRecoveryCatalog.CasBackend() {
            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest ignored, final long expected) {
                return new RecoveryCatalog.Publication(wrongPublication, expected + 1, null);
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] ignored, final long expected, final byte[] digest) {
                return null;
            }

            @Override
            public java.util.Optional<CheckpointManifest> manifest(final byte[] ignored) {
                return java.util.Optional.empty();
            }

            @Override
            public java.util.Optional<RecoveryFloor> currentFloor() {
                return java.util.Optional.empty();
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest ignored) {
            }

            @Override
            public java.util.Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(final byte[] ignored,
                                                                                           final long sequence,
                                                                                           final io.nereusstream.delay.protocol.SourcePosition... positions) {
                return java.util.Optional.empty();
            }
        };
        assertThrows(IllegalStateException.class,
                () -> new OxiaRecoveryCatalog(malformed).publish(manifest, 0));
    }

    private static CheckpointManifest manifest(final ShardId shard, final UUID topic, final byte[] lineage,
                                               final byte[] checkpointId, final long lineageGeneration,
                                               final long offset, final long mutationSequence,
                                               final CheckpointManifest.ParentCheckpoint parent) {
        final KafkaSourcePosition position = new KafkaSourcePosition(shard, "cluster", topic, offset, null,
                1_000 + offset);
        final CheckpointManifest.FileEntry file = new CheckpointManifest.FileEntry("CURRENT", 1, id32(20),
                Bytes.utf8("object/current"), Bytes.utf8("version"), null);
        return new CheckpointManifest(checkpointId, lineage, lineageGeneration, parent, null,
                new CheckpointManifest.CreatedBy(id32(21), id32(22), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_001, "TEST", id32(23), 1, offset, offset,
                        id32(24), 0, null), shard, id32(25), UUID.randomUUID(), 1, mutationSequence, position,
                id32(26), id32(27), List.of(file));
    }

    private static byte[] id16(final int value) {
        final byte[] bytes = new byte[16];
        bytes[15] = (byte) value;
        return bytes;
    }

    private static byte[] id32(final int value) {
        final byte[] bytes = new byte[32];
        bytes[31] = (byte) value;
        return bytes;
    }
}
