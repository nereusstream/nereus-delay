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
        assertEquals(4, catalog.publish(child, 4).catalogGeneration());
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
