package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RecoveryCandidateKind;
import com.nereusstream.delay.protocol.RecoveryCandidateRef;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StoreRecoveryMetadataTest {
    @TempDir
    Path tempDir;

    @Test
    void rejectsCatalogGenerationWithoutObservedFloor() {
        assertThrows(IllegalArgumentException.class, () -> new StoreRecoveryMetadata(null, null, 1, null));
    }

    @Test
    void persistsRecoveryProjectionsAtTheShardBoundary() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("recovery-meta"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 17);
        final byte[] lineage = bytes(16, 1);
        final byte[] checkpoint = bytes(16, 2);
        final RecoveryFloorRef floor = new RecoveryFloorRef(
                lineage,
                checkpoint,
                bytes(32, 3),
                7,
                new KafkaSourcePosition(shardId, "cluster-a", UUID.randomUUID(), 21, 2, 100),
                12,
                java.util.List.of());
        final StoreRecoveryMetadata persisted;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final RecoveryCandidateRef candidate = new RecoveryCandidateRef(
                    RecoveryCandidateKind.LOCAL_STORE,
                    lineage,
                    checkpoint,
                    bytes(32, 3),
                    store.metadata().storeIncarnation());
            assertEquals(
                    com.nereusstream.delay.protocol.RecoveryInstallPhase.OPEN,
                    store.recoveryMetadata().installState().phase());
            assertFalse(store.hasReusableRecoveryProof());

            store.recordRecoveryMetadata(candidate, floor);
            persisted = store.recoveryMetadata();
            assertTrue(store.hasReusableRecoveryProof());
            assertEquals(7, persisted.catalogGeneration());
            assertArrayEquals(
                    candidate.canonicalBytes(),
                    store.getValue(ColumnFamily.META, KeyCodec.metaRecovery(1), 1)
                            .payload());
            assertArrayEquals(
                    floor.canonicalBytes(),
                    store.getValue(ColumnFamily.META, KeyCodec.metaRecovery(2), 1)
                            .payload());
            assertArrayEquals(
                    Bytes.u64beBits(7),
                    store.getValue(ColumnFamily.META, KeyCodec.metaRecovery(3), 1)
                            .payload());
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore reopened = ShardStore.open(config, shardId, resources)) {
            assertEquals(persisted.lineageBase(), reopened.recoveryMetadata().lineageBase());
            assertEquals(
                    persisted.lastObservedFloor(), reopened.recoveryMetadata().lastObservedFloor());
            assertEquals(7, reopened.recoveryMetadata().catalogGeneration());
            assertEquals(
                    com.nereusstream.delay.protocol.RecoveryInstallPhase.OPEN,
                    reopened.recoveryMetadata().installState().phase());
            assertTrue(reopened.hasReusableRecoveryProof());
        }
    }

    @Test
    void reopensRecoveryProjectionWithHighBitCatalogGeneration() {
        final long highBitCatalogGeneration = Long.MIN_VALUE + 7;
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("recovery-meta-high-bit"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 18);
        final byte[] lineage = bytes(16, 11);
        final byte[] checkpoint = bytes(16, 12);
        final RecoveryFloorRef floor = new RecoveryFloorRef(
                lineage,
                checkpoint,
                bytes(32, 13),
                highBitCatalogGeneration,
                new KafkaSourcePosition(shardId, "cluster-a", UUID.randomUUID(), 22, 3, 101),
                13,
                java.util.List.of());
        final StoreRecoveryMetadata persisted;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final RecoveryCandidateRef candidate = new RecoveryCandidateRef(
                    RecoveryCandidateKind.LOCAL_STORE,
                    lineage,
                    checkpoint,
                    bytes(32, 13),
                    store.metadata().storeIncarnation());
            store.recordRecoveryMetadata(candidate, floor);
            persisted = store.recoveryMetadata();
            assertEquals(highBitCatalogGeneration, persisted.catalogGeneration());
            assertArrayEquals(
                    Bytes.u64beBits(highBitCatalogGeneration),
                    store.getValue(ColumnFamily.META, KeyCodec.metaRecovery(3), 1)
                            .payload());
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore reopened = ShardStore.open(config, shardId, resources)) {
            assertEquals(persisted, reopened.recoveryMetadata());
            assertEquals(highBitCatalogGeneration, reopened.recoveryMetadata().catalogGeneration());
            assertTrue(reopened.hasReusableRecoveryProof());
        }
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
