package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetPartitionId;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StoreRuntimeMetadataTest {
    @TempDir
    Path tempDir;

    @Test
    void canonicalProjectionRoundTripsAndKeepsEvidenceSorted() {
        final StoreRuntimeMetadata value =
                new StoreRuntimeMetadata(bytes(1), fixedBytes(2, 16), Long.MIN_VALUE, true, List.of(kafkaCursor(1)));

        final StoreRuntimeMetadata decoded = StoreRuntimeMetadata.decode(value.canonicalBytes());

        assertEquals(value, decoded);
        assertArrayEquals(bytes(1), decoded.lastIngressFenceProofId());
        assertArrayEquals(fixedBytes(2, 16), decoded.lastCheckpointId());
        assertEquals(Long.MIN_VALUE, decoded.lastOpenedOwnerEpoch());
        assertTrue(decoded.cleanCloseMarker());
        assertEquals(1, decoded.evidenceCursors().size());
    }

    @Test
    void malformedProjectionAndDuplicateEvidenceFailClosed() {
        final EvidenceCursor cursor = kafkaCursor(1);
        assertThrows(
                IllegalArgumentException.class,
                () -> new StoreRuntimeMetadata(null, null, 1, false, List.of(cursor, cursor)));
        assertThrows(
                IllegalArgumentException.class, () -> StoreRuntimeMetadata.decode(new byte[] {0x18, 0x01, 0x20, 0x02}));
    }

    @Test
    void targetEvidenceSeedsPersistOnlyInTheirExactFormatTwoStore() {
        final var shard = new ShardId(RouteIncarnation.random(), 2);
        final var scope = new EvidenceCursor.TargetScope(shard, new TargetPartitionId(fixedBytes(3, 32)),
                new TargetKeyCodec.Domain(1, 2), fixedBytes(4, 16), fixedBytes(5, 32));
        final var cursor = EvidenceCursor.targetPulsar(scope, fixedBytes(6, 32), 2, 1, 200,
                "persistent://tenant/ns/target-journal", 1, 2, 3, 0, 1);
        final var config = ShardStoreConfig.defaults(tempDir.resolve("target-evidence"));
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, shard, resources)) {
            store.recordEvidenceCursors(List.of(cursor));
            assertEquals(List.of(cursor), store.runtimeMetadata().evidenceCursors());
            final var foreignScope = new EvidenceCursor.TargetScope(new ShardId(RouteIncarnation.random(), 2),
                    scope.target(), scope.domain(), scope.accountingIncarnation(), scope.producerNameHash());
            final var foreign = EvidenceCursor.targetPulsar(foreignScope, fixedBytes(6, 32), 2, 1, 200,
                    cursor.physicalTopic(), 1, 2, 3, 0, 1);
            final long before = store.latestSequenceNumber();
            assertThrows(IllegalArgumentException.class, () -> store.recordEvidenceCursors(List.of(foreign)));
            assertEquals(before, store.latestSequenceNumber());
        }
        try (var resources = new SharedRocksDbResources(config);
                var reopened = ShardStore.openTarget(config, shard, resources)) {
            assertEquals(List.of(cursor), reopened.runtimeMetadata().evidenceCursors());
            assertEquals(List.of(cursor), StoreRuntimeMetadata.decodeEvidenceCursors(
                    TargetValueEnvelope.decode(reopened.get(ColumnFamily.META, KeyCodec.metaFixed(6)), 1).payload()));
        }
        final var legacyConfig = ShardStoreConfig.defaults(tempDir.resolve("legacy-evidence"));
        try (var resources = new SharedRocksDbResources(legacyConfig);
                var legacy = ShardStore.open(legacyConfig, shard, resources)) {
            final long before = legacy.latestSequenceNumber();
            assertThrows(IllegalArgumentException.class, () -> legacy.recordEvidenceCursors(List.of(cursor)));
            assertEquals(before, legacy.latestSequenceNumber());
        }
    }

    @Test
    void ingressFenceStateRoundTripsAndRejectsNonCanonicalBytes() {
        final IngressFenceState state = new IngressFenceState(123, bytes(5));
        assertEquals(state, IngressFenceState.decode(state.canonicalBytes()));
        assertEquals(new IngressFenceState(IngressFenceState.OPEN, null), IngressFenceState.decode(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> IngressFenceState.decode(new byte[] {0x10, 0x01}));
    }

    @Test
    void storePersistsRuntimeProjectionAndClearsCleanCloseOnOpen() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("runtime-meta"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 41);
        final StoreRuntimeMetadata closedProjection;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            assertFalse(store.runtimeMetadata().cleanCloseMarker());
            store.recordLastIngressFenceProofId(bytes(3));
            store.recordLastCheckpointId(fixedBytes(4, 16));
            store.recordOpenedOwnerEpoch(Long.MIN_VALUE);
            store.recordEvidenceCursors(List.of(kafkaCursor(2)));
            assertThrows(IllegalArgumentException.class, () -> store.recordOpenedOwnerEpoch(Long.MAX_VALUE));
            assertArrayEquals(
                    bytes(3),
                    IngressFenceState.decode(store.getValue(ColumnFamily.META, KeyCodec.metaFixed(4), 1)
                                    .payload())
                            .proofId());
            assertEquals(
                    IngressFenceState.OPEN,
                    IngressFenceState.decode(store.getValue(ColumnFamily.META, KeyCodec.metaFixed(4), 1)
                                    .payload())
                            .closedThroughEpochMs());
            assertEquals(
                    List.of(kafkaCursor(2)),
                    StoreRuntimeMetadata.decodeEvidenceCursors(
                            store.getValue(ColumnFamily.META, KeyCodec.metaFixed(6), 1)
                                    .payload()));
            assertArrayEquals(
                    fixedBytes(4, 16),
                    store.getValue(ColumnFamily.META, KeyCodec.metaFixed(7), 1).payload());
            assertArrayEquals(
                    Bytes.u64beBits(Long.MIN_VALUE),
                    store.getValue(ColumnFamily.META, KeyCodec.metaFixed(8), 1).payload());
            assertArrayEquals(
                    new byte[] {0},
                    store.getValue(ColumnFamily.META, KeyCodec.metaFixed(9), 1).payload());
            assertNull(store.get(ColumnFamily.META, KeyCodec.metaFixed(10)));
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore reopened = ShardStore.open(config, shardId, resources);
            final StoreRuntimeMetadata projection = reopened.runtimeMetadata();
            assertArrayEquals(bytes(3), projection.lastIngressFenceProofId());
            assertArrayEquals(fixedBytes(4, 16), projection.lastCheckpointId());
            assertEquals(Long.MIN_VALUE, projection.lastOpenedOwnerEpoch());
            assertEquals(List.of(kafkaCursor(2)), projection.evidenceCursors());
            assertFalse(projection.cleanCloseMarker());
            reopened.close();
            closedProjection = reopened.runtimeMetadata();
        }
        assertTrue(closedProjection.cleanCloseMarker());
    }

    private static EvidenceCursor kafkaCursor(final long generation) {
        return EvidenceCursor.kafka(
                bytes(20),
                java.util.Arrays.copyOf(bytes(21), 16),
                uuidBytes(22),
                0,
                generation,
                100,
                generation,
                generation);
    }

    private static byte[] bytes(final int seed) {
        return Bytes.sha256(Bytes.utf8("store-runtime-" + seed));
    }

    private static byte[] fixedBytes(final int seed, final int length) {
        return java.util.Arrays.copyOf(bytes(seed), length);
    }

    private static byte[] uuidBytes(final int seed) {
        final UUID uuid = UUID.nameUUIDFromBytes(Bytes.utf8("uuid-" + seed));
        return java.nio.ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }
}
