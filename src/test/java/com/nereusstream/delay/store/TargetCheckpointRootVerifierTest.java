package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.TargetMessageRecord;
import com.nereusstream.delay.runtime.TargetOrderState;
import com.nereusstream.delay.runtime.TargetRecordAccounting;
import com.nereusstream.delay.runtime.TargetTimelineWorkRef;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetCheckpointRootVerifierTest {
    @TempDir
    Path tempDir;

    @Test
    void refusesEmptyTargetWrongShardAndFormatOneImages() {
        final var shard = new ShardId(RouteIncarnation.random(), 2);
        final var another = new ShardId(RouteIncarnation.random(), 3);
        final var targetConfig = ShardStoreConfig.defaults(tempDir.resolve("target"));
        final Path targetDb;
        try (var resources = new SharedRocksDbResources(targetConfig);
                var store = ShardStore.openTarget(targetConfig, shard, resources)) {
            targetDb = store.dbPath();
        }
        final var limits = new CheckpointManifestLimits(100, 64L << 20, 64L << 20, 1024, 1 << 20, 100, 1024);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetCheckpointRootVerifier.validate(targetDb, shard, CheckpointManifestLimits.unbounded()));
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetCheckpointRootVerifier.validate(
                                targetDb,
                                shard,
                                new CheckpointManifestLimits(1, 64L << 20, 64L << 20, 1024, 1 << 20, 100, 1024)))
                .getMessage()
                .contains("file count"));
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetCheckpointRootVerifier.validate(targetDb, shard, limits))
                .getMessage()
                .contains("missing source position"));
        assertThrows(
                IllegalArgumentException.class, () -> TargetCheckpointRootVerifier.validate(targetDb, another, limits));

        final var laneConfig = ShardStoreConfig.defaults(tempDir.resolve("lane"));
        final Path laneDb;
        try (var resources = new SharedRocksDbResources(laneConfig);
                var store = ShardStore.open(laneConfig, shard, resources)) {
            laneDb = store.dbPath();
        }
        assertThrows(
                IllegalArgumentException.class, () -> TargetCheckpointRootVerifier.validate(laneDb, shard, limits));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetCheckpointRootVerifier.validate(tempDir.resolve("absent"), shard, limits));
    }

    @Test
    void strictOrderStateRequiresItsStoredHeadAndBarrierMessage() throws Exception {
        final var state = TargetOrderState.decode(vector("order.head"));
        final var message = TargetMessageRecord.decode(vector("order.message.initial"));
        final var work = TargetTimelineWorkRef.decode(vector("work.fifo.initial"));
        final var barrier = TargetOrderState.decode(vector("order.claimed"));
        final var claimed = TargetMessageRecord.decode(vector("order.message.claimed"));
        final var config = ShardStoreConfig.defaults(tempDir.resolve("strict"));
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(
                        config, message.locator().messageId().routingId().shardId(), resources)) {
            final TargetRecordAccounting.View view = new TargetRecordAccounting.View() {
                @Override
                public ShardId shardId() {
                    return store.shardId();
                }

                @Override
                public byte[] projected(
                        final ColumnFamily family,
                        final byte[] key,
                        final List<TargetStoreBackend.Edit> overlay) {
                    if (!overlay.isEmpty()) {
                        throw new IllegalArgumentException("test audit cannot use an overlay");
                    }
                    return store.get(family, key);
                }
            };
            store.write(batch -> {
                batch.put(ColumnFamily.META, state.encodedKey(),
                        TargetValueEnvelope.encode(TargetOrderState.VALUE_TYPE, state.canonicalBytes()));
                batch.put(ColumnFamily.ID, message.encodedKey(),
                        TargetValueEnvelope.encode(TargetMessageRecord.VALUE_TYPE, message.canonicalBytes()));
                batch.put(ColumnFamily.TIMELINE, state.serviceableHead().key(),
                        TargetValueEnvelope.encode(TargetTimelineWorkRef.VALUE_TYPE, work.canonicalBytes()));
            });
            TargetCheckpointLedgerAudit.auditOrderStateDependencies(state, view);

            store.write(batch -> batch.delete(ColumnFamily.TIMELINE, state.serviceableHead().key()));
            assertTrue(assertThrows(IllegalStateException.class,
                    () -> TargetCheckpointLedgerAudit.auditOrderStateDependencies(state, view))
                    .getMessage().contains("lacks its serviceable head"));
            store.write(batch -> batch.put(ColumnFamily.TIMELINE, state.serviceableHead().key(),
                    TargetValueEnvelope.encode(TargetTimelineWorkRef.VALUE_TYPE, work.canonicalBytes())));
            TargetCheckpointLedgerAudit.auditOrderStateDependencies(state, view);
            store.write(batch -> batch.put(ColumnFamily.TIMELINE, state.serviceableHead().key(),
                    TargetValueEnvelope.encode(TargetTimelineWorkRef.VALUE_TYPE,
                            work.withRuntimeRevision(work.runtimeRevision() + 1).canonicalBytes())));
            assertThrows(IllegalArgumentException.class,
                    () -> TargetCheckpointLedgerAudit.auditOrderStateDependencies(state, view));
            store.write(batch -> batch.put(ColumnFamily.TIMELINE, state.serviceableHead().key(),
                    TargetValueEnvelope.encode(TargetTimelineWorkRef.VALUE_TYPE, work.canonicalBytes())));

            store.write(batch -> {
                batch.put(ColumnFamily.META, barrier.encodedKey(),
                        TargetValueEnvelope.encode(TargetOrderState.VALUE_TYPE, barrier.canonicalBytes()));
                batch.put(ColumnFamily.ID, claimed.encodedKey(),
                        TargetValueEnvelope.encode(TargetMessageRecord.VALUE_TYPE, claimed.canonicalBytes()));
            });
            TargetCheckpointLedgerAudit.auditOrderStateDependencies(barrier, view);
            store.write(batch -> batch.put(ColumnFamily.ID, claimed.encodedKey(),
                    TargetValueEnvelope.encode(TargetMessageRecord.VALUE_TYPE, message.canonicalBytes())));
            assertThrows(IllegalArgumentException.class,
                    () -> TargetCheckpointLedgerAudit.auditOrderStateDependencies(barrier, view));
            store.write(batch -> batch.put(ColumnFamily.ID, claimed.encodedKey(),
                    TargetValueEnvelope.encode(TargetMessageRecord.VALUE_TYPE, claimed.canonicalBytes())));
            store.write(batch -> batch.delete(ColumnFamily.ID, claimed.encodedKey()));
            assertTrue(assertThrows(IllegalStateException.class,
                    () -> TargetCheckpointLedgerAudit.auditOrderStateDependencies(barrier, view))
                    .getMessage().contains("lacks its referenced Message"));
        }
    }

    private static byte[] vector(final String key) throws Exception {
        final var properties = new Properties();
        final var resource = TargetCheckpointRootVerifierTest.class
                .getResourceAsStream("/ndip3/target-identity-vectors.properties");
        try (var stream = Objects.requireNonNull(resource)) {
            properties.load(stream);
        }
        return HexFormat.of().parseHex(Objects.requireNonNull(properties.getProperty(key)));
    }
}
