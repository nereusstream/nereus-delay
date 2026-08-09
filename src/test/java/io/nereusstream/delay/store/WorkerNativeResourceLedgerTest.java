package io.nereusstream.delay.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerNativeResourceLedgerTest {
    @TempDir
    Path tempDir;

    @Test
    void disjointBucketsRejectDuplicateAndOverCapacityAllocations() {
        final WorkerNativeResourceLedger ledger = new WorkerNativeResourceLedger(100, 50);
        final WorkerNativeResourceLedger.Reservation cache = ledger.reserve("cache",
                NativeResourceUsage.blockCache(60), 0);
        final WorkerNativeResourceLedger.Reservation memtable = ledger.reserve("memtable",
                NativeResourceUsage.memtable(40), 0);
        assertEquals(100, ledger.snapshot().rocksDbUsage().rocksDbNativeBytes());
        assertEquals(2, ledger.snapshot().activeAllocations());
        assertThrows(IllegalArgumentException.class,
                () -> ledger.reserve("duplicate", NativeResourceUsage.zero(), 51));
        assertThrows(IllegalArgumentException.class,
                () -> ledger.reserve("overflow", NativeResourceUsage.blockCache(1), 0));
        assertThrows(IllegalArgumentException.class,
                () -> ledger.reserve("cache", NativeResourceUsage.zero(), 0));

        memtable.close();
        memtable.close();
        assertEquals(60, ledger.snapshot().rocksDbUsage().rocksDbNativeBytes());
        cache.close();
        assertEquals(0, ledger.snapshot().activeAllocations());
    }

    @Test
    void sharedRocksDbBudgetsAreAttributedBeforeNativeResourcesOpenAndReleasedOnClose() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("worker"));
        final WorkerResourceEnvelope envelope = new WorkerResourceEnvelope(
                256L * 1024 * 1024, 128L * 1024 * 1024, 128L * 1024 * 1024, 64L * 1024 * 1024,
                64L * 1024 * 1024, 900L * 1024 * 1024, 64L * 1024 * 1024, 1024L * 1024 * 1024,
                10_000, 1_000, 10L * 1024 * 1024 * 1024, 2L * 1024 * 1024 * 1024,
                256L * 1024 * 1024, 256L * 1024 * 1024, 16L * 1024 * 1024, 10_000);
        final SharedRocksDbResources resources = new SharedRocksDbResources(config, envelope,
                new WorkerRuntimeResourceObservation(
                        128L * 1024 * 1024, 64L * 1024 * 1024, 128L * 1024 * 1024,
                        1024L * 1024 * 1024, 10_000, 10L * 1024 * 1024 * 1024,
                        8L * 1024 * 1024 * 1024));
        final WorkerNativeResourceLedger ledger = resources.nativeResourceLedger();
        assertEquals(config.sharedBlockCacheBytes() + config.sharedWriteBufferBudgetBytes(),
                ledger.snapshot().rocksDbUsage().rocksDbNativeBytes());
        assertEquals(2, ledger.snapshot().activeAllocations());
        resources.close();
        assertEquals(0, ledger.snapshot().rocksDbUsage().rocksDbNativeBytes());
        assertEquals(0, ledger.snapshot().activeAllocations());
    }

    @Test
    void sharedResourcesCannotCloseWithAnUnreleasedPerDbNativeAllocation() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("in-flight"));
        final WorkerResourceEnvelope envelope = new WorkerResourceEnvelope(
                256L * 1024 * 1024, 128L * 1024 * 1024, 256L * 1024 * 1024, 64L * 1024 * 1024,
                64L * 1024 * 1024, 900L * 1024 * 1024, 64L * 1024 * 1024, 1024L * 1024 * 1024,
                10_000, 1_000, 10L * 1024 * 1024 * 1024, 2L * 1024 * 1024 * 1024,
                256L * 1024 * 1024, 256L * 1024 * 1024, 16L * 1024 * 1024, 10_000);
        final SharedRocksDbResources resources = new SharedRocksDbResources(config, envelope,
                new WorkerRuntimeResourceObservation(
                        128L * 1024 * 1024, 64L * 1024 * 1024, 128L * 1024 * 1024,
                        1024L * 1024 * 1024, 10_000, 10L * 1024 * 1024 * 1024,
                        8L * 1024 * 1024 * 1024));
        final WorkerNativeResourceLedger.Reservation perDb = resources.reserveNativeResource(
                "shard-db", new NativeResourceUsage(1, 2, 3, 4, 5, 6), 0);
        assertThrows(IllegalStateException.class, resources::close);
        perDb.close();
        resources.close();
    }

    @Test
    void bucketSumOverflowFailsClosed() {
        final NativeResourceUsage overflow = new NativeResourceUsage(
                Long.MAX_VALUE, 1, 0, 0, 0, 0);
        assertThrows(IllegalStateException.class, overflow::rocksDbNativeBytes);
    }

    @Test
    void underflowingReleaseLeavesReservationForRetry() throws Exception {
        final WorkerNativeResourceLedger ledger = new WorkerNativeResourceLedger(100, 50);
        final WorkerNativeResourceLedger.Reservation reservation = ledger.reserve("retry",
                NativeResourceUsage.blockCache(10), 0);
        final Field usage = WorkerNativeResourceLedger.class.getDeclaredField("rocksDbNativeUsage");
        usage.setAccessible(true);
        usage.set(ledger, NativeResourceUsage.zero());

        assertThrows(IllegalStateException.class, reservation::close);
        assertFalse(reservation.isReleased());
        assertEquals(1, ledger.snapshot().activeAllocations());

        usage.set(ledger, NativeResourceUsage.blockCache(10));
        reservation.close();
        assertTrue(reservation.isReleased());
        assertEquals(0, ledger.snapshot().activeAllocations());
    }

    @Test
    void sharedResourceCloseRetriesReservationsAfterReleaseFailure() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("close-retry"));
        final WorkerResourceEnvelope envelope = new WorkerResourceEnvelope(
                256L * 1024 * 1024, 128L * 1024 * 1024, 128L * 1024 * 1024, 64L * 1024 * 1024,
                64L * 1024 * 1024, 900L * 1024 * 1024, 64L * 1024 * 1024, 1024L * 1024 * 1024,
                10_000, 1_000, 10L * 1024 * 1024 * 1024, 2L * 1024 * 1024 * 1024,
                256L * 1024 * 1024, 256L * 1024 * 1024, 16L * 1024 * 1024, 10_000);
        final SharedRocksDbResources resources = new SharedRocksDbResources(config, envelope,
                new WorkerRuntimeResourceObservation(
                        128L * 1024 * 1024, 64L * 1024 * 1024, 128L * 1024 * 1024,
                        1024L * 1024 * 1024, 10_000, 10L * 1024 * 1024 * 1024,
                        8L * 1024 * 1024 * 1024));
        final WorkerNativeResourceLedger ledger = resources.nativeResourceLedger();
        final Field usage = WorkerNativeResourceLedger.class.getDeclaredField("rocksDbNativeUsage");
        usage.setAccessible(true);
        usage.set(ledger, NativeResourceUsage.zero());

        assertThrows(IllegalStateException.class, resources::close);
        assertEquals(2, ledger.snapshot().activeAllocations());

        usage.set(ledger, new NativeResourceUsage(config.sharedBlockCacheBytes(),
                config.sharedWriteBufferBudgetBytes(), 0, 0, 0, 0));
        resources.close();
        assertEquals(0, ledger.snapshot().activeAllocations());
    }
}
