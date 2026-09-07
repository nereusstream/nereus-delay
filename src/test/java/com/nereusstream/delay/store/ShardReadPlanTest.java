package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ShardReadPlanTest {
    @TempDir
    Path tempDir;

    @Test
    void budgetExhaustionBeforeSeekDoesNotProveAnEmptyRange() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("before-seek"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard(), resources)) {
            final AtomicLong clock = new AtomicLong();
            final BoundedReadBudget budget = new BoundedReadBudget(100, 5, clock::get);
            clock.set(5);
            final var incomplete = store.visitResult(
                    ColumnFamily.TIMELINE, new byte[] {99}, new byte[] {100}, 10, budget, (entry, ignored) -> true);
            assertEquals(ShardStore.VisitStop.INCOMPLETE, incomplete.stop());
            assertEquals(BoundedReadBudget.Exhaustion.ELAPSED, incomplete.reason());
            assertEquals(0, incomplete.visited());
            assertEquals(0, budget.actualRecords());
            final var empty = store.visitResult(
                    ColumnFamily.TIMELINE,
                    new byte[] {99},
                    new byte[] {100},
                    10,
                    budget(100),
                    (entry, ignored) -> true);
            assertEquals(ShardStore.VisitStop.RANGE_END, empty.stop());
        }
    }

    @Test
    void dependentPointReadsShareActualBytesAndFailedReadAccounting() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("points"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard(), resources)) {
            store.write(batch -> {
                batch.put(ColumnFamily.ID, new byte[] {99, 0}, new byte[] {1, 2, 3});
                batch.put(ColumnFamily.ID, new byte[] {99, 1}, new byte[] {4, 5, 6});
            });
            final BoundedReadBudget budget = budget(8);
            final long writes = store.operationStatistics().nativeWriteCalls();
            final ReadIncompleteException incomplete = assertThrows(
                    ReadIncompleteException.class,
                    () -> store.readWithBudget(budget, () -> {
                        assertNotNull(store.get(ColumnFamily.ID, new byte[] {99, 0}));
                        return store.get(ColumnFamily.ID, new byte[] {99, 1});
                    }));
            assertEquals(BoundedReadBudget.Exhaustion.BYTES, incomplete.reason());
            assertEquals(5, budget.chargedBytes());
            assertEquals(10, budget.actualBytes());
            assertEquals(2, budget.actualRecords());
            assertEquals(1, budget.deniedReads());
            assertEquals(writes, store.operationStatistics().nativeWriteCalls());
            assertArrayEquals(new byte[] {4, 5, 6}, store.get(ColumnFamily.ID, new byte[] {99, 1}));
        }
    }

    @Test
    void recordBudgetStopsBeforeTheNextPointReadAndNestedBudgetResetIsRejected() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("records"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard(), resources)) {
            final BoundedReadBudget budget = new BoundedReadBudget(1, 100, 1_000, () -> 0);
            final ReadIncompleteException incomplete = assertThrows(
                    ReadIncompleteException.class,
                    () -> store.readWithBudget(budget, () -> {
                        store.get(ColumnFamily.ID, new byte[] {99, 0});
                        return store.get(ColumnFamily.ID, new byte[] {99, 1});
                    }));
            assertEquals(BoundedReadBudget.Exhaustion.RECORDS, incomplete.reason());
            assertEquals(1, budget.actualRecords());
            assertThrows(
                    IllegalStateException.class,
                    () -> store.readWithBudget(budget(100), () -> store.readWithBudget(budget(100), () -> 1)));
            assertThrows(
                    IllegalStateException.class,
                    () -> store.readWithBudget(budget(100), () -> {
                        store.write(batch -> batch.put(ColumnFamily.ID, new byte[] {99}, new byte[] {1}));
                        return 1;
                    }));
        }
    }

    @Test
    void completedReadViewCannotCommitAcrossAnInterveningWriteOrStoreIncarnation() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("view"));
        final ShardId shard = shard();
        final ShardStore.ReadView original;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            try (ShardStore store = ShardStore.open(config, shard, resources)) {
                final var plan = store.readWithBudget(budget(100), () -> 42);
                original = plan.view();
                store.write(batch -> batch.requireReadView(plan.view()));
                assertThrows(
                        IllegalStateException.class, () -> store.write(batch -> batch.requireReadView(plan.view())));
                final var fresh = store.readWithBudget(budget(100), () -> 43);
                store.recordOpenedOwnerEpoch(1);
                assertThrows(
                        IllegalStateException.class, () -> store.write(batch -> batch.requireReadView(fresh.view())));
            }
            try (ShardStore reopened = ShardStore.open(config, shard, resources)) {
                assertThrows(
                        IllegalStateException.class, () -> reopened.write(batch -> batch.requireReadView(original)));
            }
        }
    }

    @Test
    void sharedTailAndWrapBudgetCannotInventRangeCompletion() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("wrap"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard(), resources)) {
            store.write(batch -> {
                batch.put(ColumnFamily.TIMELINE, new byte[] {99, 0}, new byte[] {1});
                batch.put(ColumnFamily.TIMELINE, new byte[] {99, 1}, new byte[] {2});
            });
            final BoundedReadBudget budget = budget(4);
            final var tail = store.visitResult(
                    ColumnFamily.TIMELINE, new byte[] {99, 1}, new byte[] {100}, 10, budget, (entry, ignored) -> true);
            assertEquals(ShardStore.VisitStop.RANGE_END, tail.stop());
            final var wrap = store.visitResult(
                    ColumnFamily.TIMELINE, new byte[] {99}, new byte[] {99, 1}, 10, budget, (entry, ignored) -> true);
            assertEquals(ShardStore.VisitStop.INCOMPLETE, wrap.stop());
            assertEquals(BoundedReadBudget.Exhaustion.BYTES, wrap.reason());
        }
    }

    private static BoundedReadBudget budget(final long bytes) {
        return new BoundedReadBudget(bytes, 1_000, () -> 0);
    }

    private static ShardId shard() {
        return new ShardId(RouteIncarnation.random(), 3);
    }
}
