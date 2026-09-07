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

    @Test
    void readViewPublicationRejectsChangedViewsBeforeCallingTheAction() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("publication-view"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard(), resources)) {
            final var plan = store.readWithBudget(budget(100), () -> 42);
            final java.util.concurrent.atomic.AtomicInteger published = new java.util.concurrent.atomic.AtomicInteger();
            assertThrows(
                    IllegalStateException.class,
                    () -> store.readWithBudget(
                            budget(100), () -> store.withReadView(plan.view(), published::incrementAndGet)));
            assertEquals(0, published.get());
            store.withReadView(plan.view(), () -> {
                org.junit.jupiter.api.Assertions.assertTrue(Thread.holdsLock(store));
                store.write(batch -> batch.requireReadView(plan.view()));
                return published.incrementAndGet();
            });
            assertEquals(1, published.get());
            assertThrows(
                    IllegalStateException.class, () -> store.withReadView(plan.view(), published::incrementAndGet));
            assertEquals(1, published.get());
        }
    }

    @Test
    void visitorDependencyExhaustionReturnsAnExplicitStopAndClosesItsIterator() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("visitor-dependency"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard(), resources)) {
            store.write(batch -> {
                batch.put(ColumnFamily.TIMELINE, new byte[] {99, 0}, new byte[] {1});
                batch.put(ColumnFamily.TIMELINE, new byte[] {99, 1}, new byte[] {2});
                batch.put(ColumnFamily.ID, new byte[] {99}, new byte[] {3});
            });
            final BoundedReadBudget budget = budget(4);
            final var plan = store.readWithBudget(
                    budget,
                    () -> store.visitResult(
                            ColumnFamily.TIMELINE, new byte[] {99}, new byte[] {100}, 2, budget, (entry, ignored) -> {
                                store.get(ColumnFamily.ID, new byte[] {99});
                                return true;
                            }));
            assertEquals(ShardStore.VisitStop.INCOMPLETE, plan.value().stop());
            assertEquals(BoundedReadBudget.Exhaustion.BYTES, plan.value().reason());
            assertEquals(1, plan.value().visited());
            assertEquals(2, budget.actualRecords());
            assertEquals(5, budget.actualBytes());
            store.write(batch -> batch.requireReadView(plan.view()));
            assertEquals(
                    2,
                    store.scan(ColumnFamily.TIMELINE, new byte[] {99}, new byte[] {100}, 2)
                            .size());
        }
    }

    @Test
    void completedProjectionWorkSharesTheDeadlineWithoutResettingItsPhysicalReadAllowance() {
        final AtomicLong clock = new AtomicLong();
        final BoundedReadBudget budget = new BoundedReadBudget(1, 3, 10, clock::get);
        org.junit.jupiter.api.Assertions.assertTrue(budget.beforeRead());
        org.junit.jupiter.api.Assertions.assertTrue(budget.tryCharge(2, 1));
        org.junit.jupiter.api.Assertions.assertFalse(budget.beforeRead());
        org.junit.jupiter.api.Assertions.assertTrue(budget.beforeTimedWork());
        clock.set(10);
        org.junit.jupiter.api.Assertions.assertFalse(budget.beforeTimedWork());
        assertEquals(1, budget.actualRecords());
        assertEquals(3, budget.actualBytes());
        assertEquals(1, budget.deniedReads());
        assertEquals(BoundedReadBudget.Exhaustion.RECORDS, budget.exhaustion());
        final BoundedReadBudget deadlineOnly = new BoundedReadBudget(10, 10, clock::get);
        clock.set(20);
        org.junit.jupiter.api.Assertions.assertFalse(deadlineOnly.beforeTimedWork());
        assertEquals(BoundedReadBudget.Exhaustion.ELAPSED, deadlineOnly.exhaustion());
        assertEquals(0, deadlineOnly.actualRecords());
    }

    private static BoundedReadBudget budget(final long bytes) {
        return new BoundedReadBudget(bytes, 1_000, () -> 0);
    }

    private static ShardId shard() {
        return new ShardId(RouteIncarnation.random(), 3);
    }
}
