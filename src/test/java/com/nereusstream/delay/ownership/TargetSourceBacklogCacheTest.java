package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TargetSourceBacklogCacheTest {
    private final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
    private final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 1, 1, 100);

    @Test
    void publishesOnlyAfterCleanupAndReadsCacheWithoutRepeatedSdkIo() throws Exception {
        final var active = new AtomicBoolean(true);
        final var time = new AtomicLong();
        final var cut = cut(active);
        final var operation = new Operation(snapshot(cut));
        try (var executor = executor()) {
            final var cache = new TargetSourceBacklogCache(1, 1000, executor, time::get);
            assertTrue(cache.refresh(shard, () -> cut, ignored -> operation));
            assertTrue(operation.readEntered.await(5, TimeUnit.SECONDS));
            await(() -> cache.state(shard).status() == TargetSourceBacklogCache.Status.CLEANUP_PENDING);
            assertFalse(cache.refresh(shard, () -> cut, ignored -> {
                throw new AssertionError("uncleaned read opened a replacement");
            }));
            assertSame(operation, cache.pendingOperation(shard).orElseThrow());
            assertThrows(IllegalStateException.class, () -> cache.get(shard));
            operation.cleanup.complete(null);
            await(() -> cache.state(shard).status() == TargetSourceBacklogCache.Status.READY);
            assertSame(operation.snapshot, cache.get(shard));
            assertSame(operation.snapshot, cache.get(shard));
            assertEquals(1, operation.reads.get());
            time.set(1000);
            assertThrows(IllegalStateException.class, () -> cache.get(shard));
            cache.stop();
            assertTrue(cache.cleanupComplete());
        }
    }

    @Test
    void failedCleanupRetainsSlotAndExactOperationForRetry() throws Exception {
        final var cut = cut(new AtomicBoolean(true));
        final var operation = new Operation(snapshot(cut));
        try (var executor = executor()) {
            final var cache = new TargetSourceBacklogCache(1, 1000, executor, () -> 0);
            cache.refresh(shard, () -> cut, ignored -> operation);
            assertTrue(operation.readEntered.await(5, TimeUnit.SECONDS));
            operation.failure = new IllegalStateException("close failed");
            await(() -> cache.state(shard).failure() != null);
            assertFalse(cache.refresh(shard, () -> cut, ignored -> operation));
            assertThrows(IllegalStateException.class, () -> cache.remove(shard));
            cache.retryCleanup(shard);
            await(() -> operation.retries.get() == 1);
            operation.cleanup.complete(null);
            await(() -> cache.state(shard).status() == TargetSourceBacklogCache.Status.READY);
            assertEquals(1, operation.reads.get());
            assertSame(operation.snapshot, cache.get(shard));
        }
    }

    @Test
    void stoppedOrChangedCutCannotPublishLateCompletionOrDiscardPendingCleanup() throws Exception {
        final var active = new AtomicBoolean(true);
        final var cut = cut(active);
        final var operation = new Operation(snapshot(cut));
        try (var executor = executor()) {
            final var cache = new TargetSourceBacklogCache(1, 1000, executor, () -> 0);
            cache.refresh(shard, () -> cut, ignored -> operation);
            assertTrue(operation.readEntered.await(5, TimeUnit.SECONDS));
            await(() -> cache.state(shard).status() == TargetSourceBacklogCache.Status.CLEANUP_PENDING);
            active.set(false);
            operation.cleanup.complete(null);
            await(() -> cache.state(shard).status() == TargetSourceBacklogCache.Status.FAILED);
            assertThrows(IllegalStateException.class, () -> cache.get(shard));
            active.set(true);
            final var next = new Operation(snapshot(cut));
            cache.refresh(shard, () -> cut, ignored -> next);
            assertTrue(next.readEntered.await(5, TimeUnit.SECONDS));
            cache.stop();
            assertFalse(cache.cleanupComplete());
            assertSame(next, cache.pendingOperation(shard).orElseThrow());
            next.cleanup.complete(null);
            await(cache::cleanupComplete);
            assertThrows(IllegalStateException.class, () -> cache.get(shard));
            assertThrows(IllegalStateException.class, () -> cache.refresh(shard, () -> cut, ignored -> next));
        }
    }

    @Test
    void executorCannotRunSdkInlineOrAcceptUnboundedQueue() {
        try (var executor = executor()) {
            executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
            assertThrows(IllegalArgumentException.class,
                    () -> new TargetSourceBacklogCache(1, 1000, executor, () -> 0));
        }
    }

    @Test
    void withdrawCannotPublishLateResultOrReuseShardUntilCleanupAndExplicitRemoval() throws Exception {
        final var cut = cut(new AtomicBoolean(true));
        final var operation = new Operation(snapshot(cut));
        try (var executor = executor()) {
            final var cache = new TargetSourceBacklogCache(1, 1000, executor, () -> 0);
            cache.refresh(shard, () -> cut, ignored -> operation);
            assertTrue(operation.readEntered.await(5, TimeUnit.SECONDS));
            cache.withdraw(shard);
            assertThrows(IllegalStateException.class, () -> cache.get(shard));
            assertFalse(cache.cleanupComplete(shard));
            assertThrows(IllegalStateException.class, () -> cache.refresh(shard, () -> cut, ignored -> operation));
            assertThrows(IllegalStateException.class, () -> cache.remove(shard));
            operation.cleanup.complete(null);
            await(() -> cache.cleanupComplete(shard));
            assertThrows(IllegalStateException.class, () -> cache.get(shard));
            cache.remove(shard);
            final var replacement = new Operation(snapshot(cut));
            cache.refresh(shard, () -> cut, ignored -> replacement);
            assertTrue(replacement.readEntered.await(5, TimeUnit.SECONDS));
            replacement.cleanup.complete(null);
            await(() -> cache.state(shard).status() == TargetSourceBacklogCache.Status.READY);
            assertSame(replacement.snapshot, cache.get(shard));
        }
    }

    @Test
    void stoppedQueuedWorkCannotCaptureCutOrOpenSdkAndCacheHasOneHostOwner() throws Exception {
        final var cut = cut(new AtomicBoolean(true));
        final var release = new CountDownLatch(1);
        final var entered = new CountDownLatch(1);
        try (var executor = executor()) {
            executor.execute(() -> {
                entered.countDown();
                try { release.await(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            final var cache = new TargetSourceBacklogCache(1, 1000, executor, () -> 0);
            final Object host = new Object();
            cache.bindHost(host);
            cache.bindHost(host);
            assertThrows(IllegalArgumentException.class, () -> cache.bindHost(new Object()));
            cache.refresh(shard, () -> { throw new AssertionError("stopped queued work captured a cut"); },
                    ignored -> { throw new AssertionError("stopped queued work opened an SDK cursor"); });
            cache.stop();
            assertFalse(cache.cleanupComplete());
            release.countDown();
            await(cache::cleanupComplete);
            assertThrows(IllegalStateException.class, () -> cache.get(shard));
        } finally {
            release.countDown();
        }
    }

    @Test
    void repeatedStopAndWithdrawDoNotQueueAnotherCloseForTheSamePendingQuery() throws Exception {
        final var cut = cut(new AtomicBoolean(true));
        final var operation = new Operation(snapshot(cut));
        final var blocked = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var queuedJob = new CountDownLatch(1);
        try (var executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1),
                new ThreadPoolExecutor.AbortPolicy())) {
            final var cache = new TargetSourceBacklogCache(1, 1000, executor, () -> 0);
            try {
                cache.refresh(shard, () -> cut, ignored -> operation);
                assertTrue(operation.readEntered.await(5, TimeUnit.SECONDS));
                executor.execute(() -> {
                    blocked.countDown();
                    try { release.await(); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                });
                assertTrue(blocked.await(5, TimeUnit.SECONDS));
                executor.execute(queuedJob::countDown);
                cache.stop();
                cache.withdraw(shard);
                cache.stop();
                assertFalse(cache.cleanupComplete());
                assertSame(operation, cache.pendingOperation(shard).orElseThrow());
            } finally {
                release.countDown();
            }
            assertTrue(queuedJob.await(5, TimeUnit.SECONDS));
            operation.cleanup.complete(null);
            await(cache::cleanupComplete);
            assertThrows(IllegalStateException.class, () -> cache.get(shard));
        } finally {
            release.countDown();
        }
    }

    private TargetSourceBacklog.Cut cut(AtomicBoolean active) {
        return new TargetSourceBacklog.Cut(source, () -> {
            if (!active.get()) { throw new IllegalStateException("Source cut changed"); }
        });
    }

    private TargetSourceBacklog.Snapshot snapshot(TargetSourceBacklog.Cut cut) {
        final var collector = new TargetSourceBacklog.Collector(cut,
                new TargetSourceBacklog.Limits(8, 1024, 128, 1000), () -> 0, () -> {});
        collector.add(source, 8);
        return collector.finish(source);
    }

    private static ThreadPoolExecutor executor() {
        return new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(8),
                new ThreadPoolExecutor.AbortPolicy());
    }

    private static void await(java.util.function.BooleanSupplier ready) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!ready.getAsBoolean()) {
            if (System.nanoTime() >= deadline) { throw new IllegalStateException("observer state did not settle"); }
            Thread.sleep(1);
        }
    }

    private static final class Operation implements TargetSourceBacklogRead {
        private final TargetSourceBacklog.Snapshot snapshot;
        private final CompletableFuture<Void> cleanup = new CompletableFuture<>();
        private final CountDownLatch readEntered = new CountDownLatch(1);
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicInteger retries = new AtomicInteger();
        private volatile Throwable failure;

        private Operation(TargetSourceBacklog.Snapshot snapshot) { this.snapshot = snapshot; }

        @Override
        public TargetSourceBacklog.Snapshot read() {
            reads.incrementAndGet(); readEntered.countDown(); return snapshot;
        }

        @Override
        public CompletableFuture<Void> requestClose() { return cleanup.copy(); }

        @Override
        public CompletableFuture<Void> retryClose() {
            retries.incrementAndGet(); failure = null; return cleanup.copy();
        }

        @Override
        public boolean cleanupComplete() { return cleanup.isDone() && !cleanup.isCompletedExceptionally(); }

        @Override
        public Throwable cleanupFailure() { return failure; }
    }
}
