package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkerRocksDbUsageMonitorTest {
    @TempDir
    Path tempDir;

    @Test
    void pollStoresCompleteObservationAndFailureIsSticky() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 3);
        final RocksDbUsageSnapshot snapshot = new RocksDbUsageSnapshot(shardId, 2, 3, 4, 5, 6, 1, 1, 1, 1, 1);
        final AtomicInteger failures = new AtomicInteger();
        try (WorkerRocksDbUsageMonitor monitor = new WorkerRocksDbUsageMonitor(
                Duration.ofSeconds(1),
                () -> List.of(snapshot),
                failure -> failures.incrementAndGet(),
                Executors.newSingleThreadScheduledExecutor())) {
            monitor.pollNow();
            assertEquals(List.of(snapshot), monitor.lastObservation());
            assertEquals(0, failures.get());
            assertFalse(monitor.isClosed());
        }

        final WorkerRocksDbUsageMonitor failing = new WorkerRocksDbUsageMonitor(
                Duration.ofSeconds(1),
                () -> {
                    throw new IllegalStateException("usage unavailable");
                },
                failure -> failures.incrementAndGet(),
                Executors.newSingleThreadScheduledExecutor());
        try (failing) {
            failing.pollNow();
            assertNotNull(failing.lastFailure());
            assertEquals(1, failures.get());
            assertEquals(List.of(), failing.lastObservation());
        }
    }

    @Test
    void fatalProbeErrorIsRecordedInsteadOfStoppingTheSafetySignal() {
        final AssertionError failure = new AssertionError("usage probe failed fatally");
        try (WorkerRocksDbUsageMonitor monitor = new WorkerRocksDbUsageMonitor(
                Duration.ofSeconds(1),
                () -> {
                    throw failure;
                },
                ignored -> {},
                Executors.newSingleThreadScheduledExecutor())) {
            monitor.pollNow();
            assertSame(failure, monitor.lastFailure());
            assertEquals(List.of(), monitor.lastObservation());
        }
    }

    @Test
    void sharedOwnerAggregatesRegisteredShardDbsAndClosesMonitorBeforeOwner() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("owner"));
        final WorkerResourceEnvelope envelope = envelope(700L * 1024 * 1024);
        final WorkerRuntimeResourceObservation observation = observation(128L * 1024 * 1024);
        final RocksDbUsageLimits limits = limits();
        try {
            Files.createDirectories(config.rootPath());
        } catch (java.io.IOException exception) {
            throw new IllegalStateException("cannot create test root", exception);
        }
        final SharedRocksDbResources resources = new SharedRocksDbResources(config, envelope, observation);
        final WorkerRocksDbUsageMonitor monitor = resources.startRocksDbUsageMonitor(limits, Duration.ofHours(1));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 9);
        final ShardStore store = ShardStore.open(config, shardId, resources);
        try {
            assertEquals(1, resources.registeredPhysicalUsageSources());
            final List<RocksDbUsageSnapshot> usage = resources.collectPhysicalUsage(limits);
            assertEquals(1, usage.size());
            assertEquals(shardId, usage.get(0).shardId());
            monitor.pollNow();
            assertEquals(1, monitor.lastObservation().size());
            assertEquals(shardId, monitor.lastObservation().get(0).shardId());
            store.close();
            assertEquals(0, resources.registeredPhysicalUsageSources());
            resources.close();
            assertTrue(monitor.isClosed());
        } finally {
            if (!store.isClosed()) {
                store.close();
            }
            if (!monitor.isClosed()) {
                resources.close();
            }
        }
    }

    @Test
    void envelopeBoundOwnerIsRequiredForDynamicMonitor() {
        final SharedRocksDbResources resources = new SharedRocksDbResources(ShardStoreConfig.defaults(tempDir));
        try (resources) {
            assertThrows(
                    IllegalStateException.class,
                    () -> resources.startRocksDbUsageMonitor(limits(), Duration.ofSeconds(1)));
        }
    }

    @Test
    void overCapacityObservationFencesTheSharedSafetyGate() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("over-capacity"));
        final WorkerResourceEnvelope envelope = envelope(700L * 1024 * 1024);
        final WorkerRuntimeResourceObservation observation = observation(128L * 1024 * 1024);
        final SharedRocksDbResources resources = new SharedRocksDbResources(config, envelope, observation);
        final ShardStore store = ShardStore.open(config, new ShardId(RouteIncarnation.random(), 11), resources);
        try {
            final WorkerRocksDbUsageMonitor monitor = WorkerRocksDbUsageMonitor.start(
                    Duration.ofHours(1),
                    () -> resources.collectPhysicalUsage(tightLimits()),
                    resources::recordRuntimeProbeFailure);
            try (monitor) {
                monitor.pollNow();
                assertEquals(WorkerRuntimeSafetyGate.State.DRAIN_OR_MIGRATE, resources.runtimeSafetyState());
                assertNotNull(monitor.lastFailure());
            }
        } finally {
            store.close();
            resources.close();
        }
    }

    @Test
    void closeRetriesExecutorShutdownAfterTheFirstFailure() {
        final AtomicInteger shutdownCalls = new AtomicInteger();
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1) {
            @Override
            public List<Runnable> shutdownNow() {
                if (shutdownCalls.incrementAndGet() == 1) {
                    throw new IllegalStateException("simulated RocksDB usage monitor shutdown failure");
                }
                return super.shutdownNow();
            }
        };
        final WorkerRocksDbUsageMonitor monitor =
                new WorkerRocksDbUsageMonitor(Duration.ofSeconds(1), List::of, ignored -> {}, executor);
        try {
            assertThrows(IllegalStateException.class, monitor::close);
            assertTrue(monitor.isClosed());

            monitor.close();

            assertEquals(2, shutdownCalls.get());
        } finally {
            executor.shutdownNow();
        }
    }

    private static RocksDbUsageLimits limits() {
        return new RocksDbUsageLimits(
                1L << 40, 1L << 42, 10_000, 100_000, 1L << 30, 1L << 32, 10_000, 100_000, 1L << 42, 1L << 44, 100_000,
                1_000_000, 1L << 45, 0, 1L << 42, 1L << 44, 100_000);
    }

    private static RocksDbUsageLimits tightLimits() {
        return new RocksDbUsageLimits(
                1L << 40, 1L << 42, 10_000, 100_000, 1L << 30, 1L << 32, 10_000, 100_000, 1L << 42, 1L << 44, 100_000,
                1_000_000, 1, 0, 1L << 42, 1L << 44, 100_000);
    }

    private static WorkerResourceEnvelope envelope(final long maxProcessRssBytes) {
        return new WorkerResourceEnvelope(
                256L * 1024 * 1024,
                128L * 1024 * 1024,
                128L * 1024 * 1024,
                64L * 1024 * 1024,
                64L * 1024 * 1024,
                maxProcessRssBytes,
                64L * 1024 * 1024,
                1024L * 1024 * 1024,
                10_000,
                1_000,
                10L * 1024 * 1024 * 1024,
                2L * 1024 * 1024 * 1024,
                256L * 1024 * 1024,
                256L * 1024 * 1024,
                16L * 1024 * 1024,
                10_000);
    }

    private static WorkerRuntimeResourceObservation observation(final long rssBytes) {
        return new WorkerRuntimeResourceObservation(
                128L * 1024 * 1024,
                64L * 1024 * 1024,
                rssBytes,
                1024L * 1024 * 1024,
                10_000,
                10L * 1024 * 1024 * 1024,
                8L * 1024 * 1024 * 1024);
    }
}
