package io.nereusstream.delay.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerRuntimeResourceMonitorTest {
    @TempDir
    Path tempDir;

    @Test
    void scheduledMonitorRevalidatesAndCanBeClosed() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("scheduled"));
        final WorkerResourceEnvelope envelope = envelope(700L * 1024 * 1024);
        final WorkerRuntimeResourceObservation healthy = observation(128L * 1024 * 1024);
        final CountDownLatch observed = new CountDownLatch(1);
        final AtomicInteger probeCount = new AtomicInteger();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config, envelope, healthy);
             WorkerRuntimeResourceMonitor monitor = new WorkerRuntimeResourceMonitor(Duration.ofMillis(5),
                     () -> {
                         probeCount.incrementAndGet();
                         return healthy;
                     }, observation -> {
                         resources.revalidateRuntime(observation);
                         observed.countDown();
                     }, resources::recordRuntimeProbeFailure,
                     Executors.newSingleThreadScheduledExecutor())) {
            monitor.start();
            assertTrue(observed.await(1, TimeUnit.SECONDS));
            assertTrue(probeCount.get() > 0);
            monitor.start();
            assertFalse(monitor.isClosed());
        }
    }

    @Test
    void envelopeMismatchBecomesStickyDrainFailure() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("mismatch"));
        final WorkerResourceEnvelope envelope = envelope(700L * 1024 * 1024);
        final WorkerRuntimeResourceObservation healthy = observation(128L * 1024 * 1024);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config, envelope, healthy);
             WorkerRuntimeResourceMonitor monitor = new WorkerRuntimeResourceMonitor(Duration.ofSeconds(1),
                     () -> observation(800L * 1024 * 1024), resources::revalidateRuntime,
                     resources::recordRuntimeProbeFailure,
                     Executors.newSingleThreadScheduledExecutor())) {
            monitor.pollNow();
            assertEquals(WorkerRuntimeSafetyGate.State.DRAIN_OR_MIGRATE, resources.runtimeSafetyState());
            assertNotNull(monitor.lastFailure());
            assertNotNull(resources.runtimeSafetyState());
        }
    }

    @Test
    void unreadableProbeAlsoFencesBusinessAdmission() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("probe-failure"));
        final WorkerResourceEnvelope envelope = envelope(700L * 1024 * 1024);
        final WorkerRuntimeResourceObservation healthy = observation(128L * 1024 * 1024);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config, envelope, healthy);
             WorkerRuntimeResourceMonitor monitor = new WorkerRuntimeResourceMonitor(Duration.ofSeconds(1),
                     () -> {
                         throw new IllegalStateException("cgroup limit unavailable");
                     }, resources::revalidateRuntime, resources::recordRuntimeProbeFailure,
                     Executors.newSingleThreadScheduledExecutor())) {
            monitor.pollNow();
            assertEquals(WorkerRuntimeSafetyGate.State.DRAIN_OR_MIGRATE, resources.runtimeSafetyState());
            assertNotNull(monitor.lastFailure());
        }
    }

    private static WorkerResourceEnvelope envelope(final long maxProcessRssBytes) {
        return new WorkerResourceEnvelope(
                256L * 1024 * 1024, 128L * 1024 * 1024, 128L * 1024 * 1024, 64L * 1024 * 1024,
                64L * 1024 * 1024, maxProcessRssBytes, 64L * 1024 * 1024, 1024L * 1024 * 1024,
                10_000, 1_000, 10L * 1024 * 1024 * 1024, 2L * 1024 * 1024 * 1024,
                256L * 1024 * 1024, 256L * 1024 * 1024, 16L * 1024 * 1024, 10_000);
    }

    private static WorkerRuntimeResourceObservation observation(final long rssBytes) {
        return new WorkerRuntimeResourceObservation(
                128L * 1024 * 1024, 64L * 1024 * 1024, rssBytes,
                1024L * 1024 * 1024, 10_000, 10L * 1024 * 1024 * 1024,
                8L * 1024 * 1024 * 1024);
    }
}
