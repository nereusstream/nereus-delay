package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.WorkerRuntimeTestSupport;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SystemWriterAppendBudgetTest {
    @TempDir
    Path root;

    @Test
    void unknownAndRetryShareWindowUntilSdkAndCallerBothFinish() {
        final var classes = classes();
        try (var resources = WorkerRuntimeTestSupport.openWithSyntheticObservation(ShardStoreConfig.defaults(root))) {
            final var pool = new SystemWriterAppendBudget(new SystemWriterAppendBudget.Limits(2, 256, 128, 1),
                    classes, resources);
            assertThrows(IllegalArgumentException.class, () -> new SystemWriterAppendBudget(
                    new SystemWriterAppendBudget.Limits(2, 256, 128, 1), classes, resources));
            final var shard = new ShardId(RouteIncarnation.random(), 0);
            final var writer = pool.registerWriter(shard);
            writer.bindProducer(new Object());
            final var first = writer.tryReserve(shard).orElseThrow();
            assertTrue(first.frameEncoded(16));
            assertTrue(writer.invokeSdk(first, () -> "native send").started());
            assertThrows(IllegalStateException.class, () -> writer.invokeSdk(first, () -> "duplicate send"));
            first.callerReturned(); // UNKNOWN with a callback still pending.
            final var retry = writer.tryReserve(shard).orElseThrow();
            assertTrue(retry.frameEncoded(32));
            assertTrue(writer.invokeSdk(retry, () -> "exact retry bytes").started());
            assertEquals(new SystemWriterAppendBudget.Usage(1, 2, 48), pool.usage());
            assertTrue(writer.tryReserve(shard).isEmpty());
            retry.sdkCompleted(); // Callback alone cannot release caller-local ownership.
            assertEquals(2, pool.usage().outstandingRecords());
            retry.callerReturned();
            assertEquals(new SystemWriterAppendBudget.Usage(1, 1, 16), pool.usage());
            first.sdkCompleted();
            first.sdkCompleted();
            assertEquals(new SystemWriterAppendBudget.Usage(1, 0, 0), pool.usage());
            writer.beginClose();
            writer.producerClosed();
            assertEquals(new SystemWriterAppendBudget.Usage(0, 0, 0), pool.usage());
        }
    }

    @Test
    void failedCloseAndReplacementCannotDiscardOldUnknownOrShareProducerIdentity() {
        try (var resources = WorkerRuntimeTestSupport.openWithSyntheticObservation(ShardStoreConfig.defaults(root))) {
            final var pool = new SystemWriterAppendBudget(new SystemWriterAppendBudget.Limits(1, 128, 128, 2),
                    classes(), resources);
            final var shard = new ShardId(RouteIncarnation.random(), 0);
            final Object producer = new Object();
            final var old = pool.registerWriter(shard);
            old.bindProducer(producer);
            final var pending = old.tryReserve(shard).orElseThrow();
            assertThrows(IllegalArgumentException.class,
                    () -> old.tryReserve(new ShardId(shard.routeIncarnation(), 1)));
            old.invokeSdk(pending, () -> "send with ambiguous response");
            pending.callerReturned();
            final var replacement = pool.registerWriter(shard);
            assertThrows(IllegalArgumentException.class, () -> replacement.bindProducer(producer));
            replacement.bindProducer(new Object());
            assertTrue(replacement.tryReserve(shard).isEmpty());
            assertThrows(IllegalStateException.class, old::producerClosed);
            old.beginClose(); // Native close failed; no producerClosed evidence.
            assertTrue(old.tryReserve(shard).isEmpty());
            assertEquals(new SystemWriterAppendBudget.Usage(2, 1, 128), pool.usage());
            assertThrows(IllegalStateException.class, () -> pool.registerWriter(shard));
            old.producerClosed(); // A successful same-producer close now settles the ambiguous SDK operation.
            pending.sdkCompleted(); // Late callback cannot release twice or affect the replacement.
            final var local = replacement.tryReserve(shard).orElseThrow();
            assertFalse(local.frameEncoded(129));
            local.callerReturned(); // No SDK handoff occurred.
            assertEquals(new SystemWriterAppendBudget.Usage(1, 0, 0), pool.usage());
            replacement.beginClose();
            replacement.producerClosed();
        }
    }

    @Test
    void closeWaitsForSdkHandoffAndNeverRacesANewSendAfterCloseProof() throws Exception {
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2);
                var resources = WorkerRuntimeTestSupport.openWithSyntheticObservation(
                        ShardStoreConfig.defaults(root))) {
            final var pool = new SystemWriterAppendBudget(new SystemWriterAppendBudget.Limits(1, 128, 128, 1),
                    classes(), resources);
            final var shard = new ShardId(RouteIncarnation.random(), 0);
            final var writer = pool.registerWriter(shard);
            writer.bindProducer(new Object());
            final var reservation = writer.tryReserve(shard).orElseThrow();
            final var handing = executor.submit(() -> writer.invokeSdk(reservation, () -> {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("handoff not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                return "started";
            }));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            final var closing = executor.submit(writer::beginClose);
            try {
                assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS));
            } finally {
                release.countDown();
            }
            assertTrue(handing.get(5, TimeUnit.SECONDS).started());
            closing.get(5, TimeUnit.SECONDS);
            writer.producerClosed();
            assertEquals(new SystemWriterAppendBudget.Usage(1, 1, 128), pool.usage());
            reservation.callerReturned();
            assertTrue(writer.tryReserve(shard).isEmpty());
            assertEquals(new SystemWriterAppendBudget.Usage(0, 0, 0), pool.usage());
        } finally {
            release.countDown();
        }
    }

    private static WorkClassExecutionRegistry classes() {
        final var policies = new EnumMap<WorkClass, WorkClassPolicy>(WorkClass.class);
        for (var value : WorkClass.values()) {
            policies.put(value, new WorkClassPolicy(1, 8, 4096, 8, 4096, 1_000_000_000L,
                    1, 1, value == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies,
                1_000_000_000L, 1_000_000_000L, 8, 8), System::nanoTime);
    }
}
