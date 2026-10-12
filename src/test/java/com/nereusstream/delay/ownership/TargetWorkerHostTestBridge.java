package com.nereusstream.delay.ownership;

import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/** Constructs the real Host admission graph without racing an unrelated maintenance tick. */
public final class TargetWorkerHostTestBridge {
    private TargetWorkerHostTestBridge() {}

    public static TargetWorkerHostRuntime withoutMaintenanceTimer(
            final WorkClassExecutionRegistry classes,
            final SharedRocksDbResources resources,
            final List<TargetWorkerShardRuntime> workers) {
        final var fleet = new TargetWorkerShardFleetRuntime(classes, resources, workers);
        final var executor = new ScheduledThreadPoolExecutor(0);
        executor.shutdown();
        final var loop = new TargetWorkerMaintenanceLoop(
                fleet, new SchedulerBudget(1, 1, 60_000_000_000L), Duration.ofDays(1), ignored -> {}, executor);
        return new TargetWorkerHostRuntime(fleet, loop, workers);
    }

    public static TargetStoreBackend.TargetQueueChangeSignal.Changes drainChanges(
            final TargetWorkerHostRuntime host) {
        return host.drainTargetQueueChanges();
    }

    /** Stops only an owned test timer; leaves the actual Host/Worker admission graph for explicit test turns. */
    public static void stopMaintenanceTimer(TargetWorkerHostRuntime host) {
        try {
            final var field = TargetWorkerHostRuntime.class.getDeclaredField("maintenanceLoop");
            field.setAccessible(true);
            ((TargetWorkerMaintenanceLoop) field.get(host)).close();
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("cannot close configured Host test timer", failure);
        }
    }
}
