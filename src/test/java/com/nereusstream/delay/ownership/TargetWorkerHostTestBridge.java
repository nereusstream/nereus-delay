package com.nereusstream.delay.ownership;

import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;
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
}
