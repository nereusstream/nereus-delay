package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.runtime.LaneRecord;

/** Test-classpath-only bridge for package-local scheduler fixture setup. */
public final class PersistentLaneSchedulerTestSupport {
    private PersistentLaneSchedulerTestSupport() {
    }

    public static void register(final PersistentLaneScheduler scheduler, final LaneRecord lane) {
        scheduler.register(lane);
    }

    public static int rebuildFromAuthoritativeReady(final PersistentLaneScheduler scheduler,
                                                    final int maxReadyEntries) {
        return scheduler.rebuildFromAuthoritativeReady(maxReadyEntries);
    }
}
