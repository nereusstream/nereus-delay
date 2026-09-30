package com.nereusstream.delay.ownership;

import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.util.Objects;

/** Constructs one Target Worker from the exact accepted assignment and activated source runtime. */
public final class TargetWorkerShardFactory {
    private TargetWorkerShardFactory() {}

    /**
     * Binds a broker source adapter to a fully activated Target source runtime. The accepted assignment is checked
     * before ownership of the source adapter transfers to this factory. After that check, a failed Worker
     * composition closes the adapter so the partially admitted source cannot remain live.
     */
    public static TargetWorkerShardRuntime create(
            final SourceRecordConsumer sourceConsumer,
            final SourceAssignment acceptedAssignment,
            final WorkClassExecutionRegistry workClasses,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final TargetSourceApplyRuntime sourceRuntime,
            final TargetWorkerShardRuntime.Maintenance maintenance) {
        final var source = Objects.requireNonNull(sourceConsumer, "sourceConsumer");
        final var runtime = Objects.requireNonNull(sourceRuntime, "sourceRuntime");
        runtime.requireAcceptedAssignment(acceptedAssignment);
        try {
            return new TargetWorkerShardRuntime(
                    source,
                    Objects.requireNonNull(workClasses, "workClasses"),
                    Objects.requireNonNull(store, "store"),
                    Objects.requireNonNull(resources, "resources"),
                    runtime,
                    Objects.requireNonNull(maintenance, "maintenance"));
        } catch (RuntimeException | Error failure) {
            try {
                source.close();
            } catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }
}
