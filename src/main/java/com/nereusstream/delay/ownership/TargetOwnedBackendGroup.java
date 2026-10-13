package com.nereusstream.delay.ownership;

import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** One actual SDK backend owned by this Host; registration and termination include its late native handles. */
public interface TargetOwnedBackendGroup {
    /** Checks the complete original fleet without transferring ownership or changing its admitted Sources. */
    void requireHostComposition(TargetWorkerHostRuntime host, List<TargetWorkerShardRuntime> fleet);

    void bindHost(TargetWorkerHostRuntime host, List<TargetWorkerShardRuntime> fleet);

    void requireWorkerGraph(WorkClassExecutionRegistry classes, SharedRocksDbResources resources);

    boolean applicationReady();
    boolean workerReady(TargetWorkerShardRuntime worker);
    void admitWorker(TargetWorkerHostRuntime host, TargetWorkerShardRuntime worker);

    /** Fences native acquire/SEND and returns the immutable identities of already registered native operations. */
    List<Object> freezeRegistrations();

    /** Receives only the private permit issued after this Host's Store/runtime commit barriers. */
    CompletableFuture<Void> closeOwnedBackend(TargetWorkerHostRuntime.BackendClosePermit permit);

    boolean resourcesTerminated();
    boolean nativeResourcesTerminated();

    Throwable cleanupFailure();
}
