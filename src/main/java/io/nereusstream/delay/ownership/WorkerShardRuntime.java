package io.nereusstream.delay.ownership;

import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.SharedRocksDbResources;

import java.security.PublicKey;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Production composition for one owned Worker shard.
 *
 * <p>The runtime is deliberately narrow: it binds the broker-neutral source
 * apply loop to the owner drain coordinator and the process-wide resource
 * gate.  It does not create a Kafka/Pulsar client or an Oxia authority.  The
 * adapters provide those boundaries through {@link SourceRecordConsumer} and
 * {@link OxiaOwnerLeaseStore}.</p>
 *
 * <p>A source record retained after apply/ACK uncertainty blocks drain until
 * the same record is retried and ACKed.  Once drain starts, source turns are
 * fenced; a checkpoint that is still queued keeps the runtime retryable.  The
 * source is closed only after the owner drain has closed the Store and
 * released the matching lease.</p>
 */
public final class WorkerShardRuntime implements AutoCloseable {
    private final SharedRocksDbResources resources;
    private final WorkerSourceApplyLoop sourceLoop;
    private final OwnerDrainCoordinator drainCoordinator;
    private boolean sourcePaused;
    private boolean terminal;

    /** Creates the source/apply/drain composition for one owned shard. */
    public WorkerShardRuntime(final SourceRecordConsumer sourceConsumer,
                              final WorkClassExecutionRegistry workClasses,
                              final OwnedDelayShard ownedShard,
                              final ShardStore store,
                              final SharedRocksDbResources resources,
                              final OxiaOwnerLeaseStore authority,
                              final PublicKey verificationKey) {
        this.resources = Objects.requireNonNull(resources, "resources");
        this.sourceLoop = new WorkerSourceApplyLoop(sourceConsumer, workClasses, ownedShard, authority,
                verificationKey);
        this.drainCoordinator = new OwnerDrainCoordinator(ownedShard, store, resources, authority, workClasses);
    }

    /** Runs one bounded source turn while the Worker runtime is admitting work. */
    public synchronized SourceApplyCoordinator.TurnResult runSourceTurn(final SchedulerBudget workBudget,
                                                                          final LongSupplier ownerClock) {
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return sourceLoop.runTurn(workBudget, ownerClock);
    }

    /** Returns the exact source entry retained across an ACK/apply boundary. */
    public synchronized Optional<SourceReplayEntry> pendingSourceEntry() {
        return sourceLoop.pendingEntry();
    }

    /** Returns whether the drain path has fenced new source turns. */
    public synchronized boolean sourcePaused() {
        return sourcePaused;
    }

    /**
     * Runs or retries the owner drain.  A pending source ACK is rejected
     * before the drain coordinator can transition the authoritative lease, so
     * the caller can retry the same source record without deadlocking the
     * source lifecycle.
     */
    public synchronized OwnerDrainCoordinator.DrainResult drain(
            final OwnerDrainCoordinator.DrainRequest request,
            final LongSupplier clock,
            final OwnerDrainCoordinator.DrainCallbacks callbacks) {
        ensureNotTerminal();
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(clock, "clock");
        final OwnerDrainCoordinator.DrainCallbacks delegate = Objects.requireNonNull(callbacks, "callbacks");
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("cannot drain with a pending source acknowledgement");
        }
        final OwnerDrainCoordinator.DrainResult result = drainCoordinator.drain(request, clock,
                new OwnerDrainCoordinator.DrainCallbacks() {
                    @Override
                    public void stopSourceAndScheduling() {
                        sourcePaused = true;
                        delegate.stopSourceAndScheduling();
                    }

                    @Override
                    public void pollOpenPublishAttempts(final List<io.nereusstream.delay.runtime.PublishAttemptLedger>
                                                                 openAttempts,
                                                         final int pollNumber) {
                        delegate.pollOpenPublishAttempts(openAttempts, pollNumber);
                    }

                    @Override
                    public void commitSourceHint(final io.nereusstream.delay.protocol.SourcePosition
                                                         persistedPosition) {
                        delegate.commitSourceHint(persistedPosition);
                    }
                });
        if (result.pendingCheckpointTask() == null) {
            // The precondition above proves that close cannot discard an
            // unacknowledged source record.  OwnerDrainCoordinator has also
            // completed Store close and exact lease release at this point.
            sourceLoop.close();
            terminal = true;
        }
        return result;
    }

    /**
     * Refuses to tear down an active owner.  Production shutdown must use the
     * same drain path so source, Store and lease boundaries remain ordered.
     */
    @Override
    public synchronized void close() {
        if (terminal) {
            return;
        }
        throw new IllegalStateException("Worker shard runtime must complete owner drain before close");
    }

    private void ensureSourceRunning() {
        ensureNotTerminal();
        if (sourcePaused) {
            throw new IllegalStateException("Worker source is paused for owner drain");
        }
    }

    private void ensureNotTerminal() {
        if (terminal) {
            throw new IllegalStateException("Worker shard runtime is closed");
        }
    }
}
