package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ChannelResourceIdentityV1;
import io.nereusstream.delay.protocol.ReadyCertificateV1;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.ScheduleWorkItem;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.ClaimRecord;
import io.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.SharedRocksDbResources;

import java.security.PrivateKey;
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
    private final WorkerSchedulingRuntime schedulingRuntime;
    private final WorkerCommandRuntime commandRuntime;
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
        this(sourceConsumer, workClasses, ownedShard, store, resources, authority, verificationKey, null);
    }

    /**
     * Creates the source/apply/drain composition with an active-owner
     * scheduling graph.  The graph shares the same WorkClass registry and is
     * fenced by this runtime when owner drain stops source admission.
     */
    public WorkerShardRuntime(final SourceRecordConsumer sourceConsumer,
                              final WorkClassExecutionRegistry workClasses,
                              final OwnedDelayShard ownedShard,
                              final ShardStore store,
                              final SharedRocksDbResources resources,
                              final OxiaOwnerLeaseStore authority,
                              final PublicKey verificationKey,
                              final WorkerSchedulingRuntime schedulingRuntime) {
        this(sourceConsumer, workClasses, ownedShard, store, resources, authority, verificationKey,
                schedulingRuntime, null);
    }

    /** Creates the source/scheduling/Claim/Publish composition for one Worker shard. */
    public WorkerShardRuntime(final SourceRecordConsumer sourceConsumer,
                              final WorkClassExecutionRegistry workClasses,
                              final OwnedDelayShard ownedShard,
                              final ShardStore store,
                              final SharedRocksDbResources resources,
                              final OxiaOwnerLeaseStore authority,
                              final PublicKey verificationKey,
                              final WorkerSchedulingRuntime schedulingRuntime,
                              final WorkerCommandRuntime commandRuntime) {
        this.resources = Objects.requireNonNull(resources, "resources");
        final WorkClassExecutionRegistry registry = Objects.requireNonNull(workClasses, "workClasses");
        this.resources.bindWorkClassExecutionRegistry(registry);
        if (schedulingRuntime != null) {
            schedulingRuntime.requireWorkClassExecutionRegistry(registry);
        }
        if (commandRuntime != null) {
            commandRuntime.requireWorkClassExecutionRegistry(registry);
        }
        this.schedulingRuntime = schedulingRuntime;
        this.commandRuntime = commandRuntime;
        this.sourceLoop = new WorkerSourceApplyLoop(sourceConsumer, registry, ownedShard, authority,
                verificationKey);
        this.drainCoordinator = new OwnerDrainCoordinator(ownedShard, store, resources, authority, registry);
    }

    /** Runs one bounded source turn while the Worker runtime is admitting work. */
    public synchronized SourceApplyCoordinator.TurnResult runSourceTurn(final SchedulerBudget workBudget,
                                                                          final LongSupplier ownerClock) {
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return sourceLoop.runTurn(workBudget, ownerClock);
    }

    /** Runs one bounded due/Claim/Publish/Checkpoint turn on the shared graph. */
    public synchronized List<WorkClassTask> runSchedulingTurn(final SchedulerBudget workBudget) {
        ensureSchedulingRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return schedulingRuntime.runTurn(workBudget);
    }

    /** Runs one due discovery action while source and scheduling admission remain open. */
    public synchronized WorkerSchedulingRuntime.DueTurn runDueTurn(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget discoveryBudget,
            final LongSupplier ownerClock) {
        ensureSchedulingRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return schedulingRuntime.runDueTurn(evidence, discoveryBudget, ownerClock);
    }

    /** Polls one strict READY candidate while the owner is still admitting work. */
    public synchronized List<ScheduleWorkItem> pollReady(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget budget,
            final LongSupplier ownerClock) {
        ensureSchedulingRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return schedulingRuntime.pollReady(evidence, budget, ownerClock);
    }

    /**
     * Polls at most one READY head and queues its derived-materialization
     * Claim handoff. The single-head bound preserves the scheduler's exact
     * requeue contract if Claim queue admission is rejected.
     */
    public synchronized Optional<ClaimHandoffWorkClassExecutor.Submission> pollAndSubmitClaim(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget budget,
            final long claimDeadlineEpochMs,
            final byte[] claimedCharge,
            final LongSupplier ownerClock) {
        ensureSchedulingRuntime();
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        final SchedulerBudget oneHead = new SchedulerBudget(1,
                Objects.requireNonNull(budget, "Claim poll budget").maxBytes(), budget.maxElapsedNanos());
        final List<ScheduleWorkItem> selected = schedulingRuntime.pollReady(
                Objects.requireNonNull(evidence, "trusted UTC evidence"), oneHead, ownerClock);
        if (selected.isEmpty()) {
            return Optional.empty();
        }
        if (selected.size() != 1) {
            throw new IllegalStateException("single-head Claim poll returned multiple READY items");
        }
        return Optional.of(commandRuntime.submitClaim(selected.get(0), evidence, claimDeadlineEpochMs,
                claimedCharge, ownerClock));
    }

    /** Queues one exact Claim handoff while source/command admission is open. */
    public synchronized ClaimHandoffWorkClassExecutor.Submission submitClaim(
            final WorkerCommandRuntime.ClaimRequest request) {
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return commandRuntime.submitClaim(request);
    }

    /** Queues a Claim handoff whose V1 materialization is derived locally. */
    public synchronized ClaimHandoffWorkClassExecutor.Submission submitClaim(
            final ScheduleWorkItem item,
            final TrustedUtcIntervalEvidence evidence,
            final long claimDeadlineEpochMs,
            final byte[] claimedCharge,
            final LongSupplier ownerClock) {
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return commandRuntime.submitClaim(item, evidence, claimDeadlineEpochMs, claimedCharge, ownerClock);
    }

    /** Queues one exact Publish Admission handoff while source/command admission is open. */
    public synchronized PublishAdmissionWorkClassExecutor.Submission submitPublish(
            final WorkerCommandRuntime.PublishRequest request) {
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return commandRuntime.submitPublish(request);
    }

    /** Queues Publish Admission with a descriptor derived from the Claim. */
    public synchronized PublishAdmissionWorkClassExecutor.Submission submitPublish(
            final ClaimRecord claim,
            final ClaimExecutionAdmission.Reservation reservation,
            final ChannelResourceIdentityV1 channel,
            final ReadyCertificateV1 readyCertificate,
            final TrustedUtcIntervalEvidence decisionTime,
            final long retryUntilEpochMs,
            final int signingKeyVersion,
            final PrivateKey signingKey,
            final LongSupplier ownerClock) {
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return commandRuntime.submitPublish(claim, reservation, channel, readyCertificate, decisionTime,
                retryUntilEpochMs, signingKeyVersion, signingKey, ownerClock);
    }

    /** Runs one bounded Claim/Publish turn through the shared Worker graph. */
    public synchronized List<WorkClassTask> runCommandTurn(final SchedulerBudget budget) {
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return commandRuntime.runTurn(budget);
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

    private void ensureSchedulingRuntime() {
        ensureNotTerminal();
        if (schedulingRuntime == null) {
            throw new IllegalStateException("Worker shard runtime has no active-owner scheduling graph");
        }
    }

    private void ensureCommandRuntime() {
        ensureNotTerminal();
        if (commandRuntime == null) {
            throw new IllegalStateException("Worker shard runtime has no Claim/Publish command graph");
        }
    }
}
