package com.nereusstream.delay.ownership;

import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Strict local drain of one Target Store after new source and GC turns have stopped. */
public final class TargetOwnerDrainCoordinator {
    public record Request(long deadlineEpochMs, SchedulerBudget gcBudget) {
        public Request {
            if (deadlineEpochMs < 0) {
                throw new IllegalArgumentException("Target drain deadline is negative");
            }
            Objects.requireNonNull(gcBudget, "gcBudget");
        }
    }

    public enum Status {
        PENDING_GC,
        RELEASED,
        UNCERTAIN_RELEASED,
        OWNER_LOST_CLOSED
    }

    public record Result(Status status, WorkClassTask pendingGcTask) {
        public Result {
            Objects.requireNonNull(status, "status");
            if ((status == Status.PENDING_GC) != (pendingGcTask != null)) {
                throw new IllegalArgumentException("Target drain pending task disagrees with status");
            }
        }
    }

    private final ShardStore store;
    private final SharedRocksDbResources resources;
    private final TargetSourceApplyRuntime target;
    private final WorkerSourceApplyLoop sourceLoop;
    private final TargetReservationGcRuntime maintenance;
    private final OxiaOwnerLeaseStore authority;
    private OwnerLease expectedLease;
    private OwnerLease drainingLease;
    private boolean storeClosed;
    private boolean releaseAttempted;
    private boolean leaseReleased;
    private boolean ownerLost;
    private boolean uncertainStore;
    private boolean terminal;

    TargetOwnerDrainCoordinator(
            final ShardStore store,
            final SharedRocksDbResources resources,
            final TargetSourceApplyRuntime target,
            final WorkerSourceApplyLoop sourceLoop,
            final TargetReservationGcRuntime maintenance) {
        this.store = Objects.requireNonNull(store, "store");
        this.resources = Objects.requireNonNull(resources, "resources");
        this.target = Objects.requireNonNull(target, "target");
        this.sourceLoop = Objects.requireNonNull(sourceLoop, "sourceLoop");
        this.maintenance = Objects.requireNonNull(maintenance, "maintenance");
        target.requireWorkerStore(store, resources);
        authority = target.drainAuthority();
    }

    /**
     * Returns a retryable pending GC task or completes the Store/lease/source sequence. The
     * caller must first stop the maintenance loop and pause new Worker turns.
     */
    public synchronized Result drain(final Request request, final LongSupplier clock) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(clock, "clock");
        if (terminal) {
            return new Result(completedStatus(), null);
        }
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("Target drain cannot discard a pending source acknowledgement");
        }
        resources.acquireDrainSlot();
        try {
            if (expectedLease == null && !ownerLost && !leaseReleased) {
                final var pending = maintenance.settlePendingTurn(request.gcBudget());
                if (pending.isPresent() && pending.orElseThrow().pending()) {
                    return new Result(Status.PENDING_GC, pending.orElseThrow().task());
                }
                expectedLease = target.snapshotDrainLease();
                if (!target.fenced() && !store.isWriteOutcomeUncertain()) {
                    expectedLease = target.requireActiveDrainLease(clock);
                }
            }
            uncertainStore |= store.isWriteOutcomeUncertain();
            if (uncertainStore) {
                drainUncertainStore(request, clock);
                return new Result(completedStatus(), null);
            }
            if (!ownerLost && !leaseReleased && drainingLease == null) {
                beginDrain(request, clock);
            }
            if (ownerLost) {
                closeLostOwner();
                return new Result(Status.OWNER_LOST_CLOSED, null);
            }
            if (!storeClosed) {
                if (!store.isCloseStarted() && !store.isWriteOutcomeUncertain()) {
                    if (!requireDrainingLease(request, clock)) {
                        closeLostOwner();
                        return new Result(Status.OWNER_LOST_CLOSED, null);
                    }
                    store.flushAndSync();
                }
                if (!requireDrainingLease(request, clock)) {
                    closeLostOwner();
                    return new Result(Status.OWNER_LOST_CLOSED, null);
                }
                store.close();
                storeClosed = true;
            }
            if (!leaseReleased) {
                releaseLease(request, clock);
            }
            if (ownerLost) {
                closeLostOwner();
                return new Result(Status.OWNER_LOST_CLOSED, null);
            }
            sourceLoop.close();
            terminal = true;
            return new Result(Status.RELEASED, null);
        } finally {
            resources.releaseDrainSlot();
        }
    }

    public synchronized boolean terminal() {
        return terminal;
    }

    private Status completedStatus() {
        return ownerLost ? Status.OWNER_LOST_CLOSED : uncertainStore ? Status.UNCERTAIN_RELEASED : Status.RELEASED;
    }

    private void drainUncertainStore(final Request request, final LongSupplier clock) {
        // A native write may have committed even though its result was lost. Do
        // not claim the normal ACTIVE -> DRAINING/flush path; fence and close
        // the exact Store before considering its current lease for release.
        target.fence();
        if (!storeClosed) {
            store.close();
            storeClosed = true;
        }
        if (!leaseReleased && !ownerLost) {
            releaseUncertainLease(request, clock);
        }
        sourceLoop.close();
        terminal = true;
    }

    private void releaseUncertainLease(final Request request, final LongSupplier clock) {
        final OwnerLease current = authority.current(store.shardId()).orElse(null);
        if (current == null) {
            if (releaseAttempted) {
                leaseReleased = true;
            } else {
                loseOwner();
            }
            return;
        }
        if (!sameOwner(current)) {
            loseOwner();
            return;
        }
        readNow(request, clock);
        releaseAttempted = true;
        if (authority.release(current)) {
            leaseReleased = true;
            return;
        }
        final OwnerLease after = authority.current(store.shardId()).orElse(null);
        if (after == null) {
            leaseReleased = true;
        } else if (sameOwner(after)) {
            throw new IllegalStateException("uncertain Target Owner lease release was not confirmed");
        } else {
            loseOwner();
        }
    }

    private void beginDrain(final Request request, final LongSupplier clock) {
        final OwnerLease observed = authority.current(store.shardId()).orElse(null);
        if (!sameOwner(observed)) {
            loseOwner();
            return;
        }
        final long now = readNow(request, clock);
        if (!observed.validAt(now) || observed.expiresAtEpochMs() < expectedLease.expiresAtEpochMs()) {
            loseOwner();
            return;
        }
        if (observed.state() == ShardLifecycleState.DRAINING) {
            drainingLease = observed;
        } else if (observed.state() == ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            try {
                drainingLease = authority
                        .transitionOrRead(observed, ShardLifecycleState.DRAINING)
                        .orElseThrow(() -> new IllegalStateException("Target Owner DRAINING CAS was not confirmed"));
            } catch (RuntimeException | Error failure) {
                target.fence();
                throw failure;
            }
        } else {
            loseOwner();
            return;
        }
        target.fence();
    }

    private boolean requireDrainingLease(final Request request, final LongSupplier clock) {
        final OwnerLease current = authority.current(store.shardId()).orElse(null);
        final long now = readNow(request, clock);
        if (!sameOwner(current)
                || current.state() != ShardLifecycleState.DRAINING
                || !current.validAt(now)
                || current.expiresAtEpochMs() < drainingLease.expiresAtEpochMs()) {
            loseOwner();
            return false;
        }
        drainingLease = current;
        return true;
    }

    private void releaseLease(final Request request, final LongSupplier clock) {
        final OwnerLease current = authority.current(store.shardId()).orElse(null);
        if (current == null) {
            if (releaseAttempted) {
                leaseReleased = true;
            } else {
                loseOwner();
            }
            return;
        }
        if (!sameOwner(current) || current.state() != ShardLifecycleState.DRAINING) {
            loseOwner();
            return;
        }
        readNow(request, clock);
        releaseAttempted = true;
        if (authority.release(current)) {
            leaseReleased = true;
            return;
        }
        final OwnerLease after = authority.current(store.shardId()).orElse(null);
        if (after == null) {
            leaseReleased = true;
        } else if (sameOwner(after)) {
            throw new IllegalStateException("Target Owner lease release was not confirmed");
        } else {
            loseOwner();
        }
    }

    private boolean sameOwner(final OwnerLease actual) {
        return expectedLease != null && expectedLease.sameIdentity(actual);
    }

    private void loseOwner() {
        ownerLost = true;
        target.fence();
    }

    private void closeLostOwner() {
        if (!storeClosed) {
            store.close();
            storeClosed = true;
        }
        sourceLoop.close();
        terminal = true;
    }

    private static long readNow(final Request request, final LongSupplier clock) {
        final long now = clock.getAsLong();
        if (now < 0 || now >= request.deadlineEpochMs()) {
            throw new IllegalStateException("Target Owner drain deadline expired or clock is invalid");
        }
        return now;
    }
}
