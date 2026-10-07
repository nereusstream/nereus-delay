package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** One-row recovery turns and one caller-owned asynchronous history load; no extra thread, queue or SEND path. */
public final class TargetPublishRecoveryMaintenance {
    public enum Status {
        DISCOVERED, SCAN_YIELD, RESTART_SCAN, LOADING, SUBMITTED, WAITING_SOURCE, SETTLED, ALREADY_HANDLED, IDLE, FAILED
    }

    /** Authenticated history and policy/key/time preparation must run outside the Store and maintenance thread. */
    @FunctionalInterface
    public interface History {
        CompletableFuture<Prepared> load(TargetPublishRecoveryDiscovery.Reference reference);
    }

    public record Prepared(SystemMutation admission, OwnerIdentity owner,
            WorkerPublishOutcomeMutationFactory.OutcomeContext context) {
        public Prepared {
            Objects.requireNonNull(admission, "admission");
            Objects.requireNonNull(owner, "owner");
            Objects.requireNonNull(context, "context");
        }
    }

    public record Turn(Status status, SystemMutation mutation, Throwable failure) {
        public Turn { Objects.requireNonNull(status, "status"); }
    }

    private final TargetWorkerShardRuntime worker;
    private final TargetPublishRecoveryExecutor executor;
    private final Supplier<BoundedReadBudget> reads;
    private final History history;
    private final LongSupplier ownerClock;
    private TargetPublishRecoveryDiscovery.Cursor cursor;
    private TargetPublishRecoveryDiscovery.Reference selected;
    private CompletableFuture<Prepared> loading;
    private TargetPublishRecoveryExecutor.Submission pending;

    public TargetPublishRecoveryMaintenance(TargetWorkerShardRuntime worker, TargetPublishRecoveryExecutor executor,
            Supplier<BoundedReadBudget> reads, History history, LongSupplier ownerClock) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.executor.requireWorker(this.worker);
        this.reads = Objects.requireNonNull(reads, "reads");
        this.history = Objects.requireNonNull(history, "history");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
    }

    public synchronized Turn runTurn() {
        try {
            worker.requirePublishRecoveryOwner(ownerClock);
            if (pending != null) {
                if (executor.settleApplied(ownerClock)) {
                    final var mutation = pending.mutation();
                    pending = null;
                    selected = null;
                    loading = null;
                    // Any Source/local delta can insert an earlier attempt; do not rebase an old cursor by guesswork.
                    cursor = null;
                    return new Turn(Status.SETTLED, mutation, null);
                }
                executor.retryHandoff(ownerClock);
                return new Turn(Status.WAITING_SOURCE, pending.mutation(), pending.failure().orElse(null));
            }
            if (selected == null) {
                final var page = worker.discoverPublishRecovery(readBudget(), cursor, 1, ownerClock);
                cursor = page.continuation();
                if (!page.entries().isEmpty()) {
                    selected = page.entries().getFirst();
                    return new Turn(Status.DISCOVERED, null, null);
                }
                if (page.complete()) {
                    cursor = null;
                    return new Turn(Status.IDLE, null, null);
                }
                return new Turn(Status.SCAN_YIELD, null, null);
            }
            if (loading == null) {
                if (!worker.publishRecoveryStillAdmitted(readBudget(), selected, ownerClock)) {
                    return clearHandled();
                }
                loading = Objects.requireNonNull(history.load(selected), "history future");
                return new Turn(Status.LOADING, null, null);
            }
            if (!loading.isDone()) {
                return new Turn(Status.LOADING, null, null);
            }
            if (!worker.publishRecoveryStillAdmitted(readBudget(), selected, ownerClock)) {
                return clearHandled();
            }
            final Prepared prepared;
            try {
                prepared = Objects.requireNonNull(loading.join(), "prepared history");
            } catch (RuntimeException failedHistory) {
                loading = null;
                return new Turn(Status.FAILED, null,
                        failedHistory.getCause() == null ? failedHistory : failedHistory.getCause());
            }
            selected.requireImage(prepared.admission(), selected.source());
            pending = executor.submit(
                    readBudget(), prepared.admission(), prepared.owner(), prepared.context(), ownerClock);
            return new Turn(Status.SUBMITTED, pending.mutation(), pending.failure().orElse(null));
        } catch (TargetPublishRecoveryDiscovery.StaleCursor changed) {
            cursor = null;
            return new Turn(Status.RESTART_SCAN, null, null);
        } catch (ReadIncompleteException incomplete) {
            return new Turn(Status.SCAN_YIELD, pending == null ? null : pending.mutation(), null);
        } catch (RuntimeException failure) {
            return new Turn(Status.FAILED, pending == null ? null : pending.mutation(), failure);
        }
    }

    public synchronized Optional<SystemMutation> pendingMutation() {
        return pending == null ? Optional.empty() : Optional.of(pending.mutation());
    }

    private BoundedReadBudget readBudget() { return Objects.requireNonNull(reads.get(), "read budget"); }

    private Turn clearHandled() {
        selected = null;
        loading = null;
        cursor = null;
        return new Turn(Status.ALREADY_HANDLED, null, null);
    }

}
