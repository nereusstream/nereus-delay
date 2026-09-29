package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Drives bounded ordinary-first Target DRR turns from committed queue changes and a safety recheck. */
public final class TargetWorkerOrdinaryLoop implements AutoCloseable {
    @FunctionalInterface
    public interface ClaimConsumer {
        /** Receives a Claim only after its guarded Store commit has returned successfully. */
        void accept(TargetClaimRecord claim);
    }

    private final TargetWorkerHostRuntime host;
    private final TargetWorkerTargetInventory.Limits inventoryLimits;
    private final TargetWorkerOrdinaryDrr.Limits drrLimits;
    private final SchedulerBudget turnBudget;
    private final long recheckNanos;
    private final TargetWorkerOrdinaryDrr.Requests requests;
    private final Closeable nativePolicyChangeSubscription;
    private final ClaimConsumer claimConsumer;
    private final LongSupplier ownerClock;
    private final LongSupplier schedulerClock;
    private final LongSupplier monotonicClock;
    private final Consumer<Throwable> failureConsumer;
    private final long maximumRecoveryTurns;
    private final long maximumCreditTurns;
    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    private final AtomicBoolean nativePolicyChangeSubscriptionClosed = new AtomicBoolean();
    private volatile boolean closed;
    private volatile boolean waitingForQueueChange;
    private Thread thread;

    static TargetWorkerOrdinaryLoop start(
            final TargetWorkerHostRuntime host,
            final TargetWorkerTargetInventory.Limits inventoryLimits,
            final TargetWorkerOrdinaryDrr.Limits drrLimits,
            final SchedulerBudget turnBudget,
            final Duration recheckInterval,
            final TargetWorkerOrdinaryDrr.Requests requests,
            final ClaimConsumer claimConsumer,
            final LongSupplier ownerClock,
            final LongSupplier schedulerClock,
            final LongSupplier monotonicClock,
            final Consumer<Throwable> failureConsumer) {
        final var loop = new TargetWorkerOrdinaryLoop(
                host,
                inventoryLimits,
                drrLimits,
                turnBudget,
                recheckInterval,
                requests,
                claimConsumer,
                ownerClock,
                schedulerClock,
                monotonicClock,
                failureConsumer);
        try {
            loop.start();
        } catch (RuntimeException | Error failure) {
            loop.closeNativePolicyChangeSubscription(failure);
            throw failure;
        }
        return loop;
    }

    private TargetWorkerOrdinaryLoop(
            final TargetWorkerHostRuntime host,
            final TargetWorkerTargetInventory.Limits inventoryLimits,
            final TargetWorkerOrdinaryDrr.Limits drrLimits,
            final SchedulerBudget turnBudget,
            final Duration recheckInterval,
            final TargetWorkerOrdinaryDrr.Requests requests,
            final ClaimConsumer claimConsumer,
            final LongSupplier ownerClock,
            final LongSupplier schedulerClock,
            final LongSupplier monotonicClock,
            final Consumer<Throwable> failureConsumer) {
        this.host = Objects.requireNonNull(host, "host");
        this.inventoryLimits = Objects.requireNonNull(inventoryLimits, "inventoryLimits");
        this.drrLimits = Objects.requireNonNull(drrLimits, "drrLimits");
        this.turnBudget = Objects.requireNonNull(turnBudget, "turnBudget");
        this.requests = Objects.requireNonNull(requests, "requests");
        this.claimConsumer = Objects.requireNonNull(claimConsumer, "claimConsumer");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
        this.schedulerClock = Objects.requireNonNull(schedulerClock, "schedulerClock");
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        this.failureConsumer = Objects.requireNonNull(failureConsumer, "failureConsumer");
        final var exactRecheckInterval = Objects.requireNonNull(recheckInterval, "recheckInterval");
        if (exactRecheckInterval.isZero() || exactRecheckInterval.isNegative()) {
            throw new IllegalArgumentException("Target ordinary recheck interval must be positive");
        }
        try {
            recheckNanos = exactRecheckInterval.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Target ordinary recheck interval exceeds nanoseconds", overflow);
        }
        if (turnBudget.maxBytes() < drrLimits.sendEnvelopeBytes()
                || turnBudget.maxElapsedNanos() < drrLimits.readElapsedNanos()) {
            throw new IllegalArgumentException("ordinary turn budget cannot serve the activated Target envelope");
        }
        final long targets = inventoryLimits.maximumTargets();
        maximumRecoveryTurns = Math.addExact(targets, 1);
        final long creditRounds = 1 + (drrLimits.maximumCostBytes() - 1) / drrLimits.quantumBytes();
        maximumCreditTurns = Math.multiplyExact(targets, creditRounds);
        nativePolicyChangeSubscription = Objects.requireNonNull(
                requests.subscribeNativePolicyChanges(host::signalNativePolicyChange),
                "nativePolicyChangeSubscription");
    }

    private synchronized void start() {
        if (thread != null || closed) {
            throw new IllegalStateException("Target ordinary loop cannot be started twice");
        }
        thread = new Thread(this::run, "nereus-delay-target-ordinary-drr");
        thread.setDaemon(true);
        thread.start();
    }

    public Throwable firstFailure() {
        return firstFailure.get();
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isWaitingForQueueChange() {
        return waitingForQueueChange;
    }

    private void run() {
        TargetWorkerOrdinaryDrr scheduler = null;
        boolean recoveryReady = false;
        boolean inventoryRefreshPending = true;
        final Set<TargetPartitionId> pendingTargetRefresh = new LinkedHashSet<>();
        long lastMonotonic = -1;
        try {
            schedule:
            while (!closed) {
                final long observedRevision = host.targetQueueChangeRevision();
                final var changes = host.drainTargetQueueChanges();
                inventoryRefreshPending |= changes.inventoryDirty() || (!recoveryReady && changes.businessRecheck());
                pendingTargetRefresh.addAll(changes.dirtyTargets());
                if (!recoveryReady && !pendingTargetRefresh.isEmpty()) {
                    inventoryRefreshPending = true;
                    pendingTargetRefresh.clear();
                }
                try {
                    if (scheduler == null || inventoryRefreshPending) {
                        final var inventory = host.rebuildTargetInventory(
                                inventoryLimits, ownerClock, monotonicClock);
                        if (inventory.stop() != TargetWorkerTargetInventory.Stop.COMPLETE) {
                            awaitChange(observedRevision);
                            continue;
                        }
                        if (scheduler == null) {
                            scheduler = host.newOrdinaryDrr(inventory, drrLimits, ownerClock, monotonicClock);
                            recoveryReady = false;
                        } else {
                            scheduler.refreshInventory(inventory);
                        }
                        host.registerTargetWakeups(
                                inventory.snapshot().targets().stream()
                                        .map(TargetWorkerTargetInventory.Target::id)
                                        .toList());
                        pendingTargetRefresh.clear();
                        inventoryRefreshPending = false;
                    } else if (scheduler != null && recoveryReady && !pendingTargetRefresh.isEmpty()) {
                        for (var target : new ArrayList<>(pendingTargetRefresh)) {
                            if (scheduler.refreshTarget(target)) {
                                pendingTargetRefresh.remove(target);
                            }
                        }
                    }
                    if (!recoveryReady) {
                        boolean ready = false;
                        boolean incomplete = false;
                        for (long turn = 0; turn < maximumRecoveryTurns; turn++) {
                            final var freeze = scheduler.freezeRecoveryFirstPass(
                                    schedulerClock.getAsLong(), turnBudget, requests);
                            if (freeze.stop() == TargetWorkerOrdinaryDrr.FreezeStop.READY) {
                                ready = true;
                                recoveryReady = true;
                                break;
                            }
                            if (freeze.stop() == TargetWorkerOrdinaryDrr.FreezeStop.READ_INCOMPLETE) {
                                awaitChange(observedRevision);
                                incomplete = true;
                                break;
                            }
                        }
                        if (!ready) {
                            if (incomplete) {
                                continue schedule;
                            }
                            throw new IllegalStateException("Target recovery pass exceeded its bounded visit count");
                        }
                    }
                    final long creditCycleStarted = monotonicClock.getAsLong();
                    if (creditCycleStarted < 0 || creditCycleStarted < lastMonotonic) {
                        throw new IllegalStateException("Target ordinary monotonic clock moved backwards");
                    }
                    lastMonotonic = creditCycleStarted;
                    int ordinaryPassVisits = 0;
                    boolean ordinaryPassSawCandidate = false;
                    boolean ordinaryPassClear = false;
                    boolean ordinaryReadIncomplete = false;
                    for (long turn = 0; turn < maximumCreditTurns; turn++) {
                        final var result = scheduler.claimOrdinary(
                                schedulerClock.getAsLong(), turnBudget, requests);
                        if (!result.claims().isEmpty()) {
                            try {
                                claimConsumer.accept(result.claims().getFirst());
                            } catch (RuntimeException handoffFailure) {
                                reportFailure(handoffFailure);
                                closed = true;
                                return;
                            }
                            continue schedule;
                        }
                        ordinaryPassVisits = Math.addExact(ordinaryPassVisits, result.targetVisits());
                        ordinaryPassSawCandidate |= result.stop() == TargetWorkerOrdinaryDrr.Stop.CREDIT_WAIT
                                || result.stop() == TargetWorkerOrdinaryDrr.Stop.BUDGET_WAIT;
                        if (result.stop() == TargetWorkerOrdinaryDrr.Stop.READ_INCOMPLETE) {
                            awaitChange(observedRevision);
                            ordinaryReadIncomplete = true;
                            break;
                        }
                        if (ordinaryPassVisits >= scheduler.targetCount()) {
                            if (!ordinaryPassSawCandidate) {
                                ordinaryPassClear = host.targetQueueChangeRevision() == observedRevision;
                                break;
                            }
                            ordinaryPassVisits = 0;
                            ordinaryPassSawCandidate = false;
                        }
                        final long now = monotonicClock.getAsLong();
                        if (now < 0 || now < lastMonotonic) {
                            throw new IllegalStateException("Target ordinary monotonic clock moved backwards");
                        }
                        lastMonotonic = now;
                        if (turn + 1 == maximumCreditTurns
                                || now - creditCycleStarted >= turnBudget.maxElapsedNanos()) {
                            break;
                        }
                    }
                    if (ordinaryReadIncomplete) {
                        continue schedule;
                    }
                    if (ordinaryPassClear) {
                        for (long turn = 0; turn < maximumCreditTurns; turn++) {
                            final var result = scheduler.claimNativeAfterOrdinaryPass(
                                    schedulerClock.getAsLong(), turnBudget, requests, observedRevision);
                            if (!result.claims().isEmpty()) {
                                try {
                                    claimConsumer.accept(result.claims().getFirst());
                                } catch (RuntimeException handoffFailure) {
                                    reportFailure(handoffFailure);
                                    closed = true;
                                    return;
                                }
                                continue schedule;
                            }
                            if (result.stop() == TargetWorkerOrdinaryDrr.Stop.READ_INCOMPLETE) {
                                awaitChange(observedRevision);
                                break;
                            }
                            if (host.targetQueueChangeRevision() != observedRevision) {
                                awaitChange(observedRevision);
                                break;
                            }
                            final long now = monotonicClock.getAsLong();
                            if (now < 0 || now < lastMonotonic) {
                                throw new IllegalStateException("Target ordinary monotonic clock moved backwards");
                            }
                            lastMonotonic = now;
                            if (scheduler.nativeWakeScanComplete(observedRevision)
                                    && result.stop() != TargetWorkerOrdinaryDrr.Stop.CREDIT_WAIT) {
                                if (result.stop() == TargetWorkerOrdinaryDrr.Stop.NORMAL
                                        && scheduler.nativeWakeNeedsImmediateRescan(
                                                schedulerClock.getAsLong(), observedRevision)
                                        && turn + 1 < maximumCreditTurns
                                        && now - creditCycleStarted < turnBudget.maxElapsedNanos()) {
                                    continue;
                                }
                                awaitSchedulerChange(observedRevision, scheduler);
                                break;
                            }
                            if (turn + 1 == maximumCreditTurns
                                    || now - creditCycleStarted >= turnBudget.maxElapsedNanos()) {
                                awaitSchedulerChange(observedRevision, scheduler);
                                break;
                            }
                        }
                    } else {
                        awaitSchedulerChange(observedRevision, scheduler);
                    }
                } catch (TargetWorkerHostRuntime.ShardAdmissionBusyException busy) {
                    host.awaitShardAdmission(busy.shardId(), Duration.ofNanos(recheckNanos));
                } catch (RuntimeException failure) {
                    reportFailure(failure);
                    scheduler = null;
                    recoveryReady = false;
                    inventoryRefreshPending = true;
                    pendingTargetRefresh.clear();
                    awaitChange(observedRevision);
                }
            }
        } catch (InterruptedException interrupted) {
            if (!closed) {
                Thread.currentThread().interrupt();
                reportFailure(interrupted);
            }
        } catch (Error fatal) {
            reportFailure(fatal);
            throw fatal;
        } finally {
            closed = true;
            closeNativePolicyChangeSubscription(null);
        }
    }

    private void awaitChange(final long observedRevision) throws InterruptedException {
        awaitChange(observedRevision, Duration.ofNanos(recheckNanos));
    }

    private void awaitSchedulerChange(final long observedRevision, final TargetWorkerOrdinaryDrr scheduler)
            throws InterruptedException {
        final long nowEpochMs = schedulerClock.getAsLong();
        if (nowEpochMs < 0) {
            throw new IllegalStateException("Target ordinary next-wake requires trusted nonnegative time");
        }
        final long timeoutNanos = changeWaitNanos(
                scheduler.nextWakeEpochMs(nowEpochMs, observedRevision), nowEpochMs, recheckNanos);
        awaitChange(observedRevision, Duration.ofNanos(timeoutNanos));
    }

    static long changeWaitNanos(
            final OptionalLong nextWakeEpochMs, final long nowEpochMs, final long safetyRecheckNanos) {
        Objects.requireNonNull(nextWakeEpochMs, "nextWakeEpochMs");
        if (nowEpochMs < 0 || safetyRecheckNanos <= 0) {
            throw new IllegalArgumentException("Target wake wait requires trusted time and positive recheck");
        }
        long timeoutNanos = safetyRecheckNanos;
        if (nextWakeEpochMs.isPresent() && nextWakeEpochMs.getAsLong() > nowEpochMs) {
            final long delayMillis = nextWakeEpochMs.getAsLong() - nowEpochMs;
            final long delayNanos = delayMillis > Long.MAX_VALUE / 1_000_000L
                    ? Long.MAX_VALUE
                    : delayMillis * 1_000_000L;
            timeoutNanos = Math.min(timeoutNanos, delayNanos);
        }
        return timeoutNanos;
    }

    private void awaitChange(final long observedRevision, final Duration timeout) throws InterruptedException {
        waitingForQueueChange = true;
        try {
            host.awaitTargetQueueChange(observedRevision, timeout);
        } finally {
            waitingForQueueChange = false;
        }
    }

    private void reportFailure(final Throwable failure) {
        firstFailure.compareAndSet(null, failure);
        try {
            failureConsumer.accept(failure);
        } catch (RuntimeException callbackFailure) {
            if (callbackFailure != failure) {
                failure.addSuppressed(callbackFailure);
            }
        }
    }

    private void closeNativePolicyChangeSubscription(final Throwable primaryFailure) {
        if (!nativePolicyChangeSubscriptionClosed.compareAndSet(false, true)) {
            return;
        }
        try {
            nativePolicyChangeSubscription.close();
        } catch (Exception closeFailure) {
            if (primaryFailure != null) {
                primaryFailure.addSuppressed(closeFailure);
            } else {
                reportFailure(new IllegalStateException("cannot close Native policy wake subscription", closeFailure));
            }
        }
    }

    @Override
    public void close() {
        final Thread current;
        synchronized (this) {
            if (thread == Thread.currentThread()) {
                throw new IllegalStateException("cannot close Target ordinary loop from its Claim turn");
            }
            closed = true;
            current = thread;
        }
        if (current != null) {
            current.interrupt();
            try {
                current.join();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while stopping Target ordinary loop", interrupted);
            }
        }
    }
}
