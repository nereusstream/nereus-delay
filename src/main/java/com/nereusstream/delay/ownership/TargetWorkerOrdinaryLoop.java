package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.semantic.TargetNativePolicyAuthority;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
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
    private final NativePolicySubscriptions nativePolicySubscriptions;
    private final Closeable nativePolicyChangeSubscription;
    private final ClaimConsumer claimConsumer;
    private final LongSupplier ownerClock;
    private final LongSupplier schedulerClock;
    private final LongSupplier monotonicClock;
    private final BoundedAsyncMetricExporter metrics;
    private final Consumer<Throwable> failureConsumer;
    private final long maximumRecoveryTurns;
    private final long maximumCreditTurns;
    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    private final AtomicBoolean nativePolicyChangeSubscriptionClosed = new AtomicBoolean();
    private volatile boolean closed;
    private volatile boolean waitingForQueueChange;
    private Thread thread;

    static TargetWorkerOrdinaryLoop prepare(
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
            final BoundedAsyncMetricExporter metrics,
            final Consumer<Throwable> failureConsumer) {
        validateStartConfiguration(
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
        return new TargetWorkerOrdinaryLoop(
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
                metrics,
                failureConsumer);
    }

    static void validateStartConfiguration(
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
        final var inventory = Objects.requireNonNull(inventoryLimits, "inventoryLimits");
        final var limits = Objects.requireNonNull(drrLimits, "drrLimits");
        final var budget = Objects.requireNonNull(turnBudget, "turnBudget");
        final var interval = Objects.requireNonNull(recheckInterval, "recheckInterval");
        Objects.requireNonNull(requests, "requests");
        Objects.requireNonNull(claimConsumer, "claimConsumer");
        Objects.requireNonNull(ownerClock, "ownerClock");
        Objects.requireNonNull(schedulerClock, "schedulerClock");
        Objects.requireNonNull(monotonicClock, "monotonicClock");
        Objects.requireNonNull(failureConsumer, "failureConsumer");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("Target ordinary recheck interval must be positive");
        }
        try {
            interval.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Target ordinary recheck interval exceeds nanoseconds", overflow);
        }
        if (budget.maxBytes() < limits.sendEnvelopeBytes()
                || budget.maxElapsedNanos() < limits.readElapsedNanos()) {
            throw new IllegalArgumentException("ordinary turn budget cannot serve the activated Target envelope");
        }
        final long targets = inventory.maximumTargets();
        Math.addExact(targets, 1);
        final long creditRounds = 1 + (limits.maximumCostBytes() - 1) / limits.quantumBytes();
        Math.multiplyExact(targets, creditRounds);
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
            final BoundedAsyncMetricExporter metrics,
            final Consumer<Throwable> failureConsumer) {
        this.host = Objects.requireNonNull(host, "host");
        this.inventoryLimits = Objects.requireNonNull(inventoryLimits, "inventoryLimits");
        this.drrLimits = Objects.requireNonNull(drrLimits, "drrLimits");
        this.turnBudget = Objects.requireNonNull(turnBudget, "turnBudget");
        final var requestProvider = Objects.requireNonNull(requests, "requests");
        this.claimConsumer = Objects.requireNonNull(claimConsumer, "claimConsumer");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
        this.schedulerClock = Objects.requireNonNull(schedulerClock, "schedulerClock");
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        this.metrics = metrics;
        this.failureConsumer = Objects.requireNonNull(failureConsumer, "failureConsumer");
        final var exactRecheckInterval = Objects.requireNonNull(recheckInterval, "recheckInterval");
        recheckNanos = exactRecheckInterval.toNanos();
        final long targets = inventoryLimits.maximumTargets();
        maximumRecoveryTurns = Math.addExact(targets, 1);
        final long creditRounds = 1 + (drrLimits.maximumCostBytes() - 1) / drrLimits.quantumBytes();
        maximumCreditTurns = Math.multiplyExact(targets, creditRounds);
        final long maximumNativeScopes = saturatedProduct(
                inventoryLimits.maximumShards(), inventoryLimits.maximumTargets(), TargetQueueState.MAX_DOMAIN_SLOTS);
        nativePolicySubscriptions = new NativePolicySubscriptions(maximumNativeScopes, host::signalNativePolicyChange);
        this.requests = requestsWithNativePolicyWakeups(requestProvider, nativePolicySubscriptions);
        nativePolicyChangeSubscription = Objects.requireNonNull(
                requestProvider.subscribeNativePolicyChanges(host::signalNativePolicyChange),
                "nativePolicyChangeSubscription");
    }

    private static long saturatedProduct(final int shards, final int targets, final int domains) {
        final long first = (long) shards * targets;
        return first > Long.MAX_VALUE / domains ? Long.MAX_VALUE : first * domains;
    }

    private static TargetWorkerOrdinaryDrr.Requests requestsWithNativePolicyWakeups(
            final TargetWorkerOrdinaryDrr.Requests delegate,
            final NativePolicySubscriptions subscriptions) {
        return new TargetWorkerOrdinaryDrr.Requests() {
            @Override
            public java.util.Optional<TargetWorkerOrdinaryDrr.Request> resolve(
                    final TargetWorkerShardRuntime shard,
                    final com.nereusstream.delay.runtime.TargetHeadCostProbe.Cost cost) {
                return delegate.resolve(shard, cost);
            }

            @Override
            public Closeable subscribeNativePolicyChanges(final Runnable wakeup) {
                return delegate.subscribeNativePolicyChanges(wakeup);
            }

            @Override
            public java.util.Optional<TargetWorkerOrdinaryDrr.NativePolicyContext> resolveNativePolicyContext(
                    final TargetWorkerShardRuntime shard,
                    final com.nereusstream.delay.runtime.TargetHeadCostProbe.Cost cost) {
                final var context = delegate.resolveNativePolicyContext(shard, cost);
                if (context.isPresent()) {
                    final var projection = cost.nativeProjection();
                    if (projection == null) {
                        throw new IllegalStateException("Native policy context resolved for an ordinary Target head");
                    }
                    final TargetNativePolicyScope scope = projection.scope();
                    if (!scope.sourceShard().equals(shard.shardId())
                            || !scope.target().equals(cost.head().target())) {
                        throw new IllegalStateException("Native policy scope differs from its selected source/Target");
                    }
                    subscriptions.observe(scope, context.orElseThrow().policies());
                }
                return context;
            }
        };
    }

    /** One watcher per live source/Target/domain slot, released when a complete Native pass no longer sees it. */
    static final class NativePolicySubscriptions implements Closeable {
        private record Slot(ShardId shard, TargetPartitionId target, TargetKeyCodec.Domain domain) {}

        private static final class AuthoritySubscription {
            private final Closeable handle;
            private int slots;

            private AuthoritySubscription(final Closeable handle) {
                this.handle = handle;
            }
        }

        private final long maximumSlots;
        private final Runnable wakeup;
        private final Map<Slot, TargetNativePolicyAuthority> authorityBySlot = new HashMap<>();
        private final IdentityHashMap<TargetNativePolicyAuthority, AuthoritySubscription> byAuthority =
                new IdentityHashMap<>();
        private final Set<Slot> observedThisPass = new HashSet<>();

        NativePolicySubscriptions(final long maximumSlots, final Runnable wakeup) {
            if (maximumSlots <= 0) {
                throw new IllegalArgumentException("Native policy wake subscriptions require a positive scope bound");
            }
            this.maximumSlots = maximumSlots;
            this.wakeup = Objects.requireNonNull(wakeup, "wakeup");
        }

        synchronized void observe(
                final TargetNativePolicyScope scope, final TargetNativePolicyAuthority authority) {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(authority, "authority");
            final Slot slot = new Slot(scope.sourceShard(), scope.target(), scope.domain());
            final TargetNativePolicyAuthority prior = authorityBySlot.get(slot);
            if (prior == authority) {
                observedThisPass.add(slot);
                return;
            }
            if (prior == null && authorityBySlot.size() >= maximumSlots) {
                throw new IllegalStateException("Native policy wake subscriptions exceed the activated scope bound");
            }
            AuthoritySubscription next = byAuthority.get(authority);
            if (next == null) {
                final Closeable handle = Objects.requireNonNull(
                        authority.subscribeCurrentHeadChanges(wakeup), "native policy authority subscription");
                next = new AuthoritySubscription(handle);
                byAuthority.put(authority, next);
            }
            next.slots++;
            authorityBySlot.put(slot, authority);
            observedThisPass.add(slot);
            if (prior != null) {
                releaseSlot(prior);
            }
        }

        synchronized void completePass() {
            for (var entry : new HashMap<>(authorityBySlot).entrySet()) {
                if (!observedThisPass.contains(entry.getKey())) {
                    authorityBySlot.remove(entry.getKey());
                    releaseSlot(entry.getValue());
                }
            }
            observedThisPass.clear();
        }

        synchronized void reset() {
            closeAll();
        }

        private void releaseSlot(final TargetNativePolicyAuthority authority) {
            final AuthoritySubscription subscription = byAuthority.get(authority);
            if (subscription == null || subscription.slots <= 0) {
                throw new IllegalStateException("Native policy authority subscription lost its slot owner");
            }
            subscription.slots--;
            if (subscription.slots == 0) {
                byAuthority.remove(authority);
                closeHandle(subscription.handle);
            }
        }

        private void closeAll() {
            IOException failure = null;
            for (AuthoritySubscription subscription : byAuthority.values()) {
                try {
                    subscription.handle.close();
                } catch (IOException closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            authorityBySlot.clear();
            byAuthority.clear();
            observedThisPass.clear();
            if (failure != null) {
                throw new IllegalStateException("cannot close Native policy authority wake subscriptions", failure);
            }
        }

        private static void closeHandle(final Closeable handle) {
            try {
                handle.close();
            } catch (IOException closeFailure) {
                throw new IllegalStateException("cannot close Native policy authority wake subscription", closeFailure);
            }
        }

        @Override
        public synchronized void close() {
            closeAll();
        }
    }

    synchronized void start() {
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
        long lastMonotonic = 0;
        boolean monotonicClockObserved = false;
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
                                inventoryLimits, ownerClock, monotonicClock, metrics);
                        if (inventory.stop() != TargetWorkerTargetInventory.Stop.COMPLETE) {
                            awaitChange(observedRevision);
                            continue;
                        }
                        nativePolicySubscriptions.reset();
                        if (scheduler == null) {
                            scheduler = host.newOrdinaryDrr(
                                    inventory, drrLimits, ownerClock, monotonicClock, metrics);
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
                    if (monotonicClockObserved && creditCycleStarted - lastMonotonic < 0) {
                        throw new IllegalStateException("Target ordinary monotonic clock moved backwards");
                    }
                    lastMonotonic = creditCycleStarted;
                    monotonicClockObserved = true;
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
                        if (now - lastMonotonic < 0) {
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
                            if (now - lastMonotonic < 0) {
                                throw new IllegalStateException("Target ordinary monotonic clock moved backwards");
                            }
                            lastMonotonic = now;
                            if (scheduler.nativeWakeScanComplete(observedRevision)) {
                                nativePolicySubscriptions.completePass();
                            }
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
                    nativePolicySubscriptions.reset();
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
        Throwable closeFailure = null;
        try {
            nativePolicySubscriptions.close();
        } catch (RuntimeException failure) {
            closeFailure = failure;
        }
        try {
            nativePolicyChangeSubscription.close();
        } catch (Exception externalCloseFailure) {
            if (closeFailure == null) {
                closeFailure = externalCloseFailure;
            } else {
                closeFailure.addSuppressed(externalCloseFailure);
            }
        }
        if (closeFailure != null) {
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
        closeNativePolicyChangeSubscription(null);
    }
}
