package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.HandoffPolicyHeadRef;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetHeadCostProbe;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TimelineWorkKind;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.TargetNativePolicyChecks;
import com.nereusstream.delay.semantic.TargetNativePolicyAuthority;
import com.nereusstream.delay.semantic.TargetNativePolicyTrust;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** One process-local byte-cost DRR share per physical Target; ordinary work precedes Native early Claims. */
public final class TargetWorkerOrdinaryDrr {
    public record Limits(
            long quantumBytes,
            long maximumCostBytes,
            long sendEnvelopeBytes,
            int maximumVisitsPerTurn,
            int readRecords,
            long readBytes,
            long readElapsedNanos) {
        public Limits {
            if (quantumBytes <= 0
                    || maximumCostBytes <= 0
                    || sendEnvelopeBytes < maximumCostBytes
                    || maximumVisitsPerTurn <= 0
                    || readRecords <= 0
                    || readBytes <= 0
                    || readElapsedNanos <= 0) {
                throw new IllegalArgumentException("Target DRR limits cannot serve the activated maximum cost");
            }
            Math.addExact(maximumCostBytes, quantumBytes);
        }

        long maximumCredit() {
            return maximumCostBytes + quantumBytes;
        }
    }

    /** Current live Owner, quota and physical-send guards are supplied per selected Claim. */
    public record Request(
            OwnerIdentity owner,
            long deadlineEpochMs,
            byte[] operationDigest,
            TargetQuotaDelta.LocalClaimAuthority quota,
            TargetStoreBackend.CommitAuthority physicalWrites) {
        public Request {
            Objects.requireNonNull(owner, "owner");
            Bytes.requireLength(operationDigest, 32, "operationDigest");
            operationDigest = Bytes.copy(operationDigest);
            Objects.requireNonNull(quota, "quota");
            Objects.requireNonNull(physicalWrites, "physicalWrites");
        }

        @Override
        public byte[] operationDigest() {
            return Bytes.copy(operationDigest);
        }
    }

    @FunctionalInterface
    public interface Requests {
        /** Checks current eligibility without consuming a permit; the Claim guard acquires it. */
        Optional<Request> resolve(TargetWorkerShardRuntime shard, TargetHeadCostProbe.Cost cost);

        /** Supplies live Target Native policy/time authority for one exact persisted Native head. */
        default Optional<NativePolicyContext> resolveNativePolicyContext(
                final TargetWorkerShardRuntime shard, final TargetHeadCostProbe.Cost cost) {
            return Optional.empty();
        }
    }

    /** Providers are sampled again under the Claim commit guard; a cached decision cannot authorize a send. */
    public record NativePolicyContext(
            TargetNativePolicyAuthority policies,
            TargetNativePolicyTrust trust,
            Supplier<SourcePosition> sourcePosition,
            Supplier<TrustedUtcIntervalEvidence> trustedTime) {
        public NativePolicyContext {
            Objects.requireNonNull(policies, "policies");
            Objects.requireNonNull(trust, "trust");
            Objects.requireNonNull(sourcePosition, "sourcePosition");
            Objects.requireNonNull(trustedTime, "trustedTime");
        }
    }

    public enum Stop {
        NORMAL,
        READ_INCOMPLETE,
        CREDIT_WAIT,
        BUDGET_WAIT
    }

    public record Turn<T>(List<T> claims, int targetVisits, long schedulingBytes, Stop stop) {
        public Turn {
            claims = List.copyOf(Objects.requireNonNull(claims, "claims"));
            Objects.requireNonNull(stop, "stop");
            if (claims.size() > 1
                    || targetVisits < 0
                    || schedulingBytes < 0
                    || (stop == Stop.READ_INCOMPLETE && !claims.isEmpty())) {
                throw new IllegalArgumentException("invalid Target ordinary Claim turn");
            }
        }

        public Turn(final List<T> claims, final int targetVisits, final long schedulingBytes) {
            this(claims, targetVisits, schedulingBytes, Stop.NORMAL);
        }
    }

    public enum FreezeStop {
        READY,
        VISIT_BUDGET,
        READ_INCOMPLETE
    }

    public record FreezeTurn(FreezeStop stop, int targetVisits, int eligibleTargets) {
        public FreezeTurn {
            Objects.requireNonNull(stop, "stop");
            if (targetVisits < 0 || eligibleTargets < 0) {
                throw new IllegalArgumentException("invalid Target recovery first-pass turn");
            }
        }
    }

    interface Reads {
        boolean membershipCurrent();

        boolean cutsCurrent(Map<ShardId, TargetQueueSnapshotReader.Cut> expected);

        Optional<TargetQueueSnapshotReader.Entry> refresh(ShardId shard, TargetPartitionId target);

        TargetHeadCostProbe.Cost probe(ShardId shard, TargetHeadRef head);
    }

    @FunctionalInterface
    interface ClaimAction<T> {
        T commit();
    }

    @FunctionalInterface
    interface Selector<T> {
        Optional<ClaimAction<T>> select(ShardId shard, TargetHeadCostProbe.Cost cost);
    }

    @FunctionalInterface
    interface NativeSelector<T> {
        NativeSelection<T> select(ShardId shard, TargetHeadCostProbe.Cost cost);
    }

    record NativeSelection<T>(Optional<ClaimAction<T>> action, OptionalLong nextWakeEpochMs) {
        NativeSelection {
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(nextWakeEpochMs, "nextWakeEpochMs");
            if (nextWakeEpochMs.isPresent() && nextWakeEpochMs.getAsLong() < 0) {
                throw new IllegalArgumentException("Target Native next-wake time must be nonnegative");
            }
        }
    }

    private record Claimed<T>(T value, long cost) {}

    private record Candidate<T>(int sourceIndex, int domainIndex, int domainCount, long cost, ClaimAction<T> action) {}

    private record Selection<T>(
            Optional<Candidate<T>> candidate, boolean due, boolean budgetBlocked, Long nextWakeEpochMs) {}

    private enum VisitKind {
        CLAIMED,
        CREDIT_WAIT,
        BUDGET_WAIT,
        UNAVAILABLE
    }

    private record Visit<T>(VisitKind kind, Claimed<T> claimed, Long nextWakeEpochMs) {}

    private final TargetWorkerHostRuntime host;
    private Map<ShardId, TargetWorkerShardRuntime> workers;
    private final Reads reads;
    private final Limits limits;
    private final LongSupplier monotonicClock;
    private final LongSupplier ownerClock;
    private List<TargetState> ring;
    private final NavigableMap<Long, Set<TargetPartitionId>> ordinaryWakeTargets = new TreeMap<>();
    private final Map<TargetPartitionId, Long> ordinaryWakeByTarget = new HashMap<>();
    private final Set<TargetPartitionId> nativeWakePassTargets = new HashSet<>();
    private final Map<TargetPartitionId, Long> nativeWakePassByTarget = new HashMap<>();
    private long nativeWakePassRevision = -1;
    private long nativeWakeCompleteRevision = -1;
    private Long nextNativeWakeEpochMs;
    private Map<ShardId, TargetQueueSnapshotReader.Cut> inventoryCuts;
    private final Set<TargetPartitionId> firstPassPending = new HashSet<>();
    private final boolean firstPassRequired;
    private int cursor;
    private int freezeCursor;
    private long freezeEpochMs = -1;
    private boolean firstPassReady;
    private long lastClockNanos = -1;

    TargetWorkerOrdinaryDrr(
            final TargetWorkerHostRuntime host,
            final TargetWorkerTargetInventory.Snapshot inventory,
            final Limits limits,
            final LongSupplier ownerClock,
            final LongSupplier monotonicClock) {
        this.host = Objects.requireNonNull(host, "host");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        Objects.requireNonNull(inventory, "inventory");
        final var current = host.currentTargetWorkers();
        final Map<ShardId, TargetWorkerShardRuntime> exact = new HashMap<>();
        for (TargetWorkerShardRuntime worker : current) {
            if (exact.put(worker.shardId(), worker) != null) {
                throw new IllegalStateException("Target DRR has a duplicate source Shard");
            }
        }
        if (!exact.keySet().equals(inventory.cuts().keySet())) {
            throw new IllegalStateException("Target DRR inventory differs from active source Shards");
        }
        for (TargetWorkerShardRuntime worker : current) {
            if (!inventory.cuts().get(worker.shardId()).equals(host.readTargetQueueCut(worker, budget(), ownerClock))) {
                throw new IllegalStateException("Target DRR inventory Store cut changed before activation");
            }
        }
        if (!host.currentTargetWorkers().equals(current)) {
            throw new IllegalStateException("Target DRR source membership changed before activation");
        }
        workers = Map.copyOf(exact);
        inventoryCuts = inventory.cuts();
        firstPassRequired = true;
        reads = new Reads() {
            @Override
            public boolean membershipCurrent() {
                return currentWorkersMatch();
            }

            @Override
            public boolean cutsCurrent(final Map<ShardId, TargetQueueSnapshotReader.Cut> expected) {
                if (!expected.keySet().equals(workers.keySet()) || !currentWorkersMatch()) {
                    return false;
                }
                for (TargetWorkerShardRuntime worker : workers.values()) {
                    if (!expected.get(worker.shardId()).equals(host.readTargetQueueCut(worker, budget(), ownerClock))) {
                        return false;
                    }
                }
                return currentWorkersMatch();
            }

            @Override
            public Optional<TargetQueueSnapshotReader.Entry> refresh(
                    final ShardId shard, final TargetPartitionId target) {
                return host.readTargetQueue(worker(shard), budget(), target, ownerClock);
            }

            @Override
            public TargetHeadCostProbe.Cost probe(final ShardId shard, final TargetHeadRef head) {
                return host.probeSelectedHead(worker(shard), budget(), head, ownerClock);
            }
        };
        ring = states(inventory, workers);
        rebuildOrdinaryWakeIndex();
    }

    /** Pure selection seam; production always uses exact Host reads and Claim above. */
    TargetWorkerOrdinaryDrr(
            final TargetWorkerTargetInventory.Snapshot inventory,
            final Limits limits,
            final Reads reads,
            final LongSupplier monotonicClock) {
        this(inventory, limits, reads, monotonicClock, false);
    }

    /** Exercises the first-pass state machine without fabricating Host or Store authority. */
    TargetWorkerOrdinaryDrr(
            final TargetWorkerTargetInventory.Snapshot inventory,
            final Limits limits,
            final Reads reads,
            final LongSupplier monotonicClock,
            final boolean requireFirstPass) {
        host = null;
        workers = Map.of();
        ownerClock = null;
        this.limits = Objects.requireNonNull(limits, "limits");
        this.reads = Objects.requireNonNull(reads, "reads");
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        final var exactInventory = Objects.requireNonNull(inventory, "inventory");
        inventoryCuts = exactInventory.cuts();
        firstPassRequired = requireFirstPass;
        firstPassReady = !requireFirstPass;
        ring = states(exactInventory, workers);
        rebuildOrdinaryWakeIndex();
    }

    /** Returns after at most one durable Claim so its receipt is never lost to a later read failure. */
    public synchronized Turn<TargetClaimRecord> claimOrdinary(
            final long nowEpochMs, final SchedulerBudget budget, final Requests requests) {
        Objects.requireNonNull(host, "host");
        return runOrdinary(nowEpochMs, budget, claimSelector(nowEpochMs, requests));
    }

    /** Native early Claim is only called after the Host completed a full ordinary pass without a candidate. */
    public synchronized Turn<TargetClaimRecord> claimNativeAfterOrdinaryPass(
            final long nowEpochMs,
            final SchedulerBudget budget,
            final Requests requests,
            final long expectedQueueRevision) {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(requests, "requests");
        if (expectedQueueRevision < 0) {
            throw new IllegalArgumentException("Target Native Claim requires a nonnegative queue revision");
        }
        if (host.targetQueueChangeRevision() != expectedQueueRevision) {
            resetNativeWakeScan();
            return new Turn<>(List.of(), 0, 0, Stop.NORMAL);
        }
        return runNative(
                nowEpochMs,
                budget,
                nativeClaimSelector(nowEpochMs, requests, expectedQueueRevision),
                expectedQueueRevision);
    }

    /** Admits a new complete Host inventory after the recovery first pass, retaining unchanged Target shares. */
    public synchronized void refreshInventory(final TargetWorkerTargetInventory.Result inventory) {
        Objects.requireNonNull(host, "host");
        resetNativeWakeScan();
        final var complete = Objects.requireNonNull(inventory, "inventory");
        if (complete.stop() != TargetWorkerTargetInventory.Stop.COMPLETE) {
            throw new IllegalArgumentException("Target DRR refresh requires a complete Host inventory");
        }
        final var snapshot = host.consumeTargetInventory(complete);
        final Map<ShardId, TargetWorkerShardRuntime> current = workersById(host.currentTargetWorkers());
        if (!current.keySet().equals(snapshot.cuts().keySet())) {
            throw new IllegalStateException("Target DRR refresh inventory differs from current source membership");
        }
        final Map<ShardId, TargetWorkerShardRuntime> previous = workers;
        workers = current;
        try {
            if (firstPassReady) {
                refreshSnapshot(snapshot);
            } else {
                restartRecoveryFirstPass(snapshot);
            }
        } catch (RuntimeException | Error failure) {
            workers = previous;
            throw failure;
        }
    }

    /** Process-state seam; production first consumes the exact inventory built by its Host. */
    synchronized void refreshSnapshot(final TargetWorkerTargetInventory.Snapshot inventory) {
        final var exact = Objects.requireNonNull(inventory, "inventory");
        resetNativeWakeScan();
        if (!firstPassReady) {
            restartRecoveryFirstPass(exact);
            return;
        }
        requireRefreshReady(exact);
        final Map<TargetPartitionId, TargetState> previous = new HashMap<>();
        for (TargetState target : ring) {
            previous.put(target.id, target);
        }
        final TargetPartitionId next = ring.isEmpty() ? null : ring.get(cursor).id;
        final List<TargetState> refreshed = new ArrayList<>();
        final Set<TargetPartitionId> retained = new HashSet<>();
        final Set<TargetPartitionId> seen = new HashSet<>();
        for (TargetWorkerTargetInventory.Target incoming : exact.targets()) {
            if (!seen.add(incoming.id())) {
                throw new IllegalStateException("Target DRR refresh contains duplicate physical Target");
            }
            final TargetState old = previous.get(incoming.id());
            if (old != null
                    && !Arrays.equals(
                            old.physical.canonicalBytes(), incoming.physical().canonicalBytes())) {
                throw new IllegalStateException("Target DRR physical identity changed during refresh");
            }
            final TargetState current;
            if (old != null && old.sameSources(incoming, exact.cuts(), workers)) {
                current = old;
                current.updateQueueSnapshots(incoming);
                retained.add(old.id);
            } else {
                current = new TargetState(incoming, exact.cuts(), workers);
            }
            refreshed.add(current);
        }
        refreshed.sort((left, right) -> Arrays.compareUnsigned(left.id.bytes(), right.id.bytes()));
        int nextCursor = 0;
        if (next != null) {
            while (nextCursor < refreshed.size()
                    && Arrays.compareUnsigned(refreshed.get(nextCursor).id.bytes(), next.bytes()) < 0) {
                nextCursor++;
            }
            if (nextCursor == refreshed.size()) {
                nextCursor = 0;
            }
        }
        firstPassPending.retainAll(retained);
        ring = List.copyOf(refreshed);
        inventoryCuts = exact.cuts();
        cursor = nextCursor;
        rebuildOrdinaryWakeIndex();
    }

    /** Returns the earliest indexed ordinary head strictly after the current trusted time. */
    synchronized OptionalLong nextOrdinaryWakeEpochMs(final long nowEpochMs) {
        if (nowEpochMs < 0) {
            throw new IllegalArgumentException("Target next-wake requires trusted nonnegative time");
        }
        final Long next = ordinaryWakeTargets.higherKey(nowEpochMs);
        return next == null ? OptionalLong.empty() : OptionalLong.of(next);
    }

    synchronized OptionalLong nextWakeEpochMs(final long nowEpochMs, final long expectedQueueRevision) {
        OptionalLong next = nextOrdinaryWakeEpochMs(nowEpochMs);
        if (expectedQueueRevision < 0
                || nativeWakeCompleteRevision != expectedQueueRevision
                || host != null && host.targetQueueChangeRevision() != expectedQueueRevision) {
            return next;
        }
        if (nextNativeWakeEpochMs != null && nextNativeWakeEpochMs > nowEpochMs
                && (next.isEmpty() || nextNativeWakeEpochMs < next.getAsLong())) {
            next = OptionalLong.of(nextNativeWakeEpochMs);
        }
        return next;
    }

    synchronized boolean nativeWakeScanComplete(final long expectedQueueRevision) {
        return expectedQueueRevision >= 0
                && nativeWakeCompleteRevision == expectedQueueRevision
                && (host == null || host.targetQueueChangeRevision() == expectedQueueRevision);
    }

    synchronized boolean nativeWakeNeedsImmediateRescan(
            final long nowEpochMs, final long expectedQueueRevision) {
        if (nowEpochMs < 0) {
            throw new IllegalArgumentException("Target Native wake check requires trusted nonnegative time");
        }
        return nativeWakeScanComplete(expectedQueueRevision)
                && nextNativeWakeEpochMs != null
                && nextNativeWakeEpochMs <= nowEpochMs;
    }

    synchronized int targetCount() {
        return ring.size();
    }

    /** Refreshes one registered Target across the exact active Shard set without rebuilding the inventory. */
    synchronized boolean refreshTarget(final TargetPartitionId targetId) {
        final var exactId = Objects.requireNonNull(targetId, "targetId");
        if (host == null || !firstPassReady) {
            throw new IllegalStateException("Target wake refresh requires an active ordinary scheduler");
        }
        if (!reads.membershipCurrent()) {
            throw new IllegalStateException("Target DRR source membership changed");
        }
        resetNativeWakeScan();
        final int targetIndex = targetIndex(exactId);
        if (targetIndex < 0) {
            host.unregisterTargetWakeup(exactId);
            return true;
        }
        final TargetState previous = ring.get(targetIndex);
        final List<TargetWorkerTargetInventory.Source> sources = new ArrayList<>();
        for (TargetWorkerShardRuntime worker : host.currentTargetWorkers()) {
            final Optional<TargetQueueSnapshotReader.Entry> entry;
            try {
                entry = reads.refresh(worker.shardId(), exactId);
            } catch (ReadIncompleteException incomplete) {
                return false;
            }
            if (entry.isEmpty()) {
                continue;
            }
            final var snapshot = entry.orElseThrow();
            if (!Arrays.equals(previous.physical.canonicalBytes(), snapshot.physical().canonicalBytes())) {
                throw new IllegalStateException("Target DRR physical identity changed during wake refresh");
            }
            sources.add(new TargetWorkerTargetInventory.Source(worker.shardId(), snapshot));
        }
        if (!reads.membershipCurrent()) {
            throw new IllegalStateException("Target DRR source membership changed during wake refresh");
        }
        if (sources.isEmpty()) {
            final TargetPartitionId next = nextTargetAfter(exactId);
            final var updated = new ArrayList<>(ring);
            updated.remove(targetIndex);
            ring = List.copyOf(updated);
            removeOrdinaryWake(exactId);
            firstPassPending.remove(exactId);
            cursor = cursorFor(next, ring);
            host.unregisterTargetWakeup(exactId);
            return true;
        }
        final var incoming = new TargetWorkerTargetInventory.Target(previous.physical, sources);
        if (!previous.sameSources(incoming, inventoryCuts, workers)) {
            final TargetState refreshed = new TargetState(incoming, inventoryCuts, workers);
            final var updated = new ArrayList<>(ring);
            updated.set(targetIndex, refreshed);
            ring = List.copyOf(updated);
            replaceOrdinaryWake(refreshed);
        } else {
            previous.updateQueueSnapshots(incoming);
            replaceOrdinaryWake(previous);
        }
        return true;
    }

    private int targetIndex(final TargetPartitionId targetId) {
        for (int index = 0; index < ring.size(); index++) {
            if (ring.get(index).id.equals(targetId)) {
                return index;
            }
        }
        return -1;
    }

    private TargetPartitionId nextTargetAfter(final TargetPartitionId targetId) {
        if (ring.isEmpty()) {
            return null;
        }
        for (int offset = 0; offset < ring.size(); offset++) {
            final TargetPartitionId next = ring.get((cursor + offset) % ring.size()).id;
            if (!next.equals(targetId)) {
                return next;
            }
        }
        return null;
    }

    private static int cursorFor(final TargetPartitionId nextTarget, final List<TargetState> targets) {
        if (targets.isEmpty() || nextTarget == null) {
            return 0;
        }
        for (int index = 0; index < targets.size(); index++) {
            if (targets.get(index).id.equals(nextTarget)) {
                return index;
            }
        }
        return 0;
    }

    private void rebuildOrdinaryWakeIndex() {
        ordinaryWakeTargets.clear();
        ordinaryWakeByTarget.clear();
        for (TargetState target : ring) {
            replaceOrdinaryWake(target);
        }
    }

    private void replaceOrdinaryWake(final TargetState target) {
        removeOrdinaryWake(target.id);
        final Long nextWake = target.nextOrdinaryWakeEpochMs;
        if (nextWake != null) {
            ordinaryWakeByTarget.put(target.id, nextWake);
            ordinaryWakeTargets.computeIfAbsent(nextWake, ignored -> new HashSet<>()).add(target.id);
        }
    }

    private void removeOrdinaryWake(final TargetPartitionId targetId) {
        final Long previousWake = ordinaryWakeByTarget.remove(targetId);
        if (previousWake == null) {
            return;
        }
        final Set<TargetPartitionId> targets = ordinaryWakeTargets.get(previousWake);
        if (targets == null || !targets.remove(targetId)) {
            throw new IllegalStateException("Target ordinary wake index lost its registered Target");
        }
        if (targets.isEmpty()) {
            ordinaryWakeTargets.remove(previousWake);
        }
    }

    private void resetNativeWakeScan() {
        nativeWakePassTargets.clear();
        nativeWakePassByTarget.clear();
        nativeWakePassRevision = -1;
        nativeWakeCompleteRevision = -1;
        nextNativeWakeEpochMs = null;
    }

    private void recordNativeWakeObservation(
            final TargetPartitionId targetId,
            final Long wakeEpochMs,
            final long expectedQueueRevision,
            final long nowEpochMs) {
        if (nativeWakePassRevision != expectedQueueRevision) {
            nativeWakePassTargets.clear();
            nativeWakePassByTarget.clear();
            nativeWakePassRevision = expectedQueueRevision;
            nativeWakeCompleteRevision = -1;
            nextNativeWakeEpochMs = null;
        } else if (nativeWakePassTargets.isEmpty() && nativeWakeCompleteRevision == expectedQueueRevision) {
            nativeWakeCompleteRevision = -1;
            nextNativeWakeEpochMs = null;
        }
        nativeWakePassTargets.add(targetId);
        if (wakeEpochMs != null && wakeEpochMs > nowEpochMs) {
            nativeWakePassByTarget.put(targetId, wakeEpochMs);
        } else {
            nativeWakePassByTarget.remove(targetId);
        }
        if (nativeWakePassTargets.size() == ring.size()
                && ring.stream().allMatch(target -> nativeWakePassTargets.contains(target.id))) {
            Long earliest = null;
            for (Long candidate : nativeWakePassByTarget.values()) {
                earliest = earliest == null ? candidate : Math.min(earliest, candidate);
            }
            nativeWakeCompleteRevision = expectedQueueRevision;
            nextNativeWakeEpochMs = earliest;
            nativeWakePassTargets.clear();
            nativeWakePassByTarget.clear();
        }
    }

    private void requireRefreshReady(final TargetWorkerTargetInventory.Snapshot inventory) {
        if (!firstPassReady) {
            throw new IllegalStateException("Target recovery first pass must be frozen before inventory refresh");
        }
        requireCurrentInventory(inventory);
    }

    private void restartRecoveryFirstPass(final TargetWorkerTargetInventory.Snapshot inventory) {
        resetNativeWakeScan();
        if (!firstPassRequired) {
            throw new IllegalStateException("Target recovery first pass is not enabled");
        }
        requireCurrentInventory(inventory);
        ring = states(inventory, workers);
        rebuildOrdinaryWakeIndex();
        inventoryCuts = inventory.cuts();
        firstPassPending.clear();
        cursor = 0;
        freezeCursor = 0;
        freezeEpochMs = -1;
        firstPassReady = false;
    }

    private void requireCurrentInventory(final TargetWorkerTargetInventory.Snapshot inventory) {
        if ((host != null && !inventory.cuts().keySet().equals(workers.keySet()))
                || !reads.membershipCurrent()
                || !reads.cutsCurrent(inventory.cuts())) {
            throw new IllegalStateException("Target DRR inventory membership or Store cut changed");
        }
    }

    /** Builds one frozen, bounded recovery set before the first ordinary Claim is admitted. */
    public synchronized FreezeTurn freezeRecoveryFirstPass(
            final long nowEpochMs, final SchedulerBudget budget, final Requests requests) {
        Objects.requireNonNull(host, "host");
        return freezeFirstPass(nowEpochMs, budget, claimSelector(nowEpochMs, requests));
    }

    private Selector<TargetClaimRecord> claimSelector(final long nowEpochMs, final Requests requests) {
        Objects.requireNonNull(requests, "requests");
        return (shard, cost) -> requests.resolve(worker(shard), cost)
                .map(request -> () -> host.claim(
                        worker(shard),
                        budget(),
                        cost.head(),
                        request.owner(),
                        nowEpochMs,
                        request.deadlineEpochMs(),
                        cost.executionBytes(),
                        request.operationDigest(),
                        request.quota(),
                        request.physicalWrites(),
                        ownerClock));
    }

    private NativeSelector<TargetClaimRecord> nativeClaimSelector(
            final long nowEpochMs, final Requests requests, final long expectedQueueRevision) {
        Objects.requireNonNull(requests, "requests");
        return (shard, cost) -> {
            if (host.targetQueueChangeRevision() != expectedQueueRevision) {
                return new NativeSelection<>(Optional.empty(), OptionalLong.empty());
            }
            final var projection = cost.nativeProjection();
            if (projection == null) {
                throw new IllegalStateException("Native Target head probe lacks its guarded message projection");
            }
            final Optional<NativePolicyContext> contextResult =
                    requests.resolveNativePolicyContext(worker(shard), cost);
            if (contextResult.isEmpty()) {
                return new NativeSelection<>(Optional.empty(), OptionalLong.empty());
            }
            final NativePolicyContext context = contextResult.orElseThrow();
            final var work = projection.work();
            final boolean initialAttempt = work.workKind() == TimelineWorkKind.INITIAL_SCHEDULE
                    && work.candidateAttemptNo() == 1;
            final SourcePosition position = context.sourcePosition().get();
            final TrustedUtcIntervalEvidence time = context.trustedTime().get();
            final var publication = context.policies().current(projection.scope().digest()).orElse(null);
            final var decision = TargetNativePolicyChecks.resolve(
                    projection.scope(),
                    initialAttempt,
                    cost.deliverAtEpochMs(),
                    work.retryEligibilityAtEpochMs(),
                    publication,
                    context.trust(),
                    position,
                    time);
            final OptionalLong nextWake = nativePolicyWake(decision, nowEpochMs, cost.expireAtEpochMs());
            if (decision.action() != TargetNativePolicyChecks.Action.NATIVE_CANDIDATE
                    || decision.nativeActionAtEpochMs() == null
                    || decision.policyHeadRef() == null
                    || nowEpochMs < decision.nativeActionAtEpochMs()
                    || nowEpochMs >= cost.deliverAtEpochMs()
                    || nowEpochMs >= cost.expireAtEpochMs()) {
                return new NativeSelection<>(Optional.empty(), nextWake);
            }
            final Optional<Request> requestResult = requests.resolve(worker(shard), cost);
            if (requestResult.isEmpty()) {
                return new NativeSelection<>(Optional.empty(), OptionalLong.empty());
            }
            final Request request = requestResult.orElseThrow();
            final var authority = nativeCommitAuthority(
                    request.physicalWrites(),
                    requests,
                    worker(shard),
                    cost,
                    projection.scope(),
                    initialAttempt,
                    decision.policyHeadRef(),
                    decision.nativeActionAtEpochMs(),
                    expectedQueueRevision);
            final Request guarded = new Request(
                    request.owner(),
                    request.deadlineEpochMs(),
                    request.operationDigest(),
                    request.quota(),
                    authority);
            return new NativeSelection<>(
                    Optional.of(() -> host.claim(
                            worker(shard),
                            budget(),
                            cost.head(),
                            guarded.owner(),
                            nowEpochMs,
                            guarded.deadlineEpochMs(),
                            cost.executionBytes(),
                            guarded.operationDigest(),
                            guarded.quota(),
                            guarded.physicalWrites(),
                            ownerClock)),
                    OptionalLong.empty());
        };
    }

    static OptionalLong nativePolicyWake(
            final TargetNativePolicyChecks.Decision decision,
            final long nowEpochMs,
            final long expireAtEpochMs) {
        if ((decision.action() == TargetNativePolicyChecks.Action.WAIT_UNTIL
                        || decision.action() == TargetNativePolicyChecks.Action.TIME_SAMPLE_REQUIRED)
                && decision.wakeAtEpochMs() > nowEpochMs
                && decision.wakeAtEpochMs() < expireAtEpochMs) {
            return OptionalLong.of(decision.wakeAtEpochMs());
        }
        return OptionalLong.empty();
    }

    private TargetStoreBackend.CommitAuthority nativeCommitAuthority(
            final TargetStoreBackend.CommitAuthority delegate,
            final Requests requests,
            final TargetWorkerShardRuntime shard,
            final TargetHeadCostProbe.Cost cost,
            final TargetNativePolicyScope scope,
            final boolean initialAttempt,
            final HandoffPolicyHeadRef expectedHead,
            final long expectedActionAt,
            final long expectedQueueRevision) {
        return (store, quotaScope, mutation) -> {
            final var guard = Objects.requireNonNull(delegate.acquire(store, quotaScope, mutation), "commit guard");
            return new TargetStoreBackend.CommitGuard() {
                @Override
                public void requireCurrent() {
                    if (host.targetQueueChangeRevision() != expectedQueueRevision) {
                        throw new IllegalStateException("Target queue changed after the ordinary scheduling pass");
                    }
                    guard.requireCurrent();
                    if (host.targetQueueChangeRevision() != expectedQueueRevision) {
                        throw new IllegalStateException("Target queue changed during Native Claim guard acquisition");
                    }
                    final NativePolicyContext context = requests.resolveNativePolicyContext(shard, cost)
                            .orElseThrow(() -> new IllegalStateException(
                                    "Target Native policy context disappeared before Claim commit"));
                    final SourcePosition position = context.sourcePosition().get();
                    final TrustedUtcIntervalEvidence time = context.trustedTime().get();
                    final var publication = context.policies().current(scope.digest()).orElse(null);
                    final var work = cost.nativeProjection().work();
                    final var current = TargetNativePolicyChecks.resolve(
                            scope,
                            initialAttempt,
                            cost.deliverAtEpochMs(),
                            work.retryEligibilityAtEpochMs(),
                            publication,
                            context.trust(),
                            position,
                            time);
                    if (current.action() != TargetNativePolicyChecks.Action.NATIVE_CANDIDATE
                            || !expectedHead.equals(current.policyHeadRef())
                            || !Objects.equals(expectedActionAt, current.nativeActionAtEpochMs())) {
                        throw new IllegalStateException("Target Native policy changed before Claim commit");
                    }
                    TargetNativePolicyChecks.freezeCurrent(
                            scope,
                            expectedHead,
                            cost.deliverAtEpochMs(),
                            expectedActionAt,
                            context.policies(),
                            context.trust(),
                            position,
                            time);
                }

                @Override
                public void close() {
                    guard.close();
                }
            };
        };
    }

    synchronized <T> FreezeTurn freezeFirstPass(
            final long nowEpochMs, final SchedulerBudget budget, final Selector<T> selector) {
        if (!firstPassRequired || firstPassReady) {
            throw new IllegalStateException("Target recovery first pass is absent or already frozen");
        }
        if (nowEpochMs < 0 || (freezeEpochMs >= 0 && freezeEpochMs != nowEpochMs)) {
            throw new IllegalArgumentException("Target recovery first pass requires one trusted freeze time");
        }
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(selector, "selector");
        if (!reads.membershipCurrent()) {
            throw new IllegalStateException("Target recovery first pass source membership changed");
        }
        freezeEpochMs = nowEpochMs;
        final long started = clock();
        int visits = 0;
        while (freezeCursor < ring.size()
                && visits < limits.maximumVisitsPerTurn()
                && clock() - started < budget.maxElapsedNanos()) {
            final TargetState target = ring.get(freezeCursor);
            final Selection<T> selection;
            try {
                selection = selectCandidate(target, nowEpochMs, limits.sendEnvelopeBytes(), selector);
            } catch (ReadIncompleteException incomplete) {
                return new FreezeTurn(FreezeStop.READ_INCOMPLETE, visits, firstPassPending.size());
            }
            if (selection.candidate().isPresent()) {
                firstPassPending.add(target.id);
            }
            freezeCursor++;
            visits++;
        }
        if (freezeCursor == ring.size()) {
            if (clock() - started >= budget.maxElapsedNanos()) {
                return new FreezeTurn(FreezeStop.VISIT_BUDGET, visits, firstPassPending.size());
            }
            try {
                if (!reads.cutsCurrent(inventoryCuts)) {
                    throw new IllegalStateException("Target recovery first pass Store cut changed; rebuild inventory");
                }
            } catch (ReadIncompleteException incomplete) {
                return new FreezeTurn(FreezeStop.READ_INCOMPLETE, visits, firstPassPending.size());
            }
            if (clock() - started >= budget.maxElapsedNanos()) {
                return new FreezeTurn(FreezeStop.VISIT_BUDGET, visits, firstPassPending.size());
            }
            firstPassReady = true;
            return new FreezeTurn(FreezeStop.READY, visits, firstPassPending.size());
        }
        return new FreezeTurn(FreezeStop.VISIT_BUDGET, visits, firstPassPending.size());
    }

    synchronized <T> Turn<T> runOrdinary(
            final long nowEpochMs, final SchedulerBudget budget, final Selector<T> selector) {
        if (nowEpochMs < 0) {
            throw new IllegalArgumentException("Target DRR requires trusted nonnegative time");
        }
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(selector, "selector");
        if (firstPassRequired && !firstPassReady) {
            throw new IllegalStateException("Target recovery first pass must be frozen before Claim");
        }
        if (!reads.membershipCurrent()) {
            throw new IllegalStateException("Target DRR source membership changed");
        }
        final long started = clock();
        final List<T> claims = new ArrayList<>();
        long bytes = 0;
        int visits = 0;
        boolean creditWait = false;
        boolean budgetWait = false;
        while (!ring.isEmpty()
                && visits < limits.maximumVisitsPerTurn()
                && claims.isEmpty()
                && bytes < budget.maxBytes()
                && clock() - started < budget.maxElapsedNanos()) {
            if (!reads.membershipCurrent()) {
                throw new IllegalStateException("Target DRR source membership changed");
            }
            final TargetState target = ring.get(cursor);
            cursor = cursor == ring.size() - 1 ? 0 : cursor + 1;
            visits++;
            if (!firstPassPending.isEmpty() && !firstPassPending.contains(target.id)) {
                continue;
            }
            final Visit<T> visit;
            try {
                visit = visit(target, nowEpochMs, budget.maxBytes() - bytes, selector);
            } catch (ReadIncompleteException incomplete) {
                return new Turn<>(claims, visits, bytes, Stop.READ_INCOMPLETE);
            }
            if (visit.kind() == VisitKind.UNAVAILABLE || visit.kind() == VisitKind.CLAIMED) {
                firstPassPending.remove(target.id);
            }
            if (visit.kind() == VisitKind.CLAIMED) {
                claims.add(visit.claimed().value());
                bytes = Math.addExact(bytes, visit.claimed().cost());
            } else if (visit.kind() == VisitKind.CREDIT_WAIT) {
                creditWait = true;
            } else if (visit.kind() == VisitKind.BUDGET_WAIT) {
                budgetWait = true;
            }
        }
        final Stop stop = creditWait ? Stop.CREDIT_WAIT : budgetWait ? Stop.BUDGET_WAIT : Stop.NORMAL;
        return new Turn<>(claims, visits, bytes, stop);
    }

    synchronized <T> Turn<T> runNative(
            final long nowEpochMs, final SchedulerBudget budget, final Selector<T> selector) {
        Objects.requireNonNull(selector, "selector");
        final long expectedQueueRevision = host == null ? 0 : host.targetQueueChangeRevision();
        return runNative(
                nowEpochMs,
                budget,
                (shard, cost) -> new NativeSelection<>(selector.select(shard, cost), OptionalLong.empty()),
                expectedQueueRevision);
    }

    synchronized <T> Turn<T> runNative(
            final long nowEpochMs,
            final SchedulerBudget budget,
            final NativeSelector<T> selector,
            final long expectedQueueRevision) {
        if (nowEpochMs < 0) {
            throw new IllegalArgumentException("Target Native DRR requires trusted nonnegative time");
        }
        if (expectedQueueRevision < 0) {
            throw new IllegalArgumentException("Target Native DRR requires a nonnegative queue revision");
        }
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(selector, "selector");
        if (firstPassRequired && !firstPassReady) {
            throw new IllegalStateException("Target recovery first pass must be frozen before Native Claim");
        }
        if (!reads.membershipCurrent()) {
            throw new IllegalStateException("Target Native DRR source membership changed");
        }
        if (host != null && host.targetQueueChangeRevision() != expectedQueueRevision) {
            resetNativeWakeScan();
            return new Turn<>(List.of(), 0, 0, Stop.NORMAL);
        }
        if (nativeWakePassRevision != expectedQueueRevision
                || nativeWakePassTargets.isEmpty() && nativeWakeCompleteRevision == expectedQueueRevision) {
            nativeWakePassTargets.clear();
            nativeWakePassByTarget.clear();
            nativeWakePassRevision = expectedQueueRevision;
            nativeWakeCompleteRevision = -1;
            nextNativeWakeEpochMs = null;
        }
        if (ring.isEmpty()) {
            nativeWakeCompleteRevision = expectedQueueRevision;
            nextNativeWakeEpochMs = null;
            return new Turn<>(List.of(), 0, 0, Stop.NORMAL);
        }
        final long started = clock();
        final List<T> claims = new ArrayList<>();
        long bytes = 0;
        int visits = 0;
        boolean creditWait = false;
        boolean budgetWait = false;
        while (!ring.isEmpty()
                && nativeWakeCompleteRevision != expectedQueueRevision
                && nativeWakePassTargets.size() < ring.size()
                && visits < limits.maximumVisitsPerTurn()
                && claims.isEmpty()
                && bytes < budget.maxBytes()
                && clock() - started < budget.maxElapsedNanos()) {
            if (!reads.membershipCurrent()) {
                throw new IllegalStateException("Target Native DRR source membership changed");
            }
            if (host != null && host.targetQueueChangeRevision() != expectedQueueRevision) {
                resetNativeWakeScan();
                return new Turn<>(claims, visits, bytes, Stop.NORMAL);
            }
            final TargetState target = ring.get(cursor);
            cursor = cursor == ring.size() - 1 ? 0 : cursor + 1;
            if (nativeWakePassTargets.contains(target.id)) {
                continue;
            }
            visits++;
            if (!firstPassPending.isEmpty() && !firstPassPending.contains(target.id)) {
                recordNativeWakeObservation(target.id, null, expectedQueueRevision, nowEpochMs);
                continue;
            }
            final Visit<T> visit;
            try {
                visit = visitNative(target, nowEpochMs, budget.maxBytes() - bytes, selector);
            } catch (ReadIncompleteException incomplete) {
                return new Turn<>(claims, visits, bytes, Stop.READ_INCOMPLETE);
            }
            if (host != null && host.targetQueueChangeRevision() != expectedQueueRevision) {
                if (visit.kind() == VisitKind.CLAIMED) {
                    claims.add(visit.claimed().value());
                    bytes = Math.addExact(bytes, visit.claimed().cost());
                }
                resetNativeWakeScan();
                return new Turn<>(claims, visits, bytes, Stop.NORMAL);
            }
            recordNativeWakeObservation(target.id, visit.nextWakeEpochMs(), expectedQueueRevision, nowEpochMs);
            if (visit.kind() == VisitKind.UNAVAILABLE || visit.kind() == VisitKind.CLAIMED) {
                firstPassPending.remove(target.id);
            }
            if (visit.kind() == VisitKind.CLAIMED) {
                claims.add(visit.claimed().value());
                bytes = Math.addExact(bytes, visit.claimed().cost());
            } else if (visit.kind() == VisitKind.CREDIT_WAIT) {
                creditWait = true;
            } else if (visit.kind() == VisitKind.BUDGET_WAIT) {
                budgetWait = true;
            }
        }
        final Stop stop = creditWait ? Stop.CREDIT_WAIT : budgetWait ? Stop.BUDGET_WAIT : Stop.NORMAL;
        return new Turn<>(claims, visits, bytes, stop);
    }

    private <T> Visit<T> visit(
            final TargetState target, final long nowEpochMs, final long remainingBytes, final Selector<T> selector) {
        final Selection<T> selection = selectCandidate(target, nowEpochMs, remainingBytes, selector);
        if (selection.candidate().isPresent()) {
            final Candidate<T> candidate = selection.candidate().orElseThrow();
            final long credited = addQuantum(target.credit);
            if (candidate.cost() > credited) {
                target.credit = credited;
                return new Visit<>(VisitKind.CREDIT_WAIT, null, null);
            }
            final T claimed = Objects.requireNonNull(candidate.action().commit(), "Claim result");
            target.credit = credited - candidate.cost();
            target.sourceCursor = (candidate.sourceIndex() + 1) % target.sources.size();
            target.sources.get(candidate.sourceIndex()).domainCursor =
                    (candidate.domainIndex() + 1) % candidate.domainCount();
            return new Visit<>(VisitKind.CLAIMED, new Claimed<>(claimed, candidate.cost()), null);
        }
        if (!selection.due() || !selection.budgetBlocked()) {
            target.credit = 0;
        }
        return new Visit<>(selection.budgetBlocked() ? VisitKind.BUDGET_WAIT : VisitKind.UNAVAILABLE, null, null);
    }

    private <T> Visit<T> visitNative(
            final TargetState target,
            final long nowEpochMs,
            final long remainingBytes,
            final NativeSelector<T> selector) {
        final Selection<T> selection = selectNativeCandidate(target, nowEpochMs, remainingBytes, selector);
        if (selection.candidate().isPresent()) {
            final Candidate<T> candidate = selection.candidate().orElseThrow();
            final long credited = addQuantum(target.credit);
            if (candidate.cost() > credited) {
                target.credit = credited;
                return new Visit<>(VisitKind.CREDIT_WAIT, null, selection.nextWakeEpochMs());
            }
            final T claimed = Objects.requireNonNull(candidate.action().commit(), "Native Claim result");
            target.credit = credited - candidate.cost();
            target.sourceCursor = (candidate.sourceIndex() + 1) % target.sources.size();
            target.sources.get(candidate.sourceIndex()).domainCursor =
                    (candidate.domainIndex() + 1) % candidate.domainCount();
            return new Visit<>(
                    VisitKind.CLAIMED,
                    new Claimed<>(claimed, candidate.cost()),
                    selection.nextWakeEpochMs());
        }
        if (!selection.due() || !selection.budgetBlocked()) {
            target.credit = 0;
        }
        return new Visit<>(
                selection.budgetBlocked() ? VisitKind.BUDGET_WAIT : VisitKind.UNAVAILABLE,
                null,
                selection.nextWakeEpochMs());
    }

    private <T> Selection<T> selectCandidate(
            final TargetState target, final long nowEpochMs, final long remainingBytes, final Selector<T> selector) {
        boolean due = false;
        boolean budgetBlocked = false;
        sources:
        for (int offset = 0; offset < target.sources.size(); offset++) {
            final int sourceIndex = (target.sourceCursor + offset) % target.sources.size();
            final SourceState source = target.sources.get(sourceIndex);
            final Optional<TargetQueueSnapshotReader.Entry> current = reads.refresh(source.shard, target.id);
            if (current.isEmpty()) {
                continue;
            }
            final var entry = current.orElseThrow();
            if (!Arrays.equals(
                    target.physical.canonicalBytes(), entry.physical().canonicalBytes())) {
                throw new IllegalStateException("Target DRR physical identity changed");
            }
            final TargetQueueState queue = entry.queue();
            if (queue.admissionState() != TargetQueueState.AdmissionState.OPEN) {
                continue;
            }
            final List<TargetDomainState> domains = queue.domains();
            for (int domainOffset = 0; domainOffset < domains.size(); domainOffset++) {
                final int domainIndex = (source.domainCursor + domainOffset) % domains.size();
                final TargetDomainState domain = domains.get(domainIndex);
                final TargetHeadRef head = domain.ordinaryHead();
                if (domain.lifecycle() != TargetDomainState.Lifecycle.ACTIVE
                        || head == null
                        || head.timeEpochMs() > nowEpochMs) {
                    continue;
                }
                due = true;
                final TargetHeadCostProbe.Cost cost;
                try {
                    cost = reads.probe(source.shard, head);
                } catch (IllegalStateException staleOrInvalid) {
                    final var latest = reads.refresh(source.shard, target.id);
                    if (latest.isEmpty()
                            || !Arrays.equals(
                                    queue.digest(), latest.orElseThrow().queue().digest())) {
                        continue sources;
                    }
                    throw staleOrInvalid;
                }
                if (cost.deliverAtEpochMs() > nowEpochMs || cost.expireAtEpochMs() <= nowEpochMs) {
                    continue;
                }
                if (cost.schedulingCost() > limits.maximumCostBytes()) {
                    throw new IllegalStateException("Target DRR accepted head exceeds activated maximum cost");
                }
                final Optional<ClaimAction<T>> action = selector.select(source.shard, cost);
                if (action.isEmpty()) {
                    continue;
                }
                if (cost.schedulingCost() > remainingBytes) {
                    budgetBlocked = true;
                    continue;
                }
                return new Selection<>(
                        Optional.of(new Candidate<>(
                                sourceIndex, domainIndex, domains.size(), cost.schedulingCost(), action.orElseThrow())),
                        true,
                        budgetBlocked,
                        null);
            }
        }
        return new Selection<>(Optional.empty(), due, budgetBlocked, null);
    }

    private <T> Selection<T> selectNativeCandidate(
            final TargetState target,
            final long nowEpochMs,
            final long remainingBytes,
            final NativeSelector<T> selector) {
        boolean due = false;
        boolean budgetBlocked = false;
        Long nextWakeEpochMs = null;
        sources:
        for (int offset = 0; offset < target.sources.size(); offset++) {
            final int sourceIndex = (target.sourceCursor + offset) % target.sources.size();
            final SourceState source = target.sources.get(sourceIndex);
            final Optional<TargetQueueSnapshotReader.Entry> current = reads.refresh(source.shard, target.id);
            if (current.isEmpty()) {
                continue;
            }
            final var entry = current.orElseThrow();
            if (!Arrays.equals(target.physical.canonicalBytes(), entry.physical().canonicalBytes())) {
                throw new IllegalStateException("Target Native DRR physical identity changed");
            }
            final TargetQueueState queue = entry.queue();
            if (queue.admissionState() != TargetQueueState.AdmissionState.OPEN) {
                continue;
            }
            final List<TargetDomainState> domains = queue.domains();
            for (int domainOffset = 0; domainOffset < domains.size(); domainOffset++) {
                final int domainIndex = (source.domainCursor + domainOffset) % domains.size();
                final TargetDomainState domain = domains.get(domainIndex);
                final TargetHeadRef head = domain.nativeHead();
                if (domain.lifecycle() != TargetDomainState.Lifecycle.ACTIVE || head == null) {
                    continue;
                }
                final TargetHeadCostProbe.Cost cost;
                try {
                    cost = reads.probe(source.shard, head);
                } catch (IllegalStateException staleOrInvalid) {
                    final var latest = reads.refresh(source.shard, target.id);
                    if (latest.isEmpty()
                            || !Arrays.equals(queue.digest(), latest.orElseThrow().queue().digest())) {
                        continue sources;
                    }
                    throw staleOrInvalid;
                }
                if (cost.deliverAtEpochMs() <= nowEpochMs || cost.expireAtEpochMs() <= nowEpochMs) {
                    continue;
                }
                if (cost.schedulingCost() > limits.maximumCostBytes()) {
                    throw new IllegalStateException("Target Native head exceeds activated maximum cost");
                }
                final NativeSelection<T> selected = Objects.requireNonNull(
                        selector.select(source.shard, cost), "Native selection");
                if (selected.nextWakeEpochMs().isPresent()
                        && selected.nextWakeEpochMs().getAsLong() > nowEpochMs) {
                    nextWakeEpochMs = nextWakeEpochMs == null
                            ? selected.nextWakeEpochMs().getAsLong()
                            : Math.min(nextWakeEpochMs, selected.nextWakeEpochMs().getAsLong());
                }
                if (selected.action().isEmpty()) {
                    continue;
                }
                due = true;
                if (cost.schedulingCost() > remainingBytes) {
                    budgetBlocked = true;
                    continue;
                }
                return new Selection<>(
                        Optional.of(new Candidate<>(
                                sourceIndex,
                                domainIndex,
                                domains.size(),
                                cost.schedulingCost(),
                                selected.action().orElseThrow())),
                        true,
                        budgetBlocked,
                        nextWakeEpochMs);
            }
        }
        return new Selection<>(Optional.empty(), due, budgetBlocked, nextWakeEpochMs);
    }

    private long addQuantum(final long current) {
        final long cap = limits.maximumCredit();
        return current > cap - limits.quantumBytes() ? cap : current + limits.quantumBytes();
    }

    private BoundedReadBudget budget() {
        return new BoundedReadBudget(
                limits.readRecords(), limits.readBytes(), limits.readElapsedNanos(), monotonicClock);
    }

    private TargetWorkerShardRuntime worker(final ShardId shard) {
        final var worker = workers.get(shard);
        if (worker == null) {
            throw new IllegalStateException("Target DRR source Shard is no longer registered");
        }
        return worker;
    }

    private long clock() {
        final long now = monotonicClock.getAsLong();
        if (now < 0 || now < lastClockNanos) {
            throw new IllegalStateException("Target DRR monotonic clock moved backwards or returned negative time");
        }
        lastClockNanos = now;
        return now;
    }

    private boolean currentWorkersMatch() {
        return workersById(host.currentTargetWorkers()).equals(workers);
    }

    private static Map<ShardId, TargetWorkerShardRuntime> workersById(
            final List<TargetWorkerShardRuntime> values) {
        final Map<ShardId, TargetWorkerShardRuntime> result = new HashMap<>();
        for (TargetWorkerShardRuntime worker : values) {
            if (result.put(worker.shardId(), worker) != null) {
                throw new IllegalStateException("Target DRR has a duplicate source Shard");
            }
        }
        return Map.copyOf(result);
    }

    private static List<TargetState> states(
            final TargetWorkerTargetInventory.Snapshot inventory,
            final Map<ShardId, TargetWorkerShardRuntime> workers) {
        final List<TargetState> states = new ArrayList<>();
        for (TargetWorkerTargetInventory.Target target : inventory.targets()) {
            states.add(new TargetState(target, inventory.cuts(), workers));
        }
        return List.copyOf(states);
    }

    private static final class TargetState {
        private final TargetPartitionId id;
        private final CanonicalTargetPartition physical;
        private final List<SourceState> sources = new ArrayList<>();
        private int sourceCursor;
        private long credit;
        private Long nextOrdinaryWakeEpochMs;

        private TargetState(
                final TargetWorkerTargetInventory.Target target,
                final Map<ShardId, TargetQueueSnapshotReader.Cut> cuts,
                final Map<ShardId, TargetWorkerShardRuntime> workers) {
            id = target.id();
            physical = target.physical();
            for (TargetWorkerTargetInventory.Source source : target.sources()) {
                final var cut = cuts.get(source.shard());
                if (cut == null) {
                    throw new IllegalStateException("Target inventory source has no Store cut");
                }
                sources.add(new SourceState(source.shard(), cut.storeIncarnation(), workers.get(source.shard())));
            }
            nextOrdinaryWakeEpochMs = earliestOrdinaryWake(target);
        }

        private void updateQueueSnapshots(final TargetWorkerTargetInventory.Target incoming) {
            if (!id.equals(incoming.id()) || sources.size() != incoming.sources().size()) {
                throw new IllegalStateException("Target queue refresh changes its source membership");
            }
            nextOrdinaryWakeEpochMs = earliestOrdinaryWake(incoming);
        }

        private static Long earliestOrdinaryWake(final TargetWorkerTargetInventory.Target target) {
            Long earliest = null;
            for (TargetWorkerTargetInventory.Source source : target.sources()) {
                final TargetQueueState queue = source.entry().queue();
                if (queue.admissionState() != TargetQueueState.AdmissionState.OPEN) {
                    continue;
                }
                for (TargetDomainState domain : queue.domains()) {
                    final TargetHeadRef head = domain.ordinaryHead();
                    if (domain.lifecycle() == TargetDomainState.Lifecycle.ACTIVE && head != null) {
                        earliest = earliest == null ? head.timeEpochMs() : Math.min(earliest, head.timeEpochMs());
                    }
                }
            }
            return earliest;
        }

        private boolean sameSources(
                final TargetWorkerTargetInventory.Target incoming,
                final Map<ShardId, TargetQueueSnapshotReader.Cut> cuts,
                final Map<ShardId, TargetWorkerShardRuntime> workers) {
            if (sources.size() != incoming.sources().size()) {
                return false;
            }
            for (int index = 0; index < sources.size(); index++) {
                final var prior = sources.get(index);
                final var source = incoming.sources().get(index);
                final var cut = cuts.get(source.shard());
                if (!prior.shard.equals(source.shard())
                        || cut == null
                        || !Arrays.equals(prior.storeIncarnation, cut.storeIncarnation())
                        || prior.worker != workers.get(source.shard())) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class SourceState {
        private final ShardId shard;
        private final byte[] storeIncarnation;
        private final TargetWorkerShardRuntime worker;
        private int domainCursor;

        private SourceState(
                final ShardId shard, final byte[] storeIncarnation, final TargetWorkerShardRuntime worker) {
            this.shard = shard;
            this.storeIncarnation = storeIncarnation;
            this.worker = worker;
        }
    }
}
