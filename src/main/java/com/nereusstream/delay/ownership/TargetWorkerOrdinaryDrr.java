package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetHeadCostProbe;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;

/** One process-local byte-cost DRR share per physical Target, for ordinary due Claim work. */
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
    }

    public enum Stop {
        NORMAL,
        READ_INCOMPLETE
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

    private record Claimed<T>(T value, long cost) {}

    private record Candidate<T>(int sourceIndex, int domainIndex, int domainCount, long cost, ClaimAction<T> action) {}

    private record Selection<T>(Optional<Candidate<T>> candidate, boolean due, boolean budgetBlocked) {}

    private enum VisitKind {
        CLAIMED,
        CREDIT_WAIT,
        BUDGET_WAIT,
        UNAVAILABLE
    }

    private record Visit<T>(VisitKind kind, Claimed<T> claimed) {}

    private final TargetWorkerHostRuntime host;
    private final Map<ShardId, TargetWorkerShardRuntime> workers;
    private final Reads reads;
    private final Limits limits;
    private final LongSupplier monotonicClock;
    private final LongSupplier ownerClock;
    private List<TargetState> ring;
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
                return host.currentTargetWorkers().equals(current);
            }

            @Override
            public boolean cutsCurrent(final Map<ShardId, TargetQueueSnapshotReader.Cut> expected) {
                for (TargetWorkerShardRuntime worker : current) {
                    if (!expected.get(worker.shardId()).equals(host.readTargetQueueCut(worker, budget(), ownerClock))) {
                        return false;
                    }
                }
                return host.currentTargetWorkers().equals(current);
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
        ring = states(inventory);
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
        ring = states(exactInventory);
    }

    /** Returns after at most one durable Claim so its receipt is never lost to a later read failure. */
    public synchronized Turn<TargetClaimRecord> claimOrdinary(
            final long nowEpochMs, final SchedulerBudget budget, final Requests requests) {
        Objects.requireNonNull(host, "host");
        return runOrdinary(nowEpochMs, budget, claimSelector(nowEpochMs, requests));
    }

    /** Admits a new complete Host inventory after the recovery first pass, retaining unchanged Target shares. */
    public synchronized void refreshInventory(final TargetWorkerTargetInventory.Result inventory) {
        Objects.requireNonNull(host, "host");
        final var complete = Objects.requireNonNull(inventory, "inventory");
        if (complete.stop() != TargetWorkerTargetInventory.Stop.COMPLETE
                || !workers.keySet().equals(complete.snapshot().cuts().keySet())) {
            throw new IllegalArgumentException("Target DRR refresh requires the same complete source membership");
        }
        if (!firstPassReady) {
            throw new IllegalStateException("Target recovery first pass must be frozen before inventory refresh");
        }
        refreshSnapshot(host.consumeTargetInventory(complete));
    }

    /** Process-state seam; production first consumes the exact inventory built by its Host. */
    synchronized void refreshSnapshot(final TargetWorkerTargetInventory.Snapshot inventory) {
        final var exact = Objects.requireNonNull(inventory, "inventory");
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
            if (old != null && old.sameSources(incoming)) {
                current = old;
                retained.add(old.id);
            } else {
                current = new TargetState(incoming);
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
    }

    private void requireRefreshReady(final TargetWorkerTargetInventory.Snapshot inventory) {
        if (!firstPassReady) {
            throw new IllegalStateException("Target recovery first pass must be frozen before inventory refresh");
        }
        if (!reads.membershipCurrent() || !reads.cutsCurrent(inventory.cuts())) {
            throw new IllegalStateException("Target DRR refresh membership or Store cut changed");
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
            }
        }
        return new Turn<>(claims, visits, bytes);
    }

    private <T> Visit<T> visit(
            final TargetState target, final long nowEpochMs, final long remainingBytes, final Selector<T> selector) {
        final Selection<T> selection = selectCandidate(target, nowEpochMs, remainingBytes, selector);
        if (selection.candidate().isPresent()) {
            final Candidate<T> candidate = selection.candidate().orElseThrow();
            final long credited = addQuantum(target.credit);
            if (candidate.cost() > credited) {
                target.credit = credited;
                return new Visit<>(VisitKind.CREDIT_WAIT, null);
            }
            final T claimed = Objects.requireNonNull(candidate.action().commit(), "Claim result");
            target.credit = credited - candidate.cost();
            target.sourceCursor = (candidate.sourceIndex() + 1) % target.sources.size();
            target.sources.get(candidate.sourceIndex()).domainCursor =
                    (candidate.domainIndex() + 1) % candidate.domainCount();
            return new Visit<>(VisitKind.CLAIMED, new Claimed<>(claimed, candidate.cost()));
        }
        if (!selection.due() || !selection.budgetBlocked()) {
            target.credit = 0;
        }
        return new Visit<>(selection.budgetBlocked() ? VisitKind.BUDGET_WAIT : VisitKind.UNAVAILABLE, null);
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
                        budgetBlocked);
            }
        }
        return new Selection<>(Optional.empty(), due, budgetBlocked);
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

    private static List<TargetState> states(final TargetWorkerTargetInventory.Snapshot inventory) {
        final List<TargetState> states = new ArrayList<>();
        for (TargetWorkerTargetInventory.Target target : inventory.targets()) {
            states.add(new TargetState(target));
        }
        return List.copyOf(states);
    }

    private static final class TargetState {
        private final TargetPartitionId id;
        private final CanonicalTargetPartition physical;
        private final List<SourceState> sources = new ArrayList<>();
        private int sourceCursor;
        private long credit;

        private TargetState(final TargetWorkerTargetInventory.Target target) {
            id = target.id();
            physical = target.physical();
            for (TargetWorkerTargetInventory.Source source : target.sources()) {
                sources.add(new SourceState(source.shard()));
            }
        }

        private boolean sameSources(final TargetWorkerTargetInventory.Target incoming) {
            if (sources.size() != incoming.sources().size()) {
                return false;
            }
            for (int index = 0; index < sources.size(); index++) {
                if (!sources.get(index)
                        .shard
                        .equals(incoming.sources().get(index).shard())) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class SourceState {
        private final ShardId shard;
        private int domainCursor;

        private SourceState(final ShardId shard) {
            this.shard = shard;
        }
    }
}
