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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

    interface Reads {
        boolean membershipCurrent();

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

    private final TargetWorkerHostRuntime host;
    private final Map<ShardId, TargetWorkerShardRuntime> workers;
    private final Reads reads;
    private final Limits limits;
    private final LongSupplier monotonicClock;
    private final LongSupplier ownerClock;
    private final List<TargetState> ring;
    private int cursor;
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
        reads = new Reads() {
            @Override
            public boolean membershipCurrent() {
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
        host = null;
        workers = Map.of();
        ownerClock = null;
        this.limits = Objects.requireNonNull(limits, "limits");
        this.reads = Objects.requireNonNull(reads, "reads");
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        ring = states(Objects.requireNonNull(inventory, "inventory"));
    }

    /** Returns after at most one durable Claim so its receipt is never lost to a later read failure. */
    public synchronized Turn<TargetClaimRecord> claimOrdinary(
            final long nowEpochMs, final SchedulerBudget budget, final Requests requests) {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(requests, "requests");
        return runOrdinary(nowEpochMs, budget, (shard, cost) -> requests.resolve(worker(shard), cost)
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
                        ownerClock)));
    }

    synchronized <T> Turn<T> runOrdinary(
            final long nowEpochMs, final SchedulerBudget budget, final Selector<T> selector) {
        if (nowEpochMs < 0) {
            throw new IllegalArgumentException("Target DRR requires trusted nonnegative time");
        }
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(selector, "selector");
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
            final Optional<Claimed<T>> claimed;
            try {
                claimed = visit(target, nowEpochMs, budget.maxBytes() - bytes, selector);
            } catch (ReadIncompleteException incomplete) {
                return new Turn<>(claims, visits, bytes, Stop.READ_INCOMPLETE);
            }
            if (claimed.isPresent()) {
                claims.add(claimed.orElseThrow().value());
                bytes = Math.addExact(bytes, claimed.orElseThrow().cost());
            }
        }
        return new Turn<>(claims, visits, bytes);
    }

    private <T> Optional<Claimed<T>> visit(
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
                if (cost.schedulingCost() > remainingBytes) {
                    budgetBlocked = true;
                    continue;
                }
                final Optional<ClaimAction<T>> action = selector.select(source.shard, cost);
                if (action.isEmpty()) {
                    continue;
                }
                target.credit = addQuantum(target.credit);
                if (cost.schedulingCost() > target.credit) {
                    return Optional.empty();
                }
                final T claimed = Objects.requireNonNull(action.orElseThrow().commit(), "Claim result");
                target.credit -= cost.schedulingCost();
                target.sourceCursor = (sourceIndex + 1) % target.sources.size();
                source.domainCursor = (domainIndex + 1) % domains.size();
                return Optional.of(new Claimed<>(claimed, cost.schedulingCost()));
            }
        }
        if (!due || !budgetBlocked) {
            target.credit = 0;
        }
        return Optional.empty();
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
    }

    private static final class SourceState {
        private final ShardId shard;
        private int domainCursor;

        private SourceState(final ShardId shard) {
            this.shard = shard;
        }
    }
}
