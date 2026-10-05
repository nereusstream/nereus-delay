package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.SelfRoutingId;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.runtime.TargetHeadCostProbe;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Opt-in, synthetic measurement of the production Target DRR selection component. */
public final class TargetSchedulerComponentBenchmark {
    private static final long NOW_EPOCH_MS = 10;
    private static final long MAX_COST_BYTES = 65_536;
    private static final TargetKeyCodec.Domain DOMAIN = new TargetKeyCodec.Domain(0, 1);
    private static final byte[] REF = filled(32, 3);
    private static final byte[] ACCOUNTING_INCARNATION = filled(16, 5);
    private static final byte[] SOURCE_ORDER_PREFIX = {1};
    private static final TargetQuotaAccounting ACCOUNTING = new TargetQuotaAccounting(filled(32, 7), 0, 0, 0, 1);
    private static final long DELAY_ID_TIMESTAMP = 1_700_000_000_000L;
    private static final int TIMED_SAMPLES = 5;

    private TargetSchedulerComponentBenchmark() {}

    public static void main(final String[] args) {
        final List<Case> cases = args.length == 0 ? defaultCases() : parseCases(args);
        run(new Case(10_000, 10, 1_024));
        System.out.println("# synthetic DRR; in-memory heads; no persistent I/O/Broker; payload affects cost only");
        System.out.println(
                "scope,N_backlog,L_targets,payload_bytes,delivered,refresh_calls,head_probe_calls,target_visits,"
                        + "scheduler_bytes,max_service_gap_turns,median_elapsed_ns,median_thread_allocated_bytes,"
                        + "median_gc_pause_ms,timed_samples");
        for (Case measurement : cases) {
            final List<Measurement> samples = new ArrayList<>(TIMED_SAMPLES);
            for (int sample = 0; sample < TIMED_SAMPLES; sample++) {
                samples.add(run(measurement));
            }
            System.out.println(summarize(samples).csv());
        }
    }

    private static List<Case> defaultCases() {
        return List.of(
                new Case(1_000, 10, 1_024),
                new Case(10_000, 10, 1_024),
                new Case(100_000, 10, 1_024),
                new Case(100_000, 1, 1_024),
                new Case(100_000, 100, 1_024),
                new Case(100_000, 1_000, 1_024));
    }

    private static List<Case> parseCases(final String[] args) {
        final List<Case> result = new ArrayList<>();
        for (String argument : args) {
            final String[] fields = argument.split(":", -1);
            if (fields.length != 3) {
                throw new IllegalArgumentException("benchmark case must be N:L:payloadBytes: " + argument);
            }
            result.add(new Case(
                    Long.parseLong(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2])));
        }
        return List.copyOf(result);
    }

    private static Measurement run(final Case measurement) {
        final int targetCount = measurement.targets();
        final ShardId shard = new ShardId(new RouteIncarnation(filled(16, 6)), 0);
        final Map<ShardId, TargetQueueSnapshotReader.Cut> cuts = Map.of(
                shard, new TargetQueueSnapshotReader.Cut(filled(16, 8), 1));
        final SyntheticReads reads = new SyntheticReads(shard, measurement, targetCount);
        final List<TargetWorkerTargetInventory.Target> inventoryTargets = new ArrayList<>(targetCount);
        for (int targetIndex = 0; targetIndex < targetCount; targetIndex++) {
            final CanonicalTargetPartition physical = physicalTarget(targetIndex);
            final long messages = messagesForTarget(measurement.backlog(), targetCount, targetIndex);
            final TargetQueueSnapshotReader.Entry entry = reads.initialize(targetIndex, physical, messages);
            inventoryTargets.add(new TargetWorkerTargetInventory.Target(
                    physical, List.of(new TargetWorkerTargetInventory.Source(shard, entry))));
        }
        final var inventory = new TargetWorkerTargetInventory.Snapshot(inventoryTargets, cuts);
        final var limits = new TargetWorkerOrdinaryDrr.Limits(
                MAX_COST_BYTES,
                MAX_COST_BYTES,
                MAX_COST_BYTES,
                Math.max(targetCount, 1),
                Math.max(targetCount, 1),
                Math.max(MAX_COST_BYTES, (long) targetCount * MAX_COST_BYTES),
                1_000_000_000L);
        final var drr = new TargetWorkerOrdinaryDrr(inventory, limits, reads, System::nanoTime);
        final var budget = new SchedulerBudget(1, MAX_COST_BYTES, 1_000_000_000L);
        final long[] lastServiceTurn = new long[targetCount];
        java.util.Arrays.fill(lastServiceTurn, -1);
        long maxServiceGap = 0;
        long delivered = 0;
        long turns = 0;
        long schedulerBytes = 0;
        long targetVisits = 0;
        final long threadId = Thread.currentThread().threadId();
        final var allocationBean = allocationBean();
        final long allocatedBefore = allocatedBytes(allocationBean, threadId);
        final long gcPauseBefore = gcPauseMillis();
        final long started = System.nanoTime();
        while (delivered < measurement.backlog()) {
            final long turnNumber = turns++;
            final TargetWorkerOrdinaryDrr.Turn<TargetPartitionId> turn = drr.<TargetPartitionId>runOrdinary(
                    NOW_EPOCH_MS,
                    budget,
                    (source, cost) -> Optional.of(() -> {
                        reads.advance(cost.head());
                        return cost.head().target();
                    }));
            schedulerBytes = Math.addExact(schedulerBytes, turn.schedulingBytes());
            targetVisits = Math.addExact(targetVisits, turn.targetVisits());
            if (!turn.claims().isEmpty()) {
                final int targetIndex = reads.targetIndex(turn.claims().getFirst());
                final long previous = lastServiceTurn[targetIndex];
                if (previous >= 0) {
                    maxServiceGap = Math.max(maxServiceGap, turnNumber - previous);
                }
                lastServiceTurn[targetIndex] = turnNumber;
                delivered = Math.addExact(delivered, 1);
            }
        }
        final long elapsedNanos = System.nanoTime() - started;
        final long allocatedAfter = allocatedBytes(allocationBean, threadId);
        final long gcPauseAfter = gcPauseMillis();
        return new Measurement(
                measurement,
                delivered,
                reads.refreshCalls,
                reads.probeCalls,
                targetVisits,
                schedulerBytes,
                maxServiceGap,
                elapsedNanos,
                allocatedBefore < 0 || allocatedAfter < allocatedBefore ? -1 : allocatedAfter - allocatedBefore,
                gcPauseBefore < 0 || gcPauseAfter < gcPauseBefore ? -1 : gcPauseAfter - gcPauseBefore,
                1);
    }

    private static Measurement summarize(final List<Measurement> samples) {
        if (samples.size() != TIMED_SAMPLES) {
            throw new IllegalArgumentException("Target benchmark sample count differs from its fixed repetition count");
        }
        final Measurement first = samples.getFirst();
        for (Measurement sample : samples) {
            if (!sample.input().equals(first.input())
                    || sample.delivered() != first.delivered()
                    || sample.refreshCalls() != first.refreshCalls()
                    || sample.probeCalls() != first.probeCalls()
                    || sample.targetVisits() != first.targetVisits()
                    || sample.schedulerBytes() != first.schedulerBytes()
                    || sample.maxServiceGap() != first.maxServiceGap()) {
                throw new IllegalStateException("Target benchmark operation counts changed across repetitions");
            }
        }
        return new Measurement(
                first.input(),
                first.delivered(),
                first.refreshCalls(),
                first.probeCalls(),
                first.targetVisits(),
                first.schedulerBytes(),
                first.maxServiceGap(),
                median(samples.stream().map(Measurement::elapsedNanos).toList()),
                median(samples.stream().map(Measurement::allocatedBytes).filter(value -> value >= 0).toList()),
                median(samples.stream().map(Measurement::gcPauseMillis).filter(value -> value >= 0).toList()),
                TIMED_SAMPLES);
    }

    private static long median(final List<Long> samples) {
        if (samples.isEmpty()) {
            return -1;
        }
        final List<Long> sorted = samples.stream().sorted().toList();
        return sorted.get(sorted.size() / 2);
    }

    private static long messagesForTarget(final long backlog, final int targetCount, final int targetIndex) {
        return backlog / targetCount + (targetIndex < backlog % targetCount ? 1 : 0);
    }

    private static CanonicalTargetPartition physicalTarget(final int targetIndex) {
        return new CanonicalTargetPartition(
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("benchmark-cluster", new UUID(1, 2))),
                targetIndex);
    }

    private static TargetDomainState domainState(final TargetHeadRef head) {
        return new TargetDomainState(
                DOMAIN, TargetDomainState.Lifecycle.ACTIVE, REF, REF, null, head, null);
    }

    private static TargetQueueState queue(
            final CanonicalTargetPartition physical, final long revision, final TargetHeadRef head) {
        return new TargetQueueState(
                physical.id(),
                revision,
                1,
                TargetQueueState.AdmissionState.OPEN,
                ACCOUNTING_INCARNATION,
                0,
                List.of(domainState(head)));
    }

    private static TargetHeadRef head(
            final CanonicalTargetPartition physical, final ShardId shard, final long sequence) {
        final long uuidMost = (DELAY_ID_TIMESTAMP << 16) | 0x7000L;
        final long uuidLeast = 0x8000_0000_0000_0000L | sequence;
        final DelayMessageId message = new DelayMessageId(
                SelfRoutingId.fromLogicalUuid(shard, new UUID(uuidMost, uuidLeast)).bytes());
        final byte[] sourceOrder = Bytes.concat(SOURCE_ORDER_PREFIX, Bytes.u64be(sequence));
        final byte[] key = TargetKeyCodec.candidate(
                TargetKeyCodec.CandidateKind.DUE, physical.id(), DOMAIN, NOW_EPOCH_MS, sourceOrder, message, 1);
        return new TargetHeadRef(key, message, 1, NOW_EPOCH_MS);
    }

    private static com.sun.management.ThreadMXBean allocationBean() {
        final var bean = ManagementFactory.getThreadMXBean();
        if (bean instanceof com.sun.management.ThreadMXBean allocation
                && allocation.isThreadAllocatedMemorySupported()) {
            if (!allocation.isThreadAllocatedMemoryEnabled()) {
                allocation.setThreadAllocatedMemoryEnabled(true);
            }
            return allocation;
        }
        return null;
    }

    private static long allocatedBytes(final com.sun.management.ThreadMXBean bean, final long threadId) {
        return bean == null ? -1 : bean.getThreadAllocatedBytes(threadId);
    }

    private static long gcPauseMillis() {
        final List<GarbageCollectorMXBean> collectors = ManagementFactory.getGarbageCollectorMXBeans();
        if (collectors.isEmpty() || collectors.stream().allMatch(collector -> collector.getCollectionTime() < 0)) {
            return -1;
        }
        return collectors.stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionTime)
                .filter(value -> value >= 0)
                .sum();
    }

    private static byte[] filled(final int size, final int value) {
        final byte[] bytes = new byte[size];
        java.util.Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private record Case(long backlog, int targets, int payloadBytes) {
        private Case {
            if (backlog <= 0 || targets <= 0 || payloadBytes <= 0 || payloadBytes + 128 > MAX_COST_BYTES) {
                throw new IllegalArgumentException("invalid N/L/payload benchmark case");
            }
        }
    }

    private record Measurement(
            Case input,
            long delivered,
            long refreshCalls,
            long probeCalls,
            long targetVisits,
            long schedulerBytes,
            long maxServiceGap,
            long elapsedNanos,
            long allocatedBytes,
            long gcPauseMillis,
            int timedSamples) {
        private String csv() {
            return String.join(
                    ",",
                    "component-only-synthetic",
                    Long.toString(input.backlog()),
                    Integer.toString(input.targets()),
                    Integer.toString(input.payloadBytes()),
                    Long.toString(delivered),
                    Long.toString(refreshCalls),
                    Long.toString(probeCalls),
                    Long.toString(targetVisits),
                    Long.toString(schedulerBytes),
                    Long.toString(maxServiceGap),
                    Long.toString(elapsedNanos),
                    allocatedBytes < 0 ? "NA" : Long.toString(allocatedBytes),
                    gcPauseMillis < 0 ? "NA" : Long.toString(gcPauseMillis),
                    Integer.toString(timedSamples));
        }
    }

    private static final class SyntheticReads implements TargetWorkerOrdinaryDrr.Reads {
        private final ShardId shard;
        private final Case input;
        private final Map<TargetPartitionId, Integer> targetIndexById = new HashMap<>();
        private final List<CanonicalTargetPartition> physicalByIndex;
        private final TargetQueueSnapshotReader.Entry[] entries;
        private final long[] remaining;
        private final long[] nextSequence;
        private long refreshCalls;
        private long probeCalls;

        private SyntheticReads(final ShardId shard, final Case input, final int targetCount) {
            this.shard = shard;
            this.input = input;
            physicalByIndex = new ArrayList<>(targetCount);
            entries = new TargetQueueSnapshotReader.Entry[targetCount];
            remaining = new long[targetCount];
            nextSequence = new long[targetCount];
        }

        private TargetQueueSnapshotReader.Entry initialize(
                final int index, final CanonicalTargetPartition physical, final long messages) {
            physicalByIndex.add(physical);
            targetIndexById.put(physical.id(), index);
            remaining[index] = messages;
            final TargetHeadRef head = messages == 0 ? null : nextHead(index);
            final var entry = new TargetQueueSnapshotReader.Entry(queue(physical, 1, head), physical);
            entries[index] = entry;
            return entry;
        }

        private void advance(final TargetHeadRef head) {
            final int index = targetIndex(head.target());
            if (remaining[index] <= 0 || !entries[index].queue().domains().getFirst().ordinaryHead().equals(head)) {
                throw new IllegalStateException("synthetic Target head advanced outside its current queue state");
            }
            remaining[index]--;
            final CanonicalTargetPartition physical = physicalByIndex.get(index);
            final TargetHeadRef next = remaining[index] == 0 ? null : nextHead(index);
            final TargetQueueState prior = entries[index].queue();
            entries[index] = new TargetQueueSnapshotReader.Entry(
                    queue(physical, Math.addExact(prior.headRevision(), 1), next), physical);
        }

        private TargetHeadRef nextHead(final int index) {
            return head(physicalByIndex.get(index), shard, ++nextSequence[index]);
        }

        private int targetIndex(final TargetPartitionId target) {
            final Integer index = targetIndexById.get(target);
            if (index == null) {
                throw new IllegalStateException("synthetic scheduler returned an unknown Target");
            }
            return index;
        }

        @Override
        public boolean membershipCurrent() {
            return true;
        }

        @Override
        public boolean cutsCurrent(final Map<ShardId, TargetQueueSnapshotReader.Cut> expected) {
            return expected.size() == 1 && expected.containsKey(shard);
        }

        @Override
        public Optional<TargetQueueSnapshotReader.Entry> refresh(
                final ShardId source, final TargetPartitionId target) {
            if (!source.equals(shard)) {
                throw new IllegalStateException("synthetic inventory selected an unknown source Shard");
            }
            refreshCalls++;
            return Optional.of(entries[targetIndex(target)]);
        }

        @Override
        public TargetHeadCostProbe.Cost probe(final ShardId source, final TargetHeadRef head) {
            if (!source.equals(shard)) {
                throw new IllegalStateException("synthetic probe selected an unknown source Shard");
            }
            final int index = targetIndex(head.target());
            if (!entries[index].queue().domains().getFirst().ordinaryHead().equals(head)) {
                throw new IllegalStateException("synthetic probe selected a stale head");
            }
            probeCalls++;
            final long schedulingBytes = input.payloadBytes() + 128L;
            return new TargetHeadCostProbe.Cost(
                    head,
                    entries[index].queue(),
                    ACCOUNTING,
                    schedulingBytes,
                    schedulingBytes,
                    NOW_EPOCH_MS,
                    Long.MAX_VALUE);
        }
    }

}
