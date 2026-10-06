package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Bounded, process-only assembly of one physical Target ring input from current Worker Shards. */
public final class TargetWorkerTargetInventory {
    public record Limits(
            int maximumShards,
            int maximumTargets,
            int pageTargets,
            int maximumPagesPerShard,
            int readRecords,
            long readBytes,
            long readElapsedNanos) {
        public Limits {
            if (maximumShards <= 0
                    || maximumTargets <= 0
                    || pageTargets <= 0
                    || maximumPagesPerShard <= 0
                    || readRecords <= 0
                    || readBytes <= 0
                    || readElapsedNanos <= 0) {
                throw new IllegalArgumentException("Target inventory requires finite positive limits");
            }
        }
    }

    public record Source(ShardId shard, TargetQueueSnapshotReader.Entry entry) {
        public Source {
            Objects.requireNonNull(shard, "shard");
            Objects.requireNonNull(entry, "entry");
        }
    }

    public record Target(CanonicalTargetPartition physical, List<Source> sources) {
        public Target {
            Objects.requireNonNull(physical, "physical");
            sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
            if (sources.isEmpty()
                    || sources.stream().anyMatch(source -> !physical.id()
                            .equals(source.entry().queue().targetId()))) {
                throw new IllegalArgumentException("Target inventory sources differ from physical Target");
            }
        }

        public TargetPartitionId id() {
            return physical.id();
        }
    }

    public record Snapshot(List<Target> targets, Map<ShardId, TargetQueueSnapshotReader.Cut> cuts) {
        public Snapshot {
            targets = List.copyOf(Objects.requireNonNull(targets, "targets"));
            cuts = Map.copyOf(Objects.requireNonNull(cuts, "cuts"));
        }
    }

    public enum Stop {
        COMPLETE,
        READ_BUDGET,
        SCAN_LIMIT,
        CUT_CHANGED,
        MEMBERSHIP_CHANGED
    }

    public record Result(Stop stop, Snapshot snapshot) {
        public Result {
            Objects.requireNonNull(stop, "stop");
            if ((stop == Stop.COMPLETE) != (snapshot != null)) {
                throw new IllegalArgumentException("incomplete Target inventory cannot publish a snapshot");
            }
        }
    }

    interface ShardSource {
        ShardId shardId();

        TargetQueueSnapshotReader.Page scan(BoundedReadBudget budget, TargetPartitionId after, int pageTargets);

        TargetQueueSnapshotReader.Cut readCut(BoundedReadBudget budget);

        default OptionalLong readActiveMessages(BoundedReadBudget budget, TargetPartitionId target) {
            Objects.requireNonNull(budget, "budget");
            Objects.requireNonNull(target, "target");
            return OptionalLong.empty();
        }
    }

    private static final Comparator<TargetPartitionId> TARGET_ORDER =
            (left, right) -> Arrays.compareUnsigned(left.bytes(), right.bytes());

    private TargetWorkerTargetInventory() {}

    static Result rebuild(
            final TargetWorkerHostRuntime host,
            final List<TargetWorkerShardRuntime> workers,
            final Limits limits,
            final LongSupplier ownerClock,
            final LongSupplier monotonicClock) {
        return rebuild(host, workers, limits, ownerClock, monotonicClock, null);
    }

    static Result rebuild(
            final TargetWorkerHostRuntime host,
            final List<TargetWorkerShardRuntime> workers,
            final Limits limits,
            final LongSupplier ownerClock,
            final LongSupplier monotonicClock,
            final BoundedAsyncMetricExporter metrics) {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(ownerClock, "ownerClock");
        Objects.requireNonNull(monotonicClock, "monotonicClock");
        final List<ShardSource> sources = new ArrayList<>();
        for (TargetWorkerShardRuntime worker : workers) {
            final var exact = Objects.requireNonNull(worker, "worker");
            sources.add(new ShardSource() {
                @Override
                public ShardId shardId() {
                    return exact.shardId();
                }

                @Override
                public TargetQueueSnapshotReader.Page scan(
                        final BoundedReadBudget budget, final TargetPartitionId after, final int pageTargets) {
                    return host.scanTargetQueues(exact, budget, after, pageTargets, ownerClock);
                }

                @Override
                public TargetQueueSnapshotReader.Cut readCut(final BoundedReadBudget budget) {
                    return host.readTargetQueueCut(exact, budget, ownerClock);
                }

                @Override
                public OptionalLong readActiveMessages(
                        final BoundedReadBudget budget, final TargetPartitionId target) {
                    return host.readTargetActiveMessages(exact, budget, target, ownerClock);
                }
            });
        }
        final Supplier<BoundedReadBudget> budgets = () -> new BoundedReadBudget(
                limits.readRecords(), limits.readBytes(), limits.readElapsedNanos(), monotonicClock);
        return rebuild(sources, limits, budgets, () -> host.currentTargetWorkers().equals(workers), metrics);
    }

    /** Each page is independently guarded; only equal per-Shard cuts and a final recheck publish. */
    static Result rebuild(
            final List<? extends ShardSource> sources,
            final Limits limits,
            final Supplier<BoundedReadBudget> budgets,
            final BooleanSupplier membershipCurrent) {
        return rebuild(sources, limits, budgets, membershipCurrent, null);
    }

    static Result rebuild(
            final List<? extends ShardSource> sources,
            final Limits limits,
            final Supplier<BoundedReadBudget> budgets,
            final BooleanSupplier membershipCurrent,
            final BoundedAsyncMetricExporter metrics) {
        Objects.requireNonNull(sources, "sources");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(budgets, "budgets");
        Objects.requireNonNull(membershipCurrent, "membershipCurrent");
        if (sources.size() > limits.maximumShards()) {
            throw new IllegalArgumentException("Target inventory exceeds activated Shard limit");
        }
        final ScanCounts counts = metrics == null ? null : new ScanCounts();
        final long started = metrics == null ? 0L : System.nanoTime();
        try {
            final Result result = rebuildPages(sources, limits, budgets, membershipCurrent, counts);
            if (metrics != null && result.snapshot() != null) {
                recordActiveMessageDepth(result.snapshot(), sources, budgets, metrics);
            }
            return result;
        } finally {
            if (metrics != null) {
                metrics.record(new BoundedAsyncMetricExporter.MetricEvent(
                        BoundedAsyncMetricExporter.Metric.TARGET_INVENTORY_SCAN_PAGES, counts.pages));
                metrics.record(new BoundedAsyncMetricExporter.MetricEvent(
                        BoundedAsyncMetricExporter.Metric.TARGET_INVENTORY_SCAN_BUDGET_RECORDS, counts.records));
                metrics.record(new BoundedAsyncMetricExporter.MetricEvent(
                        BoundedAsyncMetricExporter.Metric.TARGET_INVENTORY_SCAN_BUDGET_BYTES, counts.bytes));
                metrics.record(new BoundedAsyncMetricExporter.MetricEvent(
                        BoundedAsyncMetricExporter.Metric.TARGET_INVENTORY_REBUILD_DURATION_NANOS,
                        Math.max(0L, System.nanoTime() - started)));
            }
        }
    }

    /** Samples per-Target totals outside the ring-building budgets; an incomplete sample never rejects a ring. */
    private static void recordActiveMessageDepth(
            final Snapshot snapshot,
            final List<? extends ShardSource> sources,
            final Supplier<BoundedReadBudget> budgets,
            final BoundedAsyncMetricExporter metrics) {
        final Map<ShardId, ShardSource> byShard = new LinkedHashMap<>();
        for (ShardSource source : sources) {
            byShard.put(source.shardId(), source);
        }
        final Map<TargetPartitionId, Long> samples = new TreeMap<>(TARGET_ORDER);
        long unavailable = 0;
        for (Target target : snapshot.targets()) {
            long depth = 0;
            boolean complete = true;
            for (Source source : target.sources()) {
                final OptionalLong count;
                try {
                    count = Objects.requireNonNull(
                            Objects.requireNonNull(byShard.get(source.shard()), "inventory source")
                                    .readActiveMessages(budgets.get(), target.id()),
                            "active-message count");
                } catch (ReadIncompleteException incomplete) {
                    complete = false;
                    break;
                }
                if (count.isEmpty()) {
                    complete = false;
                    break;
                }
                try {
                    depth = Math.addExact(depth, count.getAsLong());
                } catch (ArithmeticException overflow) {
                    complete = false;
                    break;
                }
            }
            if (complete) {
                samples.put(target.id(), depth);
            } else {
                unavailable++;
            }
        }
        for (ShardSource source : sources) {
            final TargetQueueSnapshotReader.Cut current;
            try {
                current = source.readCut(budgets.get());
            } catch (ReadIncompleteException incomplete) {
                samples.clear();
                unavailable = snapshot.targets().size();
                break;
            }
            if (!snapshot.cuts().get(source.shardId()).equals(current)) {
                samples.clear();
                unavailable = snapshot.targets().size();
                break;
            }
        }
        for (long depth : samples.values()) {
            metrics.record(new BoundedAsyncMetricExporter.MetricEvent(
                    BoundedAsyncMetricExporter.Metric.TARGET_INVENTORY_ACTIVE_MESSAGE_DEPTH, depth));
        }
        if (unavailable > 0) {
            metrics.record(new BoundedAsyncMetricExporter.MetricEvent(
                    BoundedAsyncMetricExporter.Metric.TARGET_INVENTORY_ACTIVE_MESSAGE_DEPTH_UNAVAILABLE,
                    unavailable));
        }
    }

    private static Result rebuildPages(
            final List<? extends ShardSource> sources,
            final Limits limits,
            final Supplier<BoundedReadBudget> budgets,
            final BooleanSupplier membershipCurrent,
            final ScanCounts counts) {
        final Map<ShardId, TargetQueueSnapshotReader.Cut> cuts = new LinkedHashMap<>();
        final Map<TargetPartitionId, MutableTarget> grouped = new TreeMap<>(TARGET_ORDER);
        for (ShardSource source : sources) {
            final ShardId shardId = Objects.requireNonNull(source, "source").shardId();
            if (cuts.containsKey(shardId)) {
                throw new IllegalArgumentException("Target inventory contains duplicate source Shard");
            }
            TargetPartitionId after = null;
            boolean complete = false;
            for (int pageIndex = 0; pageIndex < limits.maximumPagesPerShard(); pageIndex++) {
                final BoundedReadBudget pageBudget = budgets.get();
                if (counts != null) {
                    counts.pages++;
                }
                final TargetQueueSnapshotReader.Page page;
                try {
                    page = source.scan(pageBudget, after, limits.pageTargets());
                } catch (ReadIncompleteException incomplete) {
                    return new Result(Stop.READ_BUDGET, null);
                } finally {
                    if (counts != null) {
                        counts.records = saturatedAdd(counts.records, pageBudget.actualRecords());
                        counts.bytes = saturatedAdd(counts.bytes, pageBudget.actualBytes());
                    }
                }
                final var priorCut = cuts.putIfAbsent(shardId, page.cut());
                if (priorCut != null && !priorCut.equals(page.cut())) {
                    return new Result(Stop.CUT_CHANGED, null);
                }
                TargetPartitionId seen = after;
                for (TargetQueueSnapshotReader.Entry entry : page.entries()) {
                    final TargetPartitionId id = entry.queue().targetId();
                    if (seen != null && TARGET_ORDER.compare(id, seen) <= 0) {
                        throw new IllegalStateException("Target inventory page did not advance in key order");
                    }
                    seen = id;
                    final MutableTarget target =
                            grouped.computeIfAbsent(id, ignored -> new MutableTarget(entry.physical()));
                    target.add(new Source(shardId, entry));
                    if (grouped.size() > limits.maximumTargets()) {
                        throw new IllegalStateException("Target inventory exceeds activated Target limit");
                    }
                }
                if (page.stop() == TargetQueueSnapshotReader.Stop.RANGE_END) {
                    complete = true;
                    break;
                }
                if (page.entries().isEmpty() || Objects.equals(after, page.nextAfter())) {
                    return new Result(Stop.READ_BUDGET, null);
                }
                after = page.nextAfter();
            }
            if (!complete) {
                return new Result(Stop.SCAN_LIMIT, null);
            }
        }
        for (ShardSource source : sources) {
            final TargetQueueSnapshotReader.Cut current;
            try {
                current = source.readCut(budgets.get());
            } catch (ReadIncompleteException incomplete) {
                return new Result(Stop.READ_BUDGET, null);
            }
            if (!cuts.get(source.shardId()).equals(current)) {
                return new Result(Stop.CUT_CHANGED, null);
            }
        }
        if (!membershipCurrent.getAsBoolean()) {
            return new Result(Stop.MEMBERSHIP_CHANGED, null);
        }
        final List<Target> targets =
                grouped.values().stream().map(MutableTarget::freeze).toList();
        return new Result(Stop.COMPLETE, new Snapshot(targets, cuts));
    }

    private static long saturatedAdd(final long current, final long increment) {
        return current > Long.MAX_VALUE - increment ? Long.MAX_VALUE : current + increment;
    }

    private static final class ScanCounts {
        private long pages;
        private long records;
        private long bytes;
    }

    private static final class MutableTarget {
        private final CanonicalTargetPartition physical;
        private final List<Source> sources = new ArrayList<>();

        private MutableTarget(final CanonicalTargetPartition physical) {
            this.physical = physical;
        }

        private void add(final Source source) {
            if (!Arrays.equals(
                            physical.canonicalBytes(), source.entry().physical().canonicalBytes())
                    || sources.stream().anyMatch(prior -> prior.shard().equals(source.shard()))) {
                throw new IllegalStateException(
                        "Target inventory has conflicting physical identity or duplicate Shard");
            }
            sources.add(source);
        }

        private Target freeze() {
            return new Target(physical, sources);
        }
    }
}
