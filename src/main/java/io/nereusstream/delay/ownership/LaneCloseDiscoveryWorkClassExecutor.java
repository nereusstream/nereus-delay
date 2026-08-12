package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/** Active-owner entrypoint for one bounded Lane-close cursor discovery turn. */
public final class LaneCloseDiscoveryWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN =
            Bytes.utf8("nereus-delay-lane-close-discovery-task-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;

    public LaneCloseDiscoveryWorkClassExecutor(final WorkClassExecutionRegistry workClasses,
                                               final OwnedDelayShard ownedShard,
                                               final OxiaOwnerLeaseStore authority) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
    }

    /** Registers discovery without reading Oxia, clocks or RocksDB. */
    public Submission submit(final SchedulerBudget scanBudget,
                             final LongSupplier ownerClock,
                             final LongSupplier scanClockNanos) {
        final SchedulerBudget submittedBudget = Objects.requireNonNull(scanBudget, "scanBudget");
        final LongSupplier submittedOwnerClock = Objects.requireNonNull(ownerClock, "ownerClock");
        final LongSupplier submittedScanClock = Objects.requireNonNull(scanClockNanos, "scanClockNanos");
        ownedShard.requireLaneCloseDiscoverySubmission(authority);
        final ShardId shardId = ownedShard.shard().shardId();
        final byte[] identity = Bytes.concat(TASK_ID_DOMAIN,
                shardId.routeIncarnation().bytes(), Bytes.u32beBits(shardId.partition()),
                Bytes.u32be(submittedBudget.maxMessages()), Bytes.u64be(submittedBudget.maxBytes()),
                Bytes.u64be(submittedBudget.maxElapsedNanos()));
        final long chargedBytes = Math.addExact(identity.length, submittedBudget.maxBytes());
        final WorkClassTask task = new WorkClassTask(WorkClass.GC,
                "lane-close-discovery/" + Bytes.hex(Bytes.sha256(identity)), chargedBytes);
        final Submission submission = new Submission(task);
        workClasses.submit(task, () -> submission.complete(
                ownedShard.discoverLaneCloseAuthoritativelyStrict(authority, submittedBudget,
                        submittedOwnerClock, submittedScanClock)));
        return submission;
    }

    /** Read-only process-local handle for one queued discovery action. */
    public static final class Submission {
        private final WorkClassTask task;
        private volatile List<DelayShard.LaneCloseMaterializationWork> discovered;

        private Submission(final WorkClassTask task) {
            this.task = Objects.requireNonNull(task, "task");
        }

        public WorkClassTask task() {
            return task;
        }

        public Optional<List<DelayShard.LaneCloseMaterializationWork>> discovered() {
            return Optional.ofNullable(discovered);
        }

        private synchronized void complete(final List<DelayShard.LaneCloseMaterializationWork> result) {
            if (discovered != null) {
                throw new IllegalStateException("Lane close discovery already completed");
            }
            discovered = List.copyOf(Objects.requireNonNull(result, "result"));
        }
    }
}
