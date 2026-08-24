package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/** Active-owner entrypoint for one bounded EXPIRY-index discovery turn. */
public final class ExpiryDiscoveryWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN = Bytes.utf8("nereus-delay-expiry-discovery-task-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;

    public ExpiryDiscoveryWorkClassExecutor(
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.ownedShard.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Registers a discovery action without reading Oxia, clocks or RocksDB. */
    public Submission submit(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget scanBudget,
            final LongSupplier ownerClock,
            final LongSupplier scanClockNanos) {
        final TrustedUtcIntervalEvidence submittedEvidence = Objects.requireNonNull(evidence, "evidence");
        final SchedulerBudget submittedBudget = Objects.requireNonNull(scanBudget, "scanBudget");
        final LongSupplier submittedOwnerClock = Objects.requireNonNull(ownerClock, "ownerClock");
        final LongSupplier submittedScanClock = Objects.requireNonNull(scanClockNanos, "scanClockNanos");
        ownedShard.requireExpiryDiscoverySubmission(authority);
        final byte[] evidenceBytes = submittedEvidence.canonicalBytes();
        final ShardId shardId = ownedShard.shard().shardId();
        final byte[] identity = Bytes.concat(
                TASK_ID_DOMAIN,
                shardId.routeIncarnation().bytes(),
                Bytes.u32beBits(shardId.partition()),
                Bytes.lp32(evidenceBytes),
                Bytes.u32be(submittedBudget.maxMessages()),
                Bytes.u64be(submittedBudget.maxBytes()),
                Bytes.u64be(submittedBudget.maxElapsedNanos()));
        final long chargedBytes = Math.addExact(identity.length, submittedBudget.maxBytes());
        final WorkClassTask task = new WorkClassTask(
                WorkClass.EXPIRY, "expiry-discovery/" + Bytes.hex(Bytes.sha256(identity)), chargedBytes);
        final Submission submission = new Submission(task);
        workClasses.submit(
                task,
                () -> submission.complete(ownedShard.discoverExpiryAuthoritativelyStrict(
                        authority, submittedEvidence, submittedBudget, submittedOwnerClock, submittedScanClock)));
        return submission;
    }

    /** Read-only process-local handle for one queued discovery action. */
    public static final class Submission {
        private final WorkClassTask task;
        private volatile List<DelayShard.ExpiryWork> discovered;

        private Submission(final WorkClassTask task) {
            this.task = Objects.requireNonNull(task, "task");
        }

        public WorkClassTask task() {
            return task;
        }

        public Optional<List<DelayShard.ExpiryWork>> discovered() {
            return Optional.ofNullable(discovered);
        }

        private synchronized void complete(final List<DelayShard.ExpiryWork> result) {
            if (discovered != null) {
                throw new IllegalStateException("expiry discovery already completed");
            }
            discovered = List.copyOf(Objects.requireNonNull(result, "result"));
        }
    }
}
