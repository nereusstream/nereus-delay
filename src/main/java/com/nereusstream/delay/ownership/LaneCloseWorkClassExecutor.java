package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Bounded GC-class materialization of one source-ordered Lane-close cursor.
 *
 * <p>The Close System Mutation owns the semantic terminal decision and the
 * one-time quota transfer. This executor only advances the durable cursor and
 * terminal/history projections after queue admission; it creates no System
 * Mutation and allocates no Source Position.</p>
 */
public final class LaneCloseWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN = Bytes.utf8("nereus-delay-lane-close-materialization-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;

    public LaneCloseWorkClassExecutor(
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.ownedShard.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Queues one exact close cursor without local Store I/O. */
    public Submission submit(
            final DelayShard.LaneCloseMaterializationWork candidate,
            final int maxRecords,
            final LongSupplier ownerClock) {
        final DelayShard.LaneCloseMaterializationWork submitted = Objects.requireNonNull(candidate, "candidate");
        final LongSupplier clock = Objects.requireNonNull(ownerClock, "ownerClock");
        ownedShard.requireLaneCloseMaterializationSubmission(authority, submitted, maxRecords);
        final byte[] identity = Bytes.concat(
                TASK_ID_DOMAIN,
                submitted.laneId().bytes(),
                submitted.cursor().canonicalBytes(),
                Bytes.u32be(maxRecords));
        final WorkClassTask task =
                new WorkClassTask(WorkClass.GC, "lane-close/" + Bytes.hex(Bytes.sha256(identity)), identity.length);
        final Submission submission = new Submission(task);
        workClasses.submit(task, () -> execute(submitted, maxRecords, clock, submission));
        return submission;
    }

    private void execute(
            final DelayShard.LaneCloseMaterializationWork candidate,
            final int maxRecords,
            final LongSupplier clock,
            final Submission submission) {
        try {
            submission.complete(Outcome.succeeded(
                    ownedShard.materializeLaneCloseAuthoritativelyStrict(authority, candidate, maxRecords, clock)));
        } catch (RuntimeException failure) {
            ownedShard.fence();
            submission.complete(Outcome.failed(failure));
        } catch (Error failure) {
            ownedShard.fence();
            submission.complete(Outcome.failed(failure));
            throw failure;
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private volatile Outcome outcome;

        private Submission(final WorkClassTask task) {
            this.task = Objects.requireNonNull(task, "task");
        }

        public WorkClassTask task() {
            return task;
        }

        public Optional<Outcome> outcome() {
            return Optional.ofNullable(outcome);
        }

        private synchronized void complete(final Outcome completed) {
            if (outcome != null) {
                throw new IllegalStateException("Lane close materialization outcome is already complete");
            }
            outcome = Objects.requireNonNull(completed, "completed");
        }
    }

    public record Outcome(DelayShard.LaneCloseMaterializationExecutionResult result, Throwable failure) {
        public Outcome {
            if ((result == null) == (failure == null)) {
                throw new IllegalArgumentException("Lane close outcome must contain one branch");
            }
        }

        private static Outcome succeeded(final DelayShard.LaneCloseMaterializationExecutionResult result) {
            return new Outcome(Objects.requireNonNull(result, "result"), null);
        }

        private static Outcome failed(final Throwable failure) {
            return new Outcome(null, Objects.requireNonNull(failure, "failure"));
        }
    }
}
