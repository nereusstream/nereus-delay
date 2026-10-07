package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/** Bounded Target source handoff. It retains exact mutation bytes; append success is not local application. */
public final class TargetOutcomeWorkClassExecutor {
    private final TargetWorkerShardRuntime worker;
    private final ShardLogMutationAppender appender;

    public TargetOutcomeWorkClassExecutor(TargetWorkerShardRuntime worker, ShardLogMutationAppender appender) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.appender = Objects.requireNonNull(appender, "appender");
    }

    public Submission submit(final SystemMutation mutation, final LongSupplier ownerClock) {
        final var image = Objects.requireNonNull(mutation, "mutation");
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        final var task = new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL,
                "target-outcome/" + Bytes.hex(Bytes.sha256(image.encodeFrame())), image.encodeFrame().length);
        final var submission = new Submission(image);
        worker.submitOutcomeAction(task, image, clock, () -> {
            try {
                submission.append = worker.appendOutcome(image, appender, clock);
            } catch (RuntimeException failure) {
                worker.fenceMessageExpiry();
                submission.failure = failure;
                submission.append = ShardLogMutationAppender.AppendOutcome.unknown();
            } catch (Error fatal) {
                worker.fenceMessageExpiry();
                submission.failure = fatal;
                submission.append = ShardLogMutationAppender.AppendOutcome.unknown();
                throw fatal;
            }
        });
        return submission;
    }

    public Optional<SystemMutationResult> readApplied(final SystemMutation mutation, final LongSupplier ownerClock) {
        return worker.outcomeMutationResult(mutation, ownerClock);
    }

    public static final class Submission {
        private final SystemMutation mutation;
        private volatile ShardLogMutationAppender.AppendOutcome append;
        private volatile Throwable failure;

        private Submission(SystemMutation mutation) { this.mutation = mutation; }

        public SystemMutation mutation() { return mutation; }
        public Optional<ShardLogMutationAppender.AppendOutcome> append() { return Optional.ofNullable(append); }
        public Optional<Throwable> failure() { return Optional.ofNullable(failure); }
    }
}
