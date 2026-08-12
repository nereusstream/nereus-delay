package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;

import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Bounded external handoff for an exact control-plane System Mutation.
 * Control registration/authorization and source-ordered state application
 * remain separate; this class only owns the queue and external append fence.
 */
public final class ControlWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN =
            Bytes.utf8("nereus-delay-control-mutation-handoff-task-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;
    private final ShardLogMutationAppender appender;

    public ControlWorkClassExecutor(final WorkClassExecutionRegistry workClasses,
                                    final OwnedDelayShard ownedShard,
                                    final OxiaOwnerLeaseStore authority,
                                    final ShardLogMutationAppender appender) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.appender = Objects.requireNonNull(appender, "appender");
        this.ownedShard.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Registers one exact signed control mutation for external append. */
    public Submission submit(final SystemMutation mutation, final LongSupplier ownerClock) {
        final SystemMutation exact = Objects.requireNonNull(mutation, "mutation");
        final LongSupplier clock = Objects.requireNonNull(ownerClock, "ownerClock");
        ownedShard.requireControlMutationSubmission(authority, exact);
        final byte[] frame = exact.encodeFrame();
        final WorkClassTask task = new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL,
                "control-mutation/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, frame)), frame.length);
        final Submission submission = new Submission(task, exact);
        workClasses.submit(task, () -> execute(exact, clock, submission));
        return submission;
    }

    private void execute(final SystemMutation mutation, final LongSupplier ownerClock,
                         final Submission submission) {
        try {
            ownedShard.requireControlMutationAuthoritativelyStrict(authority, mutation, ownerClock);
            final ShardLogMutationAppender.AppendOutcome appended = Objects.requireNonNull(
                    appender.append(mutation), "Shard Log append outcome");
            switch (appended.disposition()) {
                case PERSISTED -> {
                    ownedShard.requireCurrentShardLogPosition(appended.sourcePosition(), mutation.shardId(),
                            appended.sourceConnectionGeneration(), appended.guardAttestationDigest());
                    submission.complete(ControlHandoffResult.persisted(mutation, appended.sourcePosition()));
                }
                case DEFINITIVELY_NOT_PERSISTED -> submission.complete(
                        ControlHandoffResult.definitelyNotPersisted(mutation));
                case UNKNOWN -> submission.complete(ControlHandoffResult.unknown(mutation, null));
            }
        } catch (RuntimeException failure) {
            ownedShard.fence();
            submission.complete(ControlHandoffResult.unknown(mutation, failure));
        } catch (Error failure) {
            ownedShard.fence();
            submission.complete(ControlHandoffResult.unknown(mutation, failure));
            throw failure;
        }
    }

    public enum ResultKind {
        PERSISTED,
        DEFINITIVELY_NOT_PERSISTED,
        UNKNOWN
    }

    public record ControlHandoffResult(ResultKind kind, SystemMutation mutation,
                                       SourcePosition sourcePosition, Throwable failure) {
        public ControlHandoffResult {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(mutation, "mutation");
            if ((kind == ResultKind.PERSISTED) != (sourcePosition != null)) {
                throw new IllegalArgumentException("only persisted control handoffs carry a Source Position");
            }
            if (kind != ResultKind.UNKNOWN && failure != null) {
                throw new IllegalArgumentException("only UNKNOWN control handoffs carry failure evidence");
            }
        }

        private static ControlHandoffResult persisted(final SystemMutation mutation, final SourcePosition position) {
            return new ControlHandoffResult(ResultKind.PERSISTED, mutation,
                    Objects.requireNonNull(position, "position"), null);
        }

        private static ControlHandoffResult definitelyNotPersisted(final SystemMutation mutation) {
            return new ControlHandoffResult(ResultKind.DEFINITIVELY_NOT_PERSISTED, mutation, null, null);
        }

        private static ControlHandoffResult unknown(final SystemMutation mutation, final Throwable failure) {
            return new ControlHandoffResult(ResultKind.UNKNOWN, mutation, null, failure);
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private final SystemMutation mutation;
        private volatile ControlHandoffResult result;

        private Submission(final WorkClassTask task, final SystemMutation mutation) {
            this.task = Objects.requireNonNull(task, "task");
            this.mutation = Objects.requireNonNull(mutation, "mutation");
        }

        public WorkClassTask task() {
            return task;
        }

        public SystemMutation mutation() {
            return mutation;
        }

        public Optional<ControlHandoffResult> result() {
            return Optional.ofNullable(result);
        }

        private synchronized void complete(final ControlHandoffResult completed) {
            if (result != null) {
                throw new IllegalStateException("control mutation handoff already completed");
            }
            result = Objects.requireNonNull(completed, "completed");
        }
    }
}
