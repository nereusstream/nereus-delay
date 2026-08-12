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
 * Bounded external handoff for an already prepared result System Mutation.
 *
 * <p>The producer of the callback/evidence result owns the operation-specific
 * body and signature.  This executor owns only the process-local queue,
 * strict Owner/Shard fencing and the call to the external Shard Log writer.
 * It never applies the mutation to RocksDB and never allocates a local Source
 * Position; only source-ordered replay may advance the local projection.</p>
 */
public final class OutcomeWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN =
            Bytes.utf8("nereus-delay-outcome-mutation-handoff-task-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;
    private final ShardLogMutationAppender appender;

    public OutcomeWorkClassExecutor(final WorkClassExecutionRegistry workClasses,
                                    final OwnedDelayShard ownedShard,
                                    final OxiaOwnerLeaseStore authority,
                                    final ShardLogMutationAppender appender) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.appender = Objects.requireNonNull(appender, "appender");
    }

    /** Registers one exact signed result mutation for an external append. */
    public Submission submit(final SystemMutation mutation, final LongSupplier ownerClock) {
        final SystemMutation exact = Objects.requireNonNull(mutation, "mutation");
        final LongSupplier clock = Objects.requireNonNull(ownerClock, "ownerClock");
        ownedShard.requireOutcomeMutationSubmission(authority, exact);
        final byte[] frame = exact.encodeFrame();
        final WorkClassTask task = new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL,
                "outcome-mutation/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, frame)), frame.length);
        final Submission submission = new Submission(task, exact);
        workClasses.submit(task, () -> execute(exact, clock, submission));
        return submission;
    }

    private void execute(final SystemMutation mutation, final LongSupplier ownerClock,
                         final Submission submission) {
        try {
            ownedShard.requireOutcomeMutationAuthoritativelyStrict(authority, mutation, ownerClock);
            final ShardLogMutationAppender.AppendOutcome appended = Objects.requireNonNull(
                    appender.append(mutation), "Shard Log append outcome");
            switch (appended.disposition()) {
                case PERSISTED -> {
                    ownedShard.requireCurrentShardLogPosition(appended.sourcePosition(), mutation.shardId(),
                            appended.sourceConnectionGeneration(), appended.guardAttestationDigest());
                    submission.complete(OutcomeHandoffResult.persisted(mutation, appended.sourcePosition()));
                }
                case DEFINITIVELY_NOT_PERSISTED -> submission.complete(
                        OutcomeHandoffResult.definitelyNotPersisted(mutation));
                case UNKNOWN -> submission.complete(OutcomeHandoffResult.unknown(mutation, null));
            }
        } catch (RuntimeException failure) {
            // A writer exception or a failed source-position proof does not
            // prove non-persistence. Retain the exact bytes for recovery.
            ownedShard.fence();
            submission.complete(OutcomeHandoffResult.unknown(mutation, failure));
        } catch (Error failure) {
            ownedShard.fence();
            submission.complete(OutcomeHandoffResult.unknown(mutation, failure));
            throw failure;
        }
    }

    public enum ResultKind {
        PERSISTED,
        DEFINITIVELY_NOT_PERSISTED,
        UNKNOWN
    }

    public record OutcomeHandoffResult(ResultKind kind, SystemMutation mutation,
                                       SourcePosition sourcePosition, Throwable failure) {
        public OutcomeHandoffResult {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(mutation, "mutation");
            if ((kind == ResultKind.PERSISTED) != (sourcePosition != null)) {
                throw new IllegalArgumentException("only persisted outcome handoffs carry a Source Position");
            }
            if (kind != ResultKind.UNKNOWN && failure != null) {
                throw new IllegalArgumentException("only UNKNOWN outcome handoffs carry failure evidence");
            }
        }

        private static OutcomeHandoffResult persisted(final SystemMutation mutation,
                                                      final SourcePosition position) {
            return new OutcomeHandoffResult(ResultKind.PERSISTED, mutation,
                    Objects.requireNonNull(position, "position"), null);
        }

        private static OutcomeHandoffResult definitelyNotPersisted(final SystemMutation mutation) {
            return new OutcomeHandoffResult(ResultKind.DEFINITIVELY_NOT_PERSISTED, mutation, null, null);
        }

        private static OutcomeHandoffResult unknown(final SystemMutation mutation, final Throwable failure) {
            return new OutcomeHandoffResult(ResultKind.UNKNOWN, mutation, null, failure);
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private final SystemMutation mutation;
        private volatile OutcomeHandoffResult result;

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

        public Optional<OutcomeHandoffResult> result() {
            return Optional.ofNullable(result);
        }

        private synchronized void complete(final OutcomeHandoffResult completed) {
            if (result != null) {
                throw new IllegalStateException("outcome mutation handoff already completed");
            }
            result = Objects.requireNonNull(completed, "completed");
        }
    }
}
