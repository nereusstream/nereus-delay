package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;
import io.nereusstream.delay.runtime.ResourceRetireIntentRecord;

import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Bounded external handoff for an exact resource-retirement System Mutation.
 *
 * <p>The GC/provider worker prepares and signs the operation-specific bytes;
 * this class owns only the {@link WorkClass#GC} queue, strict owner fencing and
 * the external Shard Log append. It never performs a provider delete, writes a
 * local GC tombstone, applies the mutation or allocates a Source Position.</p>
 */
public final class GcWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN =
            Bytes.utf8("nereus-delay-gc-mutation-handoff-task-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;
    private final ShardLogMutationAppender appender;

    public GcWorkClassExecutor(final WorkClassExecutionRegistry workClasses,
                               final OwnedDelayShard ownedShard,
                               final OxiaOwnerLeaseStore authority,
                               final ShardLogMutationAppender appender) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.appender = Objects.requireNonNull(appender, "appender");
        this.ownedShard.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Registers one exact signed retire/delete-confirmation mutation. */
    public Submission submit(final SystemMutation mutation, final LongSupplier ownerClock) {
        final SystemMutation exact = Objects.requireNonNull(mutation, "mutation");
        final LongSupplier clock = Objects.requireNonNull(ownerClock, "ownerClock");
        return submitInternal(exact, null, clock);
    }

    /**
     * Registers a delete confirmation while retaining the exact retire Source
     * Position needed to interpret a persisted append as source-ordered.
     *
     * <p>The retire record is a caller-supplied durable projection; this
     * method does not read or mutate local GC state. It only binds the
     * confirmation intent fields to that projection and rejects an appender
     * result that is not strictly later in the same physical source.</p>
     */
    public Submission submitDeleteConfirmation(final SystemMutation mutation,
                                               final ResourceRetireIntentRecord retireIntent,
                                               final LongSupplier ownerClock) {
        final SystemMutation exact = Objects.requireNonNull(mutation, "mutation");
        final ResourceRetireIntentRecord intent = Objects.requireNonNull(retireIntent, "retireIntent");
        final SourcePosition predecessor = validateDeleteConfirmationPredecessor(exact, intent);
        return submitInternal(exact, predecessor, Objects.requireNonNull(ownerClock, "ownerClock"));
    }

    private Submission submitInternal(final SystemMutation exact, final SourcePosition predecessor,
                                      final LongSupplier clock) {
        ownedShard.requireGcMutationSubmission(authority, exact);
        final byte[] frame = exact.encodeFrame();
        final WorkClassTask task = new WorkClassTask(WorkClass.GC,
                "gc-mutation/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, frame)), frame.length);
        final Submission submission = new Submission(task, exact);
        workClasses.submit(task, () -> execute(exact, predecessor, clock, submission));
        return submission;
    }

    private void execute(final SystemMutation mutation, final SourcePosition predecessor,
                         final LongSupplier ownerClock,
                         final Submission submission) {
        try {
            ownedShard.requireGcMutationAuthoritativelyStrict(authority, mutation, ownerClock);
            final ShardLogMutationAppender.AppendOutcome appended = Objects.requireNonNull(
                    appender.append(mutation), "Shard Log append outcome");
            switch (appended.disposition()) {
                case PERSISTED -> {
                    ownedShard.requireCurrentShardLogPosition(appended.sourcePosition(), mutation.shardId(),
                            appended.sourceConnectionGeneration(), appended.guardAttestationDigest());
                    if (predecessor != null) {
                        requireStrictlyAfter(predecessor, appended.sourcePosition());
                    }
                    submission.complete(GcHandoffResult.persisted(mutation, appended.sourcePosition()));
                }
                case DEFINITIVELY_NOT_PERSISTED -> submission.complete(
                        GcHandoffResult.definitelyNotPersisted(mutation));
                case UNKNOWN -> submission.complete(GcHandoffResult.unknown(mutation, null));
            }
        } catch (RuntimeException failure) {
            ownedShard.fence();
            submission.complete(GcHandoffResult.unknown(mutation, failure));
        } catch (Error failure) {
            ownedShard.fence();
            submission.complete(GcHandoffResult.unknown(mutation, failure));
            throw failure;
        }
    }

    private static SourcePosition validateDeleteConfirmationPredecessor(
            final SystemMutation mutation, final ResourceRetireIntentRecord retireIntent) {
        if (mutation.type() != SystemMutationType.RESOURCE_DELETE_CONFIRMED) {
            throw new IllegalArgumentException("typed delete confirmation handoff requires a delete confirmation");
        }
        final ResourceDeleteConfirmedBody.RetireIntentRef reference =
                ResourceDeleteConfirmedBody.decode(mutation.canonicalBody()).intent();
        if (!Bytes.constantTimeEquals(reference.mutationId(), retireIntent.mutationId())
                || !Bytes.constantTimeEquals(reference.mutationHash(), retireIntent.mutationHash())
                || !Bytes.constantTimeEquals(reference.resourceIdentityHash(), retireIntent.resourceIdentityHash())
                || reference.expectedResourceStateVersion() != retireIntent.expectedResourceStateVersion()) {
            throw new IllegalArgumentException("delete confirmation does not bind the exact retire intent");
        }
        final SourcePosition predecessor = SourcePositionCodec.decode(retireIntent.appliedSourcePosition());
        if (!mutation.shardId().equals(predecessor.shardId())) {
            throw new IllegalArgumentException("retire intent Source Position belongs to another shard");
        }
        return predecessor;
    }

    private static void requireStrictlyAfter(final SourcePosition predecessor, final SourcePosition persisted) {
        if (!predecessor.shardId().equals(persisted.shardId())
                || !predecessor.sameSourceIdentity(persisted)
                || persisted.compareTo(predecessor) <= 0) {
            throw new IllegalStateException("delete confirmation append did not follow the retire Source Position");
        }
    }

    public enum ResultKind {
        PERSISTED,
        DEFINITIVELY_NOT_PERSISTED,
        UNKNOWN
    }

    public record GcHandoffResult(ResultKind kind, SystemMutation mutation,
                                  SourcePosition sourcePosition, Throwable failure) {
        public GcHandoffResult {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(mutation, "mutation");
            if ((kind == ResultKind.PERSISTED) != (sourcePosition != null)) {
                throw new IllegalArgumentException("only persisted GC handoffs carry a Source Position");
            }
            if (kind != ResultKind.UNKNOWN && failure != null) {
                throw new IllegalArgumentException("only UNKNOWN GC handoffs carry failure evidence");
            }
        }

        private static GcHandoffResult persisted(final SystemMutation mutation, final SourcePosition position) {
            return new GcHandoffResult(ResultKind.PERSISTED, mutation,
                    Objects.requireNonNull(position, "position"), null);
        }

        private static GcHandoffResult definitelyNotPersisted(final SystemMutation mutation) {
            return new GcHandoffResult(ResultKind.DEFINITIVELY_NOT_PERSISTED, mutation, null, null);
        }

        private static GcHandoffResult unknown(final SystemMutation mutation, final Throwable failure) {
            return new GcHandoffResult(ResultKind.UNKNOWN, mutation, null, failure);
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private final SystemMutation mutation;
        private volatile GcHandoffResult result;

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

        public Optional<GcHandoffResult> result() {
            return Optional.ofNullable(result);
        }

        private synchronized void complete(final GcHandoffResult completed) {
            if (result != null) {
                throw new IllegalStateException("GC mutation handoff already completed");
            }
            result = Objects.requireNonNull(completed, "completed");
        }
    }
}
