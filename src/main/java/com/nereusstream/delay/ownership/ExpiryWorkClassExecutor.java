package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.OwnerIdentityV1;
import com.nereusstream.delay.protocol.ShardSubjectV1;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.security.PrivateKey;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Bounded expiry-scanner handoff to the external Shard Log.
 *
 * <p>This class prepares and signs one exact {@code EXPIRE_GENERATION_V1}
 * mutation before queue admission.  It never applies the mutation locally or
 * allocates a Source Position.  The source-ordered apply path remains the only
 * authority that can transition a generation to EXPIRED.</p>
 */
public final class ExpiryWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN = Bytes.utf8("nereus-delay-expiry-handoff-task-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;
    private final ShardLogMutationAppender appender;

    public ExpiryWorkClassExecutor(
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority,
            final ShardLogMutationAppender appender) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.appender = Objects.requireNonNull(appender, "appender");
        this.ownedShard.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Prepares one exact expiry mutation and registers its bounded append action. */
    public Submission submit(
            final DelayShard.ExpiryWork candidate,
            final TrustedUtcIntervalEvidence evidence,
            final long retryUntilEpochMs,
            final OwnerIdentityV1 owner,
            final int signingKeyVersion,
            final PrivateKey signingKey,
            final LongSupplier ownerClock) {
        final Request request = Request.prepare(
                candidate, evidence, retryUntilEpochMs, owner, signingKeyVersion, signingKey, ownerClock);
        ownedShard.requireExpirySubmission(authority, request.candidate, request.evidence, request.owner);
        final byte[] frame = request.mutation.encodeFrame();
        final WorkClassTask task = new WorkClassTask(
                WorkClass.EXPIRY, "expiry-handoff/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, frame)), frame.length);
        final Submission submission = new Submission(task, request.mutation);
        workClasses.submit(task, () -> execute(request, submission));
        return submission;
    }

    private void execute(final Request request, final Submission submission) {
        try {
            ownedShard.requireExpiryAuthoritativelyStrict(
                    authority, request.candidate, request.evidence, request.owner, request.ownerClock);
            final ShardLogMutationAppender.AppendOutcome appended =
                    Objects.requireNonNull(appender.append(request.mutation), "Shard Log append outcome");
            switch (appended.disposition()) {
                case PERSISTED -> {
                    ownedShard.requireCurrentShardLogPosition(
                            appended.sourcePosition(),
                            request.mutation.shardId(),
                            appended.sourceConnectionGeneration(),
                            appended.guardAttestationDigest());
                    submission.complete(ExpiryHandoffResult.enqueued(request.mutation, appended.sourcePosition()));
                }
                case DEFINITIVELY_NOT_PERSISTED ->
                    submission.complete(ExpiryHandoffResult.notEnqueued(request.mutation));
                case UNKNOWN -> submission.complete(ExpiryHandoffResult.unknown(request.mutation, null));
            }
        } catch (RuntimeException failure) {
            ownedShard.fence();
            submission.complete(ExpiryHandoffResult.unknown(request.mutation, failure));
        } catch (Error failure) {
            ownedShard.fence();
            submission.complete(ExpiryHandoffResult.unknown(request.mutation, failure));
            throw failure;
        }
    }

    private static final class Request {
        private final DelayShard.ExpiryWork candidate;
        private final TrustedUtcIntervalEvidence evidence;
        private final OwnerIdentityV1 owner;
        private final SystemMutation mutation;
        private final LongSupplier ownerClock;

        private Request(
                final DelayShard.ExpiryWork candidate,
                final TrustedUtcIntervalEvidence evidence,
                final OwnerIdentityV1 owner,
                final SystemMutation mutation,
                final LongSupplier ownerClock) {
            this.candidate = candidate;
            this.evidence = evidence;
            this.owner = owner;
            this.mutation = mutation;
            this.ownerClock = ownerClock;
        }

        private static Request prepare(
                final DelayShard.ExpiryWork candidate,
                final TrustedUtcIntervalEvidence evidence,
                final long retryUntilEpochMs,
                final OwnerIdentityV1 owner,
                final int signingKeyVersion,
                final PrivateKey signingKey,
                final LongSupplier ownerClock) {
            final DelayShard.ExpiryWork work = Objects.requireNonNull(candidate, "candidate");
            final TrustedUtcIntervalEvidence trusted = Objects.requireNonNull(evidence, "evidence");
            final OwnerIdentityV1 typedOwner = Objects.requireNonNull(owner, "owner");
            final PrivateKey key = Objects.requireNonNull(signingKey, "signingKey");
            final LongSupplier clock = Objects.requireNonNull(ownerClock, "ownerClock");
            final byte[] author = AuthorIdentity.owner(
                            typedOwner.deploymentId(),
                            typedOwner.workerRunId(),
                            typedOwner.ownerEpoch(),
                            typedOwner.leaseFencingDigest())
                    .canonicalBytes();
            final com.nereusstream.delay.protocol.ShardId shard =
                    work.messageId().routingId().shardId();
            final byte[] body = body(shard, work, retryUntilEpochMs, trusted);
            final SystemMutation mutation = SystemMutation.signed(
                    shard,
                    SystemMutationType.EXPIRE_GENERATION,
                    retryUntilEpochMs,
                    SystemMutation.computeExpiryLogicalIdentity(
                            work.messageId(), work.generation(), work.expireAtEpochMs()),
                    body,
                    author,
                    signingKeyVersion,
                    key);
            return new Request(work, trusted, typedOwner, mutation, clock);
        }

        private static byte[] body(
                final com.nereusstream.delay.protocol.ShardId shard,
                final DelayShard.ExpiryWork work,
                final long retryUntilEpochMs,
                final TrustedUtcIntervalEvidence evidence) {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, new ShardSubjectV1(shard).canonicalBytes());
                CanonicalProtobuf.uint32(output, 2, SystemMutationType.EXPIRE_GENERATION.wireValue());
                CanonicalProtobuf.int64(output, 3, retryUntilEpochMs);
                CanonicalProtobuf.bytes(output, 10, work.messageId().bytes());
                CanonicalProtobuf.uint32(output, 11, Integer.toUnsignedLong(work.generation()));
                CanonicalProtobuf.int64(output, 12, work.expireAtEpochMs());
                CanonicalProtobuf.bytes(output, 13, evidence.canonicalBytes());
            });
        }
    }

    public enum ResultKind {
        ENQUEUED,
        DEFINITIVELY_NOT_ENQUEUED,
        UNKNOWN
    }

    public record ExpiryHandoffResult(
            ResultKind kind, SystemMutation mutation, SourcePosition sourcePosition, Throwable failure) {
        public ExpiryHandoffResult {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(mutation, "mutation");
            if ((kind == ResultKind.ENQUEUED) != (sourcePosition != null)) {
                throw new IllegalArgumentException("only enqueued expiry carries a Source Position");
            }
            if (kind != ResultKind.UNKNOWN && failure != null) {
                throw new IllegalArgumentException("only unknown expiry carries failure evidence");
            }
        }

        private static ExpiryHandoffResult enqueued(final SystemMutation mutation, final SourcePosition position) {
            return new ExpiryHandoffResult(
                    ResultKind.ENQUEUED, mutation, Objects.requireNonNull(position, "position"), null);
        }

        private static ExpiryHandoffResult notEnqueued(final SystemMutation mutation) {
            return new ExpiryHandoffResult(ResultKind.DEFINITIVELY_NOT_ENQUEUED, mutation, null, null);
        }

        private static ExpiryHandoffResult unknown(final SystemMutation mutation, final Throwable failure) {
            return new ExpiryHandoffResult(ResultKind.UNKNOWN, mutation, null, failure);
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private final SystemMutation mutation;
        private volatile ExpiryHandoffResult result;

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

        public Optional<ExpiryHandoffResult> result() {
            return Optional.ofNullable(result);
        }

        private synchronized void complete(final ExpiryHandoffResult completed) {
            if (result != null) {
                throw new IllegalStateException("expiry handoff already completed");
            }
            result = Objects.requireNonNull(completed, "completed");
        }
    }
}
