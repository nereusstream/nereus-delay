package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetExpireGenerationBody;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetExpiryDiscoveryStore;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.security.PrivateKey;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/** Appends one typed Target message expiry mutation through the bounded EXPIRY work class. */
public final class TargetMessageExpiryWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN = Bytes.utf8("nereus-delay-target-expiry-handoff-task\0");

    private final TargetWorkerShardRuntime worker;
    private final ShardLogMutationAppender appender;
    private Submission pending;

    TargetMessageExpiryWorkClassExecutor(
            final TargetWorkerShardRuntime worker, final ShardLogMutationAppender appender) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.appender = Objects.requireNonNull(appender, "appender");
    }

    /**
     * Prepares one exact source mutation before queue admission; it never applies locally. When a
     * previous append is confirmed applied, this call only settles that submission so the caller
     * can rediscover a fresh candidate before queueing another mutation.
     */
    public synchronized Submission submit(
            final TargetExpiryDiscoveryStore.Candidate candidate,
            final TrustedUtcIntervalEvidence evidence,
            final long retryUntilEpochMs,
            final OwnerIdentity owner,
            final int signingKeyVersion,
            final PrivateKey signingKey,
            final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        if (pending != null) {
            if (pending.result == null) {
                throw new IllegalStateException("Target message expiry already has an outstanding WorkClass action");
            }
            final var prior = pending;
            final ResultKind priorKind = prior.result.kind();
            if (priorKind == ResultKind.APPENDED || priorKind == ResultKind.UNKNOWN) {
                if (priorKind == ResultKind.APPENDED
                        && !worker.messageExpiryAppendApplied(prior.result.sourcePosition(), clock)) {
                    throw new IllegalStateException("previous Target expiry source append has not been applied");
                }
                final SystemMutationResult applied = worker
                        .messageExpiryMutationResult(prior.mutation, clock)
                        .orElse(null);
                if (applied == null) {
                    throw new IllegalStateException(priorKind == ResultKind.UNKNOWN
                            ? "previous Target expiry append outcome remains unknown"
                            : "previous Target expiry source result is absent");
                }
                if (applied.applyStatus() != ApplyStatus.APPLIED || applied.stableCode() != StableCode.OK) {
                    throw new IllegalStateException("previous Target expiry mutation did not apply successfully");
                }
                prior.confirmApplied();
                pending = null;
                return prior;
            }
            if (priorKind != ResultKind.DEFINITIVELY_NOT_APPENDED) {
                throw new IllegalStateException("Target expiry handoff has an invalid pending result");
            }
            pending = null;
        }

        final Request request = Request.prepare(
                candidate, evidence, retryUntilEpochMs, owner, signingKeyVersion, signingKey, clock);
        final byte[] frame = request.mutation.encodeFrame();
        final WorkClassTask task = new WorkClassTask(
                WorkClass.EXPIRY,
                "target-expiry-handoff/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, frame)),
                frame.length);
        final Submission submission = new Submission(task, request.mutation);
        pending = submission;
        try {
            worker.submitMessageExpiryAction(
                    task,
                    request.candidate,
                    request.evidence,
                    request.owner,
                    clock,
                    () -> execute(request, submission));
        } catch (RuntimeException | Error failure) {
            pending = null;
            throw failure;
        }
        return submission;
    }

    private void execute(final Request request, final Submission submission) {
        try {
            final ShardLogMutationAppender.AppendOutcome appended = worker.appendMessageExpiry(
                    request.candidate, request.evidence, request.owner, request.mutation, appender, request.ownerClock);
            switch (appended.disposition()) {
                case PERSISTED -> {
                    submission.complete(HandoffResult.appended(request.mutation, appended.sourcePosition()));
                }
                case DEFINITIVELY_NOT_PERSISTED ->
                    submission.complete(HandoffResult.notAppended(request.mutation));
                case UNKNOWN -> submission.complete(HandoffResult.unknown(request.mutation, null));
            }
        } catch (RuntimeException failure) {
            worker.fenceMessageExpiry();
            submission.complete(HandoffResult.unknown(request.mutation, failure));
        } catch (Error failure) {
            worker.fenceMessageExpiry();
            submission.complete(HandoffResult.unknown(request.mutation, failure));
            throw failure;
        }
    }

    private static final class Request {
        private final TargetExpiryDiscoveryStore.Candidate candidate;
        private final TrustedUtcIntervalEvidence evidence;
        private final OwnerIdentity owner;
        private final SystemMutation mutation;
        private final LongSupplier ownerClock;

        private Request(
                final TargetExpiryDiscoveryStore.Candidate candidate,
                final TrustedUtcIntervalEvidence evidence,
                final OwnerIdentity owner,
                final SystemMutation mutation,
                final LongSupplier ownerClock) {
            this.candidate = candidate;
            this.evidence = evidence;
            this.owner = owner;
            this.mutation = mutation;
            this.ownerClock = ownerClock;
        }

        private static Request prepare(
                final TargetExpiryDiscoveryStore.Candidate candidate,
                final TrustedUtcIntervalEvidence evidence,
                final long retryUntilEpochMs,
                final OwnerIdentity owner,
                final int signingKeyVersion,
                final PrivateKey signingKey,
                final LongSupplier ownerClock) {
            final var work = Objects.requireNonNull(candidate, "candidate");
            final var proof = Objects.requireNonNull(evidence, "evidence");
            final var author = Objects.requireNonNull(owner, "owner");
            final var key = Objects.requireNonNull(signingKey, "signingKey");
            final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
            proof.requireEarliestAtLeast(work.expireAtEpochMs());
            final var locator = work.locator();
            final var body = new TargetExpireGenerationBody(
                    locator.messageId().routingId().shardId(),
                    retryUntilEpochMs,
                    locator.messageId(),
                    locator.generation(),
                    work.expireAtEpochMs(),
                    proof);
            final var mutation = SystemMutation.signed(
                    body.shard(),
                    SystemMutationType.EXPIRE_GENERATION,
                    retryUntilEpochMs,
                    body.logicalOperationIdentity(),
                    body.canonicalBytes(),
                    AuthorIdentity.owner(
                                    author.deploymentId(),
                                    author.workerRunId(),
                                    author.ownerEpoch(),
                                    author.leaseFencingDigest())
                            .canonicalBytes(),
                    signingKeyVersion,
                    key);
            return new Request(work, proof, author, mutation, clock);
        }
    }

    public enum ResultKind {
        APPENDED,
        APPLIED,
        DEFINITIVELY_NOT_APPENDED,
        UNKNOWN
    }

    public record HandoffResult(
            ResultKind kind, SystemMutation mutation, SourcePosition sourcePosition, Throwable failure) {
        public HandoffResult {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(mutation, "mutation");
            if ((kind == ResultKind.APPENDED) != (sourcePosition != null)) {
                throw new IllegalArgumentException("only an appended Target expiry carries a Source Position");
            }
            if (kind != ResultKind.UNKNOWN && failure != null) {
                throw new IllegalArgumentException("only an unknown Target expiry carries failure evidence");
            }
        }

        private static HandoffResult appended(final SystemMutation mutation, final SourcePosition position) {
            return new HandoffResult(ResultKind.APPENDED, mutation, Objects.requireNonNull(position, "position"), null);
        }

        private static HandoffResult applied(final SystemMutation mutation) {
            return new HandoffResult(ResultKind.APPLIED, Objects.requireNonNull(mutation, "mutation"), null, null);
        }

        private static HandoffResult notAppended(final SystemMutation mutation) {
            return new HandoffResult(ResultKind.DEFINITIVELY_NOT_APPENDED, mutation, null, null);
        }

        private static HandoffResult unknown(final SystemMutation mutation, final Throwable failure) {
            return new HandoffResult(ResultKind.UNKNOWN, mutation, null, failure);
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private final SystemMutation mutation;
        private volatile HandoffResult result;

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

        public Optional<HandoffResult> result() {
            return Optional.ofNullable(result);
        }

        private synchronized void complete(final HandoffResult completed) {
            if (result != null) {
                throw new IllegalStateException("Target expiry handoff already completed");
            }
            result = Objects.requireNonNull(completed, "completed");
        }

        private synchronized void confirmApplied() {
            if (result == null
                    || (result.kind() != ResultKind.UNKNOWN && result.kind() != ResultKind.APPENDED)) {
                throw new IllegalStateException("only an appended Target expiry can be confirmed applied");
            }
            result = HandoffResult.applied(mutation);
        }
    }
}
