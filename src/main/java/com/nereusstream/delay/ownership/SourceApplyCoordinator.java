package com.nereusstream.delay.ownership;

import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.security.PublicKey;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * One-record bounded source-reader/apply/ack handoff.
 *
 * <p>The coordinator owns no broker cursor or Source Position allocation. It
 * keeps one caller-owned {@link SourceReplayCursor} look-ahead entry, or one
 * native {@link SourceRecordConsumer.PolledSourceRecord}, and uses {@link
 * SourceApplyWorkClassExecutor} for the bounded {@code SOURCE_APPLY} action.
 * The exact entry is retained until the external acknowledgement is
 * confirmed. Consequently a queue rejection, an apply failure, an ACK
 * response loss, or an ACK rejection cannot make the process-local cursor
 * outrun the broker's retry authority.</p>
 *
 * <p>Each call to {@link #runTurn(SchedulerBudget, LongSupplier)} handles at
 * most one physical record. The Worker must schedule another call while the
 * result is not {@link TurnStatus#EXHAUSTED}; this keeps source work bounded
 * independently from the WorkClass queue's own record/byte/time limits.</p>
 */
public final class SourceApplyCoordinator {
    private final SourceReplayCursor<? extends SourceReplayEntry> source;
    private final SourceRecordConsumer sourceConsumer;
    private final WorkClassExecutionRegistry workClasses;
    private final SourceApplyWorkClassExecutor executor;
    private final OwnedDelayShard ownedShard;
    private final SourceAcknowledgement acknowledgement;
    private SourceRecordConsumer.PolledSourceRecord pendingSourceRecord;
    private Pending pending;

    public SourceApplyCoordinator(
            final SourceReplayCursor<? extends SourceReplayEntry> source,
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority,
            final PublicKey verificationKey,
            final SourceAcknowledgement acknowledgement) {
        this.source = Objects.requireNonNull(source, "source");
        this.sourceConsumer = null;
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.executor = new SourceApplyWorkClassExecutor(
                this.workClasses,
                this.ownedShard,
                Objects.requireNonNull(authority, "authority"),
                Objects.requireNonNull(verificationKey, "verificationKey"));
        this.acknowledgement = Objects.requireNonNull(acknowledgement, "acknowledgement");
    }

    /**
     * Builds the Worker-facing variant. The consumer's ACK callback is also
     * its native cursor-advance authority; it must return {@code ACKED} only
     * after that broker operation is durably accepted.
     */
    public SourceApplyCoordinator(
            final SourceRecordConsumer sourceConsumer,
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority,
            final PublicKey verificationKey) {
        this.source = null;
        this.sourceConsumer = Objects.requireNonNull(sourceConsumer, "sourceConsumer");
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.executor = new SourceApplyWorkClassExecutor(
                this.workClasses,
                this.ownedShard,
                Objects.requireNonNull(authority, "authority"),
                Objects.requireNonNull(verificationKey, "verificationKey"));
        this.acknowledgement = null;
    }

    /**
     * Runs one bounded source handoff. The source cursor advances only after
     * a successful broker ACK and an exact look-ahead identity check.
     */
    public synchronized TurnResult runTurn(final SchedulerBudget workBudget, final LongSupplier ownerClock) {
        Objects.requireNonNull(workBudget, "workBudget");
        Objects.requireNonNull(ownerClock, "ownerClock");
        if (pending == null) {
            if (sourceConsumer != null) {
                try {
                    final Optional<SourceRecordConsumer.PolledSourceRecord> polled =
                            Objects.requireNonNull(sourceConsumer.poll(), "source consumer poll result");
                    if (polled.isEmpty()) {
                        return TurnResult.waitingForSource();
                    }
                    pendingSourceRecord = polled.get();
                    pending = new Pending(pendingSourceRecord.entry());
                } catch (RuntimeException | Error failure) {
                    ownedShard.fence();
                    return TurnResult.sourcePollFailure(failure);
                }
            } else {
                final SourceReplayEntry next = peekSource();
                if (next == null) {
                    return TurnResult.exhausted();
                }
                pending = new Pending(next);
            }
        }

        if (pending.appliedOutcome == null) {
            if (pending.submission == null) {
                try {
                    pending.submission = executor.submit(pending.entry, ownerClock);
                } catch (RuntimeException | Error failure) {
                    return TurnResult.rejected(TurnStatus.SUBMISSION_REJECTED, pending.entry, failure);
                }
            }
            try {
                workClasses.runTurn(workBudget);
            } catch (RuntimeException | Error failure) {
                final SourceApplyWorkClassExecutor.ApplyOutcome observed =
                        pending.submission.outcome().orElse(null);
                if (observed == null) {
                    return TurnResult.failed(TurnStatus.WORK_CLASS_FAILURE, pending.entry, failure);
                }
            }
            final SourceApplyWorkClassExecutor.ApplyOutcome applied =
                    pending.submission.outcome().orElse(null);
            if (applied == null) {
                return TurnResult.waiting(pending.entry, pending.submission.task());
            }
            if (applied.failure() != null) {
                // The executor has already fenced an unproven local boundary;
                // retain the exact source record for a fresh owner/store.
                pending.submission = null;
                return TurnResult.failed(TurnStatus.APPLY_FAILURE, pending.entry, applied.failure());
            }
            pending.appliedOutcome = Objects.requireNonNull(applied.result(), "source apply result");
        }

        if (!pending.acknowledged) {
            final SourceAcknowledgement.AcknowledgementResult result;
            try {
                final SourceAcknowledgement ack = sourceConsumer == null
                        ? acknowledgement
                        : Objects.requireNonNull(pendingSourceRecord, "pending source record")
                                .acknowledgement();
                result = Objects.requireNonNull(
                        ack.acknowledge(pending.entry, pending.appliedOutcome), "source acknowledgement result");
            } catch (RuntimeException | Error failure) {
                return TurnResult.failed(TurnStatus.ACK_UNKNOWN, pending.entry, failure);
            }
            if (result.disposition() != SourceAcknowledgement.Disposition.ACKED) {
                return TurnResult.ackPending(result.disposition(), pending.entry, result.failure());
            }
            pending.acknowledged = true;
        }

        if (sourceConsumer == null) {
            try {
                final SourceReplayEntry observed = source.peek();
                if (!sameEntry(pending.entry, observed)) {
                    throw new IllegalStateException("source look-ahead changed before cursor advance");
                }
                source.next();
            } catch (RuntimeException | Error failure) {
                // The broker ACK is already confirmed, but the caller-owned
                // cursor could not be advanced. Continuity is no longer proven;
                // fence the Owner before retaining the exact entry for recovery.
                ownedShard.fence();
                return TurnResult.failed(TurnStatus.CURSOR_ADVANCE_FAILURE, pending.entry, failure);
            }
        }
        final SourceReplayOutcome result = pending.appliedOutcome;
        final SourceReplayEntry completed = pending.entry;
        pending = null;
        pendingSourceRecord = null;
        return TurnResult.applied(completed, result);
    }

    /** Returns the exact entry retained across ACK/apply uncertainty, if any. */
    public synchronized Optional<SourceReplayEntry> pendingEntry() {
        return Optional.ofNullable(pending == null ? null : pending.entry);
    }

    private SourceReplayEntry peekSource() {
        try {
            if (!source.hasNext()) {
                return null;
            }
            return Objects.requireNonNull(source.peek(), "source look-ahead entry");
        } catch (RuntimeException | Error failure) {
            ownedShard.fence();
            return throwUnchecked(failure);
        }
    }

    private static boolean sameEntry(final SourceReplayEntry first, final SourceReplayEntry second) {
        return second != null
                && java.util.Arrays.equals(
                        first.position().canonicalBytes(), second.position().canonicalBytes())
                && java.util.Arrays.equals(frame(first), frame(second))
                && java.util.Arrays.equals(first.guardAttestationDigest(), second.guardAttestationDigest())
                && Objects.equals(first.sourceConnectionGeneration(), second.sourceConnectionGeneration());
    }

    private static byte[] frame(final SourceReplayEntry entry) {
        if (entry instanceof SourceReplayRecord command) {
            return com.nereusstream.delay.protocol.CommandCodec.encodeFrame(command.command());
        }
        return ((SourceReplayMutation) entry).mutation().encodeFrame();
    }

    private static <T> T throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected source cursor failure", failure);
    }

    private static final class Pending {
        private final SourceReplayEntry entry;
        private SourceApplyWorkClassExecutor.Submission submission;
        private SourceReplayOutcome appliedOutcome;
        private boolean acknowledged;

        private Pending(final SourceReplayEntry entry) {
            this.entry = Objects.requireNonNull(entry, "entry");
        }
    }

    public enum TurnStatus {
        EXHAUSTED,
        WAITING_FOR_SOURCE,
        SOURCE_POLL_FAILURE,
        WAITING_FOR_WORK_CLASS,
        APPLIED_AND_ACKED,
        SUBMISSION_REJECTED,
        WORK_CLASS_FAILURE,
        APPLY_FAILURE,
        ACK_DEFINITIVELY_NOT_ACKED,
        ACK_UNKNOWN,
        CURSOR_ADVANCE_FAILURE
    }

    public record TurnResult(
            TurnStatus status,
            SourceReplayEntry entry,
            SourceReplayOutcome appliedOutcome,
            WorkClassTask task,
            Throwable failure) {
        public TurnResult {
            Objects.requireNonNull(status, "status");
            if (status == TurnStatus.EXHAUSTED || status == TurnStatus.WAITING_FOR_SOURCE) {
                if (entry != null || appliedOutcome != null || task != null || failure != null) {
                    throw new IllegalArgumentException("source idle turn cannot carry work evidence");
                }
            } else if (status == TurnStatus.SOURCE_POLL_FAILURE) {
                if (entry != null || appliedOutcome != null || task != null || failure == null) {
                    throw new IllegalArgumentException("source poll failure has invalid evidence");
                }
            } else {
                Objects.requireNonNull(entry, "entry");
            }
            if (status == TurnStatus.WAITING_FOR_WORK_CLASS && task == null) {
                throw new IllegalArgumentException("waiting turn must expose its task");
            }
            if (status == TurnStatus.APPLIED_AND_ACKED && appliedOutcome == null) {
                throw new IllegalArgumentException("applied turn must carry its outcome");
            }
            if (status != TurnStatus.APPLIED_AND_ACKED && appliedOutcome != null) {
                throw new IllegalArgumentException("non-applied turn cannot carry an outcome");
            }
        }

        private static TurnResult exhausted() {
            return new TurnResult(TurnStatus.EXHAUSTED, null, null, null, null);
        }

        private static TurnResult waitingForSource() {
            return new TurnResult(TurnStatus.WAITING_FOR_SOURCE, null, null, null, null);
        }

        private static TurnResult sourcePollFailure(final Throwable failure) {
            return new TurnResult(
                    TurnStatus.SOURCE_POLL_FAILURE, null, null, null, Objects.requireNonNull(failure, "failure"));
        }

        private static TurnResult waiting(final SourceReplayEntry entry, final WorkClassTask task) {
            return new TurnResult(TurnStatus.WAITING_FOR_WORK_CLASS, entry, null, task, null);
        }

        private static TurnResult rejected(
                final TurnStatus status, final SourceReplayEntry entry, final Throwable failure) {
            return new TurnResult(status, entry, null, null, Objects.requireNonNull(failure, "failure"));
        }

        private static TurnResult failed(
                final TurnStatus status, final SourceReplayEntry entry, final Throwable failure) {
            return new TurnResult(status, entry, null, null, Objects.requireNonNull(failure, "failure"));
        }

        private static TurnResult ackPending(
                final SourceAcknowledgement.Disposition disposition,
                final SourceReplayEntry entry,
                final Throwable failure) {
            final TurnStatus status = disposition == SourceAcknowledgement.Disposition.UNKNOWN
                    ? TurnStatus.ACK_UNKNOWN
                    : TurnStatus.ACK_DEFINITIVELY_NOT_ACKED;
            return new TurnResult(status, entry, null, null, failure);
        }

        private static TurnResult applied(final SourceReplayEntry entry, final SourceReplayOutcome outcome) {
            return new TurnResult(TurnStatus.APPLIED_AND_ACKED, entry, outcome, null, null);
        }
    }
}
