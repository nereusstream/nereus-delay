package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Bounded local orchestration for one strict owner takeover.
 *
 * <p>The coordinator deliberately starts after the caller has selected and
 * opened a validated local Store incarnation.  It owns only the ordering
 * boundary from the context-bound Owner Lease CAS through source replay and
 * strict activation; source assignment publication, checkpoint selection,
 * Object Store download, and Oxia session creation remain external inputs.</p>
 *
 * <p>{@link #runTurn()} executes one bounded replay turn. Every physical
 * entry is submitted through the shared {@code SOURCE_APPLY} work class; a
 * caller must schedule another turn while {@link OwnerRecoveryTurn#complete()}
 * is false. The source cursor advances only after the exact queued action has
 * returned a valid physical-position outcome.</p>
 */
public final class OwnerRecoveryCoordinator {
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;
    private final SourceAssignment assignment;
    private final SourceReplaySuccessor successor;
    private final SourceReplayCursor<? extends SourceReplayEntry> source;
    private final PublicKey verificationKey;
    private final CompatibleControlSnapshotV1 controlSnapshot;
    private final LongSupplier clock;
    private final ReplayTurnBudget turnBudget;
    private final WorkClassExecutionRegistry workClasses;
    private final SourceApplyWorkClassExecutor sourceApply;
    private SourceReplayEntry pendingEntry;
    private SourceApplyWorkClassExecutor.Submission pendingSubmission;
    private boolean catchupStarted;
    private boolean complete;
    private int turnNumber;

    public OwnerRecoveryCoordinator(final OwnedDelayShard ownedShard,
                                     final OxiaOwnerLeaseStore authority,
                                     final SourceAssignment assignment,
                                     final SourceReplaySuccessor successor,
                                     final SourceReplayCursor<? extends SourceReplayEntry> source,
                                     final PublicKey verificationKey,
                                     final CompatibleControlSnapshotV1 controlSnapshot,
                                     final LongSupplier clock,
                                     final ReplayTurnBudget turnBudget,
                                     final WorkClassExecutionRegistry workClasses) {
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.assignment = Objects.requireNonNull(assignment, "assignment");
        this.successor = Objects.requireNonNull(successor, "successor");
        this.source = Objects.requireNonNull(source, "source");
        this.verificationKey = Objects.requireNonNull(verificationKey, "verificationKey");
        this.controlSnapshot = Objects.requireNonNull(controlSnapshot, "controlSnapshot");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.turnBudget = Objects.requireNonNull(turnBudget, "turnBudget");
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.sourceApply = new SourceApplyWorkClassExecutor(
                this.workClasses, this.ownedShard, this.authority, this.verificationKey);
        if (!ownedShard.shard().shardId().equals(assignment.shardId())) {
            throw new IllegalArgumentException("recovery assignment belongs to another shard");
        }
        if (!ownedShard.shard().shardId().equals(controlSnapshot.shard().shardId())) {
            throw new IllegalArgumentException("recovery control snapshot belongs to another shard");
        }
    }

    /**
     * Runs one bounded replay turn and, only after the source is exhausted,
     * performs the strict control-snapshot/Owner-Lease activation CAS.
     */
    public synchronized OwnerRecoveryTurn runTurn() {
        if (complete) {
            if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
                throw new IllegalStateException("completed recovery owner is no longer active");
            }
            return new OwnerRecoveryTurn(List.of(), true, turnNumber);
        }
        if (!catchupStarted) {
            ownedShard.markCatchingUp(authority, assignment, successor, readNow());
            catchupStarted = true;
        }
        ownedShard.requireRecoveryTurn(authority, clock);

        final int thisTurn = Math.incrementExact(turnNumber);
        final long startedNanos = System.nanoTime();
        int recordCount = 0;
        long canonicalBytes = 0;
        final List<SourceReplayOutcome> outcomes = new ArrayList<>();
        while (true) {
            ownedShard.requireRecoveryTurn(authority, clock);
            if (!sourceHasNext()) {
                // The final clock read is intentionally after the replay
                // actions. A turn can consume the whole lease window;
                // activation must prove the current lease again before
                // writing the local OPEN marker or ACTIVE CAS.
                ownedShard.activateForCommandsWithControlSnapshot(authority, controlSnapshot, readNow());
                complete = true;
                turnNumber = thisTurn;
                return new OwnerRecoveryTurn(outcomes, true, thisTurn);
            }
            if (recordCount >= turnBudget.maxRecords()
                    || canonicalBytes >= turnBudget.maxCanonicalBytes()
                    || System.nanoTime() - startedNanos >= turnBudget.maxElapsedNanos()) {
                turnNumber = thisTurn;
                return new OwnerRecoveryTurn(outcomes, false, thisTurn);
            }

            final SourceReplayEntry entry;
            final long entryBytes;
            if (pendingEntry == null) {
                entry = sourcePeek();
                entryBytes = canonicalReplayBytes(entry);
                if (entryBytes > turnBudget.maxCanonicalBytes()
                        || canonicalBytes > turnBudget.maxCanonicalBytes() - entryBytes) {
                    if (entryBytes > turnBudget.maxCanonicalBytes()) {
                        ownedShard.fence();
                        throw new IllegalArgumentException(
                                "single recovery source entry exceeds canonical-byte turn budget");
                    }
                    turnNumber = thisTurn;
                    return new OwnerRecoveryTurn(outcomes, false, thisTurn);
                }
                pendingEntry = entry;
                try {
                    pendingSubmission = sourceApply.submitRecovery(entry, clock);
                } catch (RuntimeException | Error failure) {
                    pendingEntry = null;
                    throw failure;
                }
            } else {
                entry = pendingEntry;
                if (!sameEntry(entry, sourcePeek())) {
                    ownedShard.fence();
                    throw new IllegalStateException("pending recovery source entry changed before execution");
                }
                entryBytes = canonicalReplayBytes(entry);
            }
            final SourceApplyWorkClassExecutor.Submission submission = pendingSubmission;
            final long elapsedNanos = System.nanoTime() - startedNanos;
            final long remainingNanos = Math.max(1L, turnBudget.maxElapsedNanos() - elapsedNanos);
            List<WorkClassTask> completedTasks;
            Throwable dispatchFailure = null;
            try {
                completedTasks = workClasses.runTurn(
                        new SchedulerBudget(1, submission.task().bytes(), remainingNanos));
            } catch (RuntimeException | Error failure) {
                final SourceApplyWorkClassExecutor.ApplyOutcome observed = submission.outcome().orElse(null);
                if (observed == null) {
                    turnNumber = thisTurn;
                    throw failure;
                }
                // Another selected class may have failed after this source
                // action completed. The source outcome is still authoritative
                // for cursor advancement; do not submit it again. Preserve
                // the unrelated dispatcher failure for the caller after the
                // source cursor is advanced.
                dispatchFailure = failure;
                completedTasks = List.of(submission.task());
            }
            if (!completedTasks.contains(submission.task())) {
                turnNumber = thisTurn;
                return new OwnerRecoveryTurn(outcomes, false, thisTurn, true, submission.task());
            }
            final SourceApplyWorkClassExecutor.ApplyOutcome applied = submission.outcome().orElseThrow(
                    () -> new IllegalStateException("recovery source action completed without an outcome"));
            if (applied.failure() != null) {
                pendingEntry = null;
                pendingSubmission = null;
                throw failure(applied.failure());
            }
            final SourceReplayOutcome outcome = Objects.requireNonNull(applied.result(),
                    "recovery source action result");
            if (!sameEntry(entry, sourcePeek())) {
                ownedShard.fence();
                throw new IllegalStateException("source look-ahead changed before recovery cursor advance");
            }
            sourceNext();
            ownedShard.recordRecoverySourceCursorAdvanced(entry);
            pendingEntry = null;
            pendingSubmission = null;
            outcomes.add(outcome);
            recordCount = Math.incrementExact(recordCount);
            canonicalBytes = Math.addExact(canonicalBytes, entryBytes);
            if (dispatchFailure != null) {
                turnNumber = thisTurn;
                throwUnchecked(dispatchFailure);
            }
        }
    }

    public synchronized boolean complete() {
        return complete;
    }

    public synchronized int turnNumber() {
        return turnNumber;
    }

    private long readNow() {
        try {
            final long now = clock.getAsLong();
            if (now < 0) {
                throw new IllegalArgumentException("recovery clock returned a negative time");
            }
            return now;
        } catch (RuntimeException | Error failure) {
            // A clock read is part of the strict Owner proof.  Close the
            // local gate even if the failure happens before the first replay
            // turn or between the last replay turn and activation.
            ownedShard.fence();
            throw failure;
        }
    }

    private boolean sourceHasNext() {
        try {
            return source.hasNext();
        } catch (RuntimeException | Error failure) {
            ownedShard.fence();
            throw failure;
        }
    }

    private SourceReplayEntry sourcePeek() {
        try {
            return Objects.requireNonNull(source.peek(), "source replay entry");
        } catch (RuntimeException | Error failure) {
            ownedShard.fence();
            throw failure;
        }
    }

    private void sourceNext() {
        try {
            source.next();
        } catch (RuntimeException | Error failure) {
            ownedShard.fence();
            throw failure;
        }
    }

    private static long canonicalReplayBytes(final SourceReplayEntry entry) {
        final SourceReplayEntry exact = Objects.requireNonNull(entry, "source replay entry");
        final byte[] frame = exact instanceof SourceReplayRecord command
                ? CommandCodec.encodeFrame(command.command())
                : ((SourceReplayMutation) exact).mutation().encodeFrame();
        return Math.addExact((long) exact.position().canonicalBytes().length, frame.length);
    }

    private static boolean sameEntry(final SourceReplayEntry first, final SourceReplayEntry second) {
        return second != null
                && Bytes.constantTimeEquals(first.position().canonicalBytes(), second.position().canonicalBytes())
                && java.util.Arrays.equals(frame(first), frame(second))
                && java.util.Arrays.equals(first.guardAttestationDigest(), second.guardAttestationDigest())
                && Objects.equals(first.sourceConnectionGeneration(), second.sourceConnectionGeneration());
    }

    private static byte[] frame(final SourceReplayEntry entry) {
        if (entry instanceof SourceReplayRecord command) {
            return CommandCodec.encodeFrame(command.command());
        }
        return ((SourceReplayMutation) entry).mutation().encodeFrame();
    }

    private static RuntimeException failure(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            return runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        return new IllegalStateException("recovery source action failed", failure);
    }

    private static void throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected recovery dispatcher failure", failure);
    }
}
