package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;

import java.security.PublicKey;
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
 * <p>{@link #runTurn()} executes at most one replay turn.  A caller must
 * schedule another turn while {@link OwnerRecoveryTurn#complete()} is false;
 * looping inside this class would defeat the source record/byte/time caps and
 * could starve lease, control, or scheduler work.</p>
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
                                     final ReplayTurnBudget turnBudget) {
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.assignment = Objects.requireNonNull(assignment, "assignment");
        this.successor = Objects.requireNonNull(successor, "successor");
        this.source = Objects.requireNonNull(source, "source");
        this.verificationKey = Objects.requireNonNull(verificationKey, "verificationKey");
        this.controlSnapshot = Objects.requireNonNull(controlSnapshot, "controlSnapshot");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.turnBudget = Objects.requireNonNull(turnBudget, "turnBudget");
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

        final SourceReplayTurn<SourceReplayOutcome> replay = ownedShard.replayTurn(
                source, verificationKey, clock, turnBudget);
        turnNumber = Math.incrementExact(turnNumber);
        if (replay.exhausted()) {
            // The final clock read is intentionally after the replay turn. A
            // turn can consume the whole lease window; activation must prove
            // the current lease again before writing the local OPEN marker or
            // issuing the ACTIVE_FOR_COMMANDS CAS.
            ownedShard.activateForCommandsWithControlSnapshot(authority, controlSnapshot, readNow());
            complete = true;
        }
        return new OwnerRecoveryTurn(replay.results(), complete, turnNumber);
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
}
