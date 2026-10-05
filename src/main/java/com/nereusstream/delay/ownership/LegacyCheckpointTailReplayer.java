package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.store.ShardStore;
import java.security.PublicKey;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Applies a finite old-format source tail to an isolated legacy checkpoint copy.
 *
 * <p>This is only the source-tail replay stage of migration preparation. It
 * does not inspect or convert the resulting state, publish an ACTIVE pointer,
 * or authorize activation. The supplied cursor must read the accepted source
 * assignment without acknowledging or otherwise advancing the broker. The
 * call consumes the cursor; if it returns {@link Status#BLOCKED}, discard both
 * the cursor and the partially replayed copy.</p>
 */
public final class LegacyCheckpointTailReplayer {
    private LegacyCheckpointTailReplayer() {}

    /** Replays through the protected cut once, using one finite total budget. */
    public static Result replay(
            final ShardStore.LegacyCheckpointReplayCopy replayCopy,
            final SourceAssignment assignment,
            final SourceReplaySuccessor successor,
            final SourceRecordConsumer.CheckpointCut sourceCut,
            final SourceReplayCursor<? extends SourceReplayEntry> sourceTail,
            final LegacyApplierFactory applierFactory,
            final PublicKey systemMutationVerificationKey,
            final LongSupplier replayClock,
            final ReplayTurnBudget totalBudget) {
        final ShardStore.LegacyCheckpointReplayCopy exactCopy = Objects.requireNonNull(replayCopy, "replayCopy");
        final SourceAssignment exactAssignment = Objects.requireNonNull(assignment, "assignment");
        final SourceReplaySuccessor exactSuccessor = Objects.requireNonNull(successor, "successor");
        final SourceRecordConsumer.CheckpointCut exactCut = Objects.requireNonNull(sourceCut, "sourceCut");
        final SourceReplayCursor<? extends SourceReplayEntry> exactTail =
                Objects.requireNonNull(sourceTail, "sourceTail");
        final LegacyApplierFactory exactFactory = Objects.requireNonNull(applierFactory, "applierFactory");
        final PublicKey verificationKey = Objects.requireNonNull(
                systemMutationVerificationKey, "systemMutationVerificationKey");
        final LongSupplier clock = Objects.requireNonNull(replayClock, "replayClock");
        final ReplayTurnBudget budget = Objects.requireNonNull(totalBudget, "totalBudget");

        final ShardStore store = exactCopy.store();
        final SourcePosition checkpointPosition = Objects.requireNonNull(
                store.appliedShardLogPosition(), "legacy checkpoint applied source position");
        final SourcePosition cutPosition = Objects.requireNonNull(exactCut.position(), "source cut position");
        validatePositions(store.shardId(), exactAssignment, checkpointPosition, cutPosition);
        requireCurrentCut(exactCut, cutPosition);

        if (sameCanonicalPosition(checkpointPosition, cutPosition)) {
            requireCurrentCut(exactCut, cutPosition);
            return new Result(
                    Status.EXACT_CUT_REACHED,
                    BlockReason.NONE,
                    checkpointPosition,
                    cutPosition,
                    checkpointPosition,
                    0,
                    0,
                    0);
        }

        final DelayShard applier = Objects.requireNonNull(exactFactory.open(store), "legacy applier");
        if (!store.shardId().equals(applier.shardId())
                || !applier.isBackedBy(store)
                || !Bytes.constantTimeEquals(store.metadata().storeIncarnation(), applier.storeIncarnation())) {
            throw new IllegalArgumentException("legacy applier is not bound to the isolated checkpoint Store");
        }

        // This OwnerLease only opens OwnedDelayShard's local catch-up gate. It
        // is never registered with an authority or exposed for activation.
        final OwnerLease replayLease = new OwnerLease(
                store.shardId(),
                "legacy-checkpoint-replay",
                1,
                Bytes.sha256(Bytes.utf8("legacy-checkpoint-replay"), exactAssignment.canonicalBytes()),
                Long.MAX_VALUE);
        final OwnedDelayShard owned = new OwnedDelayShard(applier, replayLease);
        owned.markCatchingUp(exactAssignment, exactSuccessor);

        final CutBoundIterator boundedTail = new CutBoundIterator(exactTail, exactCut, cutPosition);
        final SourceReplayTurn<SourceReplayOutcome> turn = owned.replayTurn(
                SourceReplayCursor.of(boundedTail), verificationKey, clock, budget);
        requireCurrentCut(exactCut, cutPosition);

        final SourcePosition appliedThrough = Objects.requireNonNull(
                applier.lastAppliedSourcePosition(), "legacy applier source position");
        if (!sameCanonicalPosition(appliedThrough, store.appliedShardLogPosition())) {
            throw new IllegalStateException("legacy applier and isolated Store source positions diverged");
        }
        final int commandsApplied = (int) turn.results().stream().filter(SourceReplayOutcome::isCommand).count();
        final int mutationsApplied = turn.results().size() - commandsApplied;

        if (boundedTail.cutConsumed() && sameCanonicalPosition(appliedThrough, cutPosition)) {
            return new Result(
                    Status.EXACT_CUT_REACHED,
                    BlockReason.NONE,
                    checkpointPosition,
                    cutPosition,
                    appliedThrough,
                    turn.results().size(),
                    commandsApplied,
                    mutationsApplied);
        }
        return new Result(
                Status.BLOCKED,
                turn.hasMore() ? BlockReason.REPLAY_BUDGET_EXHAUSTED : BlockReason.SOURCE_CUT_NOT_REACHED,
                checkpointPosition,
                cutPosition,
                appliedThrough,
                turn.results().size(),
                commandsApplied,
                mutationsApplied);
    }

    private static void validatePositions(
            final com.nereusstream.delay.protocol.ShardId shardId,
            final SourceAssignment assignment,
            final SourcePosition checkpointPosition,
            final SourcePosition cutPosition) {
        if (!shardId.equals(assignment.shardId())
                || !shardId.equals(checkpointPosition.shardId())
                || !shardId.equals(cutPosition.shardId())) {
            throw new IllegalArgumentException("legacy checkpoint, assignment, and source cut must share one Shard");
        }
        assignment.activationBarrier().validatePosition(checkpointPosition);
        assignment.activationBarrier().validatePosition(cutPosition);
        final int checkpointOrder = checkpointPosition.compareTo(cutPosition);
        if (checkpointOrder > 0) {
            throw new IllegalArgumentException("legacy checkpoint source position is beyond the captured cut");
        }
        if (checkpointOrder == 0 && !sameCanonicalPosition(checkpointPosition, cutPosition)) {
            throw new IllegalArgumentException("legacy checkpoint conflicts with the captured cut position");
        }
    }

    private static void requireCurrentCut(
            final SourceRecordConsumer.CheckpointCut sourceCut, final SourcePosition expectedCut) {
        sourceCut.requireCurrent();
        final SourcePosition observed = Objects.requireNonNull(sourceCut.position(), "source cut position");
        if (!sameCanonicalPosition(expectedCut, observed)) {
            throw new IllegalStateException("protected source cut changed during legacy replay");
        }
    }

    private static boolean sameCanonicalPosition(final SourcePosition first, final SourcePosition second) {
        return first != null
                && second != null
                && Bytes.constantTimeEquals(first.canonicalBytes(), second.canonicalBytes());
    }

    @FunctionalInterface
    public interface LegacyApplierFactory {
        /** Builds the old-format applier with the legacy runtime's exact dependencies. */
        DelayShard open(ShardStore isolatedStore);
    }

    public enum Status {
        EXACT_CUT_REACHED,
        BLOCKED
    }

    public enum BlockReason {
        NONE,
        SOURCE_CUT_NOT_REACHED,
        REPLAY_BUDGET_EXHAUSTED
    }

    /** Local replay evidence; exact cut reached is not a migration READY result. */
    public record Result(
            Status status,
            BlockReason blockReason,
            SourcePosition checkpointPosition,
            SourcePosition sourceCut,
            SourcePosition appliedThrough,
            int recordsApplied,
            int commandsApplied,
            int systemMutationsApplied) {
        public Result {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(blockReason, "blockReason");
            Objects.requireNonNull(checkpointPosition, "checkpointPosition");
            Objects.requireNonNull(sourceCut, "sourceCut");
            Objects.requireNonNull(appliedThrough, "appliedThrough");
            if (recordsApplied < 0 || commandsApplied < 0 || systemMutationsApplied < 0
                    || recordsApplied != commandsApplied + systemMutationsApplied
                    || (status == Status.EXACT_CUT_REACHED) != (blockReason == BlockReason.NONE)) {
                throw new IllegalArgumentException("invalid legacy source-tail replay result");
            }
        }
    }

    private static final class CutBoundIterator implements Iterator<SourceReplayEntry> {
        private final SourceReplayCursor<? extends SourceReplayEntry> source;
        private final SourceRecordConsumer.CheckpointCut sourceCut;
        private final SourcePosition cutPosition;
        private boolean consumedCut;

        private CutBoundIterator(
                final SourceReplayCursor<? extends SourceReplayEntry> source,
                final SourceRecordConsumer.CheckpointCut sourceCut,
                final SourcePosition cutPosition) {
            this.source = source;
            this.sourceCut = sourceCut;
            this.cutPosition = cutPosition;
        }

        @Override
        public boolean hasNext() {
            requireCurrentCut(sourceCut, cutPosition);
            if (consumedCut || !source.hasNext()) {
                return false;
            }
            final SourceReplayEntry next = source.peek();
            final int order = next.position().compareTo(cutPosition);
            if (order > 0) {
                return false;
            }
            if (order == 0 && !sameCanonicalPosition(next.position(), cutPosition)) {
                throw new IllegalStateException("source tail conflicts with the protected cut position");
            }
            return true;
        }

        @Override
        public SourceReplayEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException("protected source cut has been reached or is unavailable");
            }
            final SourceReplayEntry next = source.next();
            if (sameCanonicalPosition(next.position(), cutPosition)) {
                consumedCut = true;
            }
            return next;
        }

        private boolean cutConsumed() {
            return consumedCut;
        }
    }
}
