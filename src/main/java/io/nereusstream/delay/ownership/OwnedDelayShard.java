package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.SourceActivationBarrier;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.runtime.SystemMutationResult;
import io.nereusstream.delay.runtime.CommandResult;
import io.nereusstream.delay.runtime.DelayShard;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.security.PublicKey;

/** Fenced owner view; lease loss closes all new local authority gates. */
public final class OwnedDelayShard {
    private final DelayShard delegate;
    private OwnerLease lease;
    private ShardLifecycleState state;
    private SourceActivationBarrier activationBarrier;
    private SourceAssignment sourceAssignment;
    private SourcePosition lastCatchupPosition;

    public OwnedDelayShard(final DelayShard delegate, final OwnerLease lease) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.state = ShardLifecycleState.RESTORING;
    }

    public synchronized CommandResult apply(final PreparedCommand command, final SourcePosition position,
                                            final long nowEpochMs) {
        return apply(command, position, nowEpochMs, null, null);
    }

    /**
     * Applies a record from a guarded Pulsar source connection.  The
     * connection proof is required for Pulsar because a replacement consumer
     * can otherwise emit a position with the same physical topic identity.
     * Kafka has no connection-generation field and passes {@code null} proof.
     */
    public synchronized CommandResult apply(final PreparedCommand command, final SourcePosition position,
                                            final long nowEpochMs, final Long sourceConnectionGeneration,
                                            final byte[] guardAttestationDigest) {
        ensureActive(nowEpochMs);
        if (activationBarrier != null) {
            activationBarrier.validatePosition(position);
            validateSourceConnection(position, sourceConnectionGeneration, guardAttestationDigest);
        }
        return delegate.apply(command, position);
    }

    public synchronized void updateLease(final OwnerLease renewed) {
        Objects.requireNonNull(renewed, "renewed");
        if (!lease.sameIdentity(renewed) || renewed.state() != lease.state()
                || renewed.expiresAtEpochMs() < lease.expiresAtEpochMs()) {
            throw new IllegalArgumentException("lease renewal changed owner identity/epoch");
        }
        lease = renewed;
    }

    public synchronized void fence() {
        state = ShardLifecycleState.FENCED;
    }

    /**
     * @deprecated V1 requires an explicit source assignment; use
     * {@link #markCatchingUp(SourceAssignment)}.
     */
    @Deprecated
    public synchronized void markCatchingUp() {
        markCatchingUp((SourceActivationBarrier) null);
    }

    /**
     * Compatibility check for an assignment that has already been accepted.
     * This overload cannot establish source identity and therefore cannot
     * replace {@link #markCatchingUp(SourceAssignment)}.
     *
     * @deprecated use {@link #markCatchingUp(SourceAssignment)}.
     */
    @Deprecated
    public synchronized void markCatchingUp(final SourceActivationBarrier barrier) {
        if (sourceAssignment == null) {
            throw new IllegalStateException("source assignment must be accepted before catch-up");
        }
        if (!Objects.equals(sourceAssignment.activationBarrier(), barrier)) {
            throw new IllegalArgumentException("catch-up barrier is not the accepted source assignment barrier");
        }
        markCatchingUp(sourceAssignment);
    }

    /** Accepts the exact assignment/barrier pair supplied by the source adapter. */
    public synchronized void markCatchingUp(final SourceAssignment assignment) {
        if (state != ShardLifecycleState.RESTORING) {
            throw new IllegalStateException("shard is not restoring");
        }
        Objects.requireNonNull(assignment, "assignment");
        if (!delegate.shardId().equals(assignment.shardId())) {
            throw new IllegalArgumentException("source assignment does not belong to shard");
        }
        if (lease.sourceAssignmentId() != null
                && !Bytes.constantTimeEquals(lease.sourceAssignmentId(), assignment.assignmentId())) {
            throw new IllegalArgumentException("source assignment does not match owner lease context");
        }
        if (lease.sourceAssignmentEpoch() > 0
                && lease.sourceAssignmentEpoch() != assignment.assignmentEpoch()) {
            throw new IllegalArgumentException("source assignment epoch does not match owner lease context");
        }
        sourceAssignment = assignment;
        activationBarrier = assignment.activationBarrier();
        lastCatchupPosition = delegate.lastAppliedSourcePosition();
        state = ShardLifecycleState.CATCHING_UP;
    }

    public synchronized void recordCatchup(final SourcePosition position) {
        recordCatchup(position, null, null);
    }

    /** Records catch-up from the exact guarded source connection generation. */
    public synchronized void recordCatchup(final SourcePosition position, final Long sourceConnectionGeneration,
                                           final byte[] guardAttestationDigest) {
        Objects.requireNonNull(position, "position");
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("shard is not catching up");
        }
        if (!delegate.shardId().equals(position.shardId())) {
            throw new IllegalArgumentException("catch-up position does not belong to shard");
        }
        if (activationBarrier != null) {
            activationBarrier.validatePosition(position);
            validateSourceConnection(position, sourceConnectionGeneration, guardAttestationDigest);
        }
        if (lastCatchupPosition != null && position.compareTo(lastCatchupPosition) < 0) {
            throw new IllegalStateException("catch-up position regressed");
        }
        lastCatchupPosition = position;
    }

    /**
     * Applies assigned Command Topic records while the shard is still
     * catching up. Each record is validated against the accepted physical
     * source barrier, applied through the same synchronous shard WriteBatch as
     * normal commands, and only then advances the local catch-up cursor.
     *
     * <p>This is a local command-replay seam. It deliberately does not claim
     * a broker consumer, implement System Mutation replay, or replace the
     * production assignment/lease transaction.</p>
     */
    public synchronized List<CommandResult> replayCatchup(final Iterable<SourceReplayRecord> records,
                                                           final long nowEpochMs) {
        Objects.requireNonNull(records, "records");
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("shard is not catching up");
        }
        if (!lease.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease expired during source catch-up");
        }
        final List<CommandResult> results = new ArrayList<>();
        for (SourceReplayRecord record : records) {
            Objects.requireNonNull(record, "source replay record");
            final SourcePosition position = record.position();
            if (!delegate.shardId().equals(position.shardId())) {
                throw new IllegalArgumentException("source replay position does not belong to shard");
            }
            if (activationBarrier != null) {
                activationBarrier.validatePosition(position);
                validateSourceConnection(position, record.sourceConnectionGeneration(),
                        record.guardAttestationDigest());
            }
            if (lastCatchupPosition != null && position.compareTo(lastCatchupPosition) < 0) {
                throw new IllegalStateException("source replay position regressed");
            }
            final CommandResult result = delegate.apply(record.command(), position);
            lastCatchupPosition = position;
            results.add(result);
        }
        return List.copyOf(results);
    }

    /** Returns the last position applied or observed during this catch-up. */
    public synchronized SourcePosition lastCatchupPosition() {
        return lastCatchupPosition;
    }

    /**
     * Applies signed System Mutation records during the same guarded catch-up
     * window. The source cursor advances only after the shard has persisted the
     * mutation result and Source Position in its synchronous WriteBatch.
     */
    public synchronized List<SystemMutationResult> replaySystemMutations(
            final Iterable<SourceReplayMutation> records, final PublicKey verificationKey,
            final long nowEpochMs) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(verificationKey, "verificationKey");
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("shard is not catching up");
        }
        if (!lease.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease expired during system-mutation catch-up");
        }
        final List<SystemMutationResult> results = new ArrayList<>();
        for (SourceReplayMutation record : records) {
            Objects.requireNonNull(record, "source replay mutation");
            final SourcePosition position = record.position();
            if (!delegate.shardId().equals(position.shardId())) {
                throw new IllegalArgumentException("system replay position does not belong to shard");
            }
            if (activationBarrier != null) {
                activationBarrier.validatePosition(position);
                validateSourceConnection(position, record.sourceConnectionGeneration(),
                        record.guardAttestationDigest());
            }
            if (lastCatchupPosition != null && position.compareTo(lastCatchupPosition) < 0) {
                throw new IllegalStateException("system replay position regressed");
            }
            final SystemMutationResult result = delegate.applySystemMutation(record.mutation(), position,
                    verificationKey);
            lastCatchupPosition = position;
            results.add(result);
        }
        return List.copyOf(results);
    }

    /**
     * Replays the single mixed Command/System Mutation Shard Log in source
     * order.  Both branches use the same physical source guard and cursor;
     * the cursor advances only after the selected delegate WriteBatch has
     * committed.  The local seam still does not own a Kafka/Pulsar consumer or
     * the production assignment/activation transaction.
     */
    public synchronized List<SourceReplayOutcome> replay(
            final Iterable<? extends SourceReplayEntry> records, final PublicKey verificationKey,
            final long nowEpochMs) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(verificationKey, "verificationKey");
        ensureReplayWindow(nowEpochMs);
        final List<SourceReplayOutcome> results = new ArrayList<>();
        for (SourceReplayEntry record : records) {
            Objects.requireNonNull(record, "source replay entry");
            final SourcePosition position = record.position();
            validateReplayPosition(position, record.sourceConnectionGeneration(), record.guardAttestationDigest());
            if (record instanceof SourceReplayRecord commandRecord) {
                final CommandResult result = delegate.apply(commandRecord.command(), position);
                lastCatchupPosition = position;
                results.add(SourceReplayOutcome.command(position, result));
            } else if (record instanceof SourceReplayMutation mutationRecord) {
                final SystemMutationResult result = delegate.applySystemMutation(mutationRecord.mutation(), position,
                        verificationKey);
                lastCatchupPosition = position;
                results.add(SourceReplayOutcome.systemMutation(position, result));
            } else {
                throw new IllegalArgumentException("unsupported source replay entry: " + record.getClass());
            }
        }
        return List.copyOf(results);
    }

    private void ensureReplayWindow(final long nowEpochMs) {
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("shard is not catching up");
        }
        if (!lease.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease expired during source catch-up");
        }
    }

    private void validateReplayPosition(final SourcePosition position, final Long sourceConnectionGeneration,
                                        final byte[] guardAttestationDigest) {
        Objects.requireNonNull(position, "source replay position");
        if (!delegate.shardId().equals(position.shardId())) {
            throw new IllegalArgumentException("source replay position does not belong to shard");
        }
        if (activationBarrier != null) {
            activationBarrier.validatePosition(position);
            validateSourceConnection(position, sourceConnectionGeneration, guardAttestationDigest);
        }
        if (lastCatchupPosition != null && position.compareTo(lastCatchupPosition) < 0) {
            throw new IllegalStateException("source replay position regressed");
        }
    }

    private void validateSourceConnection(final SourcePosition position, final Long connectionGeneration,
                                          final byte[] guardAttestationDigest) {
        if (!(position instanceof io.nereusstream.delay.protocol.PulsarSourcePosition)
                || !(activationBarrier instanceof PulsarActivationBarrier pulsarBarrier)) {
            return;
        }
        if (connectionGeneration == null || connectionGeneration <= 0 || guardAttestationDigest == null) {
            throw new IllegalArgumentException("Pulsar source connection proof is required");
        }
        pulsarBarrier.validateSourceConnection(connectionGeneration, guardAttestationDigest);
    }

    public synchronized void activateForCommands(final long nowEpochMs) {
        ensureActivationPreconditions(nowEpochMs);
        state = ShardLifecycleState.ACTIVE_FOR_COMMANDS;
    }

    /** Completes activation only after the authority CASes the same lease to ACTIVE_FOR_COMMANDS. */
    public synchronized void activateForCommands(final OxiaOwnerLeaseStore authority, final long nowEpochMs) {
        Objects.requireNonNull(authority, "authority");
        ensureActivationPreconditions(nowEpochMs);
        final OwnerLease transitioned;
        try {
            transitioned = authority.transition(lease, ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                    .orElseThrow(() -> new IllegalStateException("owner lease activation CAS was lost"));
        } catch (RuntimeException failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
        if (!transitioned.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease expired during activation CAS");
        }
        lease = transitioned;
        state = ShardLifecycleState.ACTIVE_FOR_COMMANDS;
    }

    private void ensureActivationPreconditions(final long nowEpochMs) {
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("shard has not completed source catch-up");
        }
        if (activationBarrier == null || !activationBarrier.reachedBy(lastCatchupPosition)) {
            throw new IllegalStateException("source activation barrier has not been reached");
        }
        if (!lease.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease expired before activation");
        }
        state = ShardLifecycleState.ACTIVE_FOR_COMMANDS;
    }

    public synchronized void beginDrain() {
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("only an active shard can drain");
        }
        state = ShardLifecycleState.DRAINING;
    }

    public synchronized OwnerLease lease() {
        return lease;
    }

    public synchronized SourceAssignment sourceAssignment() {
        return sourceAssignment;
    }

    public synchronized ShardLifecycleState state() {
        return state;
    }

    private void ensureActive(final long nowEpochMs) {
        if (!lease.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("shard owner lease is not active");
        }
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("shard lifecycle is not active for commands: " + state);
        }
    }
}
