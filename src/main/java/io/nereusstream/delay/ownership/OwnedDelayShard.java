package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.SourceActivationBarrier;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.runtime.CommandResult;
import io.nereusstream.delay.runtime.DelayShard;

import java.util.Objects;

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
        if (!renewed.shardId().equals(lease.shardId()) || !renewed.ownerId().equals(lease.ownerId())
                || renewed.ownerEpoch() != lease.ownerEpoch()
                || !io.nereusstream.delay.protocol.Bytes.constantTimeEquals(renewed.leaseToken(), lease.leaseToken())
                || renewed.expiresAtEpochMs() < lease.expiresAtEpochMs()) {
            throw new IllegalArgumentException("lease renewal changed owner identity/epoch");
        }
        lease = renewed;
    }

    public synchronized void fence() {
        state = ShardLifecycleState.FENCED;
    }

    /** @deprecated use {@link #markCatchingUp(SourceActivationBarrier)}. */
    @Deprecated
    public synchronized void markCatchingUp() {
        markCatchingUp((SourceActivationBarrier) null);
    }

    public synchronized void markCatchingUp(final SourceActivationBarrier barrier) {
        if (sourceAssignment == null) {
            throw new IllegalStateException("source assignment must be accepted before catch-up");
        }
        if (sourceAssignment.activationBarrier() != barrier) {
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
