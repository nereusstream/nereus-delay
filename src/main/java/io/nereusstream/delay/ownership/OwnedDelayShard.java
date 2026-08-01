package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.PreparedCommand;
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
    private SourcePosition lastCatchupPosition;

    public OwnedDelayShard(final DelayShard delegate, final OwnerLease lease) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.state = ShardLifecycleState.RESTORING;
    }

    public synchronized CommandResult apply(final PreparedCommand command, final SourcePosition position,
                                            final long nowEpochMs) {
        ensureActive(nowEpochMs);
        return delegate.apply(command, position);
    }

    public synchronized void updateLease(final OwnerLease renewed) {
        if (!renewed.shardId().equals(lease.shardId()) || renewed.ownerEpoch() != lease.ownerEpoch()) {
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
        markCatchingUp(null);
    }

    public synchronized void markCatchingUp(final SourceActivationBarrier barrier) {
        if (state != ShardLifecycleState.RESTORING) {
            throw new IllegalStateException("shard is not restoring");
        }
        if (barrier != null && !delegate.shardId().equals(barrier.shardId())) {
            throw new IllegalArgumentException("activation barrier does not belong to shard source");
        }
        activationBarrier = barrier;
        lastCatchupPosition = delegate.lastAppliedSourcePosition();
        state = ShardLifecycleState.CATCHING_UP;
    }

    public synchronized void recordCatchup(final SourcePosition position) {
        Objects.requireNonNull(position, "position");
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("shard is not catching up");
        }
        if (!delegate.shardId().equals(position.shardId())) {
            throw new IllegalArgumentException("catch-up position does not belong to shard");
        }
        if (lastCatchupPosition != null && position.compareTo(lastCatchupPosition) < 0) {
            throw new IllegalStateException("catch-up position regressed");
        }
        lastCatchupPosition = position;
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
