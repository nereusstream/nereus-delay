package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.runtime.CommandResult;
import io.nereusstream.delay.runtime.DelayShard;

import java.util.Objects;

/** Fenced owner view; lease loss closes all new local authority gates. */
public final class OwnedDelayShard {
    private final DelayShard delegate;
    private OwnerLease lease;
    private ShardLifecycleState state;

    public OwnedDelayShard(final DelayShard delegate, final OwnerLease lease) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.state = ShardLifecycleState.ACTIVE_FOR_COMMANDS;
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

    public synchronized OwnerLease lease() {
        return lease;
    }

    public synchronized ShardLifecycleState state() {
        return state;
    }

    private void ensureActive(final long nowEpochMs) {
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS || !lease.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("shard owner lease is not active");
        }
    }
}

