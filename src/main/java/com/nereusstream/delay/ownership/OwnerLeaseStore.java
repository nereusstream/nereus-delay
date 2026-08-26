package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ShardId;
import java.util.Optional;

/** Metadata authority boundary; production wiring uses Oxia, tests use an in-memory CAS. */
public interface OwnerLeaseStore {
    Optional<OwnerLease> acquire(ShardId shardId, String ownerId, long nowEpochMs, long leaseDurationMs);

    /** Context-bound acquisition; implementations must CAS assignment/session identity atomically. */
    default Optional<OwnerLease> acquire(
            final SourceAssignment assignment,
            final String ownerId,
            final byte[] sessionIdentity,
            final long nowEpochMs,
            final long leaseDurationMs) {
        // Never fall back to shard-only acquisition: doing so would create a
        // lease without the assignment/session fence that requires. An
        // implementation must override this method when it can CAS the full
        // context atomically.
        return Optional.empty();
    }

    Optional<OwnerLease> renew(OwnerLease expected, long nowEpochMs, long leaseDurationMs);

    boolean release(OwnerLease expected);

    /** Context/state CAS; a backend may return empty when the expected lease no longer matches. */
    default Optional<OwnerLease> transition(final OwnerLease expected, final ShardLifecycleState nextState) {
        return Optional.empty();
    }

    Optional<OwnerLease> current(ShardId shardId);
}
