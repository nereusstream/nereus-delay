package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ShardId;

import java.util.Optional;

/** Metadata authority boundary; production wiring uses Oxia, tests use an in-memory CAS. */
public interface OwnerLeaseStore {
    Optional<OwnerLease> acquire(ShardId shardId, String ownerId, long nowEpochMs, long leaseDurationMs);

    /** Context-bound acquisition; implementations must CAS assignment/session identity atomically. */
    default Optional<OwnerLease> acquire(final SourceAssignment assignment, final String ownerId,
                                         final byte[] sessionIdentity, final long nowEpochMs,
                                         final long leaseDurationMs) {
        return acquire(assignment.shardId(), ownerId, nowEpochMs, leaseDurationMs);
    }

    Optional<OwnerLease> renew(OwnerLease expected, long nowEpochMs, long leaseDurationMs);

    boolean release(OwnerLease expected);

    /** Context/state CAS; a backend may return empty when the expected lease no longer matches. */
    default Optional<OwnerLease> transition(final OwnerLease expected, final ShardLifecycleState nextState) {
        return Optional.empty();
    }

    Optional<OwnerLease> current(ShardId shardId);
}
