package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ShardId;

import java.util.Optional;

/** Metadata authority boundary; production wiring uses Oxia, tests use an in-memory CAS. */
public interface OwnerLeaseStore {
    Optional<OwnerLease> acquire(ShardId shardId, String ownerId, long nowEpochMs, long leaseDurationMs);

    Optional<OwnerLease> renew(OwnerLease expected, long nowEpochMs, long leaseDurationMs);

    boolean release(OwnerLease expected);

    Optional<OwnerLease> current(ShardId shardId);
}

