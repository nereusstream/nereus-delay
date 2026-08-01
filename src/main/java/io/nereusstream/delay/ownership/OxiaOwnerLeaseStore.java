package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ShardId;

import java.util.Objects;
import java.util.Optional;

/**
 * Validation boundary for an Oxia-backed owner lease authority.
 *
 * <p>The backend owns the actual Oxia session/ephemeral record and CAS. This
 * adapter never turns a missing or malformed backend result into a local
 * success, and it keeps the in-memory authority useful as a deterministic
 * test backend.</p>
 */
public final class OxiaOwnerLeaseStore implements OwnerLeaseStore {
    private final LeaseCasBackend backend;

    public OxiaOwnerLeaseStore(final LeaseCasBackend backend) {
        this.backend = Objects.requireNonNull(backend, "backend");
    }

    /** Uses an existing lease authority as a deterministic adapter/test backend. */
    public OxiaOwnerLeaseStore(final OwnerLeaseStore backend) {
        this(new DelegatingBackend(backend));
    }

    @Override
    public Optional<OwnerLease> acquire(final ShardId shardId, final String ownerId, final long nowEpochMs,
                                        final long leaseDurationMs) {
        validateRequest(shardId, ownerId, nowEpochMs, leaseDurationMs);
        return validateAcquired(backend.acquire(shardId, ownerId, nowEpochMs, leaseDurationMs), shardId, ownerId,
                nowEpochMs);
    }

    @Override
    public Optional<OwnerLease> renew(final OwnerLease expected, final long nowEpochMs,
                                      final long leaseDurationMs) {
        Objects.requireNonNull(expected, "expected");
        validateRequest(expected.shardId(), expected.ownerId(), nowEpochMs, leaseDurationMs);
        final Optional<OwnerLease> result = backend.renew(expected, nowEpochMs, leaseDurationMs);
        if (result.isEmpty()) {
            return Optional.empty();
        }
        final OwnerLease renewed = result.get();
        if (!sameIdentity(expected, renewed) || renewed.expiresAtEpochMs() < expected.expiresAtEpochMs()
                || !renewed.validAt(nowEpochMs)) {
            throw new IllegalStateException("Oxia lease renewal changed fenced identity or expiry");
        }
        return Optional.of(renewed);
    }

    @Override
    public boolean release(final OwnerLease expected) {
        Objects.requireNonNull(expected, "expected");
        return backend.release(expected);
    }

    @Override
    public Optional<OwnerLease> current(final ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId");
        final Optional<OwnerLease> result = backend.current(shardId);
        if (result.isEmpty()) {
            return Optional.empty();
        }
        final OwnerLease lease = result.get();
        if (!shardId.equals(lease.shardId())) {
            throw new IllegalStateException("Oxia current lease belongs to another shard");
        }
        return Optional.of(lease);
    }

    private static Optional<OwnerLease> validateAcquired(final Optional<OwnerLease> result, final ShardId shardId,
                                                         final String ownerId, final long nowEpochMs) {
        Objects.requireNonNull(result, "backend acquire result");
        if (result.isEmpty()) {
            return Optional.empty();
        }
        final OwnerLease lease = result.get();
        if (!shardId.equals(lease.shardId()) || !ownerId.equals(lease.ownerId())
                || !lease.validAt(nowEpochMs)) {
            throw new IllegalStateException("Oxia acquire result is not bound to the requested lease");
        }
        return Optional.of(lease);
    }

    private static boolean sameIdentity(final OwnerLease expected, final OwnerLease actual) {
        return expected.shardId().equals(actual.shardId()) && expected.ownerId().equals(actual.ownerId())
                && expected.ownerEpoch() == actual.ownerEpoch()
                && Bytes.constantTimeEquals(expected.leaseToken(), actual.leaseToken());
    }

    private static void validateRequest(final ShardId shardId, final String ownerId, final long nowEpochMs,
                                        final long leaseDurationMs) {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(ownerId, "ownerId");
        if (ownerId.isBlank() || nowEpochMs < 0 || leaseDurationMs <= 0) {
            throw new IllegalArgumentException("invalid Oxia owner lease request");
        }
    }

    /** Minimal CAS/session surface implemented by the real Oxia client. */
    public interface LeaseCasBackend {
        Optional<OwnerLease> acquire(ShardId shardId, String ownerId, long nowEpochMs, long leaseDurationMs);

        Optional<OwnerLease> renew(OwnerLease expected, long nowEpochMs, long leaseDurationMs);

        boolean release(OwnerLease expected);

        Optional<OwnerLease> current(ShardId shardId);
    }

    private static final class DelegatingBackend implements LeaseCasBackend {
        private final OwnerLeaseStore delegate;

        private DelegatingBackend(final OwnerLeaseStore delegate) {
            this.delegate = Objects.requireNonNull(delegate, "backend");
        }

        @Override
        public Optional<OwnerLease> acquire(final ShardId shardId, final String ownerId, final long nowEpochMs,
                                            final long leaseDurationMs) {
            return delegate.acquire(shardId, ownerId, nowEpochMs, leaseDurationMs);
        }

        @Override
        public Optional<OwnerLease> renew(final OwnerLease expected, final long nowEpochMs,
                                          final long leaseDurationMs) {
            return delegate.renew(expected, nowEpochMs, leaseDurationMs);
        }

        @Override
        public boolean release(final OwnerLease expected) {
            return delegate.release(expected);
        }

        @Override
        public Optional<OwnerLease> current(final ShardId shardId) {
            return delegate.current(shardId);
        }
    }
}
