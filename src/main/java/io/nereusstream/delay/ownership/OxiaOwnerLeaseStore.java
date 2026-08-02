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

    /** Acquires a lease whose value is bound to the exact assignment and Oxia session. */
    @Override
    public Optional<OwnerLease> acquire(final SourceAssignment assignment, final String ownerId,
                                        final byte[] sessionIdentity, final long nowEpochMs,
                                        final long leaseDurationMs) {
        Objects.requireNonNull(assignment, "assignment");
        final OwnerLeaseContext context = new OwnerLeaseContext(assignment.assignmentId(), assignment.assignmentEpoch(),
                sessionIdentity);
        validateRequest(assignment.shardId(), ownerId, nowEpochMs, leaseDurationMs);
        return validateAcquired(backend.acquire(assignment, ownerId, context.sessionIdentity(), nowEpochMs,
                        leaseDurationMs), assignment.shardId(), ownerId, nowEpochMs, context);
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

    /** CASes the lifecycle state while retaining every fencing identity. */
    @Override
    public Optional<OwnerLease> transition(final OwnerLease expected, final ShardLifecycleState nextState) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(nextState, "nextState");
        if (!expected.state().canTransitionTo(nextState)) {
            return Optional.empty();
        }
        final Optional<OwnerLease> result = backend.transition(expected, nextState);
        if (result.isEmpty()) {
            return Optional.empty();
        }
        final OwnerLease transitioned = result.get();
        if (!sameIdentity(expected, transitioned) || transitioned.state() != nextState
                || transitioned.expiresAtEpochMs() < expected.expiresAtEpochMs()) {
            throw new IllegalStateException("Oxia lease transition changed identity, state, or expiry");
        }
        return Optional.of(transitioned);
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
                                                         final String ownerId, final long nowEpochMs,
                                                         final OwnerLeaseContext expectedContext) {
        final Optional<OwnerLease> validated = validateAcquired(result, shardId, ownerId, nowEpochMs);
        if (validated.isEmpty()) {
            return Optional.empty();
        }
        if (!sameContext(expectedContext, validated.get().context())) {
            throw new IllegalStateException("Oxia acquire result is not bound to assignment/session context");
        }
        return validated;
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
                && Bytes.constantTimeEquals(expected.leaseToken(), actual.leaseToken())
                && sameContext(expected.context(), actual.context());
    }

    private static boolean sameContext(final OwnerLeaseContext expected, final OwnerLeaseContext actual) {
        return Objects.equals(expected, actual);
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

        default Optional<OwnerLease> acquire(final SourceAssignment assignment, final String ownerId,
                                             final byte[] sessionIdentity, final long nowEpochMs,
                                             final long leaseDurationMs) {
            return acquire(assignment.shardId(), ownerId, nowEpochMs, leaseDurationMs);
        }

        Optional<OwnerLease> renew(OwnerLease expected, long nowEpochMs, long leaseDurationMs);

        boolean release(OwnerLease expected);

        default Optional<OwnerLease> transition(final OwnerLease expected, final ShardLifecycleState nextState) {
            return Optional.empty();
        }

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
        public Optional<OwnerLease> acquire(final SourceAssignment assignment, final String ownerId,
                                            final byte[] sessionIdentity, final long nowEpochMs,
                                            final long leaseDurationMs) {
            return delegate.acquire(assignment, ownerId, sessionIdentity, nowEpochMs, leaseDurationMs);
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
        public Optional<OwnerLease> transition(final OwnerLease expected, final ShardLifecycleState nextState) {
            return delegate.transition(expected, nextState);
        }

        @Override
        public Optional<OwnerLease> current(final ShardId shardId) {
            return delegate.current(shardId);
        }
    }
}
