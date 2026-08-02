package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ShardId;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Deterministic-CAS test authority with monotonically increasing owner epochs. */
public final class InMemoryOwnerLeaseStore implements OwnerLeaseStore {
    private final Map<ShardId, OwnerLease> leases = new HashMap<>();
    private final Map<ShardId, Long> epochs = new HashMap<>();
    private final SecureRandom random = new SecureRandom();

    @Override
    public synchronized Optional<OwnerLease> acquire(final ShardId shardId, final String ownerId,
                                                      final long nowEpochMs, final long leaseDurationMs) {
        validateDuration(nowEpochMs, leaseDurationMs);
        final OwnerLease current = leases.get(shardId);
        if (current != null && current.validAt(nowEpochMs)) {
            return Optional.empty();
        }
        final long epoch = Math.addExact(epochs.getOrDefault(shardId, 0L), 1);
        epochs.put(shardId, epoch);
        final OwnerLease next = new OwnerLease(shardId, ownerId, epoch, randomBytes(),
                Math.addExact(nowEpochMs, leaseDurationMs));
        leases.put(shardId, next);
        return Optional.of(next);
    }

    @Override
    public synchronized Optional<OwnerLease> acquire(final SourceAssignment assignment, final String ownerId,
                                                      final byte[] sessionIdentity, final long nowEpochMs,
                                                      final long leaseDurationMs) {
        Objects.requireNonNull(assignment, "assignment");
        validateDuration(nowEpochMs, leaseDurationMs);
        final OwnerLease current = leases.get(assignment.shardId());
        if (current != null && current.validAt(nowEpochMs)) {
            return Optional.empty();
        }
        final long epoch = Math.addExact(epochs.getOrDefault(assignment.shardId(), 0L), 1);
        epochs.put(assignment.shardId(), epoch);
        final OwnerLease next = new OwnerLease(assignment.shardId(), ownerId, epoch, randomBytes(),
                Math.addExact(nowEpochMs, leaseDurationMs),
                new OwnerLeaseContext(assignment.assignmentId(), assignment.assignmentEpoch(), sessionIdentity),
                ShardLifecycleState.ACQUIRING);
        leases.put(assignment.shardId(), next);
        return Optional.of(next);
    }

    @Override
    public synchronized Optional<OwnerLease> renew(final OwnerLease expected, final long nowEpochMs,
                                                    final long leaseDurationMs) {
        validateDuration(nowEpochMs, leaseDurationMs);
        final OwnerLease current = leases.get(expected.shardId());
        if (!same(current, expected) || !current.validAt(nowEpochMs)) {
            return Optional.empty();
        }
        final OwnerLease next = new OwnerLease(expected.shardId(), expected.ownerId(), expected.ownerEpoch(),
                expected.leaseToken(), Math.addExact(nowEpochMs, leaseDurationMs), expected.context(), expected.state());
        leases.put(expected.shardId(), next);
        return Optional.of(next);
    }

    @Override
    public synchronized boolean release(final OwnerLease expected) {
        final OwnerLease current = leases.get(expected.shardId());
        if (!same(current, expected)) {
            return false;
        }
        leases.remove(expected.shardId());
        return true;
    }

    @Override
    public synchronized Optional<OwnerLease> transition(final OwnerLease expected,
                                                         final ShardLifecycleState nextState) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(nextState, "nextState");
        final OwnerLease current = leases.get(expected.shardId());
        if (!same(current, expected) || current.state() != expected.state()) {
            return Optional.empty();
        }
        if (!current.state().canTransitionTo(nextState)) {
            return Optional.empty();
        }
        final OwnerLease next = new OwnerLease(current.shardId(), current.ownerId(), current.ownerEpoch(),
                current.leaseToken(), current.expiresAtEpochMs(), current.context(), nextState);
        leases.put(expected.shardId(), next);
        return Optional.of(next);
    }

    @Override
    public synchronized Optional<OwnerLease> current(final ShardId shardId) {
        return Optional.ofNullable(leases.get(shardId));
    }

    private byte[] randomBytes() {
        final byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        return bytes;
    }

    private static boolean same(final OwnerLease left, final OwnerLease right) {
        return left != null && right != null && left.shardId().equals(right.shardId())
                && left.ownerId().equals(right.ownerId()) && left.ownerEpoch() == right.ownerEpoch()
                && Bytes.constantTimeEquals(left.leaseToken(), right.leaseToken())
                && Objects.equals(left.context(), right.context());
    }

    private static void validateDuration(final long nowEpochMs, final long durationMs) {
        if (nowEpochMs < 0 || durationMs <= 0) {
            throw new IllegalArgumentException("invalid lease time");
        }
    }
}
