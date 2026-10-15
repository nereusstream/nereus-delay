package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.RouteSnapshot;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Deterministic revision-CAS assignment authority for conformance tests. */
public final class InMemoryWorkerAssignmentAuthority implements WorkerAssignmentAuthority {
    private final Map<ShardId, Publication> assignments = new HashMap<>();

    @Override
    public synchronized Publication publish(final WorkerAssignment assignment, final long expectedRevision) {
        Objects.requireNonNull(assignment, "assignment");
        requireExpectedRevision(expectedRevision);
        final Publication current =
                assignments.get(assignment.sourceAssignment().shardId());
        final long observedRevision = current == null ? 0 : current.revision();
        if (observedRevision != expectedRevision) {
            throw new IllegalStateException("worker assignment revision changed");
        }
        if (current != null && current.assignment().sameIdentity(assignment)) {
            return current;
        }
        validateEpochSuccessor(current, assignment);
        final long nextRevision;
        try {
            nextRevision = Math.addExact(expectedRevision, 1);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("worker assignment publication revision exhausted", overflow);
        }
        final Publication next = new Publication(nextRevision, assignment);
        assignments.put(assignment.sourceAssignment().shardId(), next);
        return next;
    }

    @Override
    public synchronized Optional<Publication> current(final ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId");
        return Optional.ofNullable(assignments.get(shardId));
    }

    @Override
    public synchronized Publication publishReactivation(Publication previous, WorkerAssignment successor, RouteSnapshot route) {
        previous.requireRouteProjection(route);
        final var receipt = PulsarSourceReactivationReceipt.next(previous, successor);
        final var next = new Publication(Math.addExact(previous.revision(), 1), successor, receipt);
        final var shard = successor.sourceAssignment().shardId();
        final var current = assignments.get(shard);
        if (next.sameIdentity(current)) return current;
        if (!previous.sameIdentity(current)) throw new IllegalStateException("source reactivation publication changed");
        current.requireRouteProjection(route);
        assignments.put(shard, next);
        return next;
    }

    @Override
    public synchronized boolean withdraw(final Publication expected) {
        Objects.requireNonNull(expected, "expected");
        final ShardId shardId = expected.assignment().sourceAssignment().shardId();
        final Publication current = assignments.get(shardId);
        if (current == null || !current.sameIdentity(expected)) {
            return false;
        }
        assignments.remove(shardId);
        return true;
    }

    private static void requireExpectedRevision(final long expectedRevision) {
        if (expectedRevision < 0) {
            throw new IllegalArgumentException("expected assignment revision must be non-negative");
        }
    }

    private static void validateEpochSuccessor(final Publication current, final WorkerAssignment next) {
        if (current != null
                && Long.compareUnsigned(
                                next.placementEpoch(), current.assignment().placementEpoch())
                        <= 0) {
            throw new IllegalArgumentException("replacement assignment epoch is not newer");
        }
    }
}
