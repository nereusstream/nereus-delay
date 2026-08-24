package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ShardId;
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
    public synchronized boolean withdraw(final Publication expected) {
        Objects.requireNonNull(expected, "expected");
        final ShardId shardId = expected.assignment().sourceAssignment().shardId();
        final Publication current = assignments.get(shardId);
        if (current == null
                || current.revision() != expected.revision()
                || !current.assignment().sameIdentity(expected.assignment())) {
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
