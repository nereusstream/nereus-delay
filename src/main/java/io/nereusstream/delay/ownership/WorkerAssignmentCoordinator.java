package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.store.WorkerPlacementPolicy;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Binds deterministic placement scoring to an authoritative assignment CAS.
 * The scorer never owns assignment state and the authority never invents a
 * worker choice.
 */
public final class WorkerAssignmentCoordinator {
    private final WorkerPlacementPolicy placementPolicy;
    private final WorkerAssignmentAuthority authority;

    public WorkerAssignmentCoordinator(final WorkerPlacementPolicy placementPolicy,
                                       final WorkerAssignmentAuthority authority) {
        this.placementPolicy = Objects.requireNonNull(placementPolicy, "placementPolicy");
        this.authority = Objects.requireNonNull(authority, "authority");
    }

    public PlacementResult place(final SourceAssignment sourceAssignment,
                                 final byte[] capacityEnvelopeDigest,
                                 final long placementEpoch,
                                 final List<WorkerPlacementPolicy.WorkerCandidate> candidates,
                                 final io.nereusstream.delay.protocol.CapacityVectorV1 incomingShardCapacity,
                                 final io.nereusstream.delay.protocol.CapacityVectorV1 workerFixedCost,
                                 final io.nereusstream.delay.protocol.CapacityVectorV1 transitionDemand,
                                 final String currentWorkerId,
                                 final long nowEpochMs,
                                 final long movementBytes,
                                 final long expectedRevision) {
        return place(sourceAssignment, capacityEnvelopeDigest, new byte[0], placementEpoch, candidates,
                incomingShardCapacity, workerFixedCost, transitionDemand, currentWorkerId, nowEpochMs,
                movementBytes, expectedRevision);
    }

    /**
     * Scores and publishes an assignment optionally bound to a signed Route
     * snapshot digest.  The empty digest overload above preserves the local
     * source-assignment seam used by non-Route composition tests.
     */
    public PlacementResult place(final SourceAssignment sourceAssignment,
                                 final byte[] capacityEnvelopeDigest,
                                 final byte[] routeSnapshotDigest,
                                 final long placementEpoch,
                                 final List<WorkerPlacementPolicy.WorkerCandidate> candidates,
                                 final io.nereusstream.delay.protocol.CapacityVectorV1 incomingShardCapacity,
                                 final io.nereusstream.delay.protocol.CapacityVectorV1 workerFixedCost,
                                 final io.nereusstream.delay.protocol.CapacityVectorV1 transitionDemand,
                                 final String currentWorkerId,
                                 final long nowEpochMs,
                                 final long movementBytes,
                                 final long expectedRevision) {
        Objects.requireNonNull(sourceAssignment, "sourceAssignment");
        Bytes.requireLength(capacityEnvelopeDigest, 32, "capacityEnvelopeDigest");
        Objects.requireNonNull(routeSnapshotDigest, "routeSnapshotDigest");
        if (placementEpoch == 0 || expectedRevision < 0) {
            throw new IllegalArgumentException("placement epoch or expected revision is invalid");
        }
        final WorkerPlacementPolicy.Decision decision = placementPolicy.select(candidates,
                incomingShardCapacity, workerFixedCost, transitionDemand, currentWorkerId, nowEpochMs,
                movementBytes);
        if (decision.reason() == WorkerPlacementPolicy.DecisionReason.NO_CAPACITY) {
            return new PlacementResult(decision, Optional.empty());
        }
        final WorkerAssignment assignment = new WorkerAssignment(decision.workerId(), sourceAssignment,
                placementEpoch, capacityEnvelopeDigest, routeSnapshotDigest);
        return new PlacementResult(decision, Optional.of(authority.publish(assignment, expectedRevision)));
    }

    /**
     * Reads the assignment that the Worker is about to accept and rejects a
     * stale revision or changed canonical bytes before any native setup.
     */
    public WorkerAssignment requireAccepted(final ShardId shardId, final long expectedRevision,
                                             final WorkerAssignment expectedAssignment) {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(expectedAssignment, "expectedAssignment");
        if (expectedRevision <= 0) {
            throw new IllegalArgumentException("accepted assignment revision must be positive");
        }
        final WorkerAssignmentAuthority.Publication observed = authority.current(shardId)
                .orElseThrow(() -> new IllegalStateException("Worker assignment is unavailable"));
        if (observed.revision() != expectedRevision || !observed.assignment().sameIdentity(expectedAssignment)) {
            throw new IllegalStateException("Worker assignment changed before acceptance");
        }
        return observed.assignment();
    }

    public record PlacementResult(WorkerPlacementPolicy.Decision decision,
                                  Optional<WorkerAssignmentAuthority.Publication> publication) {
        public PlacementResult {
            Objects.requireNonNull(decision, "decision");
            publication = Objects.requireNonNull(publication, "publication");
            if (decision.reason() == WorkerPlacementPolicy.DecisionReason.NO_CAPACITY && publication.isPresent()) {
                throw new IllegalArgumentException("NO_CAPACITY cannot publish an assignment");
            }
            if (decision.reason() != WorkerPlacementPolicy.DecisionReason.NO_CAPACITY && publication.isEmpty()) {
                throw new IllegalArgumentException("selected placement must publish an assignment");
            }
        }
    }
}
