package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CapacityVectorV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.route.RouteSnapshotProvider;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.store.WorkerPlacementPolicy;

import java.util.List;
import java.util.Objects;

/**
 * Publishes only assignments projected from an authorized signed Route.
 * Route lookup, local scoring and durable assignment CAS remain separate
 * seams, but a Worker cannot accept the projection after the Route identity or
 * signed partition barrier has changed.
 */
public final class RouteWorkerAssignmentCoordinator {
    private final RouteSnapshotProvider routeProvider;
    private final RouteSourceAssignmentResolver sourceResolver;
    private final WorkerAssignmentCoordinator workerCoordinator;

    public RouteWorkerAssignmentCoordinator(final RouteSnapshotProvider routeProvider,
                                            final WorkerAssignmentCoordinator workerCoordinator) {
        this.routeProvider = Objects.requireNonNull(routeProvider, "routeProvider");
        this.sourceResolver = new RouteSourceAssignmentResolver(routeProvider);
        this.workerCoordinator = Objects.requireNonNull(workerCoordinator, "workerCoordinator");
    }

    /** Publishes a new assignment from the tenant-authorized active Route alias. */
    public RoutePlacementResult placeActive(final AuthenticatedTenantContext context,
                                             final RouteSelectionHint hint,
                                             final PlacementRequest request) {
        final RouteSourceAssignmentResolver.Resolved resolved = sourceResolver.activeResolved(
                context, hint, request.partition(), request.assignmentId(), request.assignmentEpoch());
        return publish(resolved, request);
    }

    /** Publishes a replacement/recovery assignment from an exact historical Route. */
    public RoutePlacementResult placeExact(final AuthenticatedTenantContext context,
                                            final RouteIncarnation incarnation,
                                            final PlacementRequest request) {
        final RouteSourceAssignmentResolver.Resolved resolved = sourceResolver.exactResolved(
                context, incarnation, request.partition(), request.assignmentId(), request.assignmentEpoch());
        return publish(resolved, request);
    }

    /**
     * Revalidates the exact historical Route projection and authority revision
     * before a Worker opens native source state.
     */
    public WorkerAssignment requireAccepted(final AuthenticatedTenantContext context,
                                             final long expectedRevision,
                                             final WorkerAssignment expectedAssignment) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(expectedAssignment, "expectedAssignment");
        if (!expectedAssignment.routeBound()) {
            throw new IllegalArgumentException("Worker assignment is not bound to a Route snapshot");
        }
        final SourceAssignment expectedSource = expectedAssignment.sourceAssignment();
        final RouteSourceAssignmentResolver.Resolved resolved = sourceResolver.exactResolved(
                context, expectedSource.shardId().routeIncarnation(), expectedSource.shardId().partition(),
                expectedSource.assignmentId(), expectedSource.assignmentEpoch());
        if (!resolved.sourceAssignment().sameIdentity(expectedSource)
                || !Bytes.constantTimeEquals(resolved.routeSnapshot().snapshotDigest(),
                expectedAssignment.routeSnapshotDigest())) {
            throw new IllegalStateException("signed Route projection changed before Worker acceptance");
        }
        return workerCoordinator.requireAccepted(expectedSource.shardId(), expectedRevision, expectedAssignment);
    }

    private RoutePlacementResult publish(final RouteSourceAssignmentResolver.Resolved resolved,
                                         final PlacementRequest request) {
        final WorkerAssignmentCoordinator.PlacementResult placement = workerCoordinator.place(
                resolved.sourceAssignment(), request.capacityEnvelopeDigest(),
                resolved.routeSnapshot().snapshotDigest(), request.placementEpoch(), request.candidates(),
                request.incomingShardCapacity(), request.workerFixedCost(), request.transitionDemand(),
                request.currentWorkerId(), request.nowEpochMs(), request.movementBytes(), request.expectedRevision());
        return new RoutePlacementResult(routeProvider.publishedRevision(), resolved.routeSnapshot(),
                resolved.sourceAssignment(), placement);
    }

    public record PlacementRequest(
            int partition,
            byte[] assignmentId,
            long assignmentEpoch,
            byte[] capacityEnvelopeDigest,
            long placementEpoch,
            List<WorkerPlacementPolicy.WorkerCandidate> candidates,
            CapacityVectorV1 incomingShardCapacity,
            CapacityVectorV1 workerFixedCost,
            CapacityVectorV1 transitionDemand,
            String currentWorkerId,
            long nowEpochMs,
            long movementBytes,
            long expectedRevision) {
        public PlacementRequest {
            if (partition < 0) {
                throw new IllegalArgumentException("partition must be non-negative");
            }
            Bytes.requireLength(assignmentId, SourceAssignment.ID_LENGTH, "assignmentId");
            if (assignmentEpoch <= 0 || placementEpoch == 0 || expectedRevision < 0) {
                throw new IllegalArgumentException("assignment or placement epoch is invalid");
            }
            Bytes.requireLength(capacityEnvelopeDigest, 32, "capacityEnvelopeDigest");
            candidates = List.copyOf(Objects.requireNonNull(candidates, "candidates"));
            Objects.requireNonNull(incomingShardCapacity, "incomingShardCapacity");
            Objects.requireNonNull(workerFixedCost, "workerFixedCost");
            Objects.requireNonNull(transitionDemand, "transitionDemand");
            if (nowEpochMs < 0 || movementBytes < 0) {
                throw new IllegalArgumentException("placement time and movement bytes must be non-negative");
            }
            assignmentId = Bytes.copy(assignmentId);
            capacityEnvelopeDigest = Bytes.copy(capacityEnvelopeDigest);
        }

        @Override
        public byte[] assignmentId() {
            return Bytes.copy(assignmentId);
        }

        @Override
        public byte[] capacityEnvelopeDigest() {
            return Bytes.copy(capacityEnvelopeDigest);
        }
    }

    public record RoutePlacementResult(
            long routeRevision,
            io.nereusstream.delay.protocol.RouteSnapshotV1 routeSnapshot,
            SourceAssignment sourceAssignment,
            WorkerAssignmentCoordinator.PlacementResult placement) {
        public RoutePlacementResult {
            if (routeRevision < 0) {
                throw new IllegalArgumentException("routeRevision must be non-negative");
            }
            Objects.requireNonNull(routeSnapshot, "routeSnapshot");
            Objects.requireNonNull(sourceAssignment, "sourceAssignment");
            Objects.requireNonNull(placement, "placement");
        }

        public WorkerAssignmentAuthority.Publication publication() {
            return placement.publication().orElseThrow(() ->
                    new IllegalStateException("Route placement did not publish an assignment"));
        }
    }
}
