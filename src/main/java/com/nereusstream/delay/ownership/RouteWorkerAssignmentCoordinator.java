package com.nereusstream.delay.ownership;

import com.nereusstream.delay.assessment.DataResetActivationGate;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.route.RouteSnapshotProvider;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.store.WorkerPlacementPolicy;
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
    private final ProtocolActivationAuthorityCoordinator protocolAuthority;
    private final DataResetActivationGate dataResetActivationGate;

    public RouteWorkerAssignmentCoordinator(
            final RouteSnapshotProvider routeProvider, final WorkerAssignmentCoordinator workerCoordinator) {
        this(routeProvider, workerCoordinator, null, null);
    }

    /**
     * Creates a Route coordinator that also requires every placement candidate
     * and accepted Worker to advertise the Route's exact protocol tuple in the
     * external capability authority.
     */
    public RouteWorkerAssignmentCoordinator(
            final RouteSnapshotProvider routeProvider,
            final WorkerAssignmentCoordinator workerCoordinator,
            final ProtocolCapabilityAuthority capabilityAuthority) {
        this(routeProvider, workerCoordinator, capabilityAuthority, null);
    }

    /**
     * Creates a Route coordinator with an exact H6 manifest barrier. The
     * capability authority is mandatory in this form because the selected
     * assignment must be checked against the same Worker session evidence.
     */
    public RouteWorkerAssignmentCoordinator(
            final RouteSnapshotProvider routeProvider,
            final WorkerAssignmentCoordinator workerCoordinator,
            final ProtocolCapabilityAuthority capabilityAuthority,
            final DataResetActivationGate dataResetActivationGate) {
        this.routeProvider = Objects.requireNonNull(routeProvider, "routeProvider");
        this.sourceResolver = new RouteSourceAssignmentResolver(routeProvider);
        this.workerCoordinator = Objects.requireNonNull(workerCoordinator, "workerCoordinator");
        this.protocolAuthority =
                capabilityAuthority == null ? null : new ProtocolActivationAuthorityCoordinator(capabilityAuthority);
        if (dataResetActivationGate != null && this.protocolAuthority == null) {
            throw new IllegalArgumentException("H6 manifest assignment gating requires a capability authority");
        }
        this.dataResetActivationGate = dataResetActivationGate;
    }

    /** Publishes a new assignment from the tenant-authorized active Route alias. */
    public RoutePlacementResult placeActive(
            final AuthenticatedTenantContext context, final RouteSelectionHint hint, final PlacementRequest request) {
        final RouteSourceAssignmentResolver.Resolved resolved = sourceResolver.activeResolved(
                context, hint, request.partition(), request.assignmentId(), request.assignmentEpoch());
        return publish(resolved, request);
    }

    /** Publishes a replacement/recovery assignment from an exact historical Route. */
    public RoutePlacementResult placeExact(
            final AuthenticatedTenantContext context,
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
    public WorkerAssignment requireAccepted(
            final AuthenticatedTenantContext context,
            final long expectedRevision,
            final WorkerAssignment expectedAssignment) {
        if (dataResetActivationGate != null) {
            throw new IllegalStateException("trusted time is required for H6 Worker acceptance");
        }
        return requireAcceptedInternal(context, expectedRevision, expectedAssignment, null);
    }

    /** Revalidates Worker acceptance and the H6 manifest at trusted time. */
    public WorkerAssignment requireAccepted(
            final AuthenticatedTenantContext context,
            final long expectedRevision,
            final WorkerAssignment expectedAssignment,
            final long trustedNowEpochMs) {
        if (trustedNowEpochMs < 0) {
            throw new IllegalArgumentException("trustedNowEpochMs must be non-negative");
        }
        return requireAcceptedInternal(context, expectedRevision, expectedAssignment, trustedNowEpochMs);
    }

    private WorkerAssignment requireAcceptedInternal(
            final AuthenticatedTenantContext context,
            final long expectedRevision,
            final WorkerAssignment expectedAssignment,
            final Long trustedNowEpochMs) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(expectedAssignment, "expectedAssignment");
        if (!expectedAssignment.routeBound()) {
            throw new IllegalArgumentException("Worker assignment is not bound to a Route snapshot");
        }
        final SourceAssignment expectedSource = expectedAssignment.sourceAssignment();
        final RouteSourceAssignmentResolver.Resolved resolved = sourceResolver.exactResolved(
                context,
                expectedSource.shardId().routeIncarnation(),
                expectedSource.shardId().partition(),
                expectedSource.assignmentId(),
                expectedSource.assignmentEpoch());
        if (!resolved.sourceAssignment().sameIdentity(expectedSource)
                || !Bytes.constantTimeEquals(
                        resolved.routeSnapshot().snapshotDigest(), expectedAssignment.routeSnapshotDigest())) {
            throw new IllegalStateException("signed Route projection changed before Worker acceptance");
        }
        if (protocolAuthority != null) {
            if (dataResetActivationGate == null) {
                protocolAuthority.requireEligibleReaders(
                        resolved.routeSnapshot().protocolTuple(), List.of(expectedAssignment.workerId()));
            } else {
                protocolAuthority.requireEligibleReaders(
                        resolved.routeSnapshot().protocolTuple(),
                        dataResetActivationGate.artifacts(),
                        List.of(expectedAssignment.workerId()));
            }
        }
        final WorkerAssignment accepted =
                workerCoordinator.requireAccepted(expectedSource.shardId(), expectedRevision, expectedAssignment);
        if (dataResetActivationGate != null) {
            dataResetActivationGate.requireAssignment(
                    accepted,
                    protocolAuthority.requireCurrentDeclaration(accepted.workerId()),
                    Objects.requireNonNull(trustedNowEpochMs, "trustedNowEpochMs"));
        }
        return accepted;
    }

    private RoutePlacementResult publish(
            final RouteSourceAssignmentResolver.Resolved resolved, final PlacementRequest request) {
        if (protocolAuthority != null) {
            final List<String> workerIds = request.candidates().stream()
                    .map(WorkerPlacementPolicy.WorkerCandidate::workerId)
                    .toList();
            if (dataResetActivationGate == null) {
                protocolAuthority.requireEligibleReaders(
                        resolved.routeSnapshot().protocolTuple(), workerIds);
            } else {
                protocolAuthority.requireEligibleReaders(
                        resolved.routeSnapshot().protocolTuple(), dataResetActivationGate.artifacts(), workerIds);
                dataResetActivationGate.requireManifest(request.nowEpochMs());
            }
        }
        final WorkerAssignmentCoordinator.PlacementResult placement = workerCoordinator.place(
                resolved.sourceAssignment(),
                request.capacityEnvelopeDigest(),
                resolved.routeSnapshot().snapshotDigest(),
                request.placementEpoch(),
                request.candidates(),
                request.incomingShardCapacity(),
                request.workerFixedCost(),
                request.transitionDemand(),
                request.currentWorkerId(),
                request.nowEpochMs(),
                request.movementBytes(),
                request.expectedRevision());
        if (dataResetActivationGate != null) {
            final WorkerAssignment assignment =
                    placement.publication().orElseThrow().assignment();
            dataResetActivationGate.requireAssignment(
                    assignment,
                    protocolAuthority.requireCurrentDeclaration(assignment.workerId()),
                    request.nowEpochMs());
        }
        return new RoutePlacementResult(
                routeProvider.publishedRevision(), resolved.routeSnapshot(), resolved.sourceAssignment(), placement);
    }

    public record PlacementRequest(
            int partition,
            byte[] assignmentId,
            long assignmentEpoch,
            byte[] capacityEnvelopeDigest,
            long placementEpoch,
            List<WorkerPlacementPolicy.WorkerCandidate> candidates,
            CapacityVector incomingShardCapacity,
            CapacityVector workerFixedCost,
            CapacityVector transitionDemand,
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
            com.nereusstream.delay.protocol.RouteSnapshot routeSnapshot,
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
            return placement
                    .publication()
                    .orElseThrow(() -> new IllegalStateException("Route placement did not publish an assignment"));
        }
    }
}
