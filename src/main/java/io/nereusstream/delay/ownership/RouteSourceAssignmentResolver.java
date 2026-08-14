package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.route.RouteSnapshotProvider;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.RouteSelectionHint;

import java.util.Objects;

/**
 * Tenant-authorized Route lookup boundary for Worker source assignments.
 *
 * <p>The returned assignment is still a local projection.  The source
 * adapter must publish/accept the assignment and the Owner authority must CAS
 * the matching lease before catch-up or activation.  This class only makes
 * it impossible for a Worker caller to bypass the Route provider when it
 * needs a current or historical Route barrier.</p>
 */
public final class RouteSourceAssignmentResolver {
    private final RouteSnapshotProvider routeProvider;

    public RouteSourceAssignmentResolver(final RouteSnapshotProvider routeProvider) {
        this.routeProvider = Objects.requireNonNull(routeProvider, "routeProvider");
    }

    /** Resolves an ACTIVE_FOR_NEW Route through the tenant-scoped alias path. */
    public SourceAssignment active(final AuthenticatedTenantContext context,
                                   final RouteSelectionHint hint,
                                   final int partition,
                                   final byte[] assignmentId,
                                   final long assignmentEpoch) {
        final RouteSnapshotV1 route = Objects.requireNonNull(
                routeProvider.activeForNewSchedule(Objects.requireNonNull(context, "context"),
                        Objects.requireNonNull(hint, "hint")), "active Route snapshot");
        return fromRoute(route, partition, assignmentId, assignmentEpoch);
    }

    /**
     * Resolves an exact historical Route through the tenant-scoped
     * incarnation path.  A missing or unauthorized snapshot is a hard
     * admission failure; it must not create a name-only source assignment.
     */
    public SourceAssignment exact(final AuthenticatedTenantContext context,
                                  final RouteIncarnation incarnation,
                                  final int partition,
                                  final byte[] assignmentId,
                                  final long assignmentEpoch) {
        final RouteIncarnation expected = Objects.requireNonNull(incarnation, "incarnation");
        final RouteSnapshotV1 route = routeProvider.exact(expected,
                Objects.requireNonNull(context, "context"));
        if (route == null || !expected.equals(route.routeIncarnation())) {
            throw new IllegalArgumentException("authorized historical Route snapshot is unavailable");
        }
        return fromRoute(route, partition, assignmentId, assignmentEpoch);
    }

    private static SourceAssignment fromRoute(final RouteSnapshotV1 route, final int partition,
                                              final byte[] assignmentId, final long assignmentEpoch) {
        Objects.requireNonNull(route, "route");
        Bytes.requireLength(assignmentId, SourceAssignment.ID_LENGTH, "assignmentId");
        return RouteSourceAssignmentFactory.fromRoute(route, partition, assignmentId, assignmentEpoch);
    }
}
