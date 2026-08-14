package io.nereusstream.delay.route;

import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.RouteSelectionHint;

/** Read-only local view used by preparation; implementations refresh it out of band. */
public interface RouteSnapshotProvider {
    RouteSnapshotV1 activeForNewSchedule(AuthenticatedTenantContext context, RouteSelectionHint hint);

    RouteSnapshotV1 exact(RouteIncarnation incarnation, AuthenticatedTenantContext context);

    long publishedRevision();
}
