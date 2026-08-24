package com.nereusstream.delay.route;

import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteSnapshotV1;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.RouteSelectionHint;

/** Read-only local view used by preparation; implementations refresh it out of band. */
public interface RouteSnapshotProvider {
    RouteSnapshotV1 activeForNewSchedule(AuthenticatedTenantContext context, RouteSelectionHint hint);

    RouteSnapshotV1 exact(RouteIncarnation incarnation, AuthenticatedTenantContext context);

    long publishedRevision();
}
