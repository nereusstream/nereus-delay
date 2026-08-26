package com.nereusstream.delay.route;

import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.RouteSelectionHint;

/** Read-only local view used by preparation; implementations refresh it out of band. */
public interface RouteSnapshotProvider {
    RouteSnapshot activeForNewSchedule(AuthenticatedTenantContext context, RouteSelectionHint hint);

    RouteSnapshot exact(RouteIncarnation incarnation, AuthenticatedTenantContext context);

    long publishedRevision();
}
