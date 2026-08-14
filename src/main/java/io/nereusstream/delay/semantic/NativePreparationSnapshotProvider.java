package io.nereusstream.delay.semantic;

import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;

import java.util.Optional;

/** Read-only local AUTO_FAST eligibility view. */
@FunctionalInterface
public interface NativePreparationSnapshotProvider {
    Optional<NativePreparationSnapshotV1> eligibleFor(AuthenticatedTenantContext context,
                                                       RouteSnapshotV1 managedRoute,
                                                       ScheduleIntentV1 intent,
                                                       TrustedTimeSnapshot trustedTime);
}
