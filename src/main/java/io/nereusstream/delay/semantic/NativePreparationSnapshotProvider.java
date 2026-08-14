package io.nereusstream.delay.semantic;

import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ScheduleIntentV1;

import java.util.Optional;

/** Read-only local AUTO_FAST eligibility view. */
@FunctionalInterface
public interface NativePreparationSnapshotProvider {
    Optional<NativePreparationSnapshotV1> eligibleFor(AuthenticatedTenantContext context,
                                                       RouteSnapshotV1 managedRoute,
                                                       ScheduleIntentV1 intent,
                                                       TrustedTimeSnapshot trustedTime);

    /**
     * Extended zero-I/O seam that can use the already-frozen managed identity.
     * Providers that do not need the identity retain the original functional
     * method; target partition policies that hash the Delay Message ID use
     * this overload.
     */
    default Optional<NativePreparationSnapshotV1> eligibleFor(final AuthenticatedTenantContext context,
                                                               final RouteSnapshotV1 managedRoute,
                                                               final ScheduleIntentV1 intent,
                                                               final PreparedCommand managedCommand,
                                                               final TrustedTimeSnapshot trustedTime) {
        return eligibleFor(context, managedRoute, intent, trustedTime);
    }
}
