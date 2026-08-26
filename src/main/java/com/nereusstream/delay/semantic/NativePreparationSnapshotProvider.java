package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.RouteSnapshot;
import java.util.Optional;

/** Read-only local AUTO_FAST eligibility view. */
@FunctionalInterface
public interface NativePreparationSnapshotProvider {
    Optional<NativePreparationSnapshot> eligibleFor(
            AuthenticatedTenantContext context,
            RouteSnapshot managedRoute,
            CanonicalScheduleIntent intent,
            TrustedTimeSnapshot trustedTime);

    /**
     * Extended zero-I/O seam that can use the already-frozen managed identity.
     * Providers that do not need the identity retain the original functional
     * method; target partition policies that hash the Delay Message ID use
     * this overload.
     */
    default Optional<NativePreparationSnapshot> eligibleFor(
            final AuthenticatedTenantContext context,
            final RouteSnapshot managedRoute,
            final CanonicalScheduleIntent intent,
            final PreparedCommand managedCommand,
            final TrustedTimeSnapshot trustedTime) {
        return eligibleFor(context, managedRoute, intent, trustedTime);
    }
}
