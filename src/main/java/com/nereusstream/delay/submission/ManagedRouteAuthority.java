package com.nereusstream.delay.submission;

import com.nereusstream.delay.protocol.RouteSnapshot;
import java.util.Objects;

/** Historical signed Route authority for a managed prepared command. */
public record ManagedRouteAuthority(RouteSnapshot historicalRoute) implements SubmissionRouteAuthority {
    public ManagedRouteAuthority {
        Objects.requireNonNull(historicalRoute, "historicalRoute");
    }
}
