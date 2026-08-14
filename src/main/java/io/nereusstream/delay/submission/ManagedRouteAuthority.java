package io.nereusstream.delay.submission;

import io.nereusstream.delay.protocol.RouteSnapshotV1;

import java.util.Objects;

/** Historical signed Route authority for a managed prepared command. */
public record ManagedRouteAuthority(RouteSnapshotV1 historicalRoute) implements SubmissionRouteAuthority {
    public ManagedRouteAuthority {
        Objects.requireNonNull(historicalRoute, "historicalRoute");
    }
}
