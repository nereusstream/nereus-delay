package io.nereusstream.delay.submission;

import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.transport.CommandTransportKey;
import io.nereusstream.delay.transport.TransportRequest;

import java.util.Objects;

/** Immutable exact Route/transport plan derived from prepared bytes. */
public record SubmissionTransportPlan(
        PreparedSubmissionV1 submission,
        SubmissionRouteAuthority routeAuthority,
        CommandTransportKey transportKey,
        TransportRequest request,
        SubmissionProjectionKey projectionKey) {
    public SubmissionTransportPlan {
        Objects.requireNonNull(submission, "submission");
        Objects.requireNonNull(routeAuthority, "routeAuthority");
        Objects.requireNonNull(transportKey, "transportKey");
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(projectionKey, "projectionKey");
        if (projectionKey.adapterKind() != transportKey.kind()) {
            throw new IllegalArgumentException("projection and transport adapter kinds disagree");
        }
    }
}
