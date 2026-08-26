package com.nereusstream.delay.submission;

import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.transport.CommandTransportKey;
import com.nereusstream.delay.transport.TransportRequest;
import java.util.Objects;

/** Immutable exact Route/transport plan derived from prepared bytes. */
public record SubmissionTransportPlan(
        PreparedSubmission submission,
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
