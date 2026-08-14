package io.nereusstream.delay.submission;

import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;

/** Resolves only exact historical authority already committed in a prepared submission. */
public interface SubmissionTransportPlanResolver {
    SubmissionTransportPlan resolve(AuthenticatedTenantContext tenant, PreparedSubmissionV1 submission);
}
