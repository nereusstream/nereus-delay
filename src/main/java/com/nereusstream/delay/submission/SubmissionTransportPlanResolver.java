package com.nereusstream.delay.submission;

import com.nereusstream.delay.protocol.PreparedSubmissionV1;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;

/** Resolves only exact historical authority already committed in a prepared submission. */
public interface SubmissionTransportPlanResolver {
    SubmissionTransportPlan resolve(AuthenticatedTenantContext tenant, PreparedSubmissionV1 submission);
}
