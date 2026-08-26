package com.nereusstream.delay.submission;

import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;

/** Resolves only exact historical authority already committed in a prepared submission. */
public interface SubmissionTransportPlanResolver {
    SubmissionTransportPlan resolve(AuthenticatedTenantContext tenant, PreparedSubmission submission);
}
