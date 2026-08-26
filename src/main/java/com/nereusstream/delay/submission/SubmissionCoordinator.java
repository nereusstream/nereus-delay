package com.nereusstream.delay.submission;

import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.transport.TransportOwnershipPermit;
import java.util.concurrent.CompletionStage;

/** Post-preparation transport ownership and outcome coordination contract. */
public interface SubmissionCoordinator {
    CompletionStage<SubmissionOutcomeMessage> submit(
            AuthenticatedTenantContext tenant, PreparedSubmission submission, TransportOwnershipPermit ownershipPermit);
}
