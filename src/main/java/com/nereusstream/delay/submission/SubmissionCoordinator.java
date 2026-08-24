package com.nereusstream.delay.submission;

import com.nereusstream.delay.protocol.PreparedSubmissionV1;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.transport.TransportOwnershipPermit;
import java.util.concurrent.CompletionStage;

/** Post-preparation transport ownership and outcome coordination contract. */
public interface SubmissionCoordinator {
    CompletionStage<SubmissionOutcomeMessageV1> submit(
            AuthenticatedTenantContext tenant,
            PreparedSubmissionV1 submission,
            TransportOwnershipPermit ownershipPermit);
}
