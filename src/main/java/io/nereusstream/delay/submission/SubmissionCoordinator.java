package io.nereusstream.delay.submission;

import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.transport.TransportOwnershipPermit;

import java.util.concurrent.CompletionStage;

/** Post-preparation transport ownership and outcome coordination contract. */
public interface SubmissionCoordinator {
    CompletionStage<SubmissionOutcomeMessageV1> submit(AuthenticatedTenantContext tenant,
                                                        PreparedSubmissionV1 submission,
                                                        TransportOwnershipPermit ownershipPermit);
}
