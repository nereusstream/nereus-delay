package com.nereusstream.delay.submission;

import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import com.nereusstream.delay.transport.TransportResult;

/** Converts one typed physical result into the frozen managed/native outcome union. */
public interface SubmissionOutcomeProjector {
    SubmissionProjectionKey key();

    SubmissionOutcomeMessageV1 project(
            SubmissionTransportPlan plan, PhysicalEnqueueAttemptId physicalAttemptId, TransportResult result);

    SubmissionOutcomeMessageV1 localFailure(
            SubmissionTransportPlan plan, PhysicalEnqueueAttemptId physicalAttemptId, StableCode code);

    SubmissionOutcomeMessageV1 uncertain(
            SubmissionTransportPlan plan, PhysicalEnqueueAttemptId physicalAttemptId, StableCode code);
}
