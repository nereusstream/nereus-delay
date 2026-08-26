package com.nereusstream.delay.submission;

import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import com.nereusstream.delay.transport.TransportResult;

/** Converts one typed physical result into the frozen managed/native outcome union. */
public interface SubmissionOutcomeProjector {
    SubmissionProjectionKey key();

    SubmissionOutcomeMessage project(
            SubmissionTransportPlan plan, PhysicalEnqueueAttemptId physicalAttemptId, TransportResult result);

    SubmissionOutcomeMessage localFailure(
            SubmissionTransportPlan plan, PhysicalEnqueueAttemptId physicalAttemptId, StableCode code);

    SubmissionOutcomeMessage uncertain(
            SubmissionTransportPlan plan, PhysicalEnqueueAttemptId physicalAttemptId, StableCode code);
}
