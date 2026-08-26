package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.SubmissionMode;

/** Shared zero-I/O preparation API used by Direct SDK and Gateway compositions. */
public interface DelaySemanticCore {
    PreparedSubmission prepareSchedule(
            AuthenticatedTenantContext tenant,
            RouteSelectionHint route,
            CanonicalScheduleIntent intent,
            long retryUntilEpochMs,
            SubmissionMode submissionMode);

    PreparedCommand prepareLargeSchedule(
            AuthenticatedTenantContext tenant,
            RouteSelectionHint route,
            LargeSchedulePreparation request,
            long retryUntilEpochMs);

    PreparedCommand preparePayloadCommit(
            AuthenticatedTenantContext tenant,
            PayloadReservationReceipt reservation,
            CanonicalPayloadCommitProof proof,
            long retryUntilEpochMs);

    PreparedCommand prepareCancel(
            AuthenticatedTenantContext tenant,
            DelayMessageId messageId,
            MessagePrecondition precondition,
            long retryUntilEpochMs);

    PreparedCommand prepareReschedule(
            AuthenticatedTenantContext tenant,
            DelayMessageId messageId,
            MessagePrecondition precondition,
            long deliverAtEpochMs,
            long expireAtEpochMs,
            long retryUntilEpochMs);

    PreparedSubmission prepareManaged(AuthenticatedTenantContext tenant, PreparedCommand command);
}
