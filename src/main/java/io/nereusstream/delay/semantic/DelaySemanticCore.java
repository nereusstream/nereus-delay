package io.nereusstream.delay.semantic;

import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.MessagePreconditionV1;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.SubmissionModeV1;

/** Shared zero-I/O preparation API used by Direct SDK and Gateway compositions. */
public interface DelaySemanticCore {
    PreparedSubmissionV1 prepareSchedule(AuthenticatedTenantContext tenant, RouteSelectionHint route,
                                          ScheduleIntentV1 intent, long retryUntilEpochMs,
                                          SubmissionModeV1 submissionMode);

    PreparedCommand prepareLargeSchedule(AuthenticatedTenantContext tenant, RouteSelectionHint route,
                                         LargeSchedulePreparationV1 request, long retryUntilEpochMs);

    PreparedCommand preparePayloadCommit(AuthenticatedTenantContext tenant,
                                         PayloadReservationReceiptV1 reservation, PayloadCommitProofV1 proof,
                                         long retryUntilEpochMs);

    PreparedCommand prepareCancel(AuthenticatedTenantContext tenant, DelayMessageId messageId,
                                  MessagePreconditionV1 precondition, long retryUntilEpochMs);

    PreparedCommand prepareReschedule(AuthenticatedTenantContext tenant, DelayMessageId messageId,
                                      MessagePreconditionV1 precondition, long deliverAtEpochMs,
                                      long expireAtEpochMs, long retryUntilEpochMs);

    PreparedSubmissionV1 prepareManaged(AuthenticatedTenantContext tenant, PreparedCommand command);
}
