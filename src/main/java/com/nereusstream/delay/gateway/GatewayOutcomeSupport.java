package com.nereusstream.delay.gateway;

import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.FailureStage;
import com.nereusstream.delay.protocol.NativeDefinitelyNotQueued;
import com.nereusstream.delay.protocol.NativeEnqueueUncertain;
import com.nereusstream.delay.protocol.NativePreparedDelivery;
import com.nereusstream.delay.protocol.NonPersistenceProof;
import com.nereusstream.delay.protocol.NonPersistenceProofKind;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableError;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

final class GatewayOutcomeSupport {
    private GatewayOutcomeSupport() {}

    static SubmissionOutcomeMessage uncertain(
            final PreparedSubmission submission, final PhysicalEnqueueAttemptId attempt) {
        if (submission.isManaged()) {
            final PreparedCommand command = CommandCodec.decodeManagedFrame(submission.managedFrame());
            return SubmissionOutcomeMessage.managed(WireIngressOutcomeSupport.uncertain(
                    command, attempt.bytes(), StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
        final NativePreparedDelivery prepared = submission.nativePrepared();
        final var error = StableError.of(
                FailureStage.ENQUEUE,
                StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN,
                null,
                null,
                prepared.preparedRef(),
                null);
        return SubmissionOutcomeMessage.nativeUncertain(
                new NativeEnqueueUncertain(prepared.preparedRef(), attempt.bytes(), error));
    }

    static SubmissionOutcomeMessage localDefinite(final PreparedSubmission submission, final StableCode code) {
        if (submission.isManaged()) {
            final PreparedCommand command = CommandCodec.decodeManagedFrame(submission.managedFrame());
            return SubmissionOutcomeMessage.managed(WireIngressOutcomeSupport.localDefinite(command, code));
        }
        final NativePreparedDelivery prepared = submission.nativePrepared();
        final var ref = prepared.preparedRef();
        final var proof = NonPersistenceProof.create(
                NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, ref.submissionHash(), null, null, null);
        final var error = StableError.of(FailureStage.ENQUEUE, code, null, null, ref, null);
        return SubmissionOutcomeMessage.nativeDefinitelyNotQueued(new NativeDefinitelyNotQueued(ref, proof, error));
    }
}
