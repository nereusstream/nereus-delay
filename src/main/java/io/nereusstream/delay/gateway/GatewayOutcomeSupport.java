package io.nereusstream.delay.gateway;

import io.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.FailureStageV1;
import io.nereusstream.delay.protocol.NativeEnqueueUncertainV1;
import io.nereusstream.delay.protocol.NativeDefinitelyNotQueuedV1;
import io.nereusstream.delay.protocol.NativePreparedDeliveryV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import io.nereusstream.delay.protocol.NonPersistenceProofV1;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.StableErrorV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

final class GatewayOutcomeSupport {
    private GatewayOutcomeSupport() {
    }

    static SubmissionOutcomeMessageV1 uncertain(final PreparedSubmissionV1 submission,
                                                final PhysicalEnqueueAttemptId attempt) {
        if (submission.isManaged()) {
            final PreparedCommand command = CommandCodec.decodeFrameV1(submission.managedFrame());
            return SubmissionOutcomeMessageV1.managed(WireIngressOutcomeSupport.uncertain(command, attempt.bytes(),
                    StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
        final NativePreparedDeliveryV1 prepared = submission.nativePrepared();
        final var error = StableErrorV1.of(FailureStageV1.ENQUEUE, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN,
                null, null, prepared.preparedRef(), null);
        return SubmissionOutcomeMessageV1.nativeUncertain(new NativeEnqueueUncertainV1(prepared.preparedRef(),
                attempt.bytes(), error));
    }

    static SubmissionOutcomeMessageV1 localDefinite(final PreparedSubmissionV1 submission,
                                                    final StableCode code) {
        if (submission.isManaged()) {
            final PreparedCommand command = CommandCodec.decodeFrameV1(submission.managedFrame());
            return SubmissionOutcomeMessageV1.managed(WireIngressOutcomeSupport.localDefinite(command, code));
        }
        final NativePreparedDeliveryV1 prepared = submission.nativePrepared();
        final var ref = prepared.preparedRef();
        final var proof = NonPersistenceProofV1.create(NonPersistenceProofKindV1.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
                null, ref.submissionHash(), null, null, null);
        final var error = StableErrorV1.of(FailureStageV1.ENQUEUE, code, null, null, ref, null);
        return SubmissionOutcomeMessageV1.nativeDefinitelyNotQueued(
                new NativeDefinitelyNotQueuedV1(ref, proof, error));
    }
}
