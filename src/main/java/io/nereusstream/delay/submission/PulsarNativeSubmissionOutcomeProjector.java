package io.nereusstream.delay.submission;

import io.nereusstream.delay.adapter.PulsarNativeSendRequest;
import io.nereusstream.delay.adapter.PulsarSendResult;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.FailureStageV1;
import io.nereusstream.delay.protocol.NativeDefinitelyNotQueuedV1;
import io.nereusstream.delay.protocol.NativeDeliveryReceiptV1;
import io.nereusstream.delay.protocol.NativeEnqueueUncertainV1;
import io.nereusstream.delay.protocol.NativePreparedDeliveryV1;
import io.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import io.nereusstream.delay.protocol.NonPersistenceProofV1;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.RetryabilityV1;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.StableErrorV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import io.nereusstream.delay.transport.TransportResult;

/** Native Pulsar NDR1 projector; native results never become managed receipts. */
public final class PulsarNativeSubmissionOutcomeProjector implements SubmissionOutcomeProjector {
    private static final io.nereusstream.delay.protocol.AdapterKindV1 KIND =
            io.nereusstream.delay.protocol.AdapterKindV1.PULSAR;

    @Override
    public SubmissionProjectionKey key() {
        return new SubmissionProjectionKey(PreparedSubmissionBranch.NATIVE, KIND);
    }

    @Override
    public SubmissionOutcomeMessageV1 project(final SubmissionTransportPlan plan,
                                              final PhysicalEnqueueAttemptId physicalAttemptId,
                                              final TransportResult result) {
        if (!(plan.routeAuthority() instanceof NativeTargetAuthority authority)
                || !(plan.request() instanceof PulsarNativeSendRequest request)
                || !(result instanceof PulsarSendResult pulsar)) {
            return uncertain(plan, physicalAttemptId, StableCode.INTEGRITY_ERROR);
        }
        final NativePreparedDeliveryV1 prepared = authority.prepared();
        return switch (pulsar.disposition()) {
            case PERSISTED -> persisted(prepared, physicalAttemptId, pulsar);
            case DEFINITIVELY_NOT_PERSISTED -> definite(prepared, request, physicalAttemptId, pulsar);
            case UNKNOWN -> uncertain(plan, physicalAttemptId,
                    SubmissionProjectorSupport.exactNativeRetryCode(pulsar.stableCode()));
        };
    }

    @Override
    public SubmissionOutcomeMessageV1 localFailure(final SubmissionTransportPlan plan,
                                                   final PhysicalEnqueueAttemptId physicalAttemptId,
                                                   final StableCode code) {
        final NativePreparedDeliveryV1 prepared = ((NativeTargetAuthority) plan.routeAuthority()).prepared();
        final var ref = prepared.preparedRef();
        final var proof = NonPersistenceProofV1.create(
                NonPersistenceProofKindV1.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, ref.submissionHash(), null, null,
                null);
        final var error = StableErrorV1.of(FailureStageV1.ENQUEUE, code, null, null, ref, null);
        return SubmissionOutcomeMessageV1.nativeDefinitelyNotQueued(
                new NativeDefinitelyNotQueuedV1(ref, proof, error));
    }

    @Override
    public SubmissionOutcomeMessageV1 uncertain(final SubmissionTransportPlan plan,
                                                final PhysicalEnqueueAttemptId physicalAttemptId,
                                                final StableCode code) {
        final NativePreparedDeliveryV1 prepared = ((NativeTargetAuthority) plan.routeAuthority()).prepared();
        final var ref = prepared.preparedRef();
        final StableCode retryCode = RetryabilityV1.forCode(code) == RetryabilityV1.RETRY_EXACT_BYTES
                ? code : StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN;
        final var error = StableErrorV1.of(FailureStageV1.ENQUEUE, retryCode, null, null, ref, null);
        return SubmissionOutcomeMessageV1.nativeUncertain(
                new NativeEnqueueUncertainV1(ref, physicalAttemptId.bytes(), error));
    }

    private static SubmissionOutcomeMessageV1 persisted(final NativePreparedDeliveryV1 prepared,
                                                         final PhysicalEnqueueAttemptId attempt,
                                                         final PulsarSendResult result) {
        final var target = prepared.target();
        if (!target.authenticatedClusterId().equals(result.authenticatedClusterId())
                || !java.util.Arrays.equals(target.resourceIncarnation(), result.resourceIncarnation())
                || !target.physicalTopic().equals(result.physicalTopic())
                || target.physicalTopicCreationTimestamp() != result.physicalTopicCreationTimestamp()
                || prepared.physicalPartition() != result.partition() || result.responseEvidenceBytes() == null) {
            return uncertainStatic(prepared, attempt, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN);
        }
        final CommandQueuedReceiptV1.PulsarQueuedAck ack = new CommandQueuedReceiptV1.PulsarQueuedAck(
                result.authenticatedClusterId(), result.resourceIncarnation(), result.physicalTopic(),
                result.physicalTopicCreationTimestamp(), result.partition(), result.ledgerId(), result.entryId(),
                result.batchIndex(), result.batchSize(), result.brokerEntryTimestampEpochMs(),
                Bytes.sha256(result.responseEvidenceBytes()));
        return SubmissionOutcomeMessageV1.nativeReceipt(
                NativeDeliveryReceiptV1.create(prepared.preparedRef(), ack, attempt.bytes()));
    }

    private static SubmissionOutcomeMessageV1 definite(final NativePreparedDeliveryV1 prepared,
                                                       final PulsarNativeSendRequest request,
                                                       final PhysicalEnqueueAttemptId attempt,
                                                       final PulsarSendResult result) {
        final StableCode resultCode;
        try {
            resultCode = StableCode.fromWire(result.stableCode());
        } catch (IllegalArgumentException failure) {
            return uncertainStatic(prepared, attempt, StableCode.INTEGRITY_ERROR);
        }
        if ((resultCode != StableCode.BROKER_DEFINITIVE_NOT_PERSISTED
                && resultCode != StableCode.NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED)
                || result.requestEvidenceBytes() == null || result.responseEvidenceBytes() == null) {
            return uncertainStatic(prepared, attempt, StableCode.INTEGRITY_ERROR);
        }
        final var target = prepared.target();
        final BrokerResourceIdentityV1 resource = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1(target.authenticatedClusterId(), target.resourceIncarnation(),
                        target.physicalTopic(), target.physicalTopicCreationTimestamp()));
        final var proof = NonPersistenceProofV1.create(NonPersistenceProofKindV1.PULSAR_GUARD_REJECTION,
                attempt.bytes(), prepared.submissionHash(), resource, Bytes.sha256(result.requestEvidenceBytes()),
                Bytes.sha256(result.responseEvidenceBytes()));
        final var error = StableErrorV1.of(FailureStageV1.ENQUEUE,
                StableCode.NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED, null, null, prepared.preparedRef(), null);
        return SubmissionOutcomeMessageV1.nativeDefinitelyNotQueued(
                new NativeDefinitelyNotQueuedV1(prepared.preparedRef(), proof, error));
    }

    private static SubmissionOutcomeMessageV1 uncertainStatic(final NativePreparedDeliveryV1 prepared,
                                                              final PhysicalEnqueueAttemptId attempt,
                                                              final StableCode code) {
        final StableCode retryCode = RetryabilityV1.forCode(code) == RetryabilityV1.RETRY_EXACT_BYTES
                ? code : StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN;
        final var error = StableErrorV1.of(FailureStageV1.ENQUEUE, retryCode, null, null,
                prepared.preparedRef(), null);
        return SubmissionOutcomeMessageV1.nativeUncertain(
                new NativeEnqueueUncertainV1(prepared.preparedRef(), attempt.bytes(), error));
    }
}
