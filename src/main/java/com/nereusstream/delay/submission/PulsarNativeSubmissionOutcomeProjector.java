package com.nereusstream.delay.submission;

import com.nereusstream.delay.adapter.PulsarNativeSendRequest;
import com.nereusstream.delay.adapter.PulsarSendResult;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.FailureStage;
import com.nereusstream.delay.protocol.NativeDefinitelyNotQueued;
import com.nereusstream.delay.protocol.NativeDeliveryReceipt;
import com.nereusstream.delay.protocol.NativeEnqueueUncertain;
import com.nereusstream.delay.protocol.NativePreparedDelivery;
import com.nereusstream.delay.protocol.NonPersistenceProof;
import com.nereusstream.delay.protocol.NonPersistenceProofKind;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.Retryability;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableError;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import com.nereusstream.delay.transport.TransportResult;

/** Native Pulsar NDR1 projector; native results never become managed receipts. */
public final class PulsarNativeSubmissionOutcomeProjector implements SubmissionOutcomeProjector {
    private static final com.nereusstream.delay.protocol.AdapterKind KIND =
            com.nereusstream.delay.protocol.AdapterKind.PULSAR;

    @Override
    public SubmissionProjectionKey key() {
        return new SubmissionProjectionKey(PreparedSubmissionBranch.NATIVE, KIND);
    }

    @Override
    public SubmissionOutcomeMessage project(
            final SubmissionTransportPlan plan,
            final PhysicalEnqueueAttemptId physicalAttemptId,
            final TransportResult result) {
        if (!(plan.routeAuthority() instanceof NativeTargetAuthority authority)
                || !(plan.request() instanceof PulsarNativeSendRequest request)
                || !(result instanceof PulsarSendResult pulsar)
                || (pulsar.physicalAttemptId() != null
                        && !pulsar.physicalAttemptId().equals(physicalAttemptId))) {
            return uncertain(plan, physicalAttemptId, StableCode.INTEGRITY_ERROR);
        }
        final NativePreparedDelivery prepared = authority.prepared();
        return switch (pulsar.disposition()) {
            case PERSISTED -> persisted(prepared, physicalAttemptId, pulsar);
            case DEFINITIVELY_NOT_PERSISTED -> definite(prepared, request, physicalAttemptId, pulsar);
            case UNKNOWN ->
                uncertain(
                        plan, physicalAttemptId, SubmissionProjectorSupport.exactNativeRetryCode(pulsar.stableCode()));
        };
    }

    @Override
    public SubmissionOutcomeMessage localFailure(
            final SubmissionTransportPlan plan,
            final PhysicalEnqueueAttemptId physicalAttemptId,
            final StableCode code) {
        final NativePreparedDelivery prepared = ((NativeTargetAuthority) plan.routeAuthority()).prepared();
        final var ref = prepared.preparedRef();
        final var proof = NonPersistenceProof.create(
                NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, ref.submissionHash(), null, null, null);
        final var error = StableError.of(FailureStage.ENQUEUE, code, null, null, ref, null);
        return SubmissionOutcomeMessage.nativeDefinitelyNotQueued(new NativeDefinitelyNotQueued(ref, proof, error));
    }

    @Override
    public SubmissionOutcomeMessage uncertain(
            final SubmissionTransportPlan plan,
            final PhysicalEnqueueAttemptId physicalAttemptId,
            final StableCode code) {
        final NativePreparedDelivery prepared = ((NativeTargetAuthority) plan.routeAuthority()).prepared();
        final var ref = prepared.preparedRef();
        final StableCode retryCode = Retryability.forCode(code) == Retryability.RETRY_EXACT_BYTES
                ? code
                : StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN;
        final var error = StableError.of(FailureStage.ENQUEUE, retryCode, null, null, ref, null);
        return SubmissionOutcomeMessage.nativeUncertain(
                new NativeEnqueueUncertain(ref, physicalAttemptId.bytes(), error));
    }

    private static SubmissionOutcomeMessage persisted(
            final NativePreparedDelivery prepared,
            final PhysicalEnqueueAttemptId attempt,
            final PulsarSendResult result) {
        final var target = prepared.target();
        if (!target.authenticatedClusterId().equals(result.authenticatedClusterId())
                || !java.util.Arrays.equals(target.resourceIncarnation(), result.resourceIncarnation())
                || !target.physicalTopic().equals(result.physicalTopic())
                || target.physicalTopicCreationTimestamp() != result.physicalTopicCreationTimestamp()
                || prepared.physicalPartition() != result.partition()
                || result.responseEvidenceBytes() == null) {
            return uncertainStatic(prepared, attempt, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN);
        }
        final CanonicalCommandQueuedReceipt.PulsarQueuedAck ack = new CanonicalCommandQueuedReceipt.PulsarQueuedAck(
                result.authenticatedClusterId(),
                result.resourceIncarnation(),
                result.physicalTopic(),
                result.physicalTopicCreationTimestamp(),
                result.partition(),
                result.ledgerId(),
                result.entryId(),
                result.batchIndex(),
                result.batchSize(),
                result.brokerEntryTimestampEpochMs(),
                Bytes.sha256(result.responseEvidenceBytes()));
        return SubmissionOutcomeMessage.nativeReceipt(
                NativeDeliveryReceipt.create(prepared.preparedRef(), ack, attempt.bytes()));
    }

    private static SubmissionOutcomeMessage definite(
            final NativePreparedDelivery prepared,
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
                || result.requestEvidenceBytes() == null
                || result.responseEvidenceBytes() == null) {
            return uncertainStatic(prepared, attempt, StableCode.INTEGRITY_ERROR);
        }
        final var target = prepared.target();
        final BrokerResourceIdentity resource = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                target.authenticatedClusterId(),
                target.resourceIncarnation(),
                target.physicalTopic(),
                target.physicalTopicCreationTimestamp()));
        final var proof = NonPersistenceProof.create(
                NonPersistenceProofKind.PULSAR_GUARD_REJECTION,
                attempt.bytes(),
                prepared.submissionHash(),
                resource,
                Bytes.sha256(result.requestEvidenceBytes()),
                Bytes.sha256(result.responseEvidenceBytes()));
        final var error = StableError.of(
                FailureStage.ENQUEUE,
                StableCode.NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED,
                null,
                null,
                prepared.preparedRef(),
                null);
        return SubmissionOutcomeMessage.nativeDefinitelyNotQueued(
                new NativeDefinitelyNotQueued(prepared.preparedRef(), proof, error));
    }

    private static SubmissionOutcomeMessage uncertainStatic(
            final NativePreparedDelivery prepared, final PhysicalEnqueueAttemptId attempt, final StableCode code) {
        final StableCode retryCode = Retryability.forCode(code) == Retryability.RETRY_EXACT_BYTES
                ? code
                : StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN;
        final var error = StableError.of(FailureStage.ENQUEUE, retryCode, null, null, prepared.preparedRef(), null);
        return SubmissionOutcomeMessage.nativeUncertain(
                new NativeEnqueueUncertain(prepared.preparedRef(), attempt.bytes(), error));
    }
}
