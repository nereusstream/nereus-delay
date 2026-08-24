package com.nereusstream.delay.submission;

import com.nereusstream.delay.adapter.KafkaProduceRequest;
import com.nereusstream.delay.adapter.KafkaProduceResult;
import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import com.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import com.nereusstream.delay.transport.KafkaCommandTransportKey;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import com.nereusstream.delay.transport.TransportResult;

/** Managed Kafka NDR1 projector for an already completed guarded result. */
public final class KafkaManagedSubmissionOutcomeProjector implements SubmissionOutcomeProjector {
    private final KafkaCommandTransportKey key;

    public KafkaManagedSubmissionOutcomeProjector(final KafkaCommandTransportKey key) {
        this.key = java.util.Objects.requireNonNull(key, "key");
    }

    /**
     * Creates a Route-bound projector that validates the exact request
     * resource carried by each prepared submission.  A single Gateway
     * coordinator may then serve several Kafka partitions without weakening
     * the resource/partition checks to a topic name alone.
     */
    public KafkaManagedSubmissionOutcomeProjector() {
        this.key = null;
    }

    @Override
    public SubmissionProjectionKey key() {
        return new SubmissionProjectionKey(
                PreparedSubmissionBranch.MANAGED, com.nereusstream.delay.protocol.AdapterKindV1.KAFKA);
    }

    @Override
    public SubmissionOutcomeMessageV1 project(
            final SubmissionTransportPlan plan,
            final PhysicalEnqueueAttemptId physicalAttemptId,
            final TransportResult result) {
        final com.nereusstream.delay.protocol.PreparedCommand command = SubmissionProjectorSupport.managedCommand(plan);
        if (!(plan.request() instanceof KafkaProduceRequest request)
                || !(result instanceof KafkaProduceResult kafka)
                || (kafka.physicalAttemptId() != null
                        && !kafka.physicalAttemptId().equals(physicalAttemptId))) {
            return uncertain(plan, physicalAttemptId, StableCode.INTEGRITY_ERROR);
        }
        return switch (kafka.disposition()) {
            case PERSISTED -> persisted(plan, request, command, physicalAttemptId, kafka);
            case DEFINITIVELY_NOT_PERSISTED -> definite(plan, request, command, physicalAttemptId, kafka);
            case UNKNOWN ->
                uncertain(plan, physicalAttemptId, SubmissionProjectorSupport.managedCode(kafka.stableCode()));
        };
    }

    @Override
    public SubmissionOutcomeMessageV1 localFailure(
            final SubmissionTransportPlan plan,
            final PhysicalEnqueueAttemptId physicalAttemptId,
            final StableCode code) {
        return SubmissionOutcomeMessageV1.managed(
                WireIngressOutcomeSupport.localDefinite(SubmissionProjectorSupport.managedCommand(plan), code));
    }

    @Override
    public SubmissionOutcomeMessageV1 uncertain(
            final SubmissionTransportPlan plan,
            final PhysicalEnqueueAttemptId physicalAttemptId,
            final StableCode code) {
        return SubmissionOutcomeMessageV1.managed(WireIngressOutcomeSupport.uncertain(
                SubmissionProjectorSupport.managedCommand(plan), physicalAttemptId.bytes(), code, null));
    }

    private SubmissionOutcomeMessageV1 persisted(
            final SubmissionTransportPlan plan,
            final KafkaProduceRequest request,
            final com.nereusstream.delay.protocol.PreparedCommand command,
            final PhysicalEnqueueAttemptId attempt,
            final KafkaProduceResult result) {
        if (!matchesResource(request, result) || result.responseEvidenceBytes() == null) {
            return uncertain(plan, attempt, StableCode.RESOURCE_INCARNATION_MISMATCH);
        }
        final KafkaSourcePosition source = new KafkaSourcePosition(
                command.shardId(),
                result.authenticatedClusterId(),
                result.nativeTopicUuid(),
                result.offset(),
                result.leaderEpoch(),
                result.brokerLogAppendTimeEpochMs());
        final CommandQueuedReceiptV1.KafkaQueuedAck ack = new CommandQueuedReceiptV1.KafkaQueuedAck(
                result.authenticatedClusterId(),
                result.nativeTopicUuid(),
                result.partition(),
                result.offset(),
                result.leaderEpoch(),
                result.brokerLogAppendTimeEpochMs(),
                Bytes.sha256(result.responseEvidenceBytes()));
        final long queryUntil = SubmissionProjectorSupport.queryPolicy((ManagedRouteAuthority) plan.routeAuthority())
                .queryUntil(source);
        final CommandQueuedReceiptV1 receipt =
                CommandQueuedReceiptV1.create(command, source, ack, queryUntil, attempt.bytes());
        return SubmissionOutcomeMessageV1.managed(EnqueueOutcomeMessageV1.queued(receipt));
    }

    private SubmissionOutcomeMessageV1 definite(
            final SubmissionTransportPlan plan,
            final KafkaProduceRequest request,
            final com.nereusstream.delay.protocol.PreparedCommand command,
            final PhysicalEnqueueAttemptId attempt,
            final KafkaProduceResult result) {
        final StableCode code = WireIngressOutcomeSupport.definitiveManagedCode(result.stableCode());
        if (code == null || result.requestEvidenceBytes() == null || result.responseEvidenceBytes() == null) {
            return uncertain(plan, attempt, StableCode.INTEGRITY_ERROR);
        }
        final BrokerResourceIdentityV1 resource = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1(key.authenticatedClusterId(), key.nativeTopicUuid()));
        return SubmissionOutcomeMessageV1.managed(WireIngressOutcomeSupport.brokerDefinite(
                command,
                attempt.bytes(),
                code,
                NonPersistenceProofKindV1.KAFKA_DEFINITIVE_REJECTION,
                resource,
                result.requestEvidenceBytes(),
                result.responseEvidenceBytes()));
    }

    private boolean matchesResource(final KafkaProduceRequest request, final KafkaProduceResult result) {
        return (key == null
                ? request.authenticatedClusterId().equals(result.authenticatedClusterId())
                        && request.nativeTopicUuid().equals(result.nativeTopicUuid())
                        && request.partition() == result.partition()
                : key.authenticatedClusterId().equals(result.authenticatedClusterId())
                        && key.nativeTopicUuid().equals(result.nativeTopicUuid())
                        && key.partition() == result.partition());
    }
}
