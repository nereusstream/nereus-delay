package io.nereusstream.delay.submission;

import io.nereusstream.delay.adapter.KafkaProduceRequest;
import io.nereusstream.delay.adapter.KafkaProduceResult;
import io.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.transport.KafkaCommandTransportKey;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import io.nereusstream.delay.transport.TransportResult;

/** Managed Kafka NDR1 projector for an already completed guarded result. */
public final class KafkaManagedSubmissionOutcomeProjector implements SubmissionOutcomeProjector {
    private final KafkaCommandTransportKey key;

    public KafkaManagedSubmissionOutcomeProjector(final KafkaCommandTransportKey key) {
        this.key = java.util.Objects.requireNonNull(key, "key");
    }

    @Override
    public SubmissionProjectionKey key() {
        return new SubmissionProjectionKey(PreparedSubmissionBranch.MANAGED,
                io.nereusstream.delay.protocol.AdapterKindV1.KAFKA);
    }

    @Override
    public SubmissionOutcomeMessageV1 project(final SubmissionTransportPlan plan,
                                              final PhysicalEnqueueAttemptId physicalAttemptId,
                                              final TransportResult result) {
        final io.nereusstream.delay.protocol.PreparedCommand command = SubmissionProjectorSupport.managedCommand(plan);
        if (!(plan.request() instanceof KafkaProduceRequest request)
                || !(result instanceof KafkaProduceResult kafka)) {
            return uncertain(plan, physicalAttemptId, StableCode.INTEGRITY_ERROR);
        }
        return switch (kafka.disposition()) {
            case PERSISTED -> persisted(plan, request, command, physicalAttemptId, kafka);
            case DEFINITIVELY_NOT_PERSISTED -> definite(plan, request, command, physicalAttemptId, kafka);
            case UNKNOWN -> uncertain(plan, physicalAttemptId, SubmissionProjectorSupport.managedCode(
                    kafka.stableCode()));
        };
    }

    @Override
    public SubmissionOutcomeMessageV1 localFailure(final SubmissionTransportPlan plan,
                                                   final PhysicalEnqueueAttemptId physicalAttemptId,
                                                   final StableCode code) {
        return SubmissionOutcomeMessageV1.managed(
                WireIngressOutcomeSupport.localDefinite(SubmissionProjectorSupport.managedCommand(plan), code));
    }

    @Override
    public SubmissionOutcomeMessageV1 uncertain(final SubmissionTransportPlan plan,
                                                final PhysicalEnqueueAttemptId physicalAttemptId,
                                                final StableCode code) {
        return SubmissionOutcomeMessageV1.managed(WireIngressOutcomeSupport.uncertain(
                SubmissionProjectorSupport.managedCommand(plan), physicalAttemptId.bytes(), code, null));
    }

    private SubmissionOutcomeMessageV1 persisted(final SubmissionTransportPlan plan,
                                                 final KafkaProduceRequest request,
                                                 final io.nereusstream.delay.protocol.PreparedCommand command,
                                                 final PhysicalEnqueueAttemptId attempt,
                                                 final KafkaProduceResult result) {
        if (!key.authenticatedClusterId().equals(result.authenticatedClusterId())
                || !key.nativeTopicUuid().equals(result.nativeTopicUuid()) || key.partition() != result.partition()
                || result.responseEvidenceBytes() == null) {
            return uncertain(plan, attempt, StableCode.RESOURCE_INCARNATION_MISMATCH);
        }
        final KafkaSourcePosition source = new KafkaSourcePosition(command.shardId(), result.authenticatedClusterId(),
                result.nativeTopicUuid(), result.offset(), result.leaderEpoch(),
                result.brokerLogAppendTimeEpochMs());
        final CommandQueuedReceiptV1.KafkaQueuedAck ack = new CommandQueuedReceiptV1.KafkaQueuedAck(
                result.authenticatedClusterId(), result.nativeTopicUuid(), result.partition(), result.offset(),
                result.leaderEpoch(), result.brokerLogAppendTimeEpochMs(),
                Bytes.sha256(result.responseEvidenceBytes()));
        final long queryUntil = SubmissionProjectorSupport.queryPolicy(
                (ManagedRouteAuthority) plan.routeAuthority()).queryUntil(source);
        final CommandQueuedReceiptV1 receipt = CommandQueuedReceiptV1.create(command, source, ack, queryUntil,
                attempt.bytes());
        return SubmissionOutcomeMessageV1.managed(EnqueueOutcomeMessageV1.queued(receipt));
    }

    private SubmissionOutcomeMessageV1 definite(final SubmissionTransportPlan plan,
                                                final KafkaProduceRequest request,
                                                final io.nereusstream.delay.protocol.PreparedCommand command,
                                                final PhysicalEnqueueAttemptId attempt,
                                                final KafkaProduceResult result) {
        final StableCode code = WireIngressOutcomeSupport.definitiveManagedCode(result.stableCode());
        if (code == null || result.requestEvidenceBytes() == null || result.responseEvidenceBytes() == null) {
            return uncertain(plan, attempt, StableCode.INTEGRITY_ERROR);
        }
        final BrokerResourceIdentityV1 resource = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1(key.authenticatedClusterId(), key.nativeTopicUuid()));
        return SubmissionOutcomeMessageV1.managed(WireIngressOutcomeSupport.brokerDefinite(command,
                attempt.bytes(), code, NonPersistenceProofKindV1.KAFKA_DEFINITIVE_REJECTION, resource,
                result.requestEvidenceBytes(), result.responseEvidenceBytes()));
    }
}
