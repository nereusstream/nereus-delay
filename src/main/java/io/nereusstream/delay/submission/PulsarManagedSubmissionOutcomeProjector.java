package io.nereusstream.delay.submission;

import io.nereusstream.delay.adapter.PulsarSendRequest;
import io.nereusstream.delay.adapter.PulsarSendResult;
import io.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import io.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import io.nereusstream.delay.transport.PulsarCommandTransportKey;
import io.nereusstream.delay.transport.TransportResult;

/** Managed Pulsar NDR1 projector for a completed guarded SEND result. */
public final class PulsarManagedSubmissionOutcomeProjector implements SubmissionOutcomeProjector {
    private final PulsarCommandTransportKey key;

    public PulsarManagedSubmissionOutcomeProjector(final PulsarCommandTransportKey key) {
        this.key = java.util.Objects.requireNonNull(key, "key");
    }

    @Override
    public SubmissionProjectionKey key() {
        return new SubmissionProjectionKey(PreparedSubmissionBranch.MANAGED,
                io.nereusstream.delay.protocol.AdapterKindV1.PULSAR);
    }

    @Override
    public SubmissionOutcomeMessageV1 project(final SubmissionTransportPlan plan,
                                              final PhysicalEnqueueAttemptId physicalAttemptId,
                                              final TransportResult result) {
        final io.nereusstream.delay.protocol.PreparedCommand command = SubmissionProjectorSupport.managedCommand(plan);
        if (!(plan.request() instanceof PulsarSendRequest request)
                || !(result instanceof PulsarSendResult pulsar)
                || (pulsar.physicalAttemptId() != null
                && !pulsar.physicalAttemptId().equals(physicalAttemptId))) {
            return uncertain(plan, physicalAttemptId, StableCode.INTEGRITY_ERROR);
        }
        return switch (pulsar.disposition()) {
            case PERSISTED -> persisted(plan, command, physicalAttemptId, pulsar);
            case DEFINITIVELY_NOT_PERSISTED -> definite(plan, request, command, physicalAttemptId, pulsar);
            case UNKNOWN -> uncertain(plan, physicalAttemptId, SubmissionProjectorSupport.managedCode(
                    pulsar.stableCode()));
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
                                                 final io.nereusstream.delay.protocol.PreparedCommand command,
                                                 final PhysicalEnqueueAttemptId attempt,
                                                 final PulsarSendResult result) {
        if (!key.authenticatedClusterId().equals(result.authenticatedClusterId())
                || !key.resourceIncarnation().equals(new io.nereusstream.delay.transport.Bytes32(
                result.resourceIncarnation()))
                || !key.canonicalPhysicalTopic().equals(result.physicalTopic())
                || key.topicCreationTimestamp() != result.physicalTopicCreationTimestamp()
                || key.partition() != result.partition() || result.responseEvidenceBytes() == null) {
            return uncertain(plan, attempt, StableCode.RESOURCE_INCARNATION_MISMATCH);
        }
        final PulsarSourcePosition.EntryKind entryKind = result.batched()
                ? PulsarSourcePosition.EntryKind.BATCH : PulsarSourcePosition.EntryKind.NON_BATCH;
        final PulsarSourcePosition source = new PulsarSourcePosition(command.shardId(), result.resourceIncarnation(),
                result.physicalTopic(), result.ledgerId(), result.entryId(), result.batchIndex(), result.batchSize(),
                entryKind, result.brokerEntryTimestampEpochMs());
        final CommandQueuedReceiptV1.PulsarQueuedAck ack = new CommandQueuedReceiptV1.PulsarQueuedAck(
                result.authenticatedClusterId(), result.resourceIncarnation(), result.physicalTopic(),
                result.physicalTopicCreationTimestamp(), result.partition(), result.ledgerId(), result.entryId(),
                result.batchIndex(), result.batchSize(), result.brokerEntryTimestampEpochMs(),
                Bytes.sha256(result.responseEvidenceBytes()));
        final long queryUntil = SubmissionProjectorSupport.queryPolicy(
                (ManagedRouteAuthority) plan.routeAuthority()).queryUntil(source);
        final CommandQueuedReceiptV1 receipt = CommandQueuedReceiptV1.create(command, source, ack, queryUntil,
                attempt.bytes());
        return SubmissionOutcomeMessageV1.managed(EnqueueOutcomeMessageV1.queued(receipt));
    }

    private SubmissionOutcomeMessageV1 definite(final SubmissionTransportPlan plan,
                                                final PulsarSendRequest request,
                                                final io.nereusstream.delay.protocol.PreparedCommand command,
                                                final PhysicalEnqueueAttemptId attempt,
                                                final PulsarSendResult result) {
        final StableCode code = WireIngressOutcomeSupport.definitiveManagedCode(result.stableCode());
        if (code == null || result.requestEvidenceBytes() == null || result.responseEvidenceBytes() == null) {
            return uncertain(plan, attempt, StableCode.INTEGRITY_ERROR);
        }
        final BrokerResourceIdentityV1 resource = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1(key.authenticatedClusterId(), key.resourceIncarnation().bytes(),
                        key.canonicalPhysicalTopic(), key.topicCreationTimestamp()));
        return SubmissionOutcomeMessageV1.managed(WireIngressOutcomeSupport.brokerDefinite(command,
                attempt.bytes(), code, NonPersistenceProofKindV1.PULSAR_GUARD_REJECTION, resource,
                result.requestEvidenceBytes(), result.responseEvidenceBytes()));
    }
}
