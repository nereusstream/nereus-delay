package com.nereusstream.delay.submission;

import com.nereusstream.delay.adapter.PulsarSendRequest;
import com.nereusstream.delay.adapter.PulsarSendResult;
import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import com.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import com.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import com.nereusstream.delay.transport.PulsarCommandTransportKey;
import com.nereusstream.delay.transport.TransportResult;
import java.util.Arrays;

/** Managed Pulsar NDR1 projector for a completed guarded SEND result. */
public final class PulsarManagedSubmissionOutcomeProjector implements SubmissionOutcomeProjector {
    private final PulsarCommandTransportKey key;

    public PulsarManagedSubmissionOutcomeProjector(final PulsarCommandTransportKey key) {
        this.key = java.util.Objects.requireNonNull(key, "key");
    }

    /**
     * Creates a projector that fences the receipt against the exact request
     * resource.  This is required when one managed submission coordinator owns
     * more than one Pulsar physical partition; a single fixed transport key
     * would incorrectly turn every non-zero partition receipt into
     * RESOURCE_INCARNATION_MISMATCH.
     */
    public PulsarManagedSubmissionOutcomeProjector() {
        this.key = null;
    }

    @Override
    public SubmissionProjectionKey key() {
        return new SubmissionProjectionKey(
                PreparedSubmissionBranch.MANAGED, com.nereusstream.delay.protocol.AdapterKindV1.PULSAR);
    }

    @Override
    public SubmissionOutcomeMessageV1 project(
            final SubmissionTransportPlan plan,
            final PhysicalEnqueueAttemptId physicalAttemptId,
            final TransportResult result) {
        final com.nereusstream.delay.protocol.PreparedCommand command = SubmissionProjectorSupport.managedCommand(plan);
        if (!(plan.request() instanceof PulsarSendRequest request)
                || !(result instanceof PulsarSendResult pulsar)
                || (pulsar.physicalAttemptId() != null
                        && !pulsar.physicalAttemptId().equals(physicalAttemptId))) {
            return uncertain(plan, physicalAttemptId, StableCode.INTEGRITY_ERROR);
        }
        return switch (pulsar.disposition()) {
            case PERSISTED -> persisted(plan, command, physicalAttemptId, pulsar);
            case DEFINITIVELY_NOT_PERSISTED -> definite(plan, request, command, physicalAttemptId, pulsar);
            case UNKNOWN ->
                uncertain(plan, physicalAttemptId, SubmissionProjectorSupport.managedCode(pulsar.stableCode()));
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
            final com.nereusstream.delay.protocol.PreparedCommand command,
            final PhysicalEnqueueAttemptId attempt,
            final PulsarSendResult result) {
        if (!(plan.request() instanceof PulsarSendRequest request)
                || !request.authenticatedClusterId().equals(result.authenticatedClusterId())
                || !Arrays.equals(request.resourceIncarnation(), result.resourceIncarnation())
                || !request.physicalTopic().equals(result.physicalTopic())
                || request.physicalTopicCreationTimestamp() != result.physicalTopicCreationTimestamp()
                || request.partition() != result.partition()
                || (key != null
                        && (!key.authenticatedClusterId().equals(result.authenticatedClusterId())
                                || !key.resourceIncarnation()
                                        .equals(new com.nereusstream.delay.transport.Bytes32(
                                                result.resourceIncarnation()))
                                || !key.canonicalPhysicalTopic().equals(result.physicalTopic())
                                || key.topicCreationTimestamp() != result.physicalTopicCreationTimestamp()
                                || key.partition() != result.partition()))
                || result.responseEvidenceBytes() == null) {
            return uncertain(plan, attempt, StableCode.RESOURCE_INCARNATION_MISMATCH);
        }
        final PulsarSourcePosition.EntryKind entryKind =
                result.batched() ? PulsarSourcePosition.EntryKind.BATCH : PulsarSourcePosition.EntryKind.NON_BATCH;
        final PulsarSourcePosition source = new PulsarSourcePosition(
                command.shardId(),
                result.resourceIncarnation(),
                result.physicalTopic(),
                result.ledgerId(),
                result.entryId(),
                result.batchIndex(),
                result.batchSize(),
                entryKind,
                result.brokerEntryTimestampEpochMs());
        final CommandQueuedReceiptV1.PulsarQueuedAck ack = new CommandQueuedReceiptV1.PulsarQueuedAck(
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
        final long queryUntil = SubmissionProjectorSupport.queryPolicy((ManagedRouteAuthority) plan.routeAuthority())
                .queryUntil(source);
        final CommandQueuedReceiptV1 receipt =
                CommandQueuedReceiptV1.create(command, source, ack, queryUntil, attempt.bytes());
        return SubmissionOutcomeMessageV1.managed(EnqueueOutcomeMessageV1.queued(receipt));
    }

    private SubmissionOutcomeMessageV1 definite(
            final SubmissionTransportPlan plan,
            final PulsarSendRequest request,
            final com.nereusstream.delay.protocol.PreparedCommand command,
            final PhysicalEnqueueAttemptId attempt,
            final PulsarSendResult result) {
        final StableCode code = WireIngressOutcomeSupport.definitiveManagedCode(result.stableCode());
        if (code == null || result.requestEvidenceBytes() == null || result.responseEvidenceBytes() == null) {
            return uncertain(plan, attempt, StableCode.INTEGRITY_ERROR);
        }
        final BrokerResourceIdentityV1 resource = BrokerResourceIdentityV1.pulsar(new PulsarBrokerResourceIdentityV1(
                request.authenticatedClusterId(),
                request.resourceIncarnation(),
                request.physicalTopic(),
                request.physicalTopicCreationTimestamp()));
        return SubmissionOutcomeMessageV1.managed(WireIngressOutcomeSupport.brokerDefinite(
                command,
                attempt.bytes(),
                code,
                NonPersistenceProofKindV1.PULSAR_GUARD_REJECTION,
                resource,
                result.requestEvidenceBytes(),
                result.responseEvidenceBytes()));
    }
}
