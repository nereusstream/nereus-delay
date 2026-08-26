package com.nereusstream.delay.submission;

import com.nereusstream.delay.adapter.PulsarSendRequest;
import com.nereusstream.delay.adapter.PulsarSendResult;
import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.EnqueueOutcomeMessage;
import com.nereusstream.delay.protocol.NonPersistenceProofKind;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
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
     * resource. This is required when one managed submission coordinator owns
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
                PreparedSubmissionBranch.MANAGED, com.nereusstream.delay.protocol.AdapterKind.PULSAR);
    }

    @Override
    public SubmissionOutcomeMessage project(
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
    public SubmissionOutcomeMessage localFailure(
            final SubmissionTransportPlan plan,
            final PhysicalEnqueueAttemptId physicalAttemptId,
            final StableCode code) {
        return SubmissionOutcomeMessage.managed(
                WireIngressOutcomeSupport.localDefinite(SubmissionProjectorSupport.managedCommand(plan), code));
    }

    @Override
    public SubmissionOutcomeMessage uncertain(
            final SubmissionTransportPlan plan,
            final PhysicalEnqueueAttemptId physicalAttemptId,
            final StableCode code) {
        return SubmissionOutcomeMessage.managed(WireIngressOutcomeSupport.uncertain(
                SubmissionProjectorSupport.managedCommand(plan), physicalAttemptId.bytes(), code, null));
    }

    private SubmissionOutcomeMessage persisted(
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
        final long queryUntil = SubmissionProjectorSupport.queryPolicy((ManagedRouteAuthority) plan.routeAuthority())
                .queryUntil(source);
        final CanonicalCommandQueuedReceipt receipt =
                CanonicalCommandQueuedReceipt.create(command, source, ack, queryUntil, attempt.bytes());
        return SubmissionOutcomeMessage.managed(EnqueueOutcomeMessage.queued(receipt));
    }

    private SubmissionOutcomeMessage definite(
            final SubmissionTransportPlan plan,
            final PulsarSendRequest request,
            final com.nereusstream.delay.protocol.PreparedCommand command,
            final PhysicalEnqueueAttemptId attempt,
            final PulsarSendResult result) {
        final StableCode code = WireIngressOutcomeSupport.definitiveManagedCode(result.stableCode());
        if (code == null || result.requestEvidenceBytes() == null || result.responseEvidenceBytes() == null) {
            return uncertain(plan, attempt, StableCode.INTEGRITY_ERROR);
        }
        final BrokerResourceIdentity resource = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                request.authenticatedClusterId(),
                request.resourceIncarnation(),
                request.physicalTopic(),
                request.physicalTopicCreationTimestamp()));
        return SubmissionOutcomeMessage.managed(WireIngressOutcomeSupport.brokerDefinite(
                command,
                attempt.bytes(),
                code,
                NonPersistenceProofKind.PULSAR_GUARD_REJECTION,
                resource,
                result.requestEvidenceBytes(),
                result.responseEvidenceBytes()));
    }
}
