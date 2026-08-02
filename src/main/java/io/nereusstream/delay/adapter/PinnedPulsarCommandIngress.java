package io.nereusstream.delay.adapter;

import io.nereusstream.delay.client.CommandQueuedReceipt;
import io.nereusstream.delay.client.EnqueueOutcome;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import io.nereusstream.delay.protocol.StableCode;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/** Pulsar ingress adapter requiring a per-SEND resource guard at Broker ownership. */
public final class PinnedPulsarCommandIngress implements WireCommandIngressAdapter {
    private final PulsarIngressResource resource;
    private final PulsarSendTransport transport;
    private final AtomicBoolean closed = new AtomicBoolean();

    public PinnedPulsarCommandIngress(final PulsarIngressResource resource, final PulsarSendTransport transport) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    @Override
    public CompletionStage<EnqueueOutcome> enqueue(final PreparedCommand command) {
        Objects.requireNonNull(command, "command");
        if (closed.get()) {
            return completed(EnqueueOutcome.definitelyNotQueued(command, StableCode.CLIENT_CLOSED.wireValue()));
        }
        if (!resource.shardId().equals(command.shardId())) {
            return completed(EnqueueOutcome.definitelyNotQueued(command,
                    StableCode.INGRESS_ROUTE_MISMATCH.wireValue()));
        }
        final PulsarSendRequest request;
        try {
            request = PulsarSendRequest.from(resource, command, CommandCodec.encodeFrame(command));
        } catch (RuntimeException exception) {
            return completed(EnqueueOutcome.definitelyNotQueued(command, StableCode.INVALID_PREPARED_COMMAND.wireValue()));
        }
        final CompletionStage<PulsarSendResult> result;
        try {
            result = transport.send(request);
        } catch (RuntimeException exception) {
            return completed(EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue()));
        }
        if (result == null) {
            return completed(EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue()));
        }
        return result.handle((send, error) -> error == null ? map(command, send)
                : EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue()));
    }

    @Override
    public CompletionStage<EnqueueOutcomeMessageV1> enqueueOutcomeV1(final PreparedCommand command,
                                                                       final long receiptQueryUntilEpochMs,
                                                                       final byte[] physicalAttemptId) {
        Objects.requireNonNull(command, "command");
        if (closed.get()) {
            return completedWire(WireIngressOutcomeSupport.localDefinite(command, StableCode.CLIENT_CLOSED));
        }
        if (!resource.shardId().equals(command.shardId())) {
            return completedWire(WireIngressOutcomeSupport.localDefinite(command,
                    StableCode.INGRESS_ROUTE_MISMATCH));
        }
        final PulsarSendRequest request;
        try {
            request = PulsarSendRequest.from(resource, command, CommandCodec.encodeFrame(command));
        } catch (RuntimeException exception) {
            return completedWire(WireIngressOutcomeSupport.localDefinite(command,
                    StableCode.INVALID_PREPARED_COMMAND));
        }
        final CompletionStage<PulsarSendResult> result;
        try {
            result = transport.send(request);
        } catch (RuntimeException exception) {
            return completedWire(WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                    StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
        if (result == null) {
            return completedWire(WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                    StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
        return result.handle((send, error) -> error == null
                ? projectWire(command, request, send, receiptQueryUntilEpochMs, physicalAttemptId)
                : WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                        StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            transport.close();
        }
    }

    private EnqueueOutcome map(final PreparedCommand command, final PulsarSendResult result) {
        if (result == null) {
            return EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue());
        }
        final StableCode code = WireIngressOutcomeSupport.managedCode(
                WireIngressOutcomeSupport.stableCode(result.stableCode(), StableCode.INTEGRITY_ERROR));
        return switch (result.disposition()) {
            case DEFINITIVELY_NOT_PERSISTED -> EnqueueOutcome.definitelyNotQueued(command, code.wireValue());
            case UNKNOWN -> EnqueueOutcome.uncertain(command, code.wireValue());
            case PERSISTED -> persisted(command, result);
        };
    }

    private EnqueueOutcome persisted(final PreparedCommand command, final PulsarSendResult result) {
        if (!resource.authenticatedClusterId().equals(result.authenticatedClusterId())
                || !java.util.Arrays.equals(resource.resourceIncarnation(), result.resourceIncarnation())
                || !resource.physicalTopic().equals(result.physicalTopic())
                || resource.physicalTopicCreationTimestamp() != result.physicalTopicCreationTimestamp()
                || resource.partition() != result.partition()) {
            return EnqueueOutcome.uncertain(command, StableCode.RESOURCE_INCARNATION_MISMATCH.wireValue());
        }
        final PulsarSourcePosition position = new PulsarSourcePosition(command.shardId(),
                result.resourceIncarnation(), result.physicalTopic(), result.ledgerId(), result.entryId(),
                result.batchIndex(), result.batchSize(), result.batched()
                        ? PulsarSourcePosition.EntryKind.BATCH : PulsarSourcePosition.EntryKind.NON_BATCH,
                result.brokerEntryTimestampEpochMs());
        return EnqueueOutcome.queued(command, new CommandQueuedReceipt(command.commandId(), command.delayMessageId(),
                command.shardId(), position));
    }

    private EnqueueOutcomeMessageV1 projectWire(final PreparedCommand command, final PulsarSendRequest request,
                                                final PulsarSendResult result, final long receiptQueryUntilEpochMs,
                                                final byte[] physicalAttemptId) {
        if (result == null) {
            return WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                    StableCode.ENQUEUE_RESULT_UNCERTAIN, null);
        }
        final StableCode code = WireIngressOutcomeSupport.managedCode(
                WireIngressOutcomeSupport.stableCode(result.stableCode(), StableCode.INTEGRITY_ERROR));
        return switch (result.disposition()) {
            case DEFINITIVELY_NOT_PERSISTED -> WireIngressOutcomeSupport.brokerDefinite(command, physicalAttemptId,
                    code, NonPersistenceProofKindV1.PULSAR_GUARD_REJECTION,
                    BrokerResourceIdentityV1.pulsar(new PulsarBrokerResourceIdentityV1(
                            resource.authenticatedClusterId(), resource.resourceIncarnation(), resource.physicalTopic(),
                            resource.physicalTopicCreationTimestamp())), request.frame(), result.evidence());
            case UNKNOWN -> WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                    code, code == StableCode.INTEGRITY_ERROR ? result.stableCode() : null);
            case PERSISTED -> persistedWire(command, result, receiptQueryUntilEpochMs, physicalAttemptId);
        };
    }

    private EnqueueOutcomeMessageV1 persistedWire(final PreparedCommand command, final PulsarSendResult result,
                                                  final long receiptQueryUntilEpochMs,
                                                  final byte[] physicalAttemptId) {
        if (!resource.authenticatedClusterId().equals(result.authenticatedClusterId())
                || !java.util.Arrays.equals(resource.resourceIncarnation(), result.resourceIncarnation())
                || !resource.physicalTopic().equals(result.physicalTopic())
                || resource.physicalTopicCreationTimestamp() != result.physicalTopicCreationTimestamp()
                || resource.partition() != result.partition()) {
            return WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                    StableCode.RESOURCE_INCARNATION_MISMATCH, StableCode.RESOURCE_INCARNATION_MISMATCH.wireValue());
        }
        if (result.evidence() == null) {
            return WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                    StableCode.ENQUEUE_RESULT_UNCERTAIN, null);
        }
        final PulsarSourcePosition.EntryKind entryKind = result.batched()
                ? PulsarSourcePosition.EntryKind.BATCH : PulsarSourcePosition.EntryKind.NON_BATCH;
        final PulsarSourcePosition source = new PulsarSourcePosition(command.shardId(),
                result.resourceIncarnation(), result.physicalTopic(), result.ledgerId(), result.entryId(),
                result.batchIndex(), result.batchSize(), entryKind, result.brokerEntryTimestampEpochMs());
        final CommandQueuedReceiptV1.PulsarQueuedAck ack = new CommandQueuedReceiptV1.PulsarQueuedAck(
                result.authenticatedClusterId(), result.resourceIncarnation(), result.physicalTopic(),
                result.physicalTopicCreationTimestamp(), result.partition(), result.ledgerId(), result.entryId(),
                result.batchIndex(), result.batchSize(), result.brokerEntryTimestampEpochMs(),
                Bytes.sha256(result.evidence()));
        final CommandQueuedReceiptV1 receipt = CommandQueuedReceiptV1.create(command, source, ack,
                receiptQueryUntilEpochMs, WireIngressOutcomeSupport.requireAttempt(physicalAttemptId));
        return EnqueueOutcomeMessageV1.queued(receipt);
    }

    private static CompletionStage<EnqueueOutcome> completed(final EnqueueOutcome outcome) {
        return CompletableFuture.completedFuture(outcome);
    }

    private static CompletionStage<EnqueueOutcomeMessageV1> completedWire(final EnqueueOutcomeMessageV1 outcome) {
        return CompletableFuture.completedFuture(outcome);
    }

    @FunctionalInterface
    public interface PulsarSendTransport extends AutoCloseable {
        CompletionStage<PulsarSendResult> send(PulsarSendRequest request);

        @Override
        default void close() {
            // Implementations close their producer/connection here.
        }
    }
}
