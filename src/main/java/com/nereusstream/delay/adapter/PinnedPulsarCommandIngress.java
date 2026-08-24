package com.nereusstream.delay.adapter;

import com.nereusstream.delay.client.CommandQueuedReceipt;
import com.nereusstream.delay.client.EnqueueOutcome;
import com.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import com.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import com.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Pulsar ingress adapter requiring a per-SEND resource guard at Broker ownership. */
public final class PinnedPulsarCommandIngress implements PolicyBoundWireCommandIngressAdapter {
    private final PulsarIngressResource resource;
    private final PulsarSendTransport transport;
    private final QueuedReceiptQueryPolicy queuedReceiptQueryPolicy;
    private final CloseGuard closeGuard = new CloseGuard();

    public PinnedPulsarCommandIngress(final PulsarIngressResource resource, final PulsarSendTransport transport) {
        this(resource, transport, null);
    }

    public PinnedPulsarCommandIngress(
            final PulsarIngressResource resource,
            final PulsarSendTransport transport,
            final QueuedReceiptQueryPolicy queuedReceiptQueryPolicy) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.queuedReceiptQueryPolicy = queuedReceiptQueryPolicy;
    }

    @Override
    public CompletionStage<EnqueueOutcome> enqueue(final PreparedCommand command) {
        Objects.requireNonNull(command, "command");
        return closeGuard.invokeIfOpen(
                () -> enqueueOpen(command),
                () -> completed(EnqueueOutcome.definitelyNotQueued(command, StableCode.CLIENT_CLOSED.wireValue())));
    }

    private CompletionStage<EnqueueOutcome> enqueueOpen(final PreparedCommand command) {
        if (!resource.shardId().equals(command.shardId())) {
            return completed(
                    EnqueueOutcome.definitelyNotQueued(command, StableCode.INGRESS_ROUTE_MISMATCH.wireValue()));
        }
        final PulsarSendRequest request;
        try {
            request = PulsarSendRequest.from(resource, command, CommandCodec.encodeFrame(command));
        } catch (RuntimeException exception) {
            return completed(
                    EnqueueOutcome.definitelyNotQueued(command, StableCode.INVALID_PREPARED_COMMAND.wireValue()));
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
        try {
            final CompletionStage<EnqueueOutcome> handled = result.handle((send, error) -> {
                if (error != null) {
                    return EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue());
                }
                try {
                    return map(command, send);
                } catch (RuntimeException malformedResult) {
                    // A malformed transport result is not proof that the
                    // Broker rejected a request after Producer ownership.
                    return EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue());
                }
            });
            return handled == null
                    ? completed(EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue()))
                    : handled;
        } catch (RuntimeException registrationFailure) {
            // A broken CompletionStage implementation is not evidence that
            // the Broker rejected a request after Producer ownership.
            return completed(EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue()));
        }
    }

    @Override
    public CompletionStage<EnqueueOutcomeMessageV1> enqueueOutcomeV1(
            final PreparedCommand command, final long receiptQueryUntilEpochMs, final byte[] physicalAttemptId) {
        Objects.requireNonNull(command, "command");
        final byte[] v1Frame;
        try {
            v1Frame = CommandCodec.encodeFrameV1(command);
        } catch (RuntimeException exception) {
            // A compatibility body cannot be represented by a V1 ref/union;
            // fail before any local V1 outcome projection or transport call.
            return CompletableFuture.failedFuture(exception);
        }
        return closeGuard.invokeIfOpen(
                () -> enqueueOutcomeOpen(command, receiptQueryUntilEpochMs, physicalAttemptId, v1Frame),
                () -> completedWire(WireIngressOutcomeSupport.localDefinite(command, StableCode.CLIENT_CLOSED)));
    }

    @Override
    public CompletionStage<EnqueueOutcomeMessageV1> enqueueOutcomeV1(
            final PreparedCommand command, final QueuedReceiptQueryPolicy routePolicy, final byte[] physicalAttemptId) {
        Objects.requireNonNull(command, "command");
        if (queuedReceiptQueryPolicy == null || !queuedReceiptQueryPolicy.equals(routePolicy)) {
            return completedWire(
                    WireIngressOutcomeSupport.localDefinite(command, StableCode.ROUTE_SNAPSHOT_UNAVAILABLE));
        }
        final byte[] v1Frame;
        try {
            v1Frame = CommandCodec.encodeFrameV1(command);
        } catch (RuntimeException exception) {
            return CompletableFuture.failedFuture(exception);
        }
        return closeGuard.invokeIfOpen(
                () -> enqueueOutcomeOpen(command, null, physicalAttemptId, v1Frame),
                () -> completedWire(WireIngressOutcomeSupport.localDefinite(command, StableCode.CLIENT_CLOSED)));
    }

    private CompletionStage<EnqueueOutcomeMessageV1> enqueueOutcomeOpen(
            final PreparedCommand command,
            final Long receiptQueryUntilEpochMs,
            final byte[] physicalAttemptId,
            final byte[] v1Frame) {
        if (!resource.shardId().equals(command.shardId())) {
            return completedWire(WireIngressOutcomeSupport.localDefinite(command, StableCode.INGRESS_ROUTE_MISMATCH));
        }
        final PulsarSendRequest request;
        try {
            request = PulsarSendRequest.from(resource, command, v1Frame);
        } catch (RuntimeException exception) {
            return completedWire(WireIngressOutcomeSupport.localDefinite(command, StableCode.INVALID_PREPARED_COMMAND));
        }
        final byte[] attempt;
        try {
            attempt = WireIngressOutcomeSupport.requireAttempt(physicalAttemptId);
        } catch (RuntimeException exception) {
            return completedWire(WireIngressOutcomeSupport.localDefinite(command, StableCode.INVALID_PREPARED_COMMAND));
        }
        final CompletionStage<PulsarSendResult> result;
        try {
            result = transport.send(request);
        } catch (RuntimeException exception) {
            return completedWire(
                    WireIngressOutcomeSupport.uncertain(command, attempt, StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
        if (result == null) {
            return completedWire(
                    WireIngressOutcomeSupport.uncertain(command, attempt, StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
        try {
            final CompletionStage<EnqueueOutcomeMessageV1> handled = result.handle((send, error) -> {
                if (error != null) {
                    return WireIngressOutcomeSupport.uncertain(
                            command, attempt, StableCode.ENQUEUE_RESULT_UNCERTAIN, null);
                }
                try {
                    return projectWire(command, request, send, receiptQueryUntilEpochMs, attempt);
                } catch (RuntimeException malformedResult) {
                    // A malformed adapter result is not evidence of non-persistence.
                    return WireIngressOutcomeSupport.uncertain(
                            command, attempt, StableCode.INTEGRITY_ERROR, StableCode.INTEGRITY_ERROR.wireValue());
                }
            });
            return handled == null
                    ? completedWire(WireIngressOutcomeSupport.uncertain(
                            command, attempt, StableCode.ENQUEUE_RESULT_UNCERTAIN, null))
                    : handled;
        } catch (RuntimeException registrationFailure) {
            return completedWire(
                    WireIngressOutcomeSupport.uncertain(command, attempt, StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
    }

    @Override
    public void close() {
        closeGuard.close(transport::close);
    }

    private EnqueueOutcome map(final PreparedCommand command, final PulsarSendResult result) {
        if (result == null) {
            return EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue());
        }
        final StableCode code = WireIngressOutcomeSupport.managedCode(
                WireIngressOutcomeSupport.stableCode(result.stableCode(), StableCode.INTEGRITY_ERROR));
        return switch (result.disposition()) {
            case DEFINITIVELY_NOT_PERSISTED -> {
                final StableCode definitive = WireIngressOutcomeSupport.definitiveManagedCode(result.stableCode());
                yield definitive == null
                        ? EnqueueOutcome.uncertain(command, StableCode.INTEGRITY_ERROR.wireValue())
                        : EnqueueOutcome.definitelyNotQueued(command, definitive.wireValue());
            }
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
        final PulsarSourcePosition position = new PulsarSourcePosition(
                command.shardId(),
                result.resourceIncarnation(),
                result.physicalTopic(),
                result.ledgerId(),
                result.entryId(),
                result.batchIndex(),
                result.batchSize(),
                result.batched() ? PulsarSourcePosition.EntryKind.BATCH : PulsarSourcePosition.EntryKind.NON_BATCH,
                result.brokerEntryTimestampEpochMs());
        return EnqueueOutcome.queued(
                command,
                new CommandQueuedReceipt(command.commandId(), command.delayMessageId(), command.shardId(), position));
    }

    private EnqueueOutcomeMessageV1 projectWire(
            final PreparedCommand command,
            final PulsarSendRequest request,
            final PulsarSendResult result,
            final Long receiptQueryUntilEpochMs,
            final byte[] physicalAttemptId) {
        if (result == null) {
            return WireIngressOutcomeSupport.uncertain(
                    command, physicalAttemptId, StableCode.ENQUEUE_RESULT_UNCERTAIN, null);
        }
        final StableCode code = WireIngressOutcomeSupport.managedCode(
                WireIngressOutcomeSupport.stableCode(result.stableCode(), StableCode.INTEGRITY_ERROR));
        return switch (result.disposition()) {
            case DEFINITIVELY_NOT_PERSISTED -> {
                final StableCode definitive = WireIngressOutcomeSupport.definitiveManagedCode(result.stableCode());
                yield definitive == null
                        ? WireIngressOutcomeSupport.uncertain(
                                command, physicalAttemptId, StableCode.INTEGRITY_ERROR, result.stableCode())
                        : WireIngressOutcomeSupport.brokerDefinite(
                                command,
                                physicalAttemptId,
                                definitive,
                                NonPersistenceProofKindV1.PULSAR_GUARD_REJECTION,
                                BrokerResourceIdentityV1.pulsar(new PulsarBrokerResourceIdentityV1(
                                        resource.authenticatedClusterId(),
                                        resource.resourceIncarnation(),
                                        resource.physicalTopic(),
                                        resource.physicalTopicCreationTimestamp())),
                                result.requestEvidenceBytes(),
                                result.responseEvidenceBytes());
            }
            case UNKNOWN ->
                WireIngressOutcomeSupport.uncertain(
                        command,
                        physicalAttemptId,
                        code,
                        code == StableCode.INTEGRITY_ERROR ? result.stableCode() : null);
            case PERSISTED -> persistedWire(command, result, receiptQueryUntilEpochMs, physicalAttemptId);
        };
    }

    private EnqueueOutcomeMessageV1 persistedWire(
            final PreparedCommand command,
            final PulsarSendResult result,
            final Long receiptQueryUntilEpochMs,
            final byte[] physicalAttemptId) {
        if (!resource.authenticatedClusterId().equals(result.authenticatedClusterId())
                || !java.util.Arrays.equals(resource.resourceIncarnation(), result.resourceIncarnation())
                || !resource.physicalTopic().equals(result.physicalTopic())
                || resource.physicalTopicCreationTimestamp() != result.physicalTopicCreationTimestamp()
                || resource.partition() != result.partition()) {
            return WireIngressOutcomeSupport.uncertain(
                    command,
                    physicalAttemptId,
                    StableCode.RESOURCE_INCARNATION_MISMATCH,
                    StableCode.RESOURCE_INCARNATION_MISMATCH.wireValue());
        }
        if (result.responseEvidenceBytes() == null) {
            return WireIngressOutcomeSupport.uncertain(
                    command, physicalAttemptId, StableCode.ENQUEUE_RESULT_UNCERTAIN, null);
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
        final long queryUntil = receiptQueryUntil(source, receiptQueryUntilEpochMs);
        final CommandQueuedReceiptV1 receipt = CommandQueuedReceiptV1.create(
                command, source, ack, queryUntil, WireIngressOutcomeSupport.requireAttempt(physicalAttemptId));
        return EnqueueOutcomeMessageV1.queued(receipt);
    }

    private long receiptQueryUntil(final SourcePosition source, final Long suppliedBoundary) {
        if (queuedReceiptQueryPolicy != null) {
            final long derivedBoundary = queuedReceiptQueryPolicy.queryUntil(source);
            if (suppliedBoundary != null && suppliedBoundary.longValue() != derivedBoundary) {
                throw new IllegalArgumentException("receipt query boundary does not match Route policy");
            }
            return derivedBoundary;
        }
        if (suppliedBoundary == null) {
            throw new IllegalStateException("strict V1 ingress requires a bound Route query policy");
        }
        return suppliedBoundary;
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
