package io.nereusstream.delay.adapter;

import io.nereusstream.delay.client.CommandQueuedReceipt;
import io.nereusstream.delay.client.EnqueueOutcome;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.StableCode;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Kafka ingress adapter whose transport is required to carry the exact pinned
 * native topic UUID in the Produce request. A stock name-only producer cannot
 * implement this interface safely.
 */
public final class PinnedKafkaCommandIngress implements WireCommandIngressAdapter {
    private final KafkaIngressResource resource;
    private final KafkaProduceTransport transport;
    private final CloseGuard closeGuard = new CloseGuard();

    public PinnedKafkaCommandIngress(final KafkaIngressResource resource, final KafkaProduceTransport transport) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    @Override
    public CompletionStage<EnqueueOutcome> enqueue(final PreparedCommand command) {
        Objects.requireNonNull(command, "command");
        return closeGuard.invokeIfOpen(() -> enqueueOpen(command),
                () -> completed(EnqueueOutcome.definitelyNotQueued(command, StableCode.CLIENT_CLOSED.wireValue())));
    }

    private CompletionStage<EnqueueOutcome> enqueueOpen(final PreparedCommand command) {
        if (!resource.shardId().equals(command.shardId())) {
            return completed(EnqueueOutcome.definitelyNotQueued(command,
                    StableCode.INGRESS_ROUTE_MISMATCH.wireValue()));
        }
        final KafkaProduceRequest request;
        try {
            request = KafkaProduceRequest.from(resource, command, CommandCodec.encodeFrame(command));
        } catch (RuntimeException exception) {
            return completed(EnqueueOutcome.definitelyNotQueued(command, StableCode.INVALID_PREPARED_COMMAND.wireValue()));
        }
        final CompletionStage<KafkaProduceResult> result;
        try {
            result = transport.produce(request);
        } catch (RuntimeException exception) {
            return completed(EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue()));
        }
        if (result == null) {
            return completed(EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue()));
        }
        try {
            final CompletionStage<EnqueueOutcome> handled = result.handle((produce, error) -> {
                if (error != null) {
                    return EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue());
                }
                try {
                    return map(command, produce);
                } catch (RuntimeException malformedResult) {
                    // A malformed transport result is not proof that the
                    // Broker rejected a request after Producer ownership.
                    return EnqueueOutcome.uncertain(command,
                            StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue());
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
    public CompletionStage<EnqueueOutcomeMessageV1> enqueueOutcomeV1(final PreparedCommand command,
                                                                       final long receiptQueryUntilEpochMs,
                                                                       final byte[] physicalAttemptId) {
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

    private CompletionStage<EnqueueOutcomeMessageV1> enqueueOutcomeOpen(final PreparedCommand command,
                                                                          final long receiptQueryUntilEpochMs,
                                                                          final byte[] physicalAttemptId,
                                                                          final byte[] v1Frame) {
        if (!resource.shardId().equals(command.shardId())) {
            return completedWire(WireIngressOutcomeSupport.localDefinite(command,
                    StableCode.INGRESS_ROUTE_MISMATCH));
        }
        final KafkaProduceRequest request;
        try {
            request = KafkaProduceRequest.from(resource, command, v1Frame);
        } catch (RuntimeException exception) {
            return completedWire(WireIngressOutcomeSupport.localDefinite(command,
                    StableCode.INVALID_PREPARED_COMMAND));
        }
        final byte[] attempt;
        try {
            attempt = WireIngressOutcomeSupport.requireAttempt(physicalAttemptId);
        } catch (RuntimeException exception) {
            return completedWire(WireIngressOutcomeSupport.localDefinite(command,
                    StableCode.INVALID_PREPARED_COMMAND));
        }
        final CompletionStage<KafkaProduceResult> result;
        try {
            result = transport.produce(request);
        } catch (RuntimeException exception) {
            return completedWire(WireIngressOutcomeSupport.uncertain(command, attempt,
                    StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
        if (result == null) {
            return completedWire(WireIngressOutcomeSupport.uncertain(command, attempt,
                    StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
        try {
            final CompletionStage<EnqueueOutcomeMessageV1> handled = result.handle((produce, error) -> {
                if (error != null) {
                    return WireIngressOutcomeSupport.uncertain(command, attempt,
                            StableCode.ENQUEUE_RESULT_UNCERTAIN, null);
                }
                try {
                    return projectWire(command, request, produce, receiptQueryUntilEpochMs, attempt);
                } catch (RuntimeException malformedResult) {
                    // A malformed adapter result is not evidence of non-persistence.
                    return WireIngressOutcomeSupport.uncertain(command, attempt,
                            StableCode.INTEGRITY_ERROR, StableCode.INTEGRITY_ERROR.wireValue());
                }
            });
            return handled == null
                    ? completedWire(WireIngressOutcomeSupport.uncertain(command, attempt,
                    StableCode.ENQUEUE_RESULT_UNCERTAIN, null))
                    : handled;
        } catch (RuntimeException registrationFailure) {
            return completedWire(WireIngressOutcomeSupport.uncertain(command, attempt,
                    StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
    }

    @Override
    public void close() {
        closeGuard.close(transport::close);
    }

    private EnqueueOutcome map(final PreparedCommand command, final KafkaProduceResult result) {
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

    private EnqueueOutcome persisted(final PreparedCommand command, final KafkaProduceResult result) {
        if (!resource.authenticatedClusterId().equals(result.authenticatedClusterId())
                || !resource.nativeTopicUuid().equals(result.nativeTopicUuid())
                || resource.partition() != result.partition()) {
            return EnqueueOutcome.uncertain(command, StableCode.RESOURCE_INCARNATION_MISMATCH.wireValue());
        }
        final KafkaSourcePosition position = new KafkaSourcePosition(command.shardId(),
                result.authenticatedClusterId(), result.nativeTopicUuid(), result.offset(), result.leaderEpoch(),
                result.brokerLogAppendTimeEpochMs());
        return EnqueueOutcome.queued(command, new CommandQueuedReceipt(command.commandId(), command.delayMessageId(),
                command.shardId(), position));
    }

    private EnqueueOutcomeMessageV1 projectWire(final PreparedCommand command, final KafkaProduceRequest request,
                                                final KafkaProduceResult result, final long receiptQueryUntilEpochMs,
                                                final byte[] physicalAttemptId) {
        if (result == null) {
            return WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                    StableCode.ENQUEUE_RESULT_UNCERTAIN, null);
        }
        final StableCode code = WireIngressOutcomeSupport.managedCode(
                WireIngressOutcomeSupport.stableCode(result.stableCode(), StableCode.INTEGRITY_ERROR));
        return switch (result.disposition()) {
            case DEFINITIVELY_NOT_PERSISTED -> {
                final StableCode definitive = WireIngressOutcomeSupport.definitiveManagedCode(result.stableCode());
                yield definitive == null
                        ? WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                        StableCode.INTEGRITY_ERROR, result.stableCode())
                        : WireIngressOutcomeSupport.brokerDefinite(command, physicalAttemptId, definitive,
                        NonPersistenceProofKindV1.KAFKA_DEFINITIVE_REJECTION,
                        BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1(resource.authenticatedClusterId(),
                                resource.nativeTopicUuid())), request.frame(), result.evidence());
            }
            case UNKNOWN -> WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                    code, code == StableCode.INTEGRITY_ERROR ? result.stableCode() : null);
            case PERSISTED -> persistedWire(command, result, receiptQueryUntilEpochMs, physicalAttemptId);
        };
    }

    private EnqueueOutcomeMessageV1 persistedWire(final PreparedCommand command, final KafkaProduceResult result,
                                                  final long receiptQueryUntilEpochMs,
                                                  final byte[] physicalAttemptId) {
        if (!resource.authenticatedClusterId().equals(result.authenticatedClusterId())
                || !resource.nativeTopicUuid().equals(result.nativeTopicUuid())
                || resource.partition() != result.partition()) {
            return WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                    StableCode.RESOURCE_INCARNATION_MISMATCH, StableCode.RESOURCE_INCARNATION_MISMATCH.wireValue());
        }
        if (result.evidence() == null) {
            return WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                    StableCode.ENQUEUE_RESULT_UNCERTAIN, null);
        }
        final KafkaSourcePosition source = new KafkaSourcePosition(command.shardId(), result.authenticatedClusterId(),
                result.nativeTopicUuid(), result.offset(), result.leaderEpoch(), result.brokerLogAppendTimeEpochMs());
        final CommandQueuedReceiptV1.KafkaQueuedAck ack = new CommandQueuedReceiptV1.KafkaQueuedAck(
                result.authenticatedClusterId(), result.nativeTopicUuid(), result.partition(), result.offset(),
                result.leaderEpoch(), result.brokerLogAppendTimeEpochMs(), Bytes.sha256(result.evidence()));
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
    public interface KafkaProduceTransport extends AutoCloseable {
        CompletionStage<KafkaProduceResult> produce(KafkaProduceRequest request);

        @Override
        default void close() {
            // Implementations close their producer/connection here.
        }
    }
}
