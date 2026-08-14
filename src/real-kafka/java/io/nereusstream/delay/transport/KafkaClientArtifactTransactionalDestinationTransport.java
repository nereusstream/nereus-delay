package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.DestinationPublishResult;
import io.nereusstream.delay.adapter.KafkaTransactionalDestinationRequest;
import io.nereusstream.delay.adapter.KafkaTransactionalDestinationAdapter;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.StableCode;
import org.apache.kafka.clients.producer.GuardedRecordMetadata;
import org.apache.kafka.clients.producer.GuardedResponseEvidence;
import org.apache.kafka.clients.producer.GuardedTransactionalProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.clients.producer.ResourceGuardException;
import org.apache.kafka.common.Uuid;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Source-locked K2 binding. It sends the business target and keyed receipt
 * through the same transaction-scoped guarded producer and never falls back
 * to ordinary Producer.send or a second transaction.
 */
public final class KafkaClientArtifactTransactionalDestinationTransport
        implements KafkaTransactionalDestinationAdapter.KafkaTransactionalDestinationTransport {
    private final GuardedTransactionalProducer<byte[], byte[]> producer;
    private final ExecutorService completionExecutor;
    private final AtomicBoolean inFlight = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public KafkaClientArtifactTransactionalDestinationTransport(
            final GuardedTransactionalProducer<byte[], byte[]> producer) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.completionExecutor = Executors.newSingleThreadExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "nereus-delay-kafka-k2-completion");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(final KafkaTransactionalDestinationRequest request) {
        Objects.requireNonNull(request, "request");
        final CompletableFuture<DestinationPublishResult> result = new CompletableFuture<>();
        if (closed.get() || !inFlight.compareAndSet(false, true)) {
            result.complete(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
            return result;
        }

        final ProducerResourceGuard targetGuard = new ProducerResourceGuard(
                request.target().authenticatedClusterId(), request.targetPhysicalTopic(),
                toKafkaUuid(request.target().nativeTopicUuid()), request.target().partition());
        final ProducerResourceGuard receiptGuard = new ProducerResourceGuard(
                request.receiptResource().authenticatedClusterId(), request.receiptPhysicalTopic(),
                toKafkaUuid(request.receiptResource().nativeTopicUuid()), request.receiptResource().receiptPartition());
        final CompletableFuture<GuardedRecordMetadata> target = new CompletableFuture<>();
        final CompletableFuture<GuardedRecordMetadata> receipt = new CompletableFuture<>();
        boolean transactionStarted = false;
        try {
            producer.beginTransaction();
            transactionStarted = true;
            producer.sendGuardedInTransaction(new ProducerRecord<>(request.targetPhysicalTopic(),
                    request.target().partition(), null, request.target().payload()), targetGuard,
                    (metadata, failure) -> complete(target, metadata, failure));
            producer.sendGuardedInTransaction(new ProducerRecord<>(request.receiptPhysicalTopic(),
                    request.receiptResource().receiptPartition(), null, request.receiptKey(), request.receiptValue()),
                    receiptGuard, (metadata, failure) -> complete(receipt, metadata, failure));
            CompletableFuture.allOf(target, receipt).whenComplete((ignored, failure) ->
                    completionExecutor.execute(() -> finish(request, targetGuard, receiptGuard, target, receipt,
                            result, failure)));
        } catch (RuntimeException failure) {
            if (transactionStarted) {
                abortAfterFailure(result, failure, null);
            } else {
                inFlight.set(false);
                result.complete(DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
            }
        }
        return result;
    }

    private void finish(final KafkaTransactionalDestinationRequest request,
                        final ProducerResourceGuard targetGuard,
                        final ProducerResourceGuard receiptGuard,
                        final CompletableFuture<GuardedRecordMetadata> target,
                        final CompletableFuture<GuardedRecordMetadata> receipt,
                        final CompletableFuture<DestinationPublishResult> result,
                        final Throwable sendFailure) {
        if (sendFailure != null || target.isCompletedExceptionally() || receipt.isCompletedExceptionally()) {
            final Throwable failure = firstFailure(sendFailure, target, receipt);
            abortAfterFailure(result, failure, guardedEvidence(failure));
            return;
        }

        final GuardedRecordMetadata targetMetadata = target.getNow(null);
        final GuardedRecordMetadata receiptMetadata = receipt.getNow(null);
        if (!valid(targetMetadata, targetGuard) || !valid(receiptMetadata, receiptGuard)
                || targetMetadata.responseEvidence().logAppendTimeMs() < 0
                || receiptMetadata.responseEvidence().logAppendTimeMs() < 0) {
            abortAfterFailure(result, new IllegalStateException("K2 guarded response evidence did not validate"),
                    evidence(targetMetadata, receiptMetadata));
            return;
        }
        try {
            producer.commitTransaction();
            final GuardedResponseEvidence targetEvidence = targetMetadata.responseEvidence();
            result.complete(DestinationPublishResult.published(
                    BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1(
                            request.target().authenticatedClusterId(), request.target().nativeTopicUuid())),
                    request.target().partition(), request.mapping().publishAttemptId(),
                    targetEvidence.logAppendTimeMs(), evidence(targetMetadata, receiptMetadata)));
        } catch (RuntimeException commitFailure) {
            // A lost EndTxn response is not a non-publication proof. The local
            // mapping remains unresolved for the read-committed receipt path.
            result.complete(DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN,
                    evidence(targetMetadata, receiptMetadata)));
        } finally {
            inFlight.set(false);
        }
    }

    private void abortAfterFailure(final CompletableFuture<DestinationPublishResult> result,
                                   final Throwable failure, final byte[] evidence) {
        try {
            producer.abortTransaction();
            result.complete(DestinationPublishResult.definitelyNotPublished(
                    StableCode.BROKER_DEFINITIVE_NOT_PERSISTED, evidence));
        } catch (RuntimeException abortFailure) {
            result.complete(DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN,
                    evidence == null ? guardedEvidence(failure) : evidence));
        } finally {
            inFlight.set(false);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                producer.close();
            } finally {
                completionExecutor.shutdownNow();
            }
        }
    }

    private static void complete(final CompletableFuture<GuardedRecordMetadata> target,
                                 final GuardedRecordMetadata metadata, final Exception failure) {
        if (failure == null && metadata != null) {
            target.complete(metadata);
        } else {
            target.completeExceptionally(failure == null
                    ? new IllegalStateException("guarded Kafka callback returned no result") : failure);
        }
    }

    private static boolean valid(final GuardedRecordMetadata metadata, final ProducerResourceGuard guard) {
        if (metadata == null || !guard.equals(metadata.resourceGuard()) || metadata.recordMetadata() == null
                || metadata.recordMetadata().offset() < 0 || metadata.responseEvidence() == null) {
            return false;
        }
        final GuardedResponseEvidence evidence = metadata.responseEvidence();
        return evidence.errorCode() == 0 && evidence.requestVersion() >= 13
                && guard.authenticatedClusterId().equals(evidence.authenticatedClusterId())
                && guard.canonicalTopic().equals(evidence.canonicalTopic())
                && guard.expectedTopicId().equals(evidence.expectedTopicId())
                && guard.partition() == evidence.partition();
    }

    private static Throwable firstFailure(final Throwable sendFailure,
                                          final CompletableFuture<GuardedRecordMetadata> target,
                                          final CompletableFuture<GuardedRecordMetadata> receipt) {
        if (sendFailure != null) {
            return sendFailure;
        }
        for (Future<GuardedRecordMetadata> future : java.util.List.of(target, receipt)) {
            try {
                future.get();
            } catch (Exception failure) {
                return failure.getCause() == null ? failure : failure.getCause();
            }
        }
        return new IllegalStateException("guarded Kafka transaction failed without a typed cause");
    }

    private static byte[] guardedEvidence(final Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof ResourceGuardException resourceGuardException
                    && resourceGuardException.responseEvidence().isPresent()) {
                return encode(resourceGuardException.responseEvidence().get());
            }
            current = current.getCause();
        }
        return null;
    }

    private static byte[] evidence(final GuardedRecordMetadata target, final GuardedRecordMetadata receipt) {
        return Bytes.concat(encode(target.responseEvidence()), encode(receipt.responseEvidence()));
    }

    private static byte[] encode(final GuardedResponseEvidence evidence) {
        return Bytes.concat(Bytes.utf8("nereus-delay-kafka-k2-guarded-evidence-v1\0"),
                evidence.produceRequestBodySha256(), evidence.produceResponseBodySha256(),
                evidence.selectedBatchRecordsSha256(), evidence.selectedRecordValueSha256());
    }

    private static Uuid toKafkaUuid(final java.util.UUID uuid) {
        return new Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }
}
