package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.KafkaProduceRequest;
import com.nereusstream.delay.adapter.KafkaProduceResult;
import com.nereusstream.delay.adapter.PinnedKafkaCommandIngress;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.GuardedRecordMetadata;
import org.apache.kafka.clients.producer.GuardedResponseEvidence;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.clients.producer.ResourceGuardException;
import org.apache.kafka.common.Uuid;

/**
 * Binding to the source-locked K1 Kafka client artifact.
 *
 * <p>This class is compiled only by the opt-in {@code realKafka} source set.
 * Keeping the artifact dependency there prevents a stock Kafka client from
 * becoming an accidental production fallback in the shared semantic module.</p>
 */
public final class KafkaClientArtifactProduceTransport implements PinnedKafkaCommandIngress.KafkaProduceTransport {
    private final GuardedProducer<byte[], byte[]> producer;

    public KafkaClientArtifactProduceTransport(final GuardedProducer<byte[], byte[]> producer) {
        this.producer = Objects.requireNonNull(producer, "producer");
    }

    @Override
    public CompletionStage<KafkaProduceResult> produce(final KafkaProduceRequest request) {
        Objects.requireNonNull(request, "request");
        final CompletableFuture<KafkaProduceResult> result = new CompletableFuture<>();
        final ProducerResourceGuard guard;
        try {
            guard = new ProducerResourceGuard(
                    request.authenticatedClusterId(),
                    request.canonicalPhysicalTopic(),
                    new Uuid(
                            request.nativeTopicUuid().getMostSignificantBits(),
                            request.nativeTopicUuid().getLeastSignificantBits()),
                    request.partition());
            final ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(request.canonicalPhysicalTopic(), request.partition(), null, request.frame());
            producer.sendGuarded(record, guard, (metadata, failure) -> {
                if (failure == null) {
                    result.complete(success(request, metadata));
                } else {
                    result.complete(failure(request, failure));
                }
            });
        } catch (RuntimeException failure) {
            result.complete(failure(request, failure));
        }
        return result;
    }

    @Override
    public void close() {
        producer.close();
    }

    private static KafkaProduceResult success(final KafkaProduceRequest request, final GuardedRecordMetadata metadata) {
        if (metadata == null || metadata.recordMetadata() == null || metadata.responseEvidence() == null) {
            return KafkaProduceResult.unknown(
                    com.nereusstream.delay.protocol.StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue(), null);
        }
        final GuardedResponseEvidence evidence = metadata.responseEvidence();
        if (!request.canonicalPhysicalTopic().equals(evidence.canonicalTopic())
                || !request.authenticatedClusterId().equals(evidence.authenticatedClusterId())
                || !new Uuid(
                                request.nativeTopicUuid().getMostSignificantBits(),
                                request.nativeTopicUuid().getLeastSignificantBits())
                        .equals(evidence.expectedTopicId())
                || request.partition() != evidence.partition()
                || evidence.errorCode() != 0
                || !request.canonicalPhysicalTopic()
                        .equals(metadata.recordMetadata().topic())
                || request.partition() != metadata.recordMetadata().partition()
                || metadata.recordMetadata().offset() < 0
                || evidence.logAppendTimeMs() < -1) {
            return KafkaProduceResult.unknown(
                    com.nereusstream.delay.protocol.StableCode.INTEGRITY_ERROR.wireValue(),
                    encodeResponseEvidence(evidence));
        }
        // Kafka CreateTime topics legally return -1. That response is not a
        // queued-receipt authority because the Worker needs broker time.
        if (evidence.logAppendTimeMs() < 0) {
            return KafkaProduceResult.unknown(
                    com.nereusstream.delay.protocol.StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue(),
                    encodeResponseEvidence(evidence));
        }
        final Integer leaderEpoch = evidence.responseLeaderEpoch().isPresent()
                ? evidence.responseLeaderEpoch().getAsInt()
                : null;
        return new KafkaProduceResult(
                KafkaProduceResult.Disposition.PERSISTED,
                request.authenticatedClusterId(),
                request.nativeTopicUuid(),
                request.partition(),
                metadata.recordMetadata().offset(),
                leaderEpoch,
                evidence.logAppendTimeMs(),
                com.nereusstream.delay.protocol.StableCode.OK.wireValue(),
                encodeRequestEvidence(evidence),
                encodeResponseEvidence(evidence));
    }

    private static KafkaProduceResult failure(final KafkaProduceRequest request, final Exception failure) {
        final ResourceGuardException guardFailure = unwrap(failure);
        if (guardFailure != null
                && guardFailure.definitelyNotPersisted()
                && guardFailure.responseEvidence().isPresent()) {
            return KafkaProduceResult.definitelyNotPersisted(
                    com.nereusstream.delay.protocol.StableCode.BROKER_DEFINITIVE_NOT_PERSISTED.wireValue(),
                    encodeResponseEvidence(guardFailure.responseEvidence().get()));
        }
        final byte[] evidence =
                guardFailure == null || guardFailure.responseEvidence().isEmpty()
                        ? null
                        : encodeResponseEvidence(guardFailure.responseEvidence().get());
        return KafkaProduceResult.unknown(
                com.nereusstream.delay.protocol.StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue(), evidence);
    }

    private static ResourceGuardException unwrap(final Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof ResourceGuardException resourceGuardException) {
                return resourceGuardException;
            }
            current = current.getCause();
        }
        return null;
    }

    private static byte[] encodeRequestEvidence(final GuardedResponseEvidence evidence) {
        return encode(evidence, false);
    }

    private static byte[] encodeResponseEvidence(final GuardedResponseEvidence evidence) {
        return encode(evidence, true);
    }

    private static byte[] encode(final GuardedResponseEvidence evidence, final boolean response) {
        try {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
            final DataOutputStream output = new DataOutputStream(bytes);
            output.writeInt(1);
            output.writeBoolean(response);
            writeText(output, evidence.authenticatedClusterId());
            writeText(output, evidence.canonicalTopic());
            output.writeLong(evidence.expectedTopicId().getMostSignificantBits());
            output.writeLong(evidence.expectedTopicId().getLeastSignificantBits());
            output.writeInt(evidence.partition());
            output.writeShort(evidence.requestVersion());
            output.writeInt(evidence.correlationId());
            output.writeInt(evidence.brokerNodeId());
            output.writeShort(evidence.errorCode());
            output.writeLong(evidence.baseOffset());
            output.writeLong(evidence.logAppendTimeMs());
            final OptionalInt leaderEpoch = evidence.responseLeaderEpoch();
            output.writeBoolean(leaderEpoch.isPresent());
            if (leaderEpoch.isPresent()) {
                output.writeInt(leaderEpoch.getAsInt());
            }
            writeBytes(output, evidence.produceRequestBodySha256());
            writeBytes(output, evidence.produceResponseBodySha256());
            writeBytes(output, evidence.selectedBatchRecordsSha256());
            writeBytes(output, evidence.selectedRecordValueSha256());
            output.writeInt(evidence.selectedBatchRecordIndex());
            output.writeInt(evidence.selectedBatchRecordCount());
            output.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory Kafka evidence encoding failed", impossible);
        }
    }

    private static void writeText(final DataOutputStream output, final String value) throws IOException {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static void writeBytes(final DataOutputStream output, final byte[] value) throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }
}
