package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.ShardLogMutationAppender;
import com.nereusstream.delay.ownership.SystemWriterAppendBudget;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SystemMutation;
import java.time.Duration;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.GuardedRecordMetadata;
import org.apache.kafka.clients.producer.GuardedResponseEvidence;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.clients.producer.ResourceGuardException;
import org.apache.kafka.common.Uuid;

/**
 * Guarded K1 append authority for signed System Mutations on one Shard Log
 * partition.
 *
 * <p>The synchronous SPI is backed by the native guarded producer callback,
 * so a successful result is returned only after the broker response carries
 * the exact cluster/topic UUID/partition and log-append timestamp. A timeout,
 * malformed response or ordinary producer failure remains UNKNOWN; only the
 * K1 resource-guard exception that proves non-persistence can return the
 * definitive negative branch.</p>
 */
public final class KafkaClientArtifactShardLogMutationAppender implements ShardLogMutationAppender, AutoCloseable {
    private final GuardedProducer<byte[], byte[]> producer;
    private final ShardId shard;
    private final String authenticatedClusterId;
    private final String physicalTopic;
    private final UUID nativeTopicUuid;
    private final int partition;
    private final ProducerResourceGuard guard;
    private final Duration responseTimeout;
    private final SystemWriterAppendBudget.Writer writer;

    public KafkaClientArtifactShardLogMutationAppender(
            final GuardedProducer<byte[], byte[]> producer,
            final ShardId shard,
            final String authenticatedClusterId,
            final String physicalTopic,
            final UUID nativeTopicUuid,
            final Duration responseTimeout) {
        this(producer, shard, authenticatedClusterId, physicalTopic, nativeTopicUuid, responseTimeout, null);
    }

    public KafkaClientArtifactShardLogMutationAppender(
            final GuardedProducer<byte[], byte[]> producer,
            final ShardId shard,
            final String authenticatedClusterId,
            final String physicalTopic,
            final UUID nativeTopicUuid,
            final Duration responseTimeout,
            final SystemWriterAppendBudget.Writer writer) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.shard = Objects.requireNonNull(shard, "shard");
        this.authenticatedClusterId = requireText(authenticatedClusterId, "authenticatedClusterId");
        this.physicalTopic = requireText(physicalTopic, "physicalTopic");
        this.nativeTopicUuid = Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        this.partition = shard.partition();
        this.guard = new ProducerResourceGuard(
                this.authenticatedClusterId,
                this.physicalTopic,
                new Uuid(nativeTopicUuid.getMostSignificantBits(), nativeTopicUuid.getLeastSignificantBits()),
                this.partition);
        this.responseTimeout = Objects.requireNonNull(responseTimeout, "responseTimeout");
        if (responseTimeout.isZero() || responseTimeout.isNegative() || responseTimeout.toMillis() <= 0) {
            throw new IllegalArgumentException("responseTimeout must be positive");
        }
        this.writer = writer;
        if (writer != null) {
            writer.bindProducer(producer);
        }
    }

    @Override
    public AppendOutcome append(final SystemMutation mutation) {
        final SystemMutation exact = Objects.requireNonNull(mutation, "mutation");
        if (!shard.equals(exact.shardId())) {
            throw new IllegalArgumentException("Kafka Shard Log mutation belongs to another Shard");
        }
        final var reservation = writer == null ? null : writer.tryReserve(shard).orElse(null);
        if (writer != null && reservation == null) {
            return AppendOutcome.definitelyNotPersisted();
        }
        try {
            return appendReserved(exact, reservation);
        } finally {
            if (reservation != null) {
                reservation.callerReturned();
            }
        }
    }

    private AppendOutcome appendReserved(final SystemMutation exact,
            final SystemWriterAppendBudget.Reservation reservation) {
        final byte[] frame;
        try {
            frame = exact.encodeFrame();
        } catch (RuntimeException failure) {
            if (reservation == null) { throw failure; }
            reservation.sdkCompleted();
            return AppendOutcome.definitelyNotPersisted();
        }
        if (reservation != null && !reservation.frameEncoded(frame.length)) {
            reservation.sdkCompleted();
            return AppendOutcome.definitelyNotPersisted();
        }
        final CompletableFuture<Observation> completion = new CompletableFuture<>();
        try {
            final java.util.function.Supplier<java.util.concurrent.Future<GuardedRecordMetadata>> send = () ->
                    producer.sendGuarded(new ProducerRecord<>(physicalTopic, partition, null, frame), guard,
                            (metadata, failure) -> {
                                try {
                                    completion.complete(new Observation(metadata, failure));
                                } finally {
                                    if (reservation != null) { reservation.sdkCompleted(); }
                                }
                            });
            if (writer == null) {
                send.get();
            } else if (!writer.invokeSdk(reservation, send).started()) {
                return AppendOutcome.definitelyNotPersisted();
            }
        } catch (RuntimeException failure) {
            final ResourceGuardException proof = unwrap(failure);
            if (reservation != null && proof != null && proof.definitelyNotPersisted()) {
                reservation.sdkCompleted();
            }
            return failure(failure);
        }
        final Observation observation;
        try {
            observation = completion.get(responseTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return AppendOutcome.unknown();
        } catch (ExecutionException execution) {
            return failure(execution.getCause());
        } catch (TimeoutException timeout) {
            return AppendOutcome.unknown();
        }
        if (observation.failure() != null) {
            return failure(observation.failure());
        }
        return persisted(observation.metadata());
    }

    @Override
    public void close() {
        if (writer != null) { writer.beginClose(); }
        producer.close();
        if (writer != null) { writer.producerClosed(); }
    }

    private AppendOutcome persisted(final GuardedRecordMetadata metadata) {
        if (metadata == null || metadata.recordMetadata() == null || metadata.responseEvidence() == null) {
            return AppendOutcome.unknown();
        }
        final GuardedResponseEvidence evidence = metadata.responseEvidence();
        if (!authenticatedClusterId.equals(evidence.authenticatedClusterId())
                || !physicalTopic.equals(evidence.canonicalTopic())
                || !guard.expectedTopicId().equals(evidence.expectedTopicId())
                || evidence.partition() != partition
                || evidence.errorCode() != 0
                || !physicalTopic.equals(metadata.recordMetadata().topic())
                || metadata.recordMetadata().partition() != partition
                || metadata.recordMetadata().offset() < 0
                || evidence.logAppendTimeMs() < 0) {
            return AppendOutcome.unknown();
        }
        final OptionalInt leaderEpoch = evidence.responseLeaderEpoch();
        final KafkaSourcePosition position = new KafkaSourcePosition(
                shard,
                authenticatedClusterId,
                nativeTopicUuid,
                metadata.recordMetadata().offset(),
                leaderEpoch.isPresent() ? leaderEpoch.getAsInt() : null,
                evidence.logAppendTimeMs());
        return AppendOutcome.persisted(position);
    }

    private static AppendOutcome failure(final Throwable failure) {
        final ResourceGuardException guardFailure = unwrap(failure);
        return guardFailure != null && guardFailure.definitelyNotPersisted()
                ? AppendOutcome.definitelyNotPersisted()
                : AppendOutcome.unknown();
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

    private static String requireText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must be nonblank");
        }
        return value;
    }

    private record Observation(GuardedRecordMetadata metadata, Throwable failure) {}
}
