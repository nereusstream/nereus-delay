package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceAcknowledgement;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.TargetWorkerSourceStartup;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.GuardedConsumerRecords;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Source-set Kafka binding for one assigned Shard Log partition.
 *
 * <p>The consumer deliberately has no Nereus apply authority: it decodes one
 * canonical Client Command or signed System Mutation, exposes the exact
 * Kafka Source Position, and advances the broker cursor only after
 * {@code commitSync} returns. A commit exception is always {@code UNKNOWN};
 * the caller must retain the record and retry it after a fresh
 * consumer/owner boundary.</p>
 */
public final class KafkaClientArtifactSourceRecordConsumer implements SourceRecordConsumer {
    private final GuardedConsumer<byte[], byte[]> consumer;
    private final String authenticatedClusterId;
    private final UUID nativeTopicUuid;
    private final ShardId shard;
    private final TopicPartition topicPartition;
    private final ConsumerResourceGuard expectedGuard;
    private final Duration pollTimeout;
    private final boolean target;
    private final ArrayDeque<BufferedRecord> buffered = new ArrayDeque<>();
    private BufferedRecord inFlight;
    private KafkaSourcePosition lastAcknowledged;
    private TargetWorkerSourceStartup frameStartup;
    private TargetWorkerSourceStartup.FrameDecodeLoan frameDecode;
    private SourcePosition frameDecodePosition;
    private long frameDecodeBytes;
    private SourceReplayEntry decodedFrameEntry;
    private volatile boolean closed;
    private volatile boolean closing;
    private final KafkaClientArtifactOwnedSourceBackend backend;
    private java.util.concurrent.CompletableFuture<Void> firstClose;

    public KafkaClientArtifactSourceRecordConsumer(
            final GuardedConsumer<byte[], byte[]> consumer,
            final String authenticatedClusterId,
            final UUID nativeTopicUuid,
            final ShardId shard,
            final String physicalTopic,
            final Duration pollTimeout) {
        this(consumer, authenticatedClusterId, nativeTopicUuid, shard, physicalTopic, pollTimeout, false, null);
    }

    /** Explicit Target writer input path; the legacy constructor retains its Control reader boundary. */
    public static KafkaClientArtifactSourceRecordConsumer forTarget(
            final GuardedConsumer<byte[], byte[]> consumer,
            final String authenticatedClusterId,
            final UUID nativeTopicUuid,
            final ShardId shard,
            final String physicalTopic,
            final Duration pollTimeout) {
        return new KafkaClientArtifactSourceRecordConsumer(
                consumer, authenticatedClusterId, nativeTopicUuid, shard, physicalTopic, pollTimeout, true, null);
    }

    static KafkaClientArtifactSourceRecordConsumer forOwnedTarget(
            GuardedConsumer<byte[], byte[]> consumer,
            String cluster,
            UUID topic,
            ShardId shard,
            String physicalTopic,
            Duration poll,
            KafkaClientArtifactOwnedSourceBackend owner) {
        return new KafkaClientArtifactSourceRecordConsumer(
                consumer, cluster, topic, shard, physicalTopic, poll, true, Objects.requireNonNull(owner));
    }

    private KafkaClientArtifactSourceRecordConsumer(
            final GuardedConsumer<byte[], byte[]> consumer,
            final String authenticatedClusterId,
            final UUID nativeTopicUuid,
            final ShardId shard,
            final String physicalTopic,
            final Duration pollTimeout,
            final boolean target,
            final KafkaClientArtifactOwnedSourceBackend backend) {
        this.backend = backend;
        this.target = target;
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.authenticatedClusterId = Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        this.nativeTopicUuid = Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        this.shard = Objects.requireNonNull(shard, "shard");
        this.topicPartition =
                new TopicPartition(Objects.requireNonNull(physicalTopic, "physicalTopic"), shard.partition());
        this.expectedGuard = new ConsumerResourceGuard(
                authenticatedClusterId, physicalTopic, toKafkaUuid(nativeTopicUuid), shard.partition());
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
        if (pollTimeout.isNegative() || pollTimeout.isZero()) {
            throw new IllegalArgumentException("pollTimeout must be positive");
        }
        if (!expectedGuard.equals(consumer.resourceGuard())) {
            throw new IllegalArgumentException("Kafka guarded source consumer has a different resource guard");
        }
        consumer.assign(List.of(topicPartition));
    }

    @Override
    public Optional<PolledSourceRecord> poll() {
        return backend == null ? pollOpen() : backend.nativeOperation(this::pollOpen);
    }

    @Override
    public synchronized void bindFrameDecode(TargetWorkerSourceStartup actual) {
        Objects.requireNonNull(actual, "original Source frame startup");
        if (!target
                || backend == null
                || inFlight != null
                || !buffered.isEmpty()
                || !actual.acceptedAssignment().shardId().equals(shard)
                || frameStartup != null && frameStartup != actual) {
            throw new IllegalStateException("Kafka frame decoding must bind its original Source before delivery");
        }
        frameStartup = actual;
    }

    private synchronized Optional<PolledSourceRecord> pollOpen() {
        ensureOpen();
        if (inFlight != null) {
            throw new IllegalStateException("previous Kafka source record has not been ACKed");
        }
        if (buffered.isEmpty()) {
            final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(pollTimeout);
            final GuardedFetchEvidence evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, expectedGuard);
            for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, expectedGuard);
                buffered.addLast(new BufferedRecord(record, evidence));
            }
        }
        if (buffered.isEmpty()) {
            return Optional.empty();
        }
        final BufferedRecord fetched = buffered.removeFirst();
        final ConsumerRecord<byte[], byte[]> record = fetched.record();
        try {
            if (record.timestamp() < 0 || record.offset() < 0 || record.offset() == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Kafka source record lacks a bounded broker position");
            }
            final KafkaSourcePosition position = new KafkaSourcePosition(
                    shard,
                    authenticatedClusterId,
                    nativeTopicUuid,
                    record.offset(),
                    record.leaderEpoch().orElse(null),
                    record.timestamp());
            final byte[] value = requireValue(record);
            if (target) {
                retainFrameDecode(position, value.length);
            }
            final SourceReplayEntry entry = target
                    ? KafkaClientArtifactSourceRecordDecoder.decodeTarget(value, shard, position, null, null)
                    : KafkaClientArtifactSourceRecordDecoder.decode(value, shard, position, null, null);
            if (target) {
                frameDecode.retainDecoded(entry);
                decodedFrameEntry = entry;
            }
            inFlight = fetched;
            return Optional.of(new PolledSourceRecord(
                    entry, (candidate, ignoredOutcome) -> acknowledge(fetched, entry, candidate)));
        } catch (RuntimeException | Error failure) {
            buffered.addFirst(fetched);
            throw failure;
        }
    }

    private void retainFrameDecode(SourcePosition position, long bytes) {
        if (frameDecode == null) {
            if (frameStartup == null) {
                throw new TargetWorkerSourceStartup.FrameCapacityUnavailable(
                        "Kafka active Source lacks its original complete frame hold");
            }
            frameDecode = frameStartup.acquireFrameDecode(position, bytes, null, null);
            frameDecodePosition = position;
            frameDecodeBytes = bytes;
        } else if (frameDecodeBytes != bytes
                || !java.util.Arrays.equals(frameDecodePosition.canonicalBytes(), position.canonicalBytes())) {
            throw new IllegalStateException("Kafka active Source changed its retained raw frame");
        }
        frameDecode.requireCurrent();
    }

    private void releaseFrameDecode(SourceReplayEntry actual) {
        if (!target) {
            return;
        }
        if (frameDecode == null || decodedFrameEntry != actual) {
            throw new IllegalStateException("Kafka ACK changed its original frame decode loan");
        }
        frameDecode.requestRelease();
        if (!frameDecode.cleanupComplete()) {
            throw new TargetWorkerSourceStartup.FrameCapacityUnavailable(
                    "Kafka ACK retains its original frame decode cleanup");
        }
        frameDecode = null;
        frameDecodePosition = null;
        frameDecodeBytes = 0;
        decodedFrameEntry = null;
    }

    @Override
    public synchronized void close() {
        if (closed && backend != null && firstClose == null) {
            firstClose = backend.originalClose();
        }
        if (closed && (firstClose == null || !firstClose.isCompletedExceptionally())) {
            return;
        }
        closing = true;
        if (backend != null) {
            final var original = backend.closeSourceNormally();
            if (firstClose == null) {
                firstClose = original;
            }
        } else if (firstClose == null) {
            firstClose = new java.util.concurrent.CompletableFuture<>();
            try {
                consumer.close();
                firstClose.complete(null);
            } catch (RuntimeException | Error failed) {
                firstClose.completeExceptionally(failed);
            }
        }
        try {
            firstClose.copy().get(30_000, java.util.concurrent.TimeUnit.MILLISECONDS);
            closed = true;
        } catch (InterruptedException failed) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("K1 Source close interrupted", failed);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failed) {
            throw new IllegalStateException("K1 Source original close remains unconfirmed", failed);
        }
    }

    @Override
    public com.nereusstream.delay.ownership.TargetOwnedBackendGroup ownedBackendGroup() {
        return backend;
    }

    @Override
    public synchronized com.nereusstream.delay.ownership.SourceReplaySuccessor successorAfter(
            SourcePosition resumeAfter) {
        ensureOpen();
        if (!target
                || backend == null
                || inFlight != null
                || !buffered.isEmpty()
                || lastAcknowledged != null
                || !(resumeAfter instanceof KafkaSourcePosition anchor)
                || !shard.equals(anchor.shardId())
                || !authenticatedClusterId.equals(anchor.authenticatedClusterId())
                || !nativeTopicUuid.equals(anchor.nativeTopicUuid())) {
            throw new IllegalArgumentException(
                    "active K1 proof must bind to its exact original Store cut before delivery");
        }
        if (!expectedGuard.equals(consumer.resourceGuard())) {
            throw new IllegalStateException("active K1 resource guard changed");
        }
        return com.nereusstream.delay.ownership.SourceReplaySuccessor.strictKafka();
    }

    void backendTerminated(KafkaClientArtifactOwnedSourceBackend original) {
        if (original != backend || !original.nativeResourcesTerminated() || original.applicationReady()) {
            throw new IllegalStateException("K1 Source lacks its original backend termination proof");
        }
        closed = true; // retained buffered/inFlight entry and original ACK result are not discarded
    }

    @Override
    public CheckpointCut checkpointCut(final SourcePosition expectedAppliedPosition) {
        return backend == null
                ? checkpointCutOpen(expectedAppliedPosition)
                : backend.nativeReadOperation(
                        com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.ACK,
                        () -> checkpointCutOpen(expectedAppliedPosition));
    }

    private synchronized CheckpointCut checkpointCutOpen(final SourcePosition expectedAppliedPosition) {
        if (!(expectedAppliedPosition instanceof KafkaSourcePosition expected)) {
            throw new IllegalArgumentException("Kafka checkpoint requires a Kafka source position");
        }
        requireCheckpointCut(expected);
        return new CheckpointCut() {
            @Override
            public SourcePosition position() {
                return expected;
            }

            @Override
            public void requireCurrent() {
                if (backend == null) {
                    synchronized (KafkaClientArtifactSourceRecordConsumer.this) {
                        requireCheckpointCut(expected);
                    }
                } else {
                    backend.nativeReadOperation(
                            com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.ACK,
                            () -> {
                                synchronized (KafkaClientArtifactSourceRecordConsumer.this) {
                                    requireCheckpointCut(expected);
                                }
                                return null;
                            });
                }
            }
        };
    }

    private void requireCheckpointCut(final KafkaSourcePosition expected) {
        ensureOpen();
        if (inFlight != null
                || lastAcknowledged == null
                || !Bytes.constantTimeEquals(lastAcknowledged.canonicalBytes(), expected.canonicalBytes())
                || !expectedGuard.equals(consumer.resourceGuard())
                || !consumer.assignment().equals(Set.of(topicPartition))) {
            throw new IllegalStateException("Kafka checkpoint source cut is not the last durable ACK");
        }
        final OffsetAndMetadata committed =
                consumer.committed(Set.of(topicPartition), pollTimeout).get(topicPartition);
        if (committed == null || committed.offset() != expected.offset() + 1) {
            throw new IllegalStateException("Kafka checkpoint committed offset differs from the Store cut");
        }
    }

    private SourceAcknowledgement.AcknowledgementResult acknowledge(
            final BufferedRecord fetched, final SourceReplayEntry expected, final SourceReplayEntry candidate) {
        if (backend != null) {
            try {
                return backend.nativeOperation(
                        com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.ACK,
                        () -> acknowledgeOpen(fetched, expected, candidate));
            } catch (RuntimeException | Error failed) {
                return SourceAcknowledgement.AcknowledgementResult.unknown(failed);
            }
        }
        return acknowledgeOpen(fetched, expected, candidate);
    }

    private SourceAcknowledgement.AcknowledgementResult acknowledgeOpen(
            BufferedRecord fetched, SourceReplayEntry expected, SourceReplayEntry candidate) {
        if (candidate != expected) {
            return SourceAcknowledgement.AcknowledgementResult.unknown(
                    new IllegalStateException("Kafka source ACK entry identity changed"));
        }
        synchronized (this) {
            if (inFlight != fetched || closed || closing) {
                return SourceAcknowledgement.AcknowledgementResult.unknown(
                        new IllegalStateException("Kafka source ACK state changed"));
            }
        }
        try {
            consumer.commitSync(java.util.Map.of(
                    topicPartition, new OffsetAndMetadata(fetched.record().offset() + 1)));
            synchronized (this) {
                if (inFlight != fetched) {
                    return SourceAcknowledgement.AcknowledgementResult.unknown(
                            new IllegalStateException("Kafka source ACK state changed"));
                }
                releaseFrameDecode(expected);
                lastAcknowledged = (KafkaSourcePosition) expected.position();
                inFlight = null;
            }
            return SourceAcknowledgement.AcknowledgementResult.acked();
        } catch (RuntimeException failure) {
            return SourceAcknowledgement.AcknowledgementResult.unknown(failure);
        }
    }

    private static byte[] requireValue(final ConsumerRecord<byte[], byte[]> record) {
        if (record.value() == null || record.value().length == 0) {
            throw new IllegalArgumentException("Kafka source record has no NDL1 value");
        }
        return record.value();
    }

    private void ensureOpen() {
        if (closed || closing) {
            throw new IllegalStateException("Kafka source consumer is closed or closing");
        }
    }

    private static org.apache.kafka.common.Uuid toKafkaUuid(final UUID uuid) {
        return new org.apache.kafka.common.Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    private record BufferedRecord(ConsumerRecord<byte[], byte[]> record, GuardedFetchEvidence evidence) {}
}
