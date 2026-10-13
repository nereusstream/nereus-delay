package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ShardId;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.GuardedConsumerRecords;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
import org.apache.kafka.common.TopicPartition;

/**
 * Native Kafka replay input for {@code OwnerRecoveryCoordinator}.
 *
 * <p>This cursor never commits a group offset and has no ACK path. The
 * coordinator retains the same decoded record until the bounded Store apply
 * outcome is proven, then calls {@link #next()} to release only the local
 * look-ahead. The caller supplies the already validated durable replay start
 * offset; transient empty polls before the Route activation barrier are
 * retried, while an empty poll after the barrier is a proved source boundary.
 * The Route activation barrier remains an identity fence, not a substitute
 * for local Store recovery metadata.</p>
 */
public final class KafkaClientArtifactRecoverySourceCursor implements Iterator<SourceReplayEntry>,
        com.nereusstream.delay.ownership.TargetWorkerRecoverySource {
    private static final int MAX_EMPTY_POLLS_BEFORE_ACTIVATION_BARRIER = 120;
    private final GuardedConsumer<byte[], byte[]> consumer;
    private final KafkaActivationBarrier activationBarrier;
    private final SourceAssignment assignment;
    private com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt verifiedCredential;
    private final String authenticatedClusterId;
    private final java.util.UUID nativeTopicUuid;
    private final ShardId shard;
    private final TopicPartition topicPartition;
    private final ConsumerResourceGuard expectedGuard;
    private final Duration pollTimeout;
    private final boolean target;
    private final int maximumFrameBytes;
    private final Runnable protection;
    private final ArrayDeque<BufferedRecord> buffered = new ArrayDeque<>();
    private SourceReplayEntry current;
    private int emptyPollsBeforeActivationBarrier;
    private boolean activationBarrierReached;
    private boolean closed;
    private boolean failed;

    public KafkaClientArtifactRecoverySourceCursor(
            final GuardedConsumer<byte[], byte[]> consumer,
            final SourceAssignment assignment,
            final String physicalTopic,
            final long startOffsetInclusive,
            final Duration pollTimeout) {
        this(consumer, assignment, physicalTopic, startOffsetInclusive, pollTimeout, false, Integer.MAX_VALUE, () -> {});
    }

    /**
     * Explicit Target reader. Caller owns/configures this guarded manual consumer with finite Fetch buffers,
     * disabled auto commit, one max-poll record and held Source/Owner/session/credential/retention protections.
     */
    public static KafkaClientArtifactRecoverySourceCursor forTarget(GuardedConsumer<byte[], byte[]> consumer,
            SourceAssignment assignment, String physicalTopic, long startOffsetInclusive, Duration pollTimeout,
            int maximumFrameBytes, Runnable protection) {
        if (maximumFrameBytes <= 0) { throw new IllegalArgumentException("Target recovery frame bound must be positive"); }
        return new KafkaClientArtifactRecoverySourceCursor(consumer, assignment, physicalTopic, startOffsetInclusive,
                pollTimeout, true, maximumFrameBytes, Objects.requireNonNull(protection, "protection"));
    }

    /** Creates the actual no-group/no-ACK client from the verified snapshot, never a caller's borrowed consumer. */
    public static KafkaClientArtifactRecoverySourceCursor openVerified(java.util.Map<String,Object> operational,
            byte[] sdkJar, com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt credential,
            SourceAssignment assignment, long startOffsetInclusive, Duration pollTimeout, int maximumFrameBytes,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time, Runnable originalProtection) {
        if (maximumFrameBytes < 1 || maximumFrameBytes == Integer.MAX_VALUE || pollTimeout.toMillis() < 1
                || pollTimeout.compareTo(Duration.ofSeconds(60)) > 0) throw new IllegalArgumentException("Source recovery settings must be finite");
        com.nereusstream.delay.ownership.TargetSdkArtifactIdentity.requireExact(org.apache.kafka.clients.consumer.KafkaConsumer.class, sdkJar);
        credential.authorization().requireAssignment(assignment);
        final var resource = credential.authorization().resource().kafka();
        final String topic = credential.authorization().physicalTopic();
        final String name = credential.authorization().newReaderName(com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.RECOVERY);
        final var protection = credential.guardRecovery(assignment.shardId(), topic, name, time, originalProtection);
        final var config = new java.util.HashMap<>(KafkaClientArtifactMtlsConfiguration.applySource(operational, credential));
        config.remove(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG);
        config.remove(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG, name);
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maximumFrameBytes);
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG, maximumFrameBytes);
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Math.toIntExact(pollTimeout.toMillis()));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.toIntExact(pollTimeout.toMillis()));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG, false);
        protection.run();
        final var actual = KafkaClientArtifactSourceConsumerFactory.create(config, resource.authenticatedClusterId(), topic,
                resource.nativeTopicUuid(), assignment.shardId().partition(), pollTimeout);
        try {
            protection.run();
            final var cursor = forTarget(actual, assignment, topic, startOffsetInclusive, pollTimeout, maximumFrameBytes, protection);
            cursor.verifiedCredential = credential;
            return cursor;
        } catch (RuntimeException | Error failure) {
            try { actual.closeGuardedResources(pollTimeout); if (!actual.guardedResourcesTerminated()) throw new IllegalStateException("Recovery Source native cleanup is incomplete"); }
            catch (RuntimeException | Error cleanup) {
                final var retained = new KafkaClientArtifactTargetCleanupFailure(failure, () -> {
                    actual.closeGuardedResources(pollTimeout);
                    if (!actual.guardedResourcesTerminated()) throw new IllegalStateException("Recovery Source native cleanup is incomplete");
                });
                retained.addSuppressed(cleanup); throw retained;
            }
            throw failure;
        }
    }

    private KafkaClientArtifactRecoverySourceCursor(GuardedConsumer<byte[], byte[]> consumer, SourceAssignment assignment,
            String physicalTopic, long startOffsetInclusive, Duration pollTimeout, boolean target, int maximumFrameBytes,
            Runnable protection) {
        this.target = target;
        this.maximumFrameBytes = maximumFrameBytes;
        this.protection = protection;
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.assignment = Objects.requireNonNull(assignment);
        final SourceAssignment accepted = Objects.requireNonNull(assignment, "assignment");
        if (!(accepted.activationBarrier() instanceof KafkaActivationBarrier barrier)) {
            throw new IllegalArgumentException("Kafka recovery cursor requires a Kafka activation barrier");
        }
        if (startOffsetInclusive < 0 || barrier.exclusiveOffset() < 0) {
            throw new IllegalArgumentException("Kafka recovery offsets must be non-negative");
        }
        this.activationBarrier = barrier;
        this.authenticatedClusterId = barrier.authenticatedClusterId();
        this.nativeTopicUuid = barrier.nativeTopicUuid();
        this.shard = accepted.shardId();
        final String topic = requirePhysicalTopic(physicalTopic);
        if (shard.partition() < 0) {
            throw new IllegalArgumentException("Kafka recovery partition must be non-negative");
        }
        this.topicPartition = new TopicPartition(topic, shard.partition());
        this.expectedGuard = new ConsumerResourceGuard(
                authenticatedClusterId, topic, toKafkaUuid(nativeTopicUuid), shard.partition());
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
        if (pollTimeout.isNegative() || pollTimeout.isZero()) {
            throw new IllegalArgumentException("pollTimeout must be positive");
        }
        if (!expectedGuard.equals(consumer.resourceGuard())) {
            throw new IllegalArgumentException("Kafka guarded recovery consumer has a different resource guard");
        }
        activationBarrierReached = Long.compareUnsigned(startOffsetInclusive, barrier.exclusiveOffset()) >= 0;
        protection.run();
        consumer.assign(java.util.List.of(topicPartition));
        consumer.seek(topicPartition, startOffsetInclusive);
        protection.run();
    }

    @Override
    public synchronized boolean hasNext() {
        ensureOpen();
        try {
            return load();
        } catch (RuntimeException | Error failure) {
            if (target) { failed = true; }
            throw failure;
        }
    }

    private boolean load() {
        protection.run();
        if (!expectedGuard.equals(consumer.resourceGuard())) {
            throw new IllegalStateException("Kafka recovery resource guard changed");
        }
        if (current != null) {
            return true;
        }
        if (target && activationBarrierReached) { return false; }
        while (buffered.isEmpty()) {
            final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(pollTimeout);
            final GuardedFetchEvidence evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, expectedGuard);
            if (target && records.count() > 1) {
                throw new IllegalStateException("Target recovery Fetch exceeds its one-record look-ahead bound");
            }
            for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, expectedGuard);
                buffered.addLast(new BufferedRecord(record, evidence));
            }
            if (records.isEmpty()) {
                if (activationBarrierReached) {
                    protection.run();
                    return false;
                }
                if (target) {
                    throw new IllegalStateException("Target recovery Source was unavailable before its poll deadline");
                }
                if (++emptyPollsBeforeActivationBarrier >= MAX_EMPTY_POLLS_BEFORE_ACTIVATION_BARRIER) {
                    throw new IllegalStateException("Kafka recovery source did not reach the activation barrier after "
                            + MAX_EMPTY_POLLS_BEFORE_ACTIVATION_BARRIER + " empty polls");
                }
                continue;
            }
            emptyPollsBeforeActivationBarrier = 0;
        }
        final BufferedRecord fetched = buffered.removeFirst();
        final ConsumerRecord<byte[], byte[]> record = fetched.record();
        try {
            if (record.offset() < 0 || record.timestamp() < 0) {
                throw new IllegalArgumentException("Kafka recovery record lacks a bounded broker position");
            }
            final KafkaSourcePosition position = new KafkaSourcePosition(
                    shard,
                    authenticatedClusterId,
                    nativeTopicUuid,
                    record.offset(),
                    record.leaderEpoch().orElse(null),
                    record.timestamp());
            final byte[] value = requireValue(record);
            if (target && value.length > maximumFrameBytes) {
                throw new IllegalStateException("Target recovery Source frame exceeds its declared byte bound");
            }
            current = target
                    ? KafkaClientArtifactSourceRecordDecoder.decodeTarget(value, shard, position, null, null)
                    : KafkaClientArtifactSourceRecordDecoder.decode(value, shard, position, null, null);
            activationBarrierReached = activationBarrier.reachedBy(position);
            protection.run();
            return true;
        } catch (RuntimeException | Error failure) {
            buffered.addFirst(fetched);
            throw failure;
        }
    }

    @Override
    public synchronized SourceReplayEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Kafka recovery source is exhausted");
        }
        final SourceReplayEntry result = current;
        current = null;
        return result;
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            failed = true;
            if (target) {
                consumer.closeGuardedResources(pollTimeout);
                if (!consumer.guardedResourcesTerminated()) throw new IllegalStateException("Recovery Source native cleanup is incomplete");
            }
            else { consumer.close(); }
            closed = true;
        }
    }

    /** Keeps the native Source guard live across Target look-ahead and queued recovery writes. */
    public com.nereusstream.delay.ownership.SourceReplayCursor<SourceReplayEntry> replayCursor() {
        return com.nereusstream.delay.ownership.SourceReplayCursor.guarded(this, this::requireCurrent);
    }

    @Override public synchronized void requireStartup(SourceAssignment expected, long frameBytes,
            com.nereusstream.delay.scheduler.WorkClassExecutionRegistry classes,
            com.nereusstream.delay.store.SharedRocksDbResources resources) {
        if (!target || verifiedCredential == null || !assignment.sameIdentity(expected)
                || maximumFrameBytes > frameBytes) throw new IllegalStateException("recovery cursor lacks its original verified Source startup");
        verifiedCredential.authorization().requireAssignment(expected);
    }
    @Override public synchronized com.nereusstream.delay.ownership.SourceReplaySuccessor successorAfter(
            com.nereusstream.delay.protocol.SourcePosition resumeAfter) {
        requireCurrent(); activationBarrier.validatePosition(resumeAfter);
        return com.nereusstream.delay.ownership.SourceReplaySuccessor.strictKafka();
    }
    @Override public com.nereusstream.delay.ownership.TargetOwnedBackendGroup backend() { return null; }
    @Override public synchronized boolean cleanupComplete() { return closed && target && consumer.guardedResourcesTerminated(); }

    /** Rechecks this exact native Source identity and the caller-held protection without reading or committing. */
    public synchronized void requireCurrent() {
        ensureOpen();
        try {
            protection.run();
            if (!expectedGuard.equals(consumer.resourceGuard())) {
                throw new IllegalStateException("Kafka recovery resource guard changed");
            }
        } catch (RuntimeException | Error failure) {
            if (target) { failed = true; }
            throw failure;
        }
    }

    private static String requirePhysicalTopic(final String physicalTopic) {
        final String topic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("physicalTopic must be non-blank");
        }
        return topic;
    }

    private static byte[] requireValue(final ConsumerRecord<byte[], byte[]> record) {
        if (record.value() == null || record.value().length == 0) {
            throw new IllegalArgumentException("Kafka recovery record has no NDL1 value");
        }
        return record.value();
    }

    private void ensureOpen() {
        if (closed || failed) {
            throw new IllegalStateException("Kafka recovery source is closed or failed; retain it only for cleanup");
        }
    }

    private static org.apache.kafka.common.Uuid toKafkaUuid(final java.util.UUID uuid) {
        return new org.apache.kafka.common.Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    private record BufferedRecord(ConsumerRecord<byte[], byte[]> record, GuardedFetchEvidence evidence) {}
}
