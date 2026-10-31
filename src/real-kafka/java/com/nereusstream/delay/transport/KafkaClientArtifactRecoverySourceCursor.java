package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.TargetWorkerSourceStartup;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
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
 * offset. Target startup additionally captures one actual guarded Fetch native
 * bound under its original isolation mode and replays only that fixed prefix;
 * a timeout or empty poll alone is never
 * its completion proof. The Route activation barrier remains an identity
 * fence, not a substitute for local Store recovery metadata.</p>
 */
public final class KafkaClientArtifactRecoverySourceCursor implements Iterator<SourceReplayEntry>,
        com.nereusstream.delay.ownership.TargetWorkerRecoverySource {
    private static final int MAX_EMPTY_POLLS_BEFORE_ACTIVATION_BARRIER = 120;
    private final GuardedConsumer<byte[], byte[]> consumer;
    private final KafkaActivationBarrier activationBarrier;
    private final SourceAssignment assignment;
    private com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt verifiedCredential;
    private com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client allocation;
    private com.nereusstream.delay.scheduler.WorkClassExecutionRegistry allocationClasses;
    private com.nereusstream.delay.store.SharedRocksDbResources allocationResources;
    private final String authenticatedClusterId;
    private final java.util.UUID nativeTopicUuid;
    private final ShardId shard;
    private final TopicPartition topicPartition;
    private final ConsumerResourceGuard expectedGuard;
    private final Duration pollTimeout;
    private final long startOffsetInclusive;
    private final boolean target;
    private final int maximumFrameBytes;
    private final Runnable protection;
    private final ArrayDeque<BufferedRecord> buffered = new ArrayDeque<>();
    private SourceReplayEntry current;
    private int emptyPollsBeforeActivationBarrier;
    private boolean activationBarrierReached;
    private long replayTailExclusive = -1;
    private long readThrough;
    private String isolationLevel;
    private KafkaSourcePosition positionedAnchor;
    private KafkaSourcePosition lastDeliveredPosition;
    private SourceReplayEntry lastDeliveredEntry;
    private TargetWorkerSourceStartup.FrameDecodeLoan frameDecode;
    private SourcePosition frameDecodePosition;
    private long frameDecodeBytes;
    private SourceReplayEntry decodedFrameEntry;
    private com.nereusstream.delay.ownership.TargetOwnedBackendGroup migrationBackend;
    private volatile com.nereusstream.delay.ownership.TargetMigrationSourceOpening migrationOpening;
    private com.nereusstream.delay.ownership.TargetWorkerSourceStartup sourceStartup;
    private volatile com.nereusstream.delay.ownership.TargetWorkerSourceStartup.RecoveryReaderLocalProtection localProtection;
    private final Object localStateGate = new Object();
    private volatile boolean closed;
    private volatile boolean failed;

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
    public static KafkaClientArtifactRecoverySourceCursor openForStartup(
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup, java.util.Map<String,Object> operational,
            byte[] sdkJar, com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt credential,
            Duration pollTimeout, int maximumFrameBytes,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time, Runnable originalProtection,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget, String allocationId,
            int maximumConnections, com.nereusstream.delay.protocol.CapacityVector cost) {
        try (var original = startup.beginRecoveryReaderConstruction()) {
            original.requireCurrent();
            final var graph = startup.graph();
            if (maximumFrameBytes > graph.config().maximumSourceFrameBytes()) {
                throw new IllegalArgumentException("recovery reader exceeds the original Source frame envelope");
            }
            final var last = startup.recoveryReaderCut();
            if (!(last instanceof KafkaSourcePosition position) || position.offset() < 0) {
                throw new IllegalArgumentException("actual recovery Store lacks its K1 Source cut");
            }
            final var cursor = openVerified(operational, sdkJar, credential, startup.acceptedAssignment(),
                    Math.addExact(position.offset(), 1L), pollTimeout, maximumFrameBytes, time,
                    () -> { startup.requireRecoveryReaderCurrent(); originalProtection.run(); }, budget, allocationId,
                    maximumConnections, cost, graph.classes(), graph.resources());
            cursor.positionedAnchor = position;
            cursor.sourceStartup = startup;
            startup.retainRecoverySource(cursor); original.requireCurrent(); cursor.requireCurrent();
            cursor.localProtection = startup.recoveryReaderLocalProtection(cursor);
            return cursor;
        } catch (com.nereusstream.delay.ownership.TargetSdkCleanupFailure partial) {
            startup.retainRecoveryOpeningCleanup(partial); startup.retain(partial); throw partial;
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    public static KafkaClientArtifactRecoverySourceCursor openVerified(java.util.Map<String,Object> operational,
            byte[] sdkJar, com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt credential,
            SourceAssignment assignment, long startOffsetInclusive, Duration pollTimeout, int maximumFrameBytes,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time, Runnable originalProtection,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget, String allocationId,
            int maximumConnections, com.nereusstream.delay.protocol.CapacityVector cost,
            com.nereusstream.delay.scheduler.WorkClassExecutionRegistry classes,
            com.nereusstream.delay.store.SharedRocksDbResources resources) {
        budget.requireWorker(classes, resources);
        if (maximumConnections < 1 || maximumConnections == Integer.MAX_VALUE || maximumFrameBytes < 1
                || maximumFrameBytes == Integer.MAX_VALUE
                || cost.amount(com.nereusstream.delay.protocol.CapacityDimension.OTHER_NATIVE_BYTES) < 1) {
            throw new IllegalArgumentException("recovery SDK allocation requires explicit finite resources");
        }
        final long[] minimum = new long[com.nereusstream.delay.protocol.CapacityDimension.COUNT];
        minimum[com.nereusstream.delay.protocol.CapacityDimension.ADAPTER_CONNECTIONS.wireValue() - 1] = maximumConnections;
        minimum[com.nereusstream.delay.protocol.CapacityDimension.OPEN_FILES.wireValue() - 1] = maximumConnections;
        minimum[com.nereusstream.delay.protocol.CapacityDimension.ADAPTER_THREADS.wireValue() - 1] = 1;
        minimum[com.nereusstream.delay.protocol.CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = 1;
        minimum[com.nereusstream.delay.protocol.CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = maximumFrameBytes;
        if (!cost.covers(new com.nereusstream.delay.protocol.CapacityVector(minimum))) {
            throw new IllegalArgumentException("recovery SDK charge cannot cover its declared connections/Fetch envelope");
        }
        final var reserved = budget.reserve(allocationId, cost);
        try {
            reserved.beginConstruction();
            final var cursor = openVerified(operational, sdkJar, credential, assignment, startOffsetInclusive,
                    pollTimeout, maximumFrameBytes, time, originalProtection);
            cursor.allocation = reserved; cursor.allocationClasses = classes; cursor.allocationResources = resources;
            return cursor;
        } catch (com.nereusstream.delay.ownership.TargetSdkCleanupFailure partial) {
            throw new KafkaClientArtifactTargetCleanupFailure(partial, () -> {
                partial.retryClose();
                if (!partial.cleanupComplete()) throw new IllegalStateException("original recovery SDK cleanup remains live");
                reserved.closed();
            });
        } catch (RuntimeException | Error failure) {
            // This pinned factory retains unfinished construction/bind/position cleanup as TargetSdkCleanupFailure.
            reserved.closed(); throw failure;
        }
    }

    /** The original migration owner has already registered the SDK construction before this synchronous open. */
    static KafkaClientArtifactRecoverySourceCursor openForMigration(
            com.nereusstream.delay.ownership.TargetMigrationSourceOpening opening,
            com.nereusstream.delay.ownership.TargetOwnedBackendGroup backend, java.util.Map<String,Object> operational,
            byte[] sdkJar, com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt credential,
            KafkaSourcePosition actualCut, Duration pollTimeout, int maximumFrameBytes,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            String allocationId, int maximumConnections, com.nereusstream.delay.protocol.CapacityVector cost) {
        opening.requireCurrent();
        final var graph = opening.graph();
        backend.requireWorkerGraph(graph.classes(), graph.resources());
        opening.assignment().activationBarrier().validatePosition(actualCut);
        if (actualCut.offset() < 0 || actualCut.offset() == Long.MAX_VALUE
                || maximumFrameBytes > graph.config().maximumSourceFrameBytes()) {
            throw new IllegalArgumentException("migration reader requires the complete original finite K1 baseline");
        }
        final var cursor = openVerified(operational, sdkJar, credential, opening.assignment(),
                Math.addExact(actualCut.offset(), 1L), pollTimeout, maximumFrameBytes, time, opening::requireLocalCurrent,
                opening.sdkBudget(), allocationId, maximumConnections, cost, graph.classes(), graph.resources());
        cursor.positionedAnchor = actualCut; cursor.migrationBackend = backend; cursor.migrationOpening = opening;
        return cursor;
    }

    /** Component entry without a Worker charge; the process startup requires the budgeted overload above. */
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
        final String isolation = new org.apache.kafka.clients.consumer.ConsumerConfig(config)
                .getString(org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG);
        if (!"read_uncommitted".equals(isolation) && !"read_committed".equals(isolation)) {
            throw new IllegalArgumentException("Source recovery requires its original Kafka isolation mode");
        }
        protection.run();
        final var actual = KafkaClientArtifactSourceConsumerFactory.create(config, resource.authenticatedClusterId(), topic,
                resource.nativeTopicUuid(), assignment.shardId().partition(), pollTimeout);
        try {
            protection.run();
            final var cursor = forTarget(actual, assignment, topic, startOffsetInclusive, pollTimeout, maximumFrameBytes, protection);
            cursor.verifiedCredential = credential;
            cursor.isolationLevel = isolation;
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
        this.startOffsetInclusive = startOffsetInclusive;
        this.readThrough = startOffsetInclusive;
        if (pollTimeout.isNegative() || pollTimeout.isZero()) {
            throw new IllegalArgumentException("pollTimeout must be positive");
        }
        if (!expectedGuard.equals(consumer.resourceGuard())) {
            throw new IllegalArgumentException("Kafka guarded recovery consumer has a different resource guard");
        }
        activationBarrierReached = Long.compareUnsigned(startOffsetInclusive, barrier.exclusiveOffset()) >= 0;
        requireFullProtection();
        consumer.assign(java.util.List.of(topicPartition));
        consumer.seek(topicPartition, startOffsetInclusive);
        requireFullProtection();
    }

    /** Freezes one actual-mode Fetch bound; it is neither old-writer settlement nor K2 coverage. */
    @Override public synchronized void captureReplayTail() {
        requireCurrent();
        if (!target || verifiedCredential == null || allocation == null || allocation.released()
                || positionedAnchor == null) {
            throw new IllegalStateException("tail capture requires the original budgeted Target recovery reader");
        }
        if (replayTailExclusive >= 0) { return; }
        if (current != null || !buffered.isEmpty()
                || !activationBarrier.reachedBy(lastDeliveredPosition == null ? positionedAnchor : lastDeliveredPosition)) {
            throw new IllegalStateException("Source tail capture requires the fully delivered original activation barrier");
        }
        try {
            final var records = consumer.pollGuarded(pollTimeout);
            requireCurrent();
            final var evidence = KafkaClientArtifactFetchEvidence.requireResponse(records, expectedGuard);
            if (evidence == null) {
                throw new IllegalStateException("Source tail capture has no actual guarded Fetch response");
            }
            final long nativeBound = nativeResponseBound(evidence);
            if (nativeBound < activationBarrier.exclusiveOffset()) {
                throw new IllegalStateException("Source tail is behind its accepted activation barrier");
            }
            acceptReplayResponse(records, evidence);
            replayTailExclusive = nativeBound;
            requireCurrent();
        } catch (RuntimeException | Error failure) {
            revokeLocalProtection();
            throw failure;
        }
    }

    /** Actual decoded delivery through the captured native prefix; this is never a Store apply receipt. */
    @Override public synchronized java.util.Optional<SourcePosition> capturedTailReadThrough() {
        requireCurrent();
        if (!target || verifiedCredential == null || allocation == null || allocation.released()
                || positionedAnchor == null || replayTailExclusive < 0) {
            throw new IllegalStateException("Source delivery cut requires its original captured budgeted reader");
        }
        if (readThrough < replayTailExclusive
                || current != null && ((KafkaSourcePosition) current.position()).offset() < replayTailExclusive
                || !buffered.isEmpty() && buffered.peekFirst().record().offset() < replayTailExclusive) {
            return java.util.Optional.empty();
        }
        // Native offsets can include processed control/aborted batches. Return only an actual decoded
        // Source position, or the complete original positioned cut; never manufacture nativeTail - 1.
        final SourcePosition observed = lastDeliveredPosition == null ? positionedAnchor : lastDeliveredPosition;
        activationBarrier.validatePosition(observed); requireCurrent();
        return java.util.Optional.of(observed);
    }

    /** Local qualification of this reader's actually processed prefix and already delivered Source frames. */
    @Override public synchronized boolean replayTailReached(SourcePosition actualApplied) {
        requireCurrent();
        if (replayTailExclusive < 0) {
            throw new IllegalStateException("Source replay tail was not captured by its original reader");
        }
        if (actualApplied == null) { return false; }
        activationBarrier.validatePosition(actualApplied);
        final var applied = (KafkaSourcePosition) actualApplied;
        if (applied.offset() < 0 || applied.offset() < startOffsetInclusive - 1
                || readThrough < replayTailExclusive
                || current != null && ((KafkaSourcePosition) current.position()).offset() < replayTailExclusive
                || !buffered.isEmpty() && buffered.peekFirst().record().offset() < replayTailExclusive) {
            return false;
        }
        if (lastDeliveredPosition == null) {
            return positionedAnchor != null && java.util.Arrays.equals(
                    applied.canonicalBytes(), positionedAnchor.canonicalBytes());
        }
        final int relative = applied.compareTo(lastDeliveredPosition);
        return relative > 0 || relative == 0 && java.util.Arrays.equals(
                applied.canonicalBytes(), lastDeliveredPosition.canonicalBytes());
    }

    @Override
    public synchronized boolean hasNext() {
        ensureOpen();
        try {
            return load();
        } catch (TargetWorkerSourceStartup.FrameCapacityUnavailable unavailable) {
            // The same raw Fetch record and its original loan remain retryable on this reader.
            throw unavailable;
        } catch (RuntimeException | Error failure) {
            if (target) { revokeLocalProtection(); }
            throw failure;
        }
    }

    private boolean load() {
        requireFullProtection();
        if (!expectedGuard.equals(consumer.resourceGuard())) {
            throw new IllegalStateException("Kafka recovery resource guard changed");
        }
        if (current != null) {
            if (frameDecode != null) frameDecode.requireCurrent();
            return true;
        }
        if (replayTailExclusive >= 0) {
            if (!loadNativePrefix(replayTailExclusive)) { return false; }
        } else if (target && allocation != null && verifiedCredential != null) {
            // The core pauses here after the original Source action has exited. Returning false
            // before that pause/capture would permanently cache the old EOF in SourceReplayCursor.
            if (activationBarrierReached || !loadNativePrefix(activationBarrier.exclusiveOffset())) {
                throw new IllegalStateException("Source tail capture is required before probing the original recovery EOF");
            }
        } else {
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
                        requireFullProtection();
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
            if (target) retainFrameDecode(position, value.length);
            current = target
                    ? KafkaClientArtifactSourceRecordDecoder.decodeTarget(value, shard, position, null, null)
                    : KafkaClientArtifactSourceRecordDecoder.decode(value, shard, position, null, null);
            if (target) {
                frameDecode.retainDecoded(current);
                decodedFrameEntry = current;
            }
            activationBarrierReached = activationBarrier.reachedBy(position);
            requireFullProtection();
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
        if (target) { lastDeliveredPosition = (KafkaSourcePosition) result.position(); lastDeliveredEntry = result; }
        current = null;
        return result;
    }

    private void retainFrameDecode(SourcePosition position, long bytes) {
        if (frameDecode == null) {
            frameDecode = sourceStartup != null
                    ? sourceStartup.acquireFrameDecode(position, bytes, null, null)
                    : migrationOpening != null
                            ? migrationOpening.acquireFrameDecode(position, bytes, null, null)
                            : missingFrameDecode();
            frameDecodePosition = position;
            frameDecodeBytes = bytes;
        } else if (frameDecodeBytes != bytes || !java.util.Arrays.equals(
                frameDecodePosition.canonicalBytes(), position.canonicalBytes())) {
            throw new IllegalStateException("Kafka recovery changed its retained raw Source frame");
        }
        frameDecode.requireCurrent();
    }

    private static TargetWorkerSourceStartup.FrameDecodeLoan missingFrameDecode() {
        throw new TargetWorkerSourceStartup.FrameCapacityUnavailable(
                "Kafka recovery lacks the original complete Source frame hold");
    }

    @Override public synchronized void releaseDecoded(SourceReplayEntry actual) {
        if (!target) return;
        if (actual != decodedFrameEntry || actual != lastDeliveredEntry || frameDecode == null) {
            throw new IllegalStateException("Kafka replay release changed its original decoded delivery");
        }
        releaseFrameDecode();
        lastDeliveredEntry = null;
    }

    private void releaseFrameDecode() {
        if (frameDecode == null) return;
        frameDecode.requestRelease();
        if (!frameDecode.cleanupComplete()) {
            throw new TargetWorkerSourceStartup.FrameCapacityUnavailable("Kafka frame decode cleanup remains pending");
        }
        frameDecode = null;
        frameDecodePosition = null;
        frameDecodeBytes = 0;
        decodedFrameEntry = null;
    }

    private boolean loadNativePrefix(long exclusiveBound) {
        final long started = System.nanoTime();
        while (buffered.isEmpty()) {
            if (readThrough >= exclusiveBound) { return false; }
            final long elapsed = System.nanoTime() - started;
            final long remaining = pollTimeout.toNanos() - elapsed;
            if (elapsed < 0 || remaining <= 0) {
                throw new IllegalStateException("Source replay did not reach its fixed tail before the poll deadline");
            }
            final var records = consumer.pollGuarded(Duration.ofNanos(remaining));
            requireCurrent();
            final var evidence = KafkaClientArtifactFetchEvidence.requireResponse(records, expectedGuard);
            if (evidence == null) {
                throw new IllegalStateException("Source replay has no actual guarded Fetch continuation");
            }
            final long before = readThrough;
            acceptReplayResponse(records, evidence);
            requireCurrent();
            if (buffered.isEmpty() && readThrough == before && readThrough < exclusiveBound) {
                throw new IllegalStateException("Source replay was unavailable before its captured tail");
            }
        }
        // A later response can include a newer Source frame beyond the fixed cut. Keep it owned,
        // but never expose it to the recovery coordinator or silently advance its Source cut.
        return buffered.peekFirst().record().offset() < exclusiveBound;
    }

    private void acceptReplayResponse(GuardedConsumerRecords<byte[], byte[]> records, GuardedFetchEvidence evidence) {
        final long nativeBound = nativeResponseBound(evidence);
        if (evidence.logStartOffset() < 0 || evidence.logStartOffset() > readThrough
                || nativeBound < readThrough
                || replayTailExclusive >= 0 && nativeBound < replayTailExclusive
                || records.count() > 1 || !buffered.isEmpty()) {
            throw new IllegalStateException("Source replay Fetch lost its retained range, native bound or single-record bound");
        }
        final var processed = records.nextOffsets().get(topicPartition);
        if (processed == null || evidence.fetchOffset() > readThrough || processed.offset() < readThrough
                || processed.offset() > nativeBound) {
            throw new IllegalStateException("Source replay Fetch lacks its paired actual processed continuation");
        }
        BufferedRecord next = null;
        for (var record : records.records(topicPartition)) {
            KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, expectedGuard);
            if (record.offset() < readThrough || record.offset() >= nativeBound
                    || record.offset() >= processed.offset() || record.timestamp() < 0) {
                throw new IllegalStateException("Source replay record is outside its processed native prefix");
            }
            next = new BufferedRecord(record, evidence);
        }
        if (next != null) { buffered.addLast(next); }
        // This same-response SDK continuation includes actually processed control/aborted batches.
        // Neither the native bound itself nor consumer.position() proves that processing occurred.
        readThrough = processed.offset();
    }

    private long nativeResponseBound(GuardedFetchEvidence evidence) {
        if ("read_uncommitted".equals(isolationLevel)) {
            if (evidence.highWatermark() < 0) {
                throw new IllegalStateException("Source replay Fetch lacks its actual high watermark");
            }
            return evidence.highWatermark();
        }
        if ("read_committed".equals(isolationLevel)) {
            if (evidence.lastStableOffset() < 0 || evidence.highWatermark() < evidence.lastStableOffset()) {
                throw new IllegalStateException("Source replay Fetch lacks its actual LSO/HW relation");
            }
            return evidence.lastStableOffset();
        }
        throw new IllegalStateException("Source replay has no original verified isolation mode");
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            revokeLocalProtection();
            if (target) {
                consumer.closeGuardedResources(pollTimeout);
                if (!consumer.guardedResourcesTerminated()) throw new IllegalStateException("Recovery Source native cleanup is incomplete");
                current = null;
                lastDeliveredEntry = null;
                buffered.clear();
                releaseFrameDecode();
            }
            else { consumer.close(); }
            closed = true;
        }
        if (allocation != null && !allocation.released()) {
            if (!consumer.guardedResourcesTerminated()) throw new IllegalStateException("original recovery SDK components remain live");
            allocation.closed();
        }
    }

    /** Keeps the native Source guard live across Target look-ahead and queued recovery writes. */
    public com.nereusstream.delay.ownership.SourceReplayCursor<SourceReplayEntry> replayCursor() {
        return com.nereusstream.delay.ownership.SourceReplayCursor.guarded(this, this::requireCurrent, this);
    }

    @Override public synchronized void requireStartup(SourceAssignment expected, long frameBytes,
            com.nereusstream.delay.scheduler.WorkClassExecutionRegistry classes,
            com.nereusstream.delay.store.SharedRocksDbResources resources) {
        if (!target || verifiedCredential == null || allocation == null || allocation.released()
                || allocationClasses != classes || allocationResources != resources || !assignment.sameIdentity(expected)
                || maximumFrameBytes > frameBytes || positionedAnchor == null
                || positionedAnchor.offset() < 0 || positionedAnchor.offset() == Long.MAX_VALUE
                || positionedAnchor.offset() + 1 != startOffsetInclusive) {
            throw new IllegalStateException("recovery cursor lacks its original verified Source startup");
        }
        activationBarrier.validatePosition(positionedAnchor);
        verifiedCredential.authorization().requireAssignment(expected);
    }
    @Override public synchronized com.nereusstream.delay.ownership.SourceReplaySuccessor successorAfter(
            com.nereusstream.delay.protocol.SourcePosition resumeAfter) {
        requireCurrent(); activationBarrier.validatePosition(resumeAfter);
        if (positionedAnchor != null && !java.util.Arrays.equals(
                positionedAnchor.canonicalBytes(), resumeAfter.canonicalBytes())) {
            throw new IllegalStateException("recovery successor changed its original audited Source anchor");
        }
        return com.nereusstream.delay.ownership.SourceReplaySuccessor.strictKafka();
    }
    @Override public synchronized void requireReaderBinding(SourceAssignment expected, SourcePosition positionedCut) {
        if (migrationBackend == null && sourceStartup == null || verifiedCredential == null || allocation == null || allocation.released()
                || positionedAnchor == null || !assignment.sameIdentity(expected)
                || !java.util.Arrays.equals(positionedAnchor.canonicalBytes(), positionedCut.canonicalBytes())) {
            throw new IllegalStateException("migration K1 reader changed its original independent Source binding");
        }
        verifiedCredential.authorization().requireAssignment(expected); requireCurrent();
    }
    @Override public synchronized void requireReaderEntry(SourceReplayEntry entry, SourceAssignment expected, SourcePosition cut) {
        requireReaderBinding(expected, cut);
        if (entry != lastDeliveredEntry) throw new IllegalStateException("migration entry is not this K1 reader's original native delivery");
        activationBarrier.validatePosition(entry.position());
    }
    @Override public com.nereusstream.delay.ownership.TargetOwnedBackendGroup backend() { return migrationBackend; }
    @Override public synchronized boolean cleanupComplete() {
        return closed && target && consumer.guardedResourcesTerminated() && (allocation == null || allocation.released())
                && (frameDecode == null || frameDecode.cleanupComplete());
    }

    /** Rechecks this exact native Source identity and the caller-held protection without reading or committing. */
    public void requireCurrent() {
        ensureOpen();
        try {
            requireFullProtection();
            if (!expectedGuard.equals(consumer.resourceGuard())) {
                throw new IllegalStateException("Kafka recovery resource guard changed");
            }
            ensureOpen();
        } catch (RuntimeException | Error failure) {
            if (target) { revokeLocalProtection(); }
            throw failure;
        }
    }
    /** The local gate is separate from native poll/close; no full protection is called while consuming this capability. */
    @Override public void requireLocalCurrent() {
        final com.nereusstream.delay.ownership.TargetWorkerSourceStartup.RecoveryReaderLocalProtection local;
        final com.nereusstream.delay.ownership.TargetMigrationSourceOpening migration;
        synchronized (localStateGate) { ensureOpen(); local = localProtection; migration = migrationOpening; }
        try {
            if (local != null) local.requireCurrent();
            else if (migration != null) migration.requireLocalCurrent();
            else throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable(
                    "Kafka recovery reader has no actual retained local Source capability");
            if (!expectedGuard.equals(consumer.resourceGuard()))
                throw new IllegalStateException("Kafka recovery resource guard changed");
            synchronized (localStateGate) { ensureOpen(); }
        } catch (RuntimeException | Error failure) { if (target) revokeLocalProtection(); throw failure; }
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

    private void revokeLocalProtection() {
        failed = true;
        if (sourceStartup != null) sourceStartup.revokeRecoveryReaderLocalProtection(this);
    }

    private void requireFullProtection() {
        try { protection.run(); }
        catch (RuntimeException | Error failure) { if (target) revokeLocalProtection(); throw failure; }
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
