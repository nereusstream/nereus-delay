package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import com.nereusstream.delay.ownership.InMemoryWorkerAssignmentAuthority;
import com.nereusstream.delay.ownership.OwnedDelayShard;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OwnerRecoveryCoordinator;
import com.nereusstream.delay.ownership.OwnerRecoveryTurn;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.ownership.OxiaSyncWorkerAssignmentBackend;
import com.nereusstream.delay.ownership.ReplayTurnBudget;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.ownership.ShardLogMutationAppender;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceReplayCursor;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.WorkerAssignment;
import com.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import com.nereusstream.delay.ownership.WorkerAssignmentCoordinator;
import com.nereusstream.delay.ownership.WorkerShardRuntime;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CompatibleControlSnapshot;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.store.CheckpointFileInventory;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerLoadVector;
import com.nereusstream.delay.store.WorkerPlacementPolicy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Real Kafka vertical smoke for mixed System Mutation recovery and active
 * Worker apply.
 *
 * <p>The first signed mutation is consumed only by the strict recovery
 * coordinator. The second mutation starts at the accepted Kafka activation
 * barrier and is applied by the active Worker source loop before its native
 * {@code commitSync} ACK. This keeps the recovery and active paths on one
 * ordered Shard Log while proving the mutation branch reaches the Store.</p>
 */
public final class KafkaClientArtifactMutationWorkerSmoke {
    private static final long LEASE_DURATION_MS = 60_000;

    private KafkaClientArtifactMutationWorkerSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <mutation-worker-topic>");
        }
        final String bootstrap = arguments[0];
        final String topic = arguments[1] + "-" + UUID.randomUUID();
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                10_000);

        try (Admin admin = Admin.create(adminConfiguration)) {
            ensureTopic(admin, topic);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final UUID nativeTopicId = toUuid(topicId);
            final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
            final KeyPair verificationKey =
                    KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final MutationFixture recoveryMutation = timeFence(shard, "recovery", verificationKey);
            final MutationFixture activeMutation = timeFence(shard, "active", verificationKey);
            final KafkaSourcePositionPair positions = appendMutations(
                    bootstrap,
                    clusterId,
                    topic,
                    nativeTopicId,
                    shard,
                    recoveryMutation.mutation(),
                    activeMutation.mutation());
            if (positions.active().offset() != positions.recovery().offset() + 1) {
                throw new IllegalStateException("Kafka mutation Worker fixture is not physically contiguous");
            }

            final SourceAssignment sourceAssignment = new SourceAssignment(
                    shard,
                    Bytes.sha256(Bytes.utf8("kafka-mutation-worker-assignment")),
                    1,
                    new KafkaActivationBarrier(
                            shard, clusterId, nativeTopicId, positions.active().offset()));
            final WorkClassExecutionRegistry workClasses = workClasses();
            final OxiaSyncOwnerLeaseBackend.ClientHandle oxia = connectOxiaIfConfigured();
            try {
                final WorkerAssignmentAuthority assignmentAuthority = oxia == null
                        ? new InMemoryWorkerAssignmentAuthority()
                        : new OxiaSyncWorkerAssignmentBackend(
                                oxia, "nereus-delay/kafka-mutation-worker-placement/" + UUID.randomUUID());
                final WorkerAssignment accepted =
                        publishAssignment(assignmentAuthority, sourceAssignment, oxia != null);
                final SourceAssignment assignment = accepted.sourceAssignment();
                final OxiaOwnerLeaseStore authority = oxia == null
                        ? new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore())
                        : new OxiaOwnerLeaseStore(oxia.backend());
                final OwnerLease lease = authority
                        .acquire(
                                assignment,
                                "kafka-mutation-worker",
                                oxia == null
                                        ? Bytes.sha256(Bytes.utf8("kafka-mutation-worker-session"))
                                        : oxia.sessionIdentity(),
                                System.currentTimeMillis(),
                                LEASE_DURATION_MS)
                        .orElseThrow();
                final CompatibleControlSnapshot controlSnapshot = controlSnapshot(shard);
                final Path root = Files.createTempDirectory("nereus-delay-kafka-mutation-worker-");
                try {
                    final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
                    try (SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                            ShardStore store = ShardStore.open(storeConfig, shard, resources)) {
                        resources.bindWorkClassExecutionRegistry(workClasses);
                        store.recordControlSnapshot(controlSnapshot);
                        final DelayShard delayShard =
                                new DelayShard(store, DelayShardConfig.defaults(), null, null, scheduleResolver());
                        final OwnedDelayShard ownedShard = new OwnedDelayShard(
                                delayShard,
                                lease,
                                new OwnerIdentity(
                                        bytes(16, 70),
                                        bytes(16, 71),
                                        lease.ownerEpoch(),
                                        Bytes.sha256(Bytes.utf8("kafka-mutation-worker-fencing"))));

                        recoverFirstMutation(
                                bootstrap,
                                clusterId,
                                topic,
                                nativeTopicId,
                                shard,
                                assignment,
                                authority,
                                ownedShard,
                                verificationKey,
                                controlSnapshot,
                                workClasses,
                                recoveryMutation.mutation(),
                                positions.recovery());
                        requireSystemMutation(
                                delayShard, recoveryMutation.mutation(), positions.recovery(), "recovery");
                        if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                || ownedShard.lastCatchupPosition() == null
                                || !samePosition(ownedShard.lastCatchupPosition(), positions.recovery())) {
                            throw new IllegalStateException("Kafka mutation recovery did not activate at offset zero");
                        }

                        final String workerGroup = "nereus-delay-mutation-worker-" + UUID.randomUUID();
                        final GuardedConsumer<byte[], byte[]> workerConsumer =
                                workerConsumer(bootstrap, workerGroup, clusterId, topic, nativeTopicId, shard);
                        WorkerShardRuntime runtime = null;
                        boolean drained = false;
                        try {
                            runtime = KafkaClientArtifactWorkerSourceFactory.create(
                                    workerConsumer,
                                    topic,
                                    Duration.ofMillis(250),
                                    assignment,
                                    workClasses,
                                    ownedShard,
                                    store,
                                    resources,
                                    authority,
                                    verificationKey.getPublic());
                            final var result = runUntilApplied(runtime);
                            if (result.status()
                                    != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                            .APPLIED_AND_ACKED) {
                                throw new IllegalStateException(
                                        "Kafka mutation Worker source turn did not ACK: " + result.status(),
                                        result.failure());
                            }
                            if (!(result.entry() instanceof SourceReplayMutation replayed)
                                    || !activeMutation.mutation().equals(replayed.mutation())
                                    || !samePosition(replayed.position(), positions.active())) {
                                throw new IllegalStateException("Kafka active Worker exposed a different mutation");
                            }
                            requireSystemMutation(delayShard, activeMutation.mutation(), positions.active(), "active");
                            final var applied = store.appliedShardLogPosition();
                            if (!samePosition(applied, positions.active())) {
                                throw new IllegalStateException(
                                        "Kafka mutation Worker Store position is not offset one");
                            }
                            requireCommittedOffset(
                                    admin,
                                    workerGroup,
                                    topic,
                                    0,
                                    positions.active().offset() + 1);

                            final Path checkpointPath = root.resolve("mutation-worker-final-checkpoint");
                            final byte[] checkpointId = java.util.Arrays.copyOf(
                                    Bytes.sha256(Bytes.utf8("kafka-mutation-worker-final-checkpoint")), 16);
                            final var drain = runtime.drain(
                                    new com.nereusstream.delay.ownership.OwnerDrainCoordinator.DrainRequest(
                                            System.currentTimeMillis() + 30_000, 0, checkpointPath, checkpointId),
                                    System::currentTimeMillis,
                                    () -> {});
                            if (drain.pendingCheckpointTask() != null
                                    || drain.finalCheckpointPath() == null
                                    || !Files.isDirectory(checkpointPath)
                                    || CheckpointFileInventory.collect(checkpointPath)
                                            .isEmpty()
                                    || !authority.current(shard).isEmpty()) {
                                throw new IllegalStateException(
                                        "Kafka mutation Worker drain did not publish the final checkpoint "
                                                + "and release the lease");
                            }
                            runtime.close();
                            drained = true;
                            System.out.println("Kafka mutation Worker vertical smoke passed: recovery TIME_FENCE "
                                    + "offset=" + positions.recovery().offset() + ", active Store apply TIME_FENCE "
                                    + "offset=" + positions.active().offset() + ", guarded Fetch v13, RocksDB "
                                    + "WriteBatch, commitSync ACK, and final checkpoint");
                            if (oxia != null) {
                                System.out.println("Kafka mutation Worker authority smoke passed: real Oxia "
                                        + "session-bound lease");
                            }
                        } finally {
                            if (!drained) {
                                workerConsumer.close();
                            }
                        }
                    }
                } finally {
                    deleteTree(root);
                }
            } finally {
                if (oxia != null) {
                    oxia.close();
                }
            }
        }
    }

    private static KafkaSourcePositionPair appendMutations(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard,
            final SystemMutation recovery,
            final SystemMutation active) {
        try (KafkaProducer<byte[], byte[]> producer = producer(bootstrap);
                KafkaClientArtifactShardLogMutationAppender appender = new KafkaClientArtifactShardLogMutationAppender(
                        (GuardedProducer<byte[], byte[]>) producer,
                        shard,
                        clusterId,
                        topic,
                        topicId,
                        Duration.ofSeconds(15))) {
            final ShardLogMutationAppender.AppendOutcome first = appender.append(recovery);
            final ShardLogMutationAppender.AppendOutcome second = appender.append(active);
            return new KafkaSourcePositionPair(requirePersisted(first, "recovery"), requirePersisted(second, "active"));
        }
    }

    private static KafkaSourcePosition requirePersisted(
            final ShardLogMutationAppender.AppendOutcome outcome, final String phase) {
        if (outcome.disposition() != ShardLogMutationAppender.AppendDisposition.PERSISTED
                || !(outcome.sourcePosition() instanceof KafkaSourcePosition position)) {
            throw new IllegalStateException(
                    "Kafka " + phase + " mutation append was not persisted: " + outcome.disposition());
        }
        return position;
    }

    private static void recoverFirstMutation(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard,
            final SourceAssignment assignment,
            final OxiaOwnerLeaseStore authority,
            final OwnedDelayShard ownedShard,
            final KeyPair verificationKey,
            final CompatibleControlSnapshot controlSnapshot,
            final WorkClassExecutionRegistry workClasses,
            final SystemMutation expectedMutation,
            final KafkaSourcePosition expectedPosition) {
        final String groupId = "nereus-delay-mutation-worker-recovery-" + UUID.randomUUID();
        try (KafkaClientArtifactRecoverySourceCursor nativeCursor = new KafkaClientArtifactRecoverySourceCursor(
                recoveryConsumer(bootstrap, groupId, clusterId, topic, topicId, shard),
                assignment,
                topic,
                0,
                Duration.ofMillis(250))) {
            final SourceReplayCursor<SourceReplayEntry> cursor = SourceReplayCursor.of(firstOnly(nativeCursor));
            final OwnerRecoveryCoordinator recovery = new OwnerRecoveryCoordinator(
                    ownedShard,
                    authority,
                    assignment,
                    SourceReplaySuccessor.strictKafka(),
                    cursor,
                    verificationKey.getPublic(),
                    controlSnapshot,
                    System::currentTimeMillis,
                    new ReplayTurnBudget(1, 1_000_000, TimeUnit.SECONDS.toNanos(10)),
                    workClasses);
            OwnerRecoveryTurn turn;
            do {
                turn = recovery.runTurn();
            } while (!turn.complete());
            if (turn.outcomes().size() != 1 || !recovery.complete()) {
                throw new IllegalStateException("Kafka mutation recovery did not apply exactly the first mutation");
            }
            final com.nereusstream.delay.ownership.SourceReplayOutcome outcome =
                    turn.outcomes().get(0);
            final SystemMutationResult result = outcome.systemMutationResult();
            if (result == null
                    || !java.util.Arrays.equals(result.mutationId(), expectedMutation.systemMutationId())
                    || !java.util.Arrays.equals(result.mutationHash(), expectedMutation.mutationHash())
                    || result.mutationType() != expectedMutation.type()
                    || !samePosition(outcome.position(), expectedPosition)) {
                throw new IllegalStateException("Kafka recovery returned a different System Mutation");
            }
        }
    }

    private static Iterator<SourceReplayEntry> firstOnly(final KafkaClientArtifactRecoverySourceCursor cursor) {
        return new Iterator<>() {
            private boolean yielded;

            @Override
            public boolean hasNext() {
                return !yielded && cursor.hasNext();
            }

            @Override
            public SourceReplayEntry next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException("Kafka recovery first-entry view is exhausted");
                }
                yielded = true;
                return cursor.next();
            }
        };
    }

    private static com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult runUntilApplied(
            final WorkerShardRuntime runtime) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result;
        do {
            result = runtime.runSourceTurn(
                    new SchedulerBudget(1, 1_000_000, TimeUnit.SECONDS.toNanos(2)), System::currentTimeMillis);
            if (result.status()
                    == com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                return result;
            }
            if (result.status() != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                    .WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException(
                        "Kafka mutation Worker source turn failed: " + result.status(), result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Kafka active mutation did not become visible before deadline");
    }

    private static void requireSystemMutation(
            final DelayShard shard,
            final SystemMutation mutation,
            final KafkaSourcePosition expectedPosition,
            final String phase) {
        final SystemMutationResult result = shard.getSystemMutationResult(mutation.systemMutationId());
        if (result == null
                || !java.util.Arrays.equals(result.mutationHash(), mutation.mutationHash())
                || result.mutationType() != mutation.type()
                || result.applyStatus() != ApplyStatus.APPLIED
                || result.stableCode() != StableCode.OK
                || !samePosition(result.appliedSourcePosition(), expectedPosition)) {
            throw new IllegalStateException("Kafka " + phase + " mutation Store result was not APPLIED/OK: " + result);
        }
    }

    private static boolean samePosition(final byte[] encoded, final KafkaSourcePosition expected) {
        return samePosition(SourcePositionCodec.decode(encoded), expected);
    }

    private static boolean samePosition(final SourcePosition actual, final KafkaSourcePosition expected) {
        if (!(actual instanceof KafkaSourcePosition observed)
                || !observed.shardId().equals(expected.shardId())
                || !observed.sameSourceIdentity(expected)
                || observed.offset() != expected.offset()
                || observed.brokerLogAppendTimeEpochMs() != expected.brokerLogAppendTimeEpochMs()) {
            return false;
        }
        return expected.leaderEpoch() == null || expected.leaderEpoch().equals(observed.leaderEpoch());
    }

    private static MutationFixture timeFence(final ShardId shard, final String identity, final KeyPair keyPair) {
        final long now = System.currentTimeMillis();
        final long closeThrough = Math.max(0, now - 1_000);
        final long retryUntil = Math.addExact(now, 60_000);
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(
                now,
                now + 1_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("kafka-mutation-worker-clock-" + identity),
                1,
                2,
                3,
                Bytes.sha256(Bytes.utf8("kafka-mutation-worker-evidence-" + identity)),
                0,
                null);
        final int keyVersion = 1;
        final byte[] proofId = Bytes.sha256(
                Bytes.utf8("nereus-delay-time-fence-proof\0"),
                shard.routeIncarnation().bytes(),
                Bytes.u32beBits(shard.partition()),
                Bytes.i64be(closeThrough),
                Bytes.u32beBits(keyVersion),
                Bytes.lp32(evidence.canonicalBytes()));
        final byte[] body = com.nereusstream.delay.protocol.CanonicalProtobuf.message(output -> {
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(
                    output, 1, new ShardSubject(shard).canonicalBytes());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(
                    output, 2, SystemMutationType.TIME_FENCE.wireValue());
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(output, 3, retryUntil);
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(output, 10, closeThrough);
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32Bits(output, 11, keyVersion);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 12, proofId);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 13, evidence.canonicalBytes());
        });
        return new MutationFixture(SystemMutation.signed(
                shard,
                SystemMutationType.TIME_FENCE,
                retryUntil,
                proofId,
                body,
                com.nereusstream.delay.protocol.AuthorIdentity.fence(
                                Bytes.utf8("kafka-mutation-worker-fence"), keyVersion)
                        .canonicalBytes(),
                keyVersion,
                keyPair.getPrivate()));
    }

    private static OxiaSyncOwnerLeaseBackend.ClientHandle connectOxiaIfConfigured() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        if (endpoint == null || endpoint.isBlank()) {
            return null;
        }
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        return OxiaSyncOwnerLeaseBackend.connectUnchecked(
                endpoint,
                namespace,
                "nereus-delay-kafka-mutation-worker-" + UUID.randomUUID(),
                Duration.ofSeconds(15),
                "nereus-delay-kafka-mutation-worker/" + UUID.randomUUID());
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static WorkerAssignment publishAssignment(
            final WorkerAssignmentAuthority authority,
            final SourceAssignment sourceAssignment,
            final boolean realOxia) {
        final WorkerAssignmentCoordinator coordinator = new WorkerAssignmentCoordinator(
                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
        final long now = System.currentTimeMillis();
        final WorkerPlacementPolicy.WorkerCandidate candidate = new WorkerPlacementPolicy.WorkerCandidate(
                "kafka-mutation-worker",
                capacity(1),
                CapacityVector.empty(),
                0,
                16,
                0,
                16,
                WorkerLoadVector.empty(),
                WorkerLoadVector.empty(),
                now,
                true,
                0);
        final WorkerAssignmentCoordinator.PlacementResult result = coordinator.place(
                sourceAssignment,
                Bytes.sha256(Bytes.utf8("kafka-mutation-worker-capacity-envelope")),
                1,
                List.of(candidate),
                capacity(1),
                CapacityVector.empty(),
                CapacityVector.empty(),
                null,
                now,
                0,
                0);
        final WorkerAssignmentAuthority.Publication publication =
                result.publication().orElseThrow();
        final WorkerAssignment accepted = coordinator.requireAccepted(
                sourceAssignment.shardId(), publication.revision(), publication.assignment());
        System.out.println("Kafka mutation Worker assignment publication/acceptance passed: revision="
                + publication.revision() + ", worker=" + accepted.workerId() + ", authority="
                + (realOxia ? "real Oxia session-bound" : "in-memory"));
        return accepted;
    }

    private static CapacityVector capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[CapacityDimension.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVector(values);
    }

    private static GuardedConsumer<byte[], byte[]> workerConsumer(
            final String bootstrap,
            final String groupId,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard) {
        return KafkaClientArtifactSourceConsumerFactory.create(
                configuration(bootstrap, groupId), clusterId, topic, topicId, shard.partition());
    }

    private static GuardedConsumer<byte[], byte[]> recoveryConsumer(
            final String bootstrap,
            final String groupId,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard) {
        return KafkaClientArtifactSourceConsumerFactory.create(
                configuration(bootstrap, groupId), clusterId, topic, topicId, shard.partition());
    }

    private static Map<String, Object> configuration(final String bootstrap, final String groupId) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return configuration;
    }

    private static KafkaProducer<byte[], byte[]> producer(final String bootstrap) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, "nereus-delay-mutation-worker-smoke");
        return new KafkaProducer<>(configuration, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static com.nereusstream.delay.runtime.ScheduleResolver scheduleResolver() {
        final byte[] tuple = Bytes.utf8("kafka-mutation-worker-canonical-lane-tuple");
        final com.nereusstream.delay.protocol.DestinationLaneId lane =
                com.nereusstream.delay.protocol.DestinationLaneId.derive(tuple);
        return new com.nereusstream.delay.runtime.ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(
                    final ShardId shard,
                    final com.nereusstream.delay.protocol.DelayMessageId message,
                    final com.nereusstream.delay.protocol.CanonicalScheduleIntent intent,
                    final com.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedSchedule(lane, tuple, intent.inlinePayload(), null);
            }

            @Override
            public ResolvedPrepare resolvePrepare(
                    final ShardId shard,
                    final com.nereusstream.delay.protocol.DelayMessageId message,
                    final com.nereusstream.delay.protocol.PrepareLargeScheduleBody body,
                    final com.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedPrepare(lane, tuple);
            }
        };
    }

    private static CompatibleControlSnapshot controlSnapshot(final ShardId shard) {
        return new CompatibleControlSnapshot(
                new ShardSubject(shard),
                List.of(new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRef(bytes(32, 50), 1, bytes(32, 51), ProfileKind.DESTINATION)),
                new QuotaGrantRef(
                        bytes(32, 52),
                        1,
                        new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    private static WorkClassExecutionRegistry workClasses() {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass =
                    switch (workClass) {
                        case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                        case QUERY, CHECKPOINT -> false;
                    };
            policies.put(
                    workClass,
                    new WorkClassPolicy(
                            1,
                            8,
                            1_000_000,
                            1,
                            1_000_000,
                            1_000_000,
                            protectedClass ? 1 : 0,
                            protectedClass ? 1 : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(
                new WorkClassRuntimeConfig(
                        policies, TimeUnit.SECONDS.toNanos(5), TimeUnit.SECONDS.toNanos(30), 16, 8_000_000),
                System::nanoTime);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static void requireCommittedOffset(
            final Admin admin, final String groupId, final String topic, final int partition, final long expected)
            throws Exception {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final var offset = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS)
                .get(topicPartition);
        if (offset == null || offset.offset() != expected) {
            throw new IllegalStateException("Kafka mutation Worker group offset mismatch: expected=" + expected
                    + ", actual=" + (offset == null ? "missing" : offset.offset()));
        }
    }

    private static void ensureTopic(final Admin admin, final String topic) throws Exception {
        try {
            if (describe(admin, topic) != null) {
                return;
            }
        } catch (Exception missing) {
            // Create below.
        }
        final NewTopic newTopic = new NewTopic(topic, 1, (short) 3);
        newTopic.configs(Map.of("message.timestamp.type", "LogAppendTime"));
        admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                if (describe(admin, topic) != null) {
                    return;
                }
            } catch (Exception ignored) {
                // Retry while metadata converges.
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("mutation Worker topic metadata did not converge");
    }

    private static TopicDescription describe(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(List.of(topic))
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .get(topic);
    }

    private static UUID toUuid(final Uuid value) {
        return new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private static void deleteTree(final Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        }
    }

    private record MutationFixture(SystemMutation mutation) {
        private MutationFixture {
            Objects.requireNonNull(mutation, "mutation");
        }
    }

    private record KafkaSourcePositionPair(KafkaSourcePosition recovery, KafkaSourcePosition active) {
        private KafkaSourcePositionPair {
            Objects.requireNonNull(recovery, "recovery");
            Objects.requireNonNull(active, "active");
        }
    }
}
