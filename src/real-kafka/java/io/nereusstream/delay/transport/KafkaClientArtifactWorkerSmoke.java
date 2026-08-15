package io.nereusstream.delay.transport;

import io.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import io.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import io.nereusstream.delay.ownership.OwnerLease;
import io.nereusstream.delay.ownership.OwnerRecoveryCoordinator;
import io.nereusstream.delay.ownership.OwnerRecoveryTurn;
import io.nereusstream.delay.ownership.ReplayTurnBudget;
import io.nereusstream.delay.ownership.ShardLifecycleState;
import io.nereusstream.delay.ownership.SourceAssignment;
import io.nereusstream.delay.ownership.SourceReplayCursor;
import io.nereusstream.delay.ownership.SourceReplayEntry;
import io.nereusstream.delay.ownership.SourceReplaySuccessor;
import io.nereusstream.delay.ownership.WorkerShardRuntime;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.V1ScheduleResolver;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Real Kafka vertical smoke for assignment, recovery, active Worker apply and
 * synchronous source ACK.
 *
 * <p>The authority in this opt-in smoke is the deterministic in-memory
 * implementation. It proves the native Kafka plus RocksDB Worker vertical;
 * the separate Oxia Docker harness remains the authority/session evidence.</p>
 */
public final class KafkaClientArtifactWorkerSmoke {
    private static final long LEASE_DURATION_MS = 60_000;

    private KafkaClientArtifactWorkerSmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <worker-topic>");
        }
        final String bootstrap = arguments[0];
        final String topic = arguments[1] + "-" + UUID.randomUUID();
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        try (Admin admin = Admin.create(adminConfiguration)) {
            ensureTopic(admin, topic);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final UUID nativeTopicId = toUuid(topicId);
            final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
            final PreparedCommand recoveryCommand = command(shard, "worker-recovery");
            produce(bootstrap, clusterId, topic, topicId, recoveryCommand);

            final SourceAssignment assignment = new SourceAssignment(shard,
                    Bytes.sha256(Bytes.utf8("kafka-worker-assignment")), 1,
                    new KafkaActivationBarrier(shard, clusterId, nativeTopicId, 1));
            final WorkClassExecutionRegistry workClasses = workClasses();
            final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
            final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
            final long ownerNow = System.currentTimeMillis();
            final byte[] sessionIdentity = Bytes.sha256(Bytes.utf8("kafka-worker-session"));
            final OwnerLease lease = authority.acquire(assignment, "kafka-worker", sessionIdentity,
                    ownerNow, LEASE_DURATION_MS).orElseThrow();
            final KeyPair verificationKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshot(shard);
            final Path root = Files.createTempDirectory("nereus-delay-kafka-worker-");

            try {
                final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
                try (SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                     ShardStore store = ShardStore.open(storeConfig, shard, resources)) {
                    resources.bindWorkClassExecutionRegistry(workClasses);
                    store.recordControlSnapshot(controlSnapshot);
                    final DelayShard delayShard = new DelayShard(store, DelayShardConfig.defaults(), null, null,
                            scheduleResolver());
                    final io.nereusstream.delay.ownership.OwnedDelayShard ownedShard =
                            new io.nereusstream.delay.ownership.OwnedDelayShard(delayShard, lease,
                                    new OwnerIdentityV1(bytes(16, 70), bytes(16, 71), lease.ownerEpoch(),
                                            Bytes.sha256(Bytes.utf8("kafka-worker-fencing"))));

                    recoverFirstRecord(bootstrap, clusterId, topic, nativeTopicId, shard, assignment,
                            authority, ownedShard, verificationKey, controlSnapshot, workClasses);
                    if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                            || ownedShard.lastCatchupPosition() == null
                            || ownedShard.lastCatchupPosition().compareTo(
                            new io.nereusstream.delay.protocol.KafkaSourcePosition(shard, clusterId,
                                    nativeTopicId, 0, null, 0)) < 0) {
                        throw new IllegalStateException("Kafka Worker recovery did not activate at offset zero");
                    }

                    final PreparedCommand activeCommand = command(shard, "worker-active");
                    produce(bootstrap, clusterId, topic, topicId, activeCommand);
                    final String workerGroup = "nereus-delay-worker-e2e-" + UUID.randomUUID();
                    final GuardedConsumer<byte[], byte[]> workerConsumer = workerConsumer(
                            bootstrap, workerGroup, clusterId, topic, nativeTopicId, shard);
                    WorkerShardRuntime runtime = null;
                    boolean drained = false;
                    try {
                        runtime = KafkaClientArtifactWorkerSourceFactory.create(workerConsumer, topic,
                                Duration.ofMillis(250), assignment, workClasses, ownedShard, store, resources,
                                authority, verificationKey.getPublic());
                        final io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result =
                                runUntilApplied(runtime);
                        if (result.status()
                                != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                            throw new IllegalStateException("Kafka Worker source turn did not ACK: " + result.status(),
                                    result.failure());
                        }
                        final var applied = store.appliedShardLogPosition();
                        if (!(applied instanceof io.nereusstream.delay.protocol.KafkaSourcePosition position)
                                || position.offset() != 1 || !position.shardId().equals(shard)
                                || !position.authenticatedClusterId().equals(clusterId)
                                || !position.nativeTopicUuid().equals(nativeTopicId)) {
                            throw new IllegalStateException("Kafka Worker Store did not persist exact active position");
                        }
                        requireCommittedOffset(admin, workerGroup, topic, 0, 2);
                        final var drain = runtime.drain(
                                new io.nereusstream.delay.ownership.OwnerDrainCoordinator.DrainRequest(
                                        System.currentTimeMillis() + 5_000, 0, null),
                                System::currentTimeMillis, () -> { });
                        if (drain.pendingCheckpointTask() != null || !backend.current(shard).isEmpty()) {
                            throw new IllegalStateException("Kafka Worker drain did not release the exact owner lease");
                        }
                        runtime.close();
                        drained = true;
                        System.out.println("Kafka Worker vertical smoke passed: assignment recovery offset=0, "
                                + "active apply offset=1, guarded Fetch v13, RocksDB WriteBatch and commitSync ACK");
                    } finally {
                        if (!drained) {
                            workerConsumer.close();
                        }
                    }
                }
            } finally {
                deleteTree(root);
            }
        }
    }

    private static void recoverFirstRecord(final String bootstrap, final String clusterId, final String topic,
                                           final UUID topicId, final ShardId shard, final SourceAssignment assignment,
                                           final OxiaOwnerLeaseStore authority,
                                           final io.nereusstream.delay.ownership.OwnedDelayShard ownedShard,
                                           final KeyPair verificationKey,
                                           final CompatibleControlSnapshotV1 controlSnapshot,
                                           final WorkClassExecutionRegistry workClasses) {
        final String groupId = "nereus-delay-worker-recovery-" + UUID.randomUUID();
        try (KafkaClientArtifactRecoverySourceCursor nativeCursor =
                     new KafkaClientArtifactRecoverySourceCursor(
                             recoveryConsumer(bootstrap, groupId, clusterId, topic, topicId, shard), assignment, topic,
                             0, Duration.ofMillis(250))) {
            final SourceReplayCursor<SourceReplayEntry> cursor = SourceReplayCursor.of(nativeCursor);
            final OwnerRecoveryCoordinator recovery = new OwnerRecoveryCoordinator(ownedShard, authority, assignment,
                    SourceReplaySuccessor.strictKafka(), cursor, verificationKey.getPublic(), controlSnapshot,
                    System::currentTimeMillis,
                    new ReplayTurnBudget(1, 1_000_000, TimeUnit.SECONDS.toNanos(10)), workClasses);
            OwnerRecoveryTurn turn;
            do {
                turn = recovery.runTurn();
            } while (!turn.complete());
            if (turn.outcomes().size() != 1 || !recovery.complete()) {
                throw new IllegalStateException("Kafka Worker recovery did not apply exactly one source record");
            }
        }
    }

    private static io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult runUntilApplied(
            final WorkerShardRuntime runtime) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result;
        do {
            result = runtime.runSourceTurn(new SchedulerBudget(1, 1_000_000, TimeUnit.SECONDS.toNanos(2)),
                    System::currentTimeMillis);
            if (result.status()
                    == io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                return result;
            }
            if (result.status() != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status()
                    != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException("Kafka Worker source turn failed: " + result.status(),
                        result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Kafka Worker source record did not become visible before deadline");
    }

    private static GuardedConsumer<byte[], byte[]> workerConsumer(final String bootstrap, final String groupId,
                                                                    final String clusterId, final String topic,
                                                                    final UUID topicId, final ShardId shard) {
        return KafkaClientArtifactSourceConsumerFactory.create(configuration(bootstrap, groupId), clusterId, topic,
                topicId, shard.partition());
    }

    private static GuardedConsumer<byte[], byte[]> recoveryConsumer(final String bootstrap, final String groupId,
                                                                      final String clusterId, final String topic,
                                                                      final UUID topicId, final ShardId shard) {
        return KafkaClientArtifactSourceConsumerFactory.create(configuration(bootstrap, groupId), clusterId, topic,
                topicId, shard.partition());
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

    private static void produce(final String bootstrap, final String clusterId, final String topic,
                                final Uuid topicId, final PreparedCommand command) throws Exception {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, "nereus-delay-worker-smoke");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configuration,
                new ByteArraySerializer(), new ByteArraySerializer())) {
            final GuardedProducer<byte[], byte[]> guarded = (GuardedProducer<byte[], byte[]>) producer;
            final ProducerResourceGuard guard = new ProducerResourceGuard(clusterId, topic, topicId, 0);
            guarded.sendGuarded(new ProducerRecord<>(topic, 0, null,
                    io.nereusstream.delay.protocol.CommandCodec.encodeFrameV1(command)), guard)
                    .get(10, TimeUnit.SECONDS);
        }
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("destination-" + identity), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)), ProfileKindV1.DESTINATION);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(Bytes.utf8("retry-" + identity), 1,
                Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination, retryPolicy, deliverAt,
                deliverAt + 10_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0],
                Bytes.utf8("source-" + identity), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        return PreparedCommand.scheduleV1(shard, intent, deliverAt + 20_000);
    }

    private static V1ScheduleResolver scheduleResolver() {
        final byte[] tuple = Bytes.utf8("kafka-worker-canonical-lane-tuple-v1");
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        return new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(final ShardId shard, final io.nereusstream.delay.protocol.DelayMessageId message,
                                                     final ScheduleIntentV1 intent,
                                                     final io.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedSchedule(lane, tuple, intent.inlinePayload(), null);
            }

            @Override
            public ResolvedPrepare resolvePrepare(final ShardId shard,
                                                  final io.nereusstream.delay.protocol.DelayMessageId message,
                                                  final io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1 body,
                                                  final io.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedPrepare(lane, tuple);
            }
        };
    }

    private static CompatibleControlSnapshotV1 controlSnapshot(final ShardId shard) {
        return new CompatibleControlSnapshotV1(new ShardSubjectV1(shard),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRefV1(bytes(32, 50), 1, bytes(32, 51), ProfileKindV1.DESTINATION)),
                new QuotaGrantRefV1(bytes(32, 52), 1, new PublishAdmissionBody.ChargeVector(
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    private static WorkClassExecutionRegistry workClasses() {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, 8, 1_000_000,
                    1, 1_000_000, 1_000_000, protectedClass ? 1 : 0,
                    protectedClass ? 1 : 0, workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies,
                TimeUnit.SECONDS.toNanos(5), TimeUnit.SECONDS.toNanos(30),
                16, 8_000_000), System::nanoTime);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static void requireCommittedOffset(final Admin admin, final String groupId, final String topic,
                                               final int partition, final long expected) throws Exception {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final var offset = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS).get(topicPartition);
        if (offset == null || offset.offset() != expected) {
            throw new IllegalStateException("Kafka Worker group offset mismatch: expected=" + expected
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
        throw new IllegalStateException("worker topic metadata did not converge");
    }

    private static TopicDescription describe(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
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
}
