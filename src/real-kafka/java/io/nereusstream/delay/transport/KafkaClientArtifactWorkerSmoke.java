package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import io.nereusstream.delay.adapter.DestinationPublishResult;
import io.nereusstream.delay.adapter.KafkaReceiptJournal;
import io.nereusstream.delay.adapter.KafkaReceiptResource;
import io.nereusstream.delay.adapter.KafkaTargetResource;
import io.nereusstream.delay.adapter.KafkaTransactionalDestinationAdapter;
import io.nereusstream.delay.adapter.KafkaProduceRequest;
import io.nereusstream.delay.adapter.KafkaProduceResult;
import io.nereusstream.delay.ownership.ClaimHandoffWorkClassExecutor;
import io.nereusstream.delay.ownership.OutcomeWorkClassExecutor;
import io.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import io.nereusstream.delay.ownership.InMemoryWorkerAssignmentAuthority;
import io.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.nereusstream.delay.ownership.OxiaSyncWorkerAssignmentBackend;
import io.nereusstream.delay.ownership.OwnerLease;
import io.nereusstream.delay.ownership.OwnerRecoveryCoordinator;
import io.nereusstream.delay.ownership.OwnerRecoveryTurn;
import io.nereusstream.delay.ownership.OwnedDelayShard;
import io.nereusstream.delay.ownership.ReplayTurnBudget;
import io.nereusstream.delay.ownership.ShardLifecycleState;
import io.nereusstream.delay.ownership.SourceAssignment;
import io.nereusstream.delay.ownership.SourceReplayCursor;
import io.nereusstream.delay.ownership.SourceReplayEntry;
import io.nereusstream.delay.ownership.SourceReplayMutation;
import io.nereusstream.delay.ownership.SourceReplayRecord;
import io.nereusstream.delay.ownership.SourceReplaySuccessor;
import io.nereusstream.delay.ownership.WorkerAssignment;
import io.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import io.nereusstream.delay.ownership.WorkerAssignmentCoordinator;
import io.nereusstream.delay.ownership.WorkerCommandRuntime;
import io.nereusstream.delay.ownership.WorkerPhysicalPublishExecutor;
import io.nereusstream.delay.ownership.WorkerPublishOutcomeMutationFactory;
import io.nereusstream.delay.ownership.WorkerPublishPreparationCoordinator;
import io.nereusstream.delay.ownership.WorkerSchedulingRuntime;
import io.nereusstream.delay.ownership.WorkerShardRuntime;
import io.nereusstream.delay.ownership.PublishAdmissionWorkClassExecutor;
import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CapacityDimensionV1;
import io.nereusstream.delay.protocol.CapacityVectorV1;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ChannelKindV1;
import io.nereusstream.delay.protocol.ChannelResourceIdentityV1;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.CredentialUseKindV1;
import io.nereusstream.delay.protocol.CredentialUseLeaseV1;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PublishEvidenceKindV1;
import io.nereusstream.delay.protocol.PublishEvidenceV1;
import io.nereusstream.delay.protocol.PublishOutcomeBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.ReadyCertificateV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.V1ScheduleResolver;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import io.nereusstream.delay.store.CheckpointFileInventory;
import io.nereusstream.delay.store.WorkerLoadVector;
import io.nereusstream.delay.store.WorkerPlacementPolicy;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.GuardedConsumerRecords;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.GuardedTransactionalProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Real Kafka vertical smoke for assignment, recovery, active Worker apply and
 * synchronous source ACK.
 *
 * <p>By default this smoke uses a deterministic in-memory authority. When
 * {@code NEREUS_DELAY_OXIA_ENDPOINT} is configured, it instead connects to a
 * real Oxia service and acquires the assignment through an ephemeral,
 * session-bound lease. Neither mode claims production placement, Route
 * publication, due-time scheduling, or the full multi-shard Worker wiring.</p>
 */
public final class KafkaClientArtifactWorkerSmoke {
    private static final long LEASE_DURATION_MS = 60_000;
    private static final long DUE_DISCOVERY_MAX_BYTES = 900_000;

    private KafkaClientArtifactWorkerSmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2 && arguments.length != 3 && arguments.length != 4) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <worker-topic> "
                    + "[run|prepare|resume|crash-wait|ack-crash-wait] [destination-topic]");
        }
        final String bootstrap = arguments[0];
        final String mode = arguments.length >= 3 ? arguments[2] : "run";
        final String destinationPhysicalTopic = arguments.length >= 4 ? arguments[3] : null;
        if (!mode.equals("run") && !mode.equals("prepare") && !mode.equals("resume")
                && !mode.equals("crash-wait") && !mode.equals("ack-crash-wait")) {
            throw new IllegalArgumentException("unknown Worker smoke mode: " + mode);
        }
        final String topic = mode.equals("run") ? arguments[1] + "-" + UUID.randomUUID() : arguments[1];
        final String receiptPhysicalTopic = destinationPhysicalTopic == null
                ? null : destinationPhysicalTopic + "-receipt";
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        try (Admin admin = Admin.create(adminConfiguration)) {
            ensureTopic(admin, topic);
            if (destinationPhysicalTopic != null) {
                ensureTopic(admin, destinationPhysicalTopic);
                ensureTopic(admin, receiptPhysicalTopic);
            }
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final UUID nativeTopicId = toUuid(topicId);
            final UUID destinationTopicId = destinationPhysicalTopic == null
                    ? null : toUuid(describe(admin, destinationPhysicalTopic).topicId());
            final UUID receiptTopicId = receiptPhysicalTopic == null
                    ? null : toUuid(describe(admin, receiptPhysicalTopic).topicId());
            final ShardId shard = mode.equals("run")
                    ? new ShardId(RouteIncarnation.random(), 0) : restartShard(topic);
            if (!mode.equals("resume")) {
                final String commandIdentity = mode.equals("prepare")
                        ? "worker-restart-prepared" : "worker-recovery";
                produce(bootstrap, clusterId, topic, topicId, command(shard, commandIdentity));
            }
            if (mode.equals("prepare")) {
                System.out.println("Kafka Worker restart preparation passed: one guarded record persisted before broker failover");
                return;
            }

            final SourceAssignment sourceAssignment = new SourceAssignment(shard,
                    Bytes.sha256(Bytes.utf8("kafka-worker-assignment")), 1,
                    new KafkaActivationBarrier(shard, clusterId, nativeTopicId, 1));
            final WorkClassExecutionRegistry workClasses = workClasses();
            final OxiaSyncOwnerLeaseBackend.ClientHandle oxia = connectOxiaIfConfigured();
            try {
                final WorkerAssignmentAuthority assignmentAuthority = oxia == null
                        ? new InMemoryWorkerAssignmentAuthority()
                        : new OxiaSyncWorkerAssignmentBackend(oxia,
                                "nereus-delay/kafka-worker-placement/" + UUID.randomUUID());
                final WorkerAssignment assignmentProjection = publishAssignment(assignmentAuthority,
                        sourceAssignment, oxia != null);
                final SourceAssignment assignment = assignmentProjection.sourceAssignment();
                final OxiaOwnerLeaseStore authority;
                if (oxia == null) {
                    authority = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());
                } else {
                    authority = new OxiaOwnerLeaseStore(oxia.backend());
                }
                final long ownerNow = System.currentTimeMillis();
                final byte[] sessionIdentity = oxia == null
                        ? Bytes.sha256(Bytes.utf8("kafka-worker-session")) : oxia.sessionIdentity();
                final OwnerLease lease = authority.acquire(assignment, "kafka-worker", sessionIdentity,
                        ownerNow, LEASE_DURATION_MS).orElseThrow();
                final KeyPair verificationKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
                final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshot(shard);
                    final Path root = configuredWorkerRoot();
                try {
                    final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
                    try (SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                         ShardStore store = ShardStore.open(storeConfig, shard, resources)) {
                        resources.bindWorkClassExecutionRegistry(workClasses);
                        store.recordControlSnapshot(controlSnapshot);
                        final DelayShard delayShard = new DelayShard(store, DelayShardConfig.defaults(), null, null,
                                scheduleResolver(clusterId, destinationTopicId, destinationPhysicalTopic));
                        final OwnerIdentityV1 ownerIdentity = new OwnerIdentityV1(bytes(16, 70), bytes(16, 71),
                                lease.ownerEpoch(), Bytes.sha256(Bytes.utf8("kafka-worker-fencing")));
                        final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease, ownerIdentity);

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
                        final PreparedCommand physicalCommand = destinationPhysicalTopic == null ? null
                                : command(shard, "worker-physical-publish", Bytes.utf8(
                                "kafka-worker-source-applied-payload"), 2_000);
                        final KafkaSourcePosition physicalSchedulePosition = physicalCommand == null ? null
                                : produceAndPosition(bootstrap, clusterId, topic, topicId, physicalCommand);
                        final String workerGroup = "nereus-delay-worker-e2e-" + UUID.randomUUID();
                        final GuardedConsumer<byte[], byte[]> rawWorkerConsumer = workerConsumer(
                                bootstrap, workerGroup, clusterId, topic, nativeTopicId, shard);
                        final AtomicBoolean sourceAckResponseLossObserved = new AtomicBoolean();
                        final GuardedConsumer<byte[], byte[]> workerConsumer;
                        if (mode.equals("ack-crash-wait")) {
                            workerConsumer = workerAckProcessCrashConsumer(rawWorkerConsumer);
                        } else if (hasSourceAckResponseLoss()) {
                            workerConsumer = sourceAckResponseLossConsumer(rawWorkerConsumer,
                                    sourceAckResponseLossObserved);
                        } else {
                            workerConsumer = rawWorkerConsumer;
                        }
                        final PhysicalPublishBridge physicalBridge = physicalCommand == null ? null
                                : createPhysicalPublishBridge(bootstrap, clusterId, topic, nativeTopicId, shard,
                                physicalSchedulePosition, destinationPhysicalTopic, destinationTopicId,
                                receiptPhysicalTopic, receiptTopicId, store, ownedShard, ownerIdentity, authority,
                                workClasses, verificationKey);
                        WorkerShardRuntime runtime = null;
                        boolean drained = false;
                        try (physicalBridge) {
                            runtime = KafkaClientArtifactWorkerSourceFactory.create(workerConsumer, topic,
                                    Duration.ofMillis(250), assignment, workClasses, ownedShard, store, resources,
                                    authority, verificationKey.getPublic(), null, null, null, null,
                                    physicalBridge == null ? null : physicalBridge.executor());
                            awaitWorkerProcessCrashCutIfRequested(mode);
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
                            if (physicalBridge != null) {
                                final io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult physicalSchedule =
                                        runUntilApplied(runtime);
                                if (!(physicalSchedule.entry() instanceof SourceReplayRecord physicalRecord)
                                        || !physicalRecord.command().equals(physicalCommand)
                                        || !(physicalSchedule.entry().position() instanceof KafkaSourcePosition appliedPhysical)) {
                                    throw new IllegalStateException(
                                            "Kafka Worker physical Schedule was not source-applied");
                                }
                                if (!appliedPhysical.equals(physicalSchedulePosition)) {
                                    throw new IllegalStateException(
                                            "Kafka Worker physical Schedule Source Position changed across apply: "
                                                    + "produced=" + physicalSchedulePosition + ", applied="
                                                    + appliedPhysical);
                                }
                                runSourceAppliedPhysicalPublish(runtime, delayShard, ownedShard, ownerIdentity,
                                        authority, store, workClasses, verificationKey, physicalBridge, physicalCommand,
                                        physicalSchedulePosition, bootstrap, clusterId);
                            }
                            requireCommittedOffset(admin, workerGroup, topic, 0,
                                    physicalBridge == null ? 2 : 5);
                            final Path checkpointPath = root.resolve("worker-final-checkpoint");
                            final byte[] checkpointId = Arrays.copyOf(
                                    Bytes.sha256(Bytes.utf8("kafka-worker-final-checkpoint")), 16);
                            final var drain = runtime.drain(
                                    new io.nereusstream.delay.ownership.OwnerDrainCoordinator.DrainRequest(
                                            System.currentTimeMillis() + 30_000, 0, checkpointPath, checkpointId),
                                    System::currentTimeMillis, () -> { });
                            if (drain.pendingCheckpointTask() != null || drain.finalCheckpointPath() == null
                                    || !Files.isDirectory(checkpointPath)
                                    || CheckpointFileInventory.collect(checkpointPath).isEmpty()
                                    || !authority.current(shard).isEmpty()) {
                                throw new IllegalStateException(
                                        "Kafka Worker drain did not publish the final checkpoint and release the exact owner lease");
                            }
                            runtime.close();
                            drained = true;
                            System.out.println("Kafka Worker vertical smoke passed: assignment recovery offset=0, "
                                    + "active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, "
                                    + (physicalBridge == null ? "" : "source-applied physical publish with typed "
                                    + "KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, ")
                                    + "and final checkpoint");
                            if (oxia != null) {
                                System.out.println("Kafka Worker authority smoke passed: real Oxia session-bound lease");
                            }
                            if (hasSourceAckResponseLoss()) {
                                if (!sourceAckResponseLossObserved.get()) {
                                    throw new IllegalStateException(
                                            "Kafka Worker source ACK response-loss wrapper did not lose a response");
                                }
                                System.out.println("Kafka Worker source ACK response-loss smoke passed: real commitSync "
                                        + "ACK was accepted before the local response was discarded, and the same "
                                        + "source record was ACKed on the next bounded Worker turn");
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

    private static ShardId restartShard(final String topic) {
        return new ShardId(new RouteIncarnation(java.util.Arrays.copyOf(
                Bytes.sha256(Bytes.utf8("nereus-delay-kafka-worker-restart/" + topic)),
                RouteIncarnation.LENGTH)), 0);
    }

    private static Path configuredWorkerRoot() throws Exception {
        final String configured = System.getenv("NEREUS_DELAY_KAFKA_WORKER_ROOT");
        if (configured == null || configured.isBlank()) {
            return Files.createTempDirectory("nereus-delay-kafka-worker-");
        }
        return Files.createDirectories(Path.of(configured));
    }

    /**
     * Holds a real Worker JVM after it has opened the source and local Store but
     * before it can ACK the next source record.  The E2E harness kills the PID
     * written here, then starts a fresh JVM against the same exact root.
     */
    private static void awaitWorkerProcessCrashCutIfRequested(final String mode) throws Exception {
        if (!mode.equals("crash-wait")) {
            return;
        }
        final String gatePath = System.getenv("NEREUS_DELAY_KAFKA_WORKER_CRASH_GATE");
        final String pidPath = System.getenv("NEREUS_DELAY_KAFKA_WORKER_CRASH_PID_FILE");
        if (gatePath == null || gatePath.isBlank() || pidPath == null || pidPath.isBlank()) {
            throw new IllegalArgumentException(
                    "crash-wait requires NEREUS_DELAY_KAFKA_WORKER_CRASH_GATE and PID_FILE");
        }
        final Path gate = Path.of(gatePath);
        final Path pid = Path.of(pidPath);
        Files.createDirectories(gate.toAbsolutePath().getParent());
        Files.writeString(pid, Long.toString(ProcessHandle.current().pid()) + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.writeString(gate, "worker-source-runtime-ready\n",
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("Kafka Worker process-crash cut reached: pid=" + ProcessHandle.current().pid()
                + ", sourceRuntimeReady=true, nextSourceRecordUnacked=true");
        while (Files.exists(gate)) {
            Thread.sleep(100);
        }
    }

    /**
     * Places the OS-process cut after the local Worker WriteBatch has completed
     * and immediately before the guarded Kafka commitSync ACK.
     */
    @SuppressWarnings("unchecked")
    private static GuardedConsumer<byte[], byte[]> workerAckProcessCrashConsumer(
            final GuardedConsumer<byte[], byte[]> delegate) {
        return (GuardedConsumer<byte[], byte[]>) Proxy.newProxyInstance(
                KafkaClientArtifactWorkerSmoke.class.getClassLoader(), new Class<?>[]{GuardedConsumer.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("commitSync") && method.getParameterCount() == 1) {
                        awaitWorkerAckProcessCrashCut();
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static void awaitWorkerAckProcessCrashCut() throws Exception {
        final String gatePath = System.getenv("NEREUS_DELAY_KAFKA_WORKER_ACK_CRASH_GATE");
        final String pidPath = System.getenv("NEREUS_DELAY_KAFKA_WORKER_ACK_CRASH_PID_FILE");
        if (gatePath == null || gatePath.isBlank() || pidPath == null || pidPath.isBlank()) {
            throw new IllegalArgumentException(
                    "ack-crash-wait requires NEREUS_DELAY_KAFKA_WORKER_ACK_CRASH_GATE and PID_FILE");
        }
        final Path gate = Path.of(gatePath);
        final Path pid = Path.of(pidPath);
        Files.createDirectories(gate.toAbsolutePath().getParent());
        Files.writeString(pid, Long.toString(ProcessHandle.current().pid()) + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.writeString(gate, "worker-store-durable-before-kafka-ack\n",
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("Kafka Worker ACK process-crash cut reached: pid=" + ProcessHandle.current().pid()
                + ", storeWriteBatchDurable=true, kafkaCommitSyncStarted=false");
        while (Files.exists(gate)) {
            Thread.sleep(100);
        }
    }

    private static WorkerAssignment publishAssignment(final WorkerAssignmentAuthority authority,
                                                       final SourceAssignment sourceAssignment,
                                                       final boolean realOxia) {
        final WorkerAssignmentCoordinator coordinator = new WorkerAssignmentCoordinator(
                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
        final long now = System.currentTimeMillis();
        final WorkerPlacementPolicy.WorkerCandidate candidate = new WorkerPlacementPolicy.WorkerCandidate(
                "kafka-worker", capacity(1), CapacityVectorV1.empty(), 0, 16, 0, 16,
                WorkerLoadVector.empty(), WorkerLoadVector.empty(), now, true, 0);
        final WorkerAssignmentCoordinator.PlacementResult result = coordinator.place(sourceAssignment,
                Bytes.sha256(Bytes.utf8("kafka-worker-capacity-envelope")), 1, List.of(candidate),
                capacity(1), CapacityVectorV1.empty(), CapacityVectorV1.empty(), null, now, 0, 0);
        final WorkerAssignmentAuthority.Publication publication = result.publication().orElseThrow();
        final WorkerAssignment accepted = coordinator.requireAccepted(sourceAssignment.shardId(),
                publication.revision(), publication.assignment());
        System.out.println("Kafka Worker assignment publication/acceptance passed: revision="
                + publication.revision() + ", worker=" + accepted.workerId() + ", authority="
                + (realOxia ? "real Oxia session-bound" : "in-memory"));
        return accepted;
    }

    private static CapacityVectorV1 capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimensionV1.COUNT];
        values[CapacityDimensionV1.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVectorV1(values);
    }

    private static OxiaSyncOwnerLeaseBackend.ClientHandle connectOxiaIfConfigured() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        if (endpoint == null || endpoint.isBlank()) {
            return null;
        }
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        return OxiaSyncOwnerLeaseBackend.connectUnchecked(endpoint, namespace,
                "nereus-delay-kafka-worker-" + UUID.randomUUID(), Duration.ofSeconds(15),
                "nereus-delay-kafka-worker/" + UUID.randomUUID());
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
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
                    != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS
                    && result.status()
                    != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN
                    && result.status()
                    != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.ACK_DEFINITIVELY_NOT_ACKED) {
                throw new IllegalStateException("Kafka Worker source turn failed: " + result.status(),
                        result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Kafka Worker source record did not become visible before deadline");
    }

    private static boolean hasSourceAckResponseLoss() {
        return "1".equals(System.getenv("NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS"));
    }

    @SuppressWarnings("unchecked")
    private static GuardedConsumer<byte[], byte[]> sourceAckResponseLossConsumer(
            final GuardedConsumer<byte[], byte[]> delegate, final AtomicBoolean responseLossObserved) {
        return (GuardedConsumer<byte[], byte[]>) Proxy.newProxyInstance(
                KafkaClientArtifactWorkerSmoke.class.getClassLoader(), new Class<?>[]{GuardedConsumer.class},
                (proxy, method, arguments) -> {
                    final Object result;
                    try {
                        result = method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    if (method.getName().equals("commitSync")
                            && method.getParameterCount() == 1
                            && responseLossObserved.compareAndSet(false, true)) {
                        throw new IllegalStateException("simulated committed Kafka source ACK response loss");
                    }
                    return result;
                });
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
        configuration.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 2_000_000);
        configuration.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 4_000_000);
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return configuration;
    }

    private static void produce(final String bootstrap, final String clusterId, final String topic,
                                final Uuid topicId, final PreparedCommand command) throws Exception {
        produceAndPosition(bootstrap, clusterId, topic, topicId, command);
    }

    private static KafkaSourcePosition produceAndPosition(final String bootstrap, final String clusterId,
                                                          final String topic, final Uuid topicId,
                                                          final PreparedCommand command) throws Exception {
        final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerConfiguration(
                bootstrap, "nereus-delay-worker-smoke"), new ByteArraySerializer(), new ByteArraySerializer());
        final KafkaClientArtifactProduceTransport transport = new KafkaClientArtifactProduceTransport(
                (GuardedProducer<byte[], byte[]>) producer);
        try {
            final KafkaProduceResult result = transport.produce(new KafkaProduceRequest(clusterId, topic,
                    toUuid(topicId), 0, command.commandId(),
                    io.nereusstream.delay.protocol.CommandCodec.encodeFrameV1(command)))
                    .toCompletableFuture().get(15, TimeUnit.SECONDS);
            if (result.disposition() != KafkaProduceResult.Disposition.PERSISTED
                    || !clusterId.equals(result.authenticatedClusterId())
                    || !toUuid(topicId).equals(result.nativeTopicUuid())
                    || result.partition() != 0) {
                throw new IllegalStateException("guarded Kafka Worker producer did not persist: "
                        + result.disposition() + "/" + result.stableCode());
            }
            final KafkaSourcePosition producedPosition = new KafkaSourcePosition(command.shardId(), clusterId,
                    toUuid(topicId), result.offset(), result.leaderEpoch(), result.brokerLogAppendTimeEpochMs());
            return readBackSourcePosition(bootstrap, clusterId, topic, topicId, command, producedPosition);
        } finally {
            transport.close();
        }
    }

    /** Completes the optional Produce position with the exact guarded Fetch position. */
    private static KafkaSourcePosition readBackSourcePosition(final String bootstrap, final String clusterId,
                                                              final String topic, final Uuid topicId,
                                                              final PreparedCommand command,
                                                              final KafkaSourcePosition producedPosition) {
        final ConsumerResourceGuard guard = new ConsumerResourceGuard(clusterId, topic, topicId, 0);
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                configuration(bootstrap, "kafka-worker-position-readback-" + UUID.randomUUID()), clusterId,
                topic, toUuid(topicId), 0);
        final TopicPartition partition = new TopicPartition(topic, 0);
        final byte[] expectedValue = io.nereusstream.delay.protocol.CommandCodec.encodeFrameV1(command);
        try {
            consumer.assign(List.of(partition));
            consumer.seek(partition, producedPosition.offset());
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(Duration.ofMillis(250));
                final GuardedFetchEvidence evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, guard);
                if (evidence == null) {
                    continue;
                }
                for (ConsumerRecord<byte[], byte[]> record : records.records(partition)) {
                    KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, guard);
                    if (record.offset() != producedPosition.offset()) {
                        continue;
                    }
                    if (!Arrays.equals(expectedValue, record.value())) {
                        throw new IllegalStateException("Kafka Worker source readback value changed at the produced offset");
                    }
                    if (record.timestamp() != producedPosition.brokerLogAppendTimeEpochMs()) {
                        throw new IllegalStateException("Kafka Worker source readback broker append time changed at the produced offset: "
                                + "produced=" + producedPosition.brokerLogAppendTimeEpochMs() + ", fetched="
                                + record.timestamp());
                    }
                    final Integer fetchedLeaderEpoch = record.leaderEpoch().orElse(null);
                    if (producedPosition.leaderEpoch() != null
                            && !producedPosition.leaderEpoch().equals(fetchedLeaderEpoch)) {
                        throw new IllegalStateException("Kafka Worker source readback leader epoch changed at the produced offset: "
                                + "produced=" + producedPosition.leaderEpoch() + ", fetched=" + fetchedLeaderEpoch);
                    }
                    return new KafkaSourcePosition(command.shardId(), clusterId, toUuid(topicId), record.offset(),
                            fetchedLeaderEpoch, record.timestamp());
                }
            }
        } finally {
            consumer.close();
        }
        throw new IllegalStateException("Kafka Worker source readback did not find the exact produced record");
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        return command(shard, identity, new byte[0], 1_000);
    }

    private static PreparedCommand command(final ShardId shard, final String identity, final byte[] payload,
                                           final long delayMs) {
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("destination-" + identity), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)), ProfileKindV1.DESTINATION);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(Bytes.utf8("retry-" + identity), 1,
                Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + delayMs;
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination, retryPolicy, deliverAt,
                deliverAt + 10_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, payload,
                Bytes.utf8("source-" + identity), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        return PreparedCommand.scheduleV1(shard, intent, deliverAt + 20_000);
    }

    private static V1ScheduleResolver scheduleResolver(final String clusterId, final UUID destinationTopicId,
                                                        final String destinationPhysicalTopic) {
        if (destinationPhysicalTopic != null) {
            return new V1ScheduleResolver() {
                @Override
                public ResolvedSchedule resolveSchedule(final ShardId shard, final DelayMessageId message,
                                                         final ScheduleIntentV1 intent,
                                                         final io.nereusstream.delay.protocol.SourcePosition source) {
                    final byte[] tuple = canonicalLaneTuple(clusterId, destinationTopicId,
                            destinationPhysicalTopic, intent.profile(), capabilityProfile());
                    return new ResolvedSchedule(DestinationLaneId.derive(tuple), tuple, intent.inlinePayload(), null);
                }

                @Override
                public ResolvedPrepare resolvePrepare(final ShardId shard, final DelayMessageId message,
                                                      final io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1 body,
                                                      final io.nereusstream.delay.protocol.SourcePosition source) {
                    final byte[] tuple = canonicalLaneTuple(clusterId, destinationTopicId,
                            destinationPhysicalTopic, body.intentWithoutPayload().profile(), capabilityProfile());
                    return new ResolvedPrepare(DestinationLaneId.derive(tuple), tuple);
                }
            };
        }
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

    /** Runs the source-ordered Admission, physical K2 publish and Outcome path. */
    static void runSourceAppliedPhysicalPublish(
            final WorkerShardRuntime runtime,
            final DelayShard delayShard,
            final OwnedDelayShard ownedShard,
            final OwnerIdentityV1 ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final PreparedCommand physicalCommand,
            final KafkaSourcePosition physicalSchedulePosition,
            final String bootstrap,
            final String clusterId) throws Exception {
        final var message = delayShard.getMessage(physicalCommand.delayMessageId());
        if (message == null) {
            throw new IllegalStateException("source-applied physical Schedule message is missing");
        }
        runSourceAppliedPhysicalPublish(runtime, delayShard, ownedShard, ownerIdentity, authority, store,
                workClasses, verificationKey, bridge, physicalCommand.delayMessageId(), physicalSchedulePosition,
                message.payload(), bootstrap, clusterId);
    }

    static void runSourceAppliedPhysicalPublish(
            final WorkerShardRuntime runtime,
            final DelayShard delayShard,
            final OwnedDelayShard ownedShard,
            final OwnerIdentityV1 ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final io.nereusstream.delay.protocol.DelayMessageId physicalMessageId,
            final KafkaSourcePosition physicalSchedulePosition,
            final byte[] expectedPayload,
            final String bootstrap,
            final String clusterId) throws Exception {
        runSourceAppliedPhysicalPublish(runtime, delayShard, ownedShard, ownerIdentity, authority, store,
                workClasses, verificationKey, bridge, physicalMessageId, physicalSchedulePosition, expectedPayload,
                bootstrap, clusterId, 1_000_000);
    }

    static void runSourceAppliedPhysicalPublish(
            final WorkerShardRuntime runtime,
            final DelayShard delayShard,
            final OwnedDelayShard ownedShard,
            final OwnerIdentityV1 ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final io.nereusstream.delay.protocol.DelayMessageId physicalMessageId,
            final KafkaSourcePosition physicalSchedulePosition,
            final byte[] expectedPayload,
            final String bootstrap,
            final String clusterId,
            final long maxClaimBytes) throws Exception {
        final var message = delayShard.getMessage(physicalMessageId);
        if (message == null || message.status() != MessageStatus.SCHEDULED
                || !message.laneId().equals(bridge.laneId())) {
            throw new IllegalStateException(
                    "source-applied physical Schedule did not create the expected SCHEDULED message");
        }
        delayShard.activateLaneReadiness(bridge.laneId(), bridge.laneIncarnation(), bridge.channel(),
                bridge.readyCertificate(), bridge.evidenceCursors());
        final var lane = delayShard.getLane(bridge.laneId());
        if (lane == null || !lane.schedulable()) {
            throw new IllegalStateException("source-applied physical Lane did not become schedulable");
        }
        bindActiveOwnerPublishGraph(runtime, ownedShard, ownerIdentity, authority, store, workClasses,
                verificationKey, bridge, maxClaimBytes);
        waitUntil(message.deliverAtEpochMs());

        final byte[] payload = Bytes.copy(Objects.requireNonNull(expectedPayload, "expectedPayload"));
        WorkerShardRuntime.DueClaimPublishPhysicalTurn dueClaimPublish = null;
        // The default Lane DRR quantum is deliberately bounded below one
        // large payload. Spend a bounded number of normal scheduler turns to
        // accumulate the exact head credit; do not bypass the scheduler with
        // a larger one-off deficit or an unbounded loop.
        final long schedulerBudgetBytes = Math.max(DUE_DISCOVERY_MAX_BYTES, (long) payload.length);
        for (int schedulerTurn = 0; schedulerTurn < 32; schedulerTurn++) {
            final long dueEarliest = Math.max(System.currentTimeMillis(), message.deliverAtEpochMs());
            final TrustedUtcIntervalEvidence dueEvidence = evidence(dueEarliest, dueEarliest + 500,
                    "kafka-worker-due-clock");
            dueClaimPublish = runtime.runDueClaimPublishPhysicalTurn(dueEvidence,
                    new SchedulerBudget(1, schedulerBudgetBytes, TimeUnit.SECONDS.toNanos(2)),
                    message.expireAtEpochMs() - 1, claimCharge(payload.length), System::currentTimeMillis,
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)), 16,
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)), 16,
                    ignored -> Optional.of(payload));
            if (dueClaimPublish.dueClaimPublishTurn().claimResult().isPresent()) {
                break;
            }
        }
        final var dueClaim = dueClaimPublish.dueClaimPublishTurn();
        final var claimResult = dueClaim.claimResult().orElseThrow(
                () -> new IllegalStateException("provider-driven Worker turns did not return a Claim result"));
        if (claimResult.kind() != ClaimHandoffWorkClassExecutor.ResultKind.CLAIMED) {
            throw new IllegalStateException("provider-driven Worker Claim was not admitted: " + claimResult.kind()
                    + ", permitRejection=" + claimResult.permitRejection()
                    + ", prerequisiteRejection=" + claimResult.prerequisiteRejection());
        }
        final var admissionSubmission = dueClaim.publishSubmission().orElseThrow(
                () -> new IllegalStateException("provider-driven Worker turn did not queue Publish Admission"));
        final var admissionResult = admissionSubmission.result().orElseThrow(
                () -> new IllegalStateException("provider-driven Publish Admission has no result"));
        if (admissionResult.kind() != io.nereusstream.delay.ownership.PublishAdmissionWorkClassExecutor.ResultKind.ENQUEUED
                || !(admissionResult.sourcePosition() instanceof KafkaSourcePosition admissionPosition)
                || admissionPosition.compareTo(physicalSchedulePosition) <= 0) {
            throw new IllegalStateException("Kafka Worker provider-driven Publish Admission was not source-bound: "
                    + admissionResult.kind());
        }
        final PublishAdmissionBody admissionBody = PublishAdmissionBody.decode(
                admissionResult.mutation().canonicalBody());
        final byte[] publishAttemptId = admissionBody.publishAttemptId();
        final WorkerShardRuntime.SourceBoundPhysicalPublishTurn physicalTurn =
                dueClaimPublish.physicalTurn().orElseThrow(
                        () -> new IllegalStateException("provider-driven Worker turn did not start physical publish"));
        if (physicalTurn.status() != WorkerShardRuntime.SourceBoundPhysicalPublishStatus.PHYSICAL_SUBMITTED) {
            throw new IllegalStateException("source-applied PUBLISHING did not submit physical publish: "
                    + physicalTurn.status() + "/" + physicalTurn.failure());
        }
        final WorkerPhysicalPublishExecutor.Submission submission = physicalTurn.physicalSubmission().orElseThrow();
        waitForPhysicalCompletion(submission);
        final DestinationPublishResult physicalResult = submission.physicalResult().orElseThrow();
        if (physicalResult.disposition() != DestinationPublishResult.Disposition.PUBLISHED
                || physicalResult.evidence() == null) {
            throw new IllegalStateException("source-applied physical publish did not return typed PUBLISHED evidence: "
                    + physicalResult.disposition() + "/" + physicalResult.stableCode());
        }

        SourceReplayMutation outcomeRecord = null;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            workClasses.runTurn(new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)));
            final io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult turn = runtime.runSourceTurn(
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)), System::currentTimeMillis);
            if (turn.status()
                    == io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                if (turn.entry() instanceof SourceReplayMutation mutation
                        && mutation.mutation().type() == SystemMutationType.PUBLISH_OUTCOME) {
                    outcomeRecord = mutation;
                    break;
                }
                continue;
            }
            if (turn.status()
                    != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && turn.status()
                    != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException("Kafka Worker Publish Outcome source turn failed: "
                        + turn.status(), turn.failure());
            }
            TimeUnit.MILLISECONDS.sleep(25);
        }
        if (outcomeRecord == null) {
            throw new IllegalStateException("source-applied PUBLISH_OUTCOME did not become visible before deadline");
        }
        final PublishOutcomeBody outcome = PublishOutcomeBody.decode(outcomeRecord.mutation().canonicalBody());
        if (outcome.sideEffect() != 1 || outcome.stableCode() != StableCode.OK
                || !Arrays.equals(outcome.publishAttemptId(), publishAttemptId)) {
            throw new IllegalStateException("Kafka Worker Publish Outcome was not a definitive PUBLISHED result");
        }
        final PublishEvidenceV1 publishEvidence = PublishEvidenceV1.decode(outcome.evidence());
        if (publishEvidence.evidenceKind() != PublishEvidenceKindV1.KAFKA_TRANSACTIONAL_RECEIPT
                || publishEvidence.verificationStatus() != EvidenceVerificationStatusV1.VERIFIED_PUBLISHED) {
            throw new IllegalStateException("Kafka Worker Publish Outcome carried the wrong evidence branch");
        }
        publishEvidence.requireBusinessMutation(publishAttemptId, true);
        if (!(outcomeRecord.position() instanceof KafkaSourcePosition outcomePosition)) {
            throw new IllegalStateException("source-applied typed Publish Outcome has a non-Kafka source position");
        }
        final var finalMessage = delayShard.getMessage(physicalMessageId);
        final var openAttempt = delayShard.findOpenPublishAttempt(publishAttemptId);
        if (finalMessage == null || finalMessage.status() != MessageStatus.PUBLISHED || openAttempt != null) {
            final var appliedResult = delayShard.getSystemMutationResult(
                    outcomeRecord.mutation().systemMutationId());
            throw new IllegalStateException("source-applied typed Publish Outcome did not close the PUBLISHED attempt: "
                    + "messageStatus=" + (finalMessage == null ? "missing" : finalMessage.status())
                    + ", openAttemptState=" + (openAttempt == null ? "none" : openAttempt.state())
                    + ", applyStatus=" + (appliedResult == null ? "missing" : appliedResult.applyStatus())
                    + ", stableCode=" + (appliedResult == null ? "missing" : appliedResult.stableCode()));
        }
        requirePayload(bootstrap, clusterId, bridge.destinationPhysicalTopic(), bridge.destinationTopicId(), payload);
        bridge.requireDestinationResponseLossResolved(physicalResult);
        System.out.println("Kafka Worker source-applied physical publish passed: Admission source offset="
                + admissionPosition.offset() + ", typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset="
                + branchNumber(publishEvidence, 2) + ", Outcome source offset=" + outcomePosition.offset()
                + ", exact payload readback");
    }

    static void bindActiveOwnerPublishGraph(
            final WorkerShardRuntime runtime,
            final OwnedDelayShard ownedShard,
            final OwnerIdentityV1 ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge) {
        bindActiveOwnerPublishGraph(runtime, ownedShard, ownerIdentity, authority, store, workClasses,
                verificationKey, bridge, 1_000_000);
    }

    static void bindActiveOwnerPublishGraph(
            final WorkerShardRuntime runtime,
            final OwnedDelayShard ownedShard,
            final OwnerIdentityV1 ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final long maxClaimBytes) {
        if (maxClaimBytes <= 0) {
            throw new IllegalArgumentException("maxClaimBytes must be positive");
        }
        final WorkerSchedulingRuntime scheduling = WorkerSchedulingRuntime.openForActiveOwnerFromTypedLanes(
                workClasses, ownedShard, authority, store, ownerIdentity, List.of(bridge.laneId()), 8);
        final ClaimExecutionAdmission permits = new ClaimExecutionAdmission(1, maxClaimBytes);
        permits.registerShard(new ClaimExecutionAdmission.ShardSpec(runtime.shardId(), 1, maxClaimBytes));
        permits.registerLane(new ClaimExecutionAdmission.LaneSpec(runtime.shardId(), bridge.laneId(),
                bridge.laneIncarnation(), 0, 0, 1, maxClaimBytes));
        permits.openReady(runtime.shardId(), bridge.laneId(), bridge.laneIncarnation());
        final ClaimHandoffWorkClassExecutor claimExecutor = new ClaimHandoffWorkClassExecutor(
                workClasses, ownedShard, authority, scheduling.scheduler(), permits,
                ignored -> ClaimHandoffWorkClassExecutor.PrerequisiteDecision.available());
        final PublishAdmissionWorkClassExecutor publishExecutor = new PublishAdmissionWorkClassExecutor(
                workClasses, ownedShard, authority, permits, bridge.appender(),
                ignored -> PublishAdmissionWorkClassExecutor.PrerequisiteDecision.available());
        final WorkerCommandRuntime commandRuntime = new WorkerCommandRuntime(workClasses,
                store.sharedResources(), claimExecutor, publishExecutor);
        final WorkerPublishPreparationCoordinator preparation = new WorkerPublishPreparationCoordinator(
                ownedShard, authority, System::currentTimeMillis, request -> {
                    final long expiry = Math.min(request.claim().materialization().expireAtEpochMs(),
                            request.readyCertificate().validUntilEpochMs());
                    final long retryUntil = expiry - 1;
                    final long earliest = Math.max(Math.max(System.currentTimeMillis(),
                                    request.claim().materialization().actionAtEpochMs()),
                            request.readyCertificate().issuedAt().latestEpochMs());
                    if (retryUntil <= earliest) {
                        return Optional.empty();
                    }
                    final long latest = Math.min(retryUntil - 1, Math.addExact(earliest, 500));
                    if (latest < earliest) {
                        return Optional.empty();
                    }
                    return Optional.of(new WorkerCommandRuntime.PublishPreparation(
                            request.channel(), request.readyCertificate(),
                            evidence(earliest, latest, "kafka-worker-provider-preparation"), retryUntil, 1,
                            verificationKey.getPrivate(), System::currentTimeMillis));
                });
        runtime.bindActiveOwnerPublishGraph(scheduling, commandRuntime, preparation);
    }

    static void waitForPhysicalCompletion(final WorkerPhysicalPublishExecutor.Submission submission)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (submission.state() == WorkerPhysicalPublishExecutor.SubmissionState.PENDING
                && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(25);
        }
        if (submission.state() != WorkerPhysicalPublishExecutor.SubmissionState.OUTCOME_HANDOFF_QUEUED) {
            final Throwable failure = submission.failure().orElse(null);
            throw new IllegalStateException("Kafka Worker physical submission did not reach Outcome handoff: "
                    + submission.state() + "/" + failure, failure);
        }
    }

    static PhysicalPublishBridge createPhysicalPublishBridge(
            final String bootstrap,
            final String clusterId,
            final String sourcePhysicalTopic,
            final UUID sourceTopicId,
            final ShardId shard,
            final KafkaSourcePosition physicalSchedulePosition,
            final String destinationPhysicalTopic,
            final UUID destinationTopicId,
            final String receiptPhysicalTopic,
            final UUID receiptTopicId,
            final ShardStore store,
            final OwnedDelayShard ownedShard,
            final OwnerIdentityV1 ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey) throws Exception {
        return createPhysicalPublishBridge(bootstrap, clusterId, sourcePhysicalTopic, sourceTopicId, shard,
                physicalSchedulePosition, destinationPhysicalTopic, destinationTopicId, receiptPhysicalTopic,
                receiptTopicId, store, ownedShard, ownerIdentity, authority, workClasses, verificationKey,
                destinationProfile("worker-physical-publish"), capabilityProfile(), null, null, 1_000_000);
    }

    static PhysicalPublishBridge createPhysicalPublishBridge(
            final String bootstrap,
            final String clusterId,
            final String sourcePhysicalTopic,
            final UUID sourceTopicId,
            final ShardId shard,
            final KafkaSourcePosition physicalSchedulePosition,
            final String destinationPhysicalTopic,
            final UUID destinationTopicId,
            final String receiptPhysicalTopic,
            final UUID receiptTopicId,
            final ShardStore store,
            final OwnedDelayShard ownedShard,
            final OwnerIdentityV1 ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final ProfileRefV1 destinationProfile,
            final ProfileRefV1 capabilityProfile,
            final DestinationLaneId requestedLaneId,
            final byte[] requestedLaneIncarnation,
            final long maxPhysicalBytes) throws Exception {
        final ProfileRefV1 exactDestinationProfile = Objects.requireNonNull(destinationProfile,
                "destinationProfile");
        final ProfileRefV1 exactCapabilityProfile = Objects.requireNonNull(capabilityProfile,
                "capabilityProfile");
        if (maxPhysicalBytes <= 0) {
            throw new IllegalArgumentException("maxPhysicalBytes must be positive");
        }
        final DestinationLaneId laneId;
        if (requestedLaneId == null) {
            final byte[] laneTuple = canonicalLaneTuple(clusterId, destinationTopicId, destinationPhysicalTopic,
                    exactDestinationProfile, exactCapabilityProfile);
            laneId = DestinationLaneId.derive(laneTuple);
        } else {
            laneId = requestedLaneId;
        }
        final byte[] laneIncarnation = requestedLaneIncarnation == null
                ? LaneRecord.initial(laneId, physicalSchedulePosition).laneIncarnation()
                : Bytes.copy(requestedLaneIncarnation);
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        final BrokerResourceIdentityV1 target = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1(clusterId, destinationTopicId));
        final BrokerResourceIdentityV1 evidenceResource = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1(clusterId, receiptTopicId));
        final KafkaTargetResource targetResource = new KafkaTargetResource(clusterId, destinationTopicId, 0);
        final KafkaReceiptResource receiptResource = new KafkaReceiptResource(clusterId, receiptTopicId,
                shard.routeIncarnation(), shard.partition(), 0, 1, 1, 0);
        final String transactionalIdentity = "nereus-delay-kafka-worker-" + UUID.randomUUID();
        final byte[] transactionalIdentitySha256 = Bytes.sha256(Bytes.utf8(transactionalIdentity));
        final KafkaReceiptJournal journal = new KafkaReceiptJournal(shard, receiptResource);
        final boolean destinationResponseLossExpected = hasWorkerDestinationResponseLoss();
        final AtomicBoolean destinationResponseLossObserved = new AtomicBoolean();
        final KafkaProducer<byte[], byte[]> destinationProducer = new KafkaProducer<>(
                transactionalProducerConfiguration(bootstrap, transactionalIdentity,
                        Math.toIntExact(maxPhysicalBytes)),
                new ByteArraySerializer(), new ByteArraySerializer());
        destinationProducer.initTransactions();
        final GuardedTransactionalProducer<byte[], byte[]> guardedDestinationProducer =
                (GuardedTransactionalProducer<byte[], byte[]>) destinationProducer;
        final KafkaClientArtifactTransactionalDestinationTransport transport =
                new KafkaClientArtifactTransactionalDestinationTransport(
                        destinationResponseLossExpected
                                ? destinationResponseLossProducer(guardedDestinationProducer,
                                destinationResponseLossObserved) : guardedDestinationProducer,
                        new KafkaClientArtifactTransactionalReceiptEvidenceProvider(
                                configuration(bootstrap, "kafka-worker-k2-evidence"), 1,
                                Duration.ofMillis(250)));
        final KafkaTransactionalDestinationAdapter adapter = new KafkaTransactionalDestinationAdapter(
                targetResource, receiptResource, destinationPhysicalTopic, receiptPhysicalTopic, journal, laneId,
                laneIncarnation, transactionalIdentitySha256, transport);
        final DestinationPhysicalAdmission physicalAdmission = new DestinationPhysicalAdmission(1, maxPhysicalBytes);
        physicalAdmission.registerTargetCluster(clusterId, 1, maxPhysicalBytes);
        physicalAdmission.registerLane(new DestinationPhysicalAdmission.LaneSpec(laneId, laneIncarnation, clusterId,
                1, 1, 1, maxPhysicalBytes, 1, maxPhysicalBytes));
        physicalAdmission.openReady(laneId);
        final KafkaProducer<byte[], byte[]> mutationProducer = new KafkaProducer<>(
                producerConfiguration(bootstrap, "nereus-delay-kafka-worker-mutation"),
                new ByteArraySerializer(), new ByteArraySerializer());
        final KafkaClientArtifactShardLogMutationAppender appender =
                new KafkaClientArtifactShardLogMutationAppender((GuardedProducer<byte[], byte[]>) mutationProducer,
                        shard, clusterId, sourcePhysicalTopic, sourceTopicId, Duration.ofSeconds(20));
        final AuthorIdentity author = AuthorIdentity.owner(ownerIdentity.deploymentId(), ownerIdentity.workerRunId(),
                ownerIdentity.ownerEpoch(), ownerIdentity.leaseFencingDigest());
        final WorkerPublishOutcomeMutationFactory outcomeFactory = new WorkerPublishOutcomeMutationFactory(
                (attempt, request, result) -> {
                    final PublishAdmissionBody admissionBody = PublishAdmissionBody.decode(attempt.admissionBytes());
                    final long retryDeadline = attempt.hasRetryWindow() ? attempt.retryDeadlineEpochMs()
                            : request.deliverAtEpochMs();
                    return new WorkerPublishOutcomeMutationFactory.OutcomeContext(retryDeadline, 0,
                            admissionBody.chargeVector().canonicalBytes(),
                            evidence(result.brokerPersistenceTimeEpochMs(), result.brokerPersistenceTimeEpochMs(),
                                    "kafka-worker-publish-observed"),
                            retryDecision(admissionBody.decisionTime().latestEpochMs(), retryDeadline,
                                    attempt.attemptNo()));
                }, author.canonicalBytes(), 1, verificationKey.getPrivate());
        final OutcomeWorkClassExecutor outcomes = new OutcomeWorkClassExecutor(workClasses, ownedShard, authority,
                appender);
        final WorkerPhysicalPublishExecutor executor = new WorkerPhysicalPublishExecutor(adapter, physicalAdmission,
                workClasses, Runnable::run, outcomes,
                (attempt, request, ownerClock) -> WorkerPhysicalPublishExecutor.Decision.allowed(), outcomeFactory,
                ownedShard::fence);
        final byte[] attestationDigest = Bytes.sha256(Bytes.utf8("kafka-worker-channel-attestation"),
                target.canonicalBytes(), evidenceResource.canonicalBytes());
        final long now = Math.max(1, System.currentTimeMillis());
        final TrustedUtcIntervalEvidence issuedAt = evidence(Math.max(0, now - 1), now,
                "kafka-worker-channel-issued");
        final ChannelResourceIdentityV1 channel = channel(laneId, laneIncarnation, target, evidenceResource,
                0, transactionalIdentity, attestationDigest, issuedAt, exactDestinationProfile);
        final long validUntil = Math.addExact(now, 60_000);
        final EvidenceCursorV1 cursor = EvidenceCursorV1.kafka(laneId.bytes(), laneIncarnation,
                uuidBytes(receiptTopicId), 0, 1, 0, 1, 1);
        final ReadyCertificateV1 readyCertificate = readyCertificate(ownerIdentity,
                store.metadata().storeIncarnation(), laneId, laneIncarnation, channel, target, cursor,
                issuedAt, validUntil);
        return new PhysicalPublishBridge(executor, appender, laneId, laneIncarnation, exactDestinationProfile,
                exactCapabilityProfile, target, channel, readyCertificate, List.of(cursor), destinationPhysicalTopic,
                destinationTopicId, destinationResponseLossExpected, destinationResponseLossObserved);
    }

    private static boolean hasWorkerDestinationResponseLoss() {
        return "1".equals(System.getenv("NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS"));
    }

    @SuppressWarnings("unchecked")
    private static GuardedTransactionalProducer<byte[], byte[]> destinationResponseLossProducer(
            final GuardedTransactionalProducer<byte[], byte[]> delegate,
            final AtomicBoolean responseLossObserved) {
        return (GuardedTransactionalProducer<byte[], byte[]>) Proxy.newProxyInstance(
                GuardedTransactionalProducer.class.getClassLoader(),
                new Class<?>[]{GuardedTransactionalProducer.class},
                (proxy, method, arguments) -> {
                    final Object result;
                    try {
                        result = method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    if (method.getName().equals("commitTransaction")
                            && method.getParameterCount() == 0
                            && responseLossObserved.compareAndSet(false, true)) {
                        throw new IllegalStateException("simulated committed Kafka Worker EndTxn response loss");
                    }
                    return result;
                });
    }

    private static ChannelResourceIdentityV1 channel(final DestinationLaneId laneId, final byte[] laneIncarnation,
                                                      final BrokerResourceIdentityV1 target,
                                                      final BrokerResourceIdentityV1 evidenceResource,
                                                      final long physicalPartition,
                                                      final String transactionalIdentity,
                                                      final byte[] attestationDigest,
                                                      final TrustedUtcIntervalEvidence issuedAt,
                                                      final ProfileRefV1 destinationProfile) {
        final byte[] producer = Bytes.utf8(transactionalIdentity);
        final byte[] binding = Bytes.sha256(Bytes.utf8("kafka-worker-channel-binding"),
                target.canonicalBytes(), evidenceResource.canonicalBytes(), laneId.bytes(), laneIncarnation);
        final byte[] fingerprint = Bytes.sha256(Bytes.utf8("kafka-worker-channel-fingerprint"), producer,
                target.canonicalBytes(), evidenceResource.canonicalBytes());
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, AdapterKindV1.KAFKA.wireValue());
            CanonicalProtobuf.uint32(output, 2, ChannelKindV1.KAFKA_TRANSACTIONAL_RECEIPT.wireValue());
            CanonicalProtobuf.bytes(output, 3, laneId.bytes());
            CanonicalProtobuf.bytes(output, 4, laneIncarnation);
            CanonicalProtobuf.bytes(output, 5, target.canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, physicalPartition);
            CanonicalProtobuf.uint64(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, 0);
            CanonicalProtobuf.bytes(output, 9, producer);
            CanonicalProtobuf.bytes(output, 10, Bytes.sha256(producer));
            CanonicalProtobuf.bytes(output, 11, evidenceResource.canonicalBytes());
            CanonicalProtobuf.uint64(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, attestationDigest);
        });
        final CredentialUseLeaseV1 lease = new CredentialUseLeaseV1(Objects.requireNonNull(destinationProfile,
                "destinationProfile"),
                CredentialUseKindV1.DESTINATION_CHANNEL,
                CredentialUseLeaseV1.destinationChannelHolderScope(prefix), 1, binding, fingerprint, issuedAt,
                Math.addExact(issuedAt.latestEpochMs(), 60_000), 1);
        return new ChannelResourceIdentityV1(AdapterKindV1.KAFKA, ChannelKindV1.KAFKA_TRANSACTIONAL_RECEIPT,
                laneId.bytes(), laneIncarnation, target, physicalPartition, 1, 0, producer, Bytes.sha256(producer),
                evidenceResource, 1L, attestationDigest, 1, binding, fingerprint, lease);
    }

    private static ReadyCertificateV1 readyCertificate(final OwnerIdentityV1 owner,
                                                       final byte[] storeIncarnation,
                                                       final DestinationLaneId laneId,
                                                       final byte[] laneIncarnation,
                                                       final ChannelResourceIdentityV1 channel,
                                                       final BrokerResourceIdentityV1 target,
                                                       final EvidenceCursorV1 cursor,
                                                       final TrustedUtcIntervalEvidence issuedAt,
                                                       final long validUntil) {
        final byte[] barrier = ActivationBarrierV1.kafka(target, (int) channel.physicalPartition(), 0, 0)
                .canonicalBytes();
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, storeIncarnation);
            CanonicalProtobuf.bytes(output, 4, laneId.bytes());
            CanonicalProtobuf.bytes(output, 5, laneIncarnation);
            CanonicalProtobuf.bytes(output, 6, channel.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, barrier);
            CanonicalProtobuf.bytes(output, 8, cursor.canonicalBytes());
            CanonicalProtobuf.uint32(output, 9, 1);
            CanonicalProtobuf.uint32(output, 10, 1);
            CanonicalProtobuf.int64(output, 11, validUntil);
            CanonicalProtobuf.bytes(output, 12, issuedAt.canonicalBytes());
            CanonicalProtobuf.uint64(output, 13, channel.credentialBindingGeneration());
            CanonicalProtobuf.bytes(output, 14, channel.credentialBindingDigest());
            CanonicalProtobuf.bytes(output, 15, channel.resolvedCredentialVersionFingerprintDigest());
        });
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(prefix);
            while (reader.hasRemaining()) {
                writeField(output, reader.next());
            }
            CanonicalProtobuf.bytes(output, 16,
                    Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate-v1\0"), prefix));
        });
        return ReadyCertificateV1.decode(encoded);
    }

    private static byte[] canonicalLaneTuple(final String clusterId, final UUID topicId,
                                              final String physicalTopic, final ProfileRefV1 destination,
                                              final ProfileRefV1 capability) {
        if (physicalTopic == null || physicalTopic.isBlank()) {
            throw new IllegalArgumentException("Kafka physical topic must be nonblank");
        }
        final byte[] topicUuid = uuidBytes(topicId);
        return Bytes.concat(
                Bytes.sha256(Bytes.utf8("kafka-worker-tenant-routing-scope")),
                Bytes.u8(AdapterKindV1.KAFKA.wireValue()),
                Bytes.lp32(Bytes.utf8(clusterId)),
                Bytes.u8(1),
                topicUuid,
                Bytes.lp32(topicUuid),
                Bytes.u32be(0),
                Bytes.lp32(destination.profileId()),
                Bytes.u64beBits(destination.version()),
                destination.semanticHash(),
                Bytes.lp32(capability.profileId()),
                Bytes.u64beBits(capability.version()),
                capability.semanticHash(),
                Bytes.u8(1),
                Bytes.sha256(Bytes.utf8("kafka-worker-ordering-domain")));
    }

    private static ProfileRefV1 destinationProfile(final String identity) {
        return new ProfileRefV1(Bytes.utf8("destination-" + identity), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)), ProfileKindV1.DESTINATION);
    }

    private static ProfileRefV1 capabilityProfile() {
        return new ProfileRefV1(Bytes.utf8("kafka-worker-capability"), 1,
                Bytes.sha256(Bytes.utf8("kafka-worker-capability-semantic")),
                ProfileKindV1.DELIVERY_CAPABILITY);
    }

    private static byte[] zeroCharge() {
        return zeroChargeVector().canonicalBytes();
    }

    private static PublishAdmissionBody.ChargeVector zeroChargeVector() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static byte[] claimCharge(final long payloadBytes) {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 1, payloadBytes,
                0, 0, 0, 0, 0, 0, 0, 0, 0).canonicalBytes();
    }

    private static byte[] retryDecision(final long firstAttemptAt, final long retryDeadline,
                                        final int completedAttemptNo) {
        final RetryPolicyRefV1 policy = new RetryPolicyRefV1(Bytes.utf8("kafka-worker-retry"), 1,
                Bytes.sha256(Bytes.utf8("kafka-worker-retry-semantic")));
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, policy.canonicalBytes());
            CanonicalProtobuf.uint32(output, 3, completedAttemptNo);
            CanonicalProtobuf.int64(output, 4, firstAttemptAt);
            CanonicalProtobuf.int64(output, 5, retryDeadline);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, StableCode.OK.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest, final long latest,
                                                        final String identity) {
        return new TrustedUtcIntervalEvidence(earliest, latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8(identity), 1, 1, 1,
                Bytes.sha256(Bytes.utf8(identity + "-proof")), 0, null);
    }

    private static void waitUntil(final long epochMs) throws Exception {
        while (System.currentTimeMillis() < epochMs) {
            TimeUnit.MILLISECONDS.sleep(Math.min(50, Math.max(1, epochMs - System.currentTimeMillis())));
        }
    }

    private static DelayMessageId messageId(final PreparedCommand command) {
        return command.delayMessageId();
    }

    private static long branchNumber(final PublishEvidenceV1 publishEvidence, final int number) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(publishEvidence.branch());
        while (reader.hasRemaining()) {
            final CanonicalProtobuf.Reader.Field field = reader.next();
            if (field.number() == number) {
                return field.unsignedValue();
            }
        }
        throw new IllegalStateException("Kafka transactional receipt branch is missing field " + number);
    }

    private static void requirePayload(final String bootstrap, final String clusterId, final String topic,
                                       final UUID topicId, final byte[] expectedPayload) {
        final ConsumerResourceGuard guard = new ConsumerResourceGuard(clusterId, topic,
                new Uuid(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits()), 0);
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                configuration(bootstrap, "kafka-worker-destination-readback"), clusterId, topic, topicId, 0);
        final TopicPartition partition = new TopicPartition(topic, 0);
        try {
            consumer.assign(List.of(partition));
            consumer.seek(partition, 0);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(Duration.ofMillis(250));
                final GuardedFetchEvidence fetchEvidence = KafkaClientArtifactFetchEvidence.requireBatch(records,
                        guard);
                if (fetchEvidence == null) {
                    continue;
                }
                for (ConsumerRecord<byte[], byte[]> record : records.records(partition)) {
                    KafkaClientArtifactFetchEvidence.requireRecord(record, fetchEvidence, guard);
                    if (Arrays.equals(expectedPayload, record.value())) {
                        return;
                    }
                }
            }
        } finally {
            consumer.close();
        }
        throw new IllegalStateException("source-applied typed destination payload was not read back exactly");
    }

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }

    private static void writeField(final java.io.ByteArrayOutputStream output,
                                   final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
        } else {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        }
    }

    private static Map<String, Object> producerConfiguration(final String bootstrap, final String clientId) {
        return producerConfiguration(bootstrap, clientId, 1_048_576);
    }

    private static Map<String, Object> producerConfiguration(final String bootstrap, final String clientId,
                                                              final int maxRequestBytes) {
        if (maxRequestBytes <= 0) {
            throw new IllegalArgumentException("maxRequestBytes must be positive");
        }
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        configuration.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        configuration.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        configuration.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestBytes);
        return configuration;
    }

    private static Map<String, Object> transactionalProducerConfiguration(final String bootstrap,
                                                                           final String transactionalIdentity) {
        return transactionalProducerConfiguration(bootstrap, transactionalIdentity, 1_048_576);
    }

    private static Map<String, Object> transactionalProducerConfiguration(final String bootstrap,
                                                                            final String transactionalIdentity,
                                                                            final int maxRequestBytes) {
        final Map<String, Object> configuration = producerConfiguration(bootstrap,
                "nereus-delay-kafka-worker-k2", maxRequestBytes);
        configuration.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalIdentity);
        return configuration;
    }

    static final class PhysicalPublishBridge implements AutoCloseable {
        private final WorkerPhysicalPublishExecutor executor;
        private final KafkaClientArtifactShardLogMutationAppender appender;
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final ProfileRefV1 destinationProfile;
        private final ProfileRefV1 capabilityProfile;
        private final BrokerResourceIdentityV1 targetResource;
        private final ChannelResourceIdentityV1 channel;
        private final ReadyCertificateV1 readyCertificate;
        private final List<EvidenceCursorV1> evidenceCursors;
        private final String destinationPhysicalTopic;
        private final UUID destinationTopicId;
        private final boolean destinationResponseLossExpected;
        private final AtomicBoolean destinationResponseLossObserved;

        private PhysicalPublishBridge(final WorkerPhysicalPublishExecutor executor,
                                      final KafkaClientArtifactShardLogMutationAppender appender,
                                      final DestinationLaneId laneId, final byte[] laneIncarnation,
                                      final ProfileRefV1 destinationProfile,
                                      final ProfileRefV1 capabilityProfile,
                                      final BrokerResourceIdentityV1 targetResource,
                                      final ChannelResourceIdentityV1 channel,
                                      final ReadyCertificateV1 readyCertificate,
                                      final List<EvidenceCursorV1> evidenceCursors,
                                      final String destinationPhysicalTopic,
                                      final UUID destinationTopicId,
                                      final boolean destinationResponseLossExpected,
                                      final AtomicBoolean destinationResponseLossObserved) {
            this.executor = executor;
            this.appender = appender;
            this.laneId = laneId;
            this.laneIncarnation = Bytes.copy(laneIncarnation);
            this.destinationProfile = destinationProfile;
            this.capabilityProfile = capabilityProfile;
            this.targetResource = targetResource;
            this.channel = channel;
            this.readyCertificate = readyCertificate;
            this.evidenceCursors = List.copyOf(evidenceCursors);
            this.destinationPhysicalTopic = destinationPhysicalTopic;
            this.destinationTopicId = destinationTopicId;
            this.destinationResponseLossExpected = destinationResponseLossExpected;
            this.destinationResponseLossObserved = destinationResponseLossObserved;
        }

        WorkerPhysicalPublishExecutor executor() {
            return executor;
        }

        KafkaClientArtifactShardLogMutationAppender appender() {
            return appender;
        }

        DestinationLaneId laneId() {
            return laneId;
        }

        byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }

        private ProfileRefV1 destinationProfile() {
            return destinationProfile;
        }

        private ProfileRefV1 capabilityProfile() {
            return capabilityProfile;
        }

        private BrokerResourceIdentityV1 targetResource() {
            return targetResource;
        }

        ChannelResourceIdentityV1 channel() {
            return channel;
        }

        ReadyCertificateV1 readyCertificate() {
            return readyCertificate;
        }

        List<EvidenceCursorV1> evidenceCursors() {
            return evidenceCursors;
        }

        String destinationPhysicalTopic() {
            return destinationPhysicalTopic;
        }

        UUID destinationTopicId() {
            return destinationTopicId;
        }

        void requireDestinationResponseLossResolved(final DestinationPublishResult result) {
            if (!destinationResponseLossExpected) {
                return;
            }
            if (!destinationResponseLossObserved.get()) {
                throw new IllegalStateException(
                        "Kafka Worker destination response-loss proxy did not discard a committed EndTxn response");
            }
            final PublishEvidenceV1 evidence = PublishEvidenceV1.decode(result.evidence());
            if (evidence.evidenceKind() != PublishEvidenceKindV1.KAFKA_TRANSACTIONAL_RECEIPT
                    || evidence.verificationStatus() != EvidenceVerificationStatusV1.VERIFIED_PUBLISHED) {
                throw new IllegalStateException(
                        "Kafka Worker destination response-loss did not resolve typed published evidence");
            }
            evidence.requireBusinessMutation(result.externalDeliveryIdentity(), true);
            System.out.println("Kafka Worker destination response-loss smoke passed: real EndTxn committed the exact "
                    + "target-plus-receipt pair, the local response was discarded, and typed read_committed "
                    + "KAFKA_TRANSACTIONAL_RECEIPT evidence resolved the source-applied PUBLISHED Outcome");
        }

        @Override
        public void close() {
            RuntimeException failure = null;
            try {
                executor.close();
            } catch (RuntimeException closeFailure) {
                failure = closeFailure;
            }
            try {
                appender.close();
            } catch (RuntimeException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
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
