package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.KafkaProduceRequest;
import com.nereusstream.delay.adapter.KafkaProduceResult;
import com.nereusstream.delay.adapter.KafkaReceiptJournal;
import com.nereusstream.delay.adapter.KafkaReceiptResource;
import com.nereusstream.delay.adapter.KafkaTargetResource;
import com.nereusstream.delay.adapter.KafkaTransactionalDestinationAdapter;
import com.nereusstream.delay.ownership.ClaimHandoffWorkClassExecutor;
import com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import com.nereusstream.delay.ownership.InMemoryWorkerAssignmentAuthority;
import com.nereusstream.delay.ownership.OutcomeWorkClassExecutor;
import com.nereusstream.delay.ownership.OwnedDelayShard;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OwnerRecoveryCoordinator;
import com.nereusstream.delay.ownership.OwnerRecoveryTurn;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.ownership.OxiaSyncWorkerAssignmentBackend;
import com.nereusstream.delay.ownership.PublishAdmissionWorkClassExecutor;
import com.nereusstream.delay.ownership.ReplayTurnBudget;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.ownership.ShardLogMutationAppender;
import com.nereusstream.delay.ownership.SourceAcknowledgement;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayCursor;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.WorkerAssignment;
import com.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import com.nereusstream.delay.ownership.WorkerAssignmentCoordinator;
import com.nereusstream.delay.ownership.WorkerCommandRuntime;
import com.nereusstream.delay.ownership.WorkerPhysicalPublishExecutor;
import com.nereusstream.delay.ownership.WorkerPublishOutcomeMutationFactory;
import com.nereusstream.delay.ownership.WorkerPublishPreparationCoordinator;
import com.nereusstream.delay.ownership.WorkerSchedulingRuntime;
import com.nereusstream.delay.ownership.WorkerShardRuntime;
import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.ChannelResourceIdentity;
import com.nereusstream.delay.protocol.CompatibleControlSnapshot;
import com.nereusstream.delay.protocol.CredentialUseKind;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.EvidenceVerificationStatus;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishEvidenceKind;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.ReadyCertificate;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.LaneRecord;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.runtime.ScheduleResolver;
import com.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.store.CheckpointFileInventory;
import com.nereusstream.delay.store.CheckpointManifest;
import com.nereusstream.delay.store.RecoveryCatalog;
import com.nereusstream.delay.store.RecoveryCatalogAuthority;
import com.nereusstream.delay.store.RecoveryFloor;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerLoadVector;
import com.nereusstream.delay.store.WorkerPlacementPolicy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.GuardedTransactionalProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

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

    private KafkaClientArtifactWorkerSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2 && arguments.length != 3 && arguments.length != 4) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <worker-topic> "
                    + "[run|prepare|resume|crash-wait|ack-crash-wait] [destination-topic]");
        }
        final String bootstrap = arguments[0];
        final String mode = arguments.length >= 3 ? arguments[2] : "run";
        final String destinationPhysicalTopic = arguments.length >= 4 ? arguments[3] : null;
        if (!mode.equals("run")
                && !mode.equals("prepare")
                && !mode.equals("resume")
                && !mode.equals("crash-wait")
                && !mode.equals("ack-crash-wait")) {
            throw new IllegalArgumentException("unknown Worker smoke mode: " + mode);
        }
        final String topic = mode.equals("run") ? arguments[1] + "-" + UUID.randomUUID() : arguments[1];
        final String receiptPhysicalTopic =
                destinationPhysicalTopic == null ? null : destinationPhysicalTopic + "-receipt";
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                10_000);

        try (Admin admin = Admin.create(adminConfiguration)) {
            if (mode.equals("resume")) {
                requireExistingTopic(admin, topic);
                if (destinationPhysicalTopic != null) {
                    requireExistingTopic(admin, destinationPhysicalTopic);
                    requireExistingTopic(admin, receiptPhysicalTopic);
                }
            } else {
                ensureTopic(admin, topic);
                if (destinationPhysicalTopic != null) {
                    ensureTopic(admin, destinationPhysicalTopic);
                    ensureTopic(admin, receiptPhysicalTopic);
                }
            }
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final UUID nativeTopicId = toUuid(topicId);
            final UUID destinationTopicId = destinationPhysicalTopic == null
                    ? null
                    : toUuid(describe(admin, destinationPhysicalTopic).topicId());
            final UUID receiptTopicId = receiptPhysicalTopic == null
                    ? null
                    : toUuid(describe(admin, receiptPhysicalTopic).topicId());
            final ShardId shard = mode.equals("run") ? new ShardId(RouteIncarnation.random(), 0) : restartShard(topic);
            if (!mode.equals("resume")) {
                final String commandIdentity = mode.equals("prepare") ? "worker-restart-prepared" : "worker-recovery";
                produce(bootstrap, clusterId, topic, topicId, command(shard, commandIdentity));
            }
            if (mode.equals("prepare")) {
                System.out.println(
                        "Kafka Worker restart preparation passed: one guarded record persisted before broker failover");
                return;
            }

            final SourceAssignment sourceAssignment = new SourceAssignment(
                    shard,
                    Bytes.sha256(Bytes.utf8("kafka-worker-assignment")),
                    1,
                    new KafkaActivationBarrier(shard, clusterId, nativeTopicId, 1));
            final WorkClassExecutionRegistry workClasses = workClasses();
            final OxiaSyncOwnerLeaseBackend.ClientHandle oxia = connectOxiaIfConfigured();
            try {
                final WorkerAssignmentAuthority assignmentAuthority = oxia == null
                        ? new InMemoryWorkerAssignmentAuthority()
                        : new OxiaSyncWorkerAssignmentBackend(
                                oxia, "nereus-delay/kafka-worker-placement/" + UUID.randomUUID());
                final WorkerAssignment assignmentProjection =
                        publishAssignment(assignmentAuthority, sourceAssignment, oxia != null);
                final SourceAssignment assignment = assignmentProjection.sourceAssignment();
                final OxiaOwnerLeaseStore authority;
                if (oxia == null) {
                    authority = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());
                } else {
                    authority = new OxiaOwnerLeaseStore(oxia.backend());
                }
                final long ownerNow = System.currentTimeMillis();
                final byte[] sessionIdentity =
                        oxia == null ? Bytes.sha256(Bytes.utf8("kafka-worker-session")) : oxia.sessionIdentity();
                final OwnerLease lease = authority
                        .acquire(assignment, "kafka-worker", sessionIdentity, ownerNow, LEASE_DURATION_MS)
                        .orElseThrow();
                final KeyPair verificationKey =
                        KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
                final CompatibleControlSnapshot controlSnapshot = controlSnapshot(shard);
                final boolean explicitWorkerRoot = hasConfiguredWorkerRoot();
                final Path root = configuredWorkerRoot();
                try {
                    final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
                    try (SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                            ShardStore store = mode.equals("resume") && explicitWorkerRoot
                                    ? ShardStore.openForLocalRecoveryReuse(
                                            storeConfig, shard, resources, localCrashRecoveryAuthority())
                                    : ShardStore.open(storeConfig, shard, resources)) {
                        resources.bindWorkClassExecutionRegistry(workClasses);
                        store.recordControlSnapshot(controlSnapshot);
                        final com.nereusstream.delay.protocol.SourcePosition persistedPositionBeforeRecovery =
                                store.appliedShardLogPosition();
                        final boolean crashRecoveryResume =
                                mode.equals("resume") && explicitWorkerRoot && persistedPositionBeforeRecovery != null;
                        final DelayShard delayShard = new DelayShard(
                                store,
                                DelayShardConfig.defaults(),
                                null,
                                null,
                                scheduleResolver(clusterId, destinationTopicId, destinationPhysicalTopic));
                        final OwnerIdentity ownerIdentity = new OwnerIdentity(
                                bytes(16, 70),
                                bytes(16, 71),
                                lease.ownerEpoch(),
                                Bytes.sha256(Bytes.utf8("kafka-worker-fencing")));
                        final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease, ownerIdentity);

                        recoverFirstRecord(
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
                                persistedPositionBeforeRecovery);
                        if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                || ownedShard.lastCatchupPosition() == null
                                || ownedShard
                                                .lastCatchupPosition()
                                                .compareTo(new com.nereusstream.delay.protocol.KafkaSourcePosition(
                                                        shard, clusterId, nativeTopicId, 0, null, 0))
                                        < 0) {
                            throw new IllegalStateException("Kafka Worker recovery did not activate at offset zero");
                        }

                        if (!crashRecoveryResume) {
                            produce(bootstrap, clusterId, topic, topicId, command(shard, "worker-active"));
                        }
                        final PreparedCommand physicalCommand = destinationPhysicalTopic == null
                                ? null
                                : command(
                                        shard,
                                        "worker-physical-publish",
                                        Bytes.utf8("kafka-worker-source-applied-payload"),
                                        2_000);
                        final KafkaSourcePosition physicalSchedulePosition = physicalCommand == null
                                ? null
                                : produceAndPosition(bootstrap, clusterId, topic, topicId, physicalCommand);
                        final String configuredWorkerGroup = System.getenv("NEREUS_DELAY_KAFKA_WORKER_GROUP_ID");
                        final String workerGroup = configuredWorkerGroup == null || configuredWorkerGroup.isBlank()
                                ? (explicitWorkerRoot
                                        ? "nereus-delay-worker-crash-" + topic
                                        : "nereus-delay-worker-e2e-" + UUID.randomUUID())
                                : configuredWorkerGroup;
                        final KafkaSourcePosition durableAckPosition = durableAckRecoveryPosition(
                                crashRecoveryResume, persistedPositionBeforeRecovery, shard, clusterId, nativeTopicId);
                        if (durableAckPosition != null) {
                            acknowledgeDurableKafkaRecord(
                                    bootstrap,
                                    clusterId,
                                    topic,
                                    nativeTopicId,
                                    shard,
                                    workerGroup,
                                    durableAckPosition,
                                    store,
                                    admin);
                        }
                        final GuardedConsumer<byte[], byte[]> rawWorkerConsumer =
                                workerConsumer(bootstrap, workerGroup, clusterId, topic, nativeTopicId, shard);
                        final AtomicBoolean sourceAckResponseLossObserved = new AtomicBoolean();
                        final GuardedConsumer<byte[], byte[]> workerConsumer;
                        if (mode.equals("ack-crash-wait")) {
                            workerConsumer = workerAckProcessCrashConsumer(
                                    rawWorkerConsumer, store, topic, clusterId, topicId, shard);
                        } else if (hasSourceAckResponseLoss()) {
                            workerConsumer =
                                    sourceAckResponseLossConsumer(rawWorkerConsumer, sourceAckResponseLossObserved);
                        } else {
                            workerConsumer = rawWorkerConsumer;
                        }
                        final PhysicalPublishBridge physicalBridge = physicalCommand == null
                                ? null
                                : createPhysicalPublishBridge(
                                        bootstrap,
                                        clusterId,
                                        topic,
                                        nativeTopicId,
                                        shard,
                                        physicalSchedulePosition,
                                        destinationPhysicalTopic,
                                        destinationTopicId,
                                        receiptPhysicalTopic,
                                        receiptTopicId,
                                        store,
                                        ownedShard,
                                        ownerIdentity,
                                        authority,
                                        workClasses,
                                        verificationKey);
                        WorkerShardRuntime runtime = null;
                        boolean drained = false;
                        try (physicalBridge) {
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
                                    verificationKey.getPublic(),
                                    null,
                                    null,
                                    null,
                                    null,
                                    physicalBridge == null ? null : physicalBridge.executor());
                            if (durableAckPosition != null) {
                                rawWorkerConsumer.seek(
                                        new TopicPartition(topic, shard.partition()),
                                        Math.addExact(durableAckPosition.offset(), 1));
                            } else {
                                awaitWorkerProcessCrashCutIfRequested(mode, store, topic, clusterId, topicId, shard);
                                final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result =
                                        runUntilApplied(runtime);
                                if (result.status()
                                        != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                                .APPLIED_AND_ACKED) {
                                    throw new IllegalStateException(
                                            "Kafka Worker source turn did not ACK: " + result.status(),
                                            result.failure());
                                }
                            }
                            final var applied = store.appliedShardLogPosition();
                            if (!(applied instanceof com.nereusstream.delay.protocol.KafkaSourcePosition position)
                                    || position.offset() != 1
                                    || !position.shardId().equals(shard)
                                    || !position.authenticatedClusterId().equals(clusterId)
                                    || !position.nativeTopicUuid().equals(nativeTopicId)) {
                                throw new IllegalStateException(
                                        "Kafka Worker Store did not persist exact active position");
                            }
                            writeKafkaWorkerStateDump(
                                    mode,
                                    "RECOVERED_AFTER_FRESH_PROCESS",
                                    store,
                                    topic,
                                    clusterId,
                                    topicId,
                                    shard,
                                    true);
                            if (physicalBridge != null) {
                                final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult
                                        physicalSchedule = runUntilApplied(runtime);
                                if (!(physicalSchedule.entry() instanceof SourceReplayRecord physicalRecord)
                                        || !physicalRecord.command().equals(physicalCommand)
                                        || !(physicalSchedule.entry().position()
                                                instanceof KafkaSourcePosition appliedPhysical)) {
                                    throw new IllegalStateException(
                                            "Kafka Worker physical Schedule was not source-applied");
                                }
                                if (!appliedPhysical.equals(physicalSchedulePosition)) {
                                    throw new IllegalStateException(
                                            "Kafka Worker physical Schedule Source Position changed across apply: "
                                                    + "produced=" + physicalSchedulePosition + ", applied="
                                                    + appliedPhysical);
                                }
                                runSourceAppliedPhysicalPublish(
                                        runtime,
                                        delayShard,
                                        ownedShard,
                                        ownerIdentity,
                                        authority,
                                        store,
                                        workClasses,
                                        verificationKey,
                                        physicalBridge,
                                        physicalCommand,
                                        physicalSchedulePosition,
                                        bootstrap,
                                        clusterId);
                            }
                            requireCommittedOffset(admin, workerGroup, topic, 0, physicalBridge == null ? 2 : 5);
                            final Path checkpointPath = root.resolve("worker-final-checkpoint");
                            final byte[] checkpointId =
                                    Arrays.copyOf(Bytes.sha256(Bytes.utf8("kafka-worker-final-checkpoint")), 16);
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
                                        "Kafka Worker drain did not publish the final checkpoint and release "
                                                + "the exact owner lease");
                            }
                            runtime.close();
                            drained = true;
                            System.out.println("Kafka Worker vertical smoke passed: assignment recovery offset=0, "
                                    + "active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, "
                                    + (physicalBridge == null
                                            ? ""
                                            : "source-applied physical publish with typed "
                                                    + "KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, ")
                                    + "and final checkpoint");
                            if (oxia != null) {
                                System.out.println(
                                        "Kafka Worker authority smoke passed: real Oxia session-bound lease");
                            }
                            if (hasSourceAckResponseLoss()) {
                                if (!sourceAckResponseLossObserved.get()) {
                                    throw new IllegalStateException(
                                            "Kafka Worker source ACK response-loss wrapper did not lose a response");
                                }
                                System.out.println(
                                        "Kafka Worker source ACK response-loss smoke passed: real commitSync "
                                                + "ACK was accepted before the local response was discarded, "
                                                + "and the same "
                                                + "source record was ACKed on the next bounded Worker turn");
                            }
                        } finally {
                            if (!drained) {
                                workerConsumer.close();
                            }
                        }
                    }
                } finally {
                    if (!preserveWorkerCrashRoot()) {
                        deleteTree(root);
                    }
                }
            } finally {
                if (oxia != null) {
                    oxia.close();
                }
            }
        }
    }

    private static ShardId restartShard(final String topic) {
        return new ShardId(
                new RouteIncarnation(java.util.Arrays.copyOf(
                        Bytes.sha256(Bytes.utf8("nereus-delay-kafka-worker-restart/" + topic)),
                        RouteIncarnation.LENGTH)),
                0);
    }

    private static Path configuredWorkerRoot() throws Exception {
        final String configured = System.getenv("NEREUS_DELAY_KAFKA_WORKER_ROOT");
        if (configured == null || configured.isBlank()) {
            return Files.createTempDirectory("nereus-delay-kafka-worker-");
        }
        return Files.createDirectories(Path.of(configured));
    }

    private static boolean hasConfiguredWorkerRoot() {
        final String configured = System.getenv("NEREUS_DELAY_KAFKA_WORKER_ROOT");
        return configured != null && !configured.isBlank();
    }

    private static boolean preserveWorkerCrashRoot() {
        return "1".equals(System.getenv("NEREUS_DELAY_KAFKA_PRESERVE_WORKER_CRASH_ROOT"));
    }

    /**
     * Local crash-cut seam used only to prove ACTIVE DB reuse in this focused
     * harness. Production recovery must replace this with the Oxia-backed
     * catalog/Floor transaction; it deliberately does not claim that
     * authority here.
     */
    private static RecoveryCatalogAuthority localCrashRecoveryAuthority() {
        return new RecoveryCatalogAuthority() {
            @Override
            public RecoveryCatalog.Publication publish(
                    final CheckpointManifest manifest, final long expectedCatalogGeneration) {
                throw new UnsupportedOperationException("crash-cut harness does not publish a catalog manifest");
            }

            @Override
            public RecoveryFloor advanceFloor(
                    final byte[] checkpointId,
                    final long expectedCatalogGeneration,
                    final byte[] evidenceCursorDigest) {
                throw new UnsupportedOperationException("crash-cut harness does not advance a Recovery Floor");
            }

            @Override
            public java.util.Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
                throw new UnsupportedOperationException("crash-cut harness does not read a catalog manifest");
            }

            @Override
            public java.util.Optional<RecoveryFloor> currentFloor() {
                throw new UnsupportedOperationException("crash-cut harness does not read a Recovery Floor");
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest manifest) {
                throw new UnsupportedOperationException("crash-cut harness does not validate a restore candidate");
            }

            @Override
            public java.util.Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] candidateCheckpointId,
                    final long requiredMutationSequence,
                    final com.nereusstream.delay.protocol.SourcePosition... requiredPositions) {
                throw new UnsupportedOperationException("crash-cut harness does not prove Floor coverage");
            }

            @Override
            public void validateLocalStoreRecovery(
                    final ShardId candidateShard, final com.nereusstream.delay.store.StoreRecoveryMetadata metadata) {
                if (metadata == null) {
                    throw new IllegalArgumentException("crash-cut recovery metadata is missing");
                }
            }
        };
    }

    /**
     * Holds a real Worker JVM after it has opened the source and local Store but
     * before it can ACK the next source record. The E2E harness kills the PID
     * written here, then starts a fresh JVM against the same exact root.
     */
    private static void awaitWorkerProcessCrashCutIfRequested(
            final String mode,
            final ShardStore store,
            final String topic,
            final String clusterId,
            final Uuid topicId,
            final ShardId shard)
            throws Exception {
        if (!mode.equals("crash-wait")) {
            return;
        }
        writeKafkaWorkerStateDump(mode, "WORKER_PROCESS_CRASH_READY", store, topic, clusterId, topicId, shard, false);
        final String gatePath = System.getenv("NEREUS_DELAY_KAFKA_WORKER_CRASH_GATE");
        final String pidPath = System.getenv("NEREUS_DELAY_KAFKA_WORKER_CRASH_PID_FILE");
        if (gatePath == null || gatePath.isBlank() || pidPath == null || pidPath.isBlank()) {
            throw new IllegalArgumentException("crash-wait requires NEREUS_DELAY_KAFKA_WORKER_CRASH_GATE and PID_FILE");
        }
        final Path gate = Path.of(gatePath);
        final Path pid = Path.of(pidPath);
        Files.createDirectories(gate.toAbsolutePath().getParent());
        Files.writeString(
                pid,
                Long.toString(ProcessHandle.current().pid()) + "\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        Files.writeString(
                gate, "worker-source-runtime-ready\n", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("Kafka Worker process-crash cut reached: pid="
                + ProcessHandle.current().pid() + ", sourceRuntimeReady=true, nextSourceRecordUnacked=true");
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
            final GuardedConsumer<byte[], byte[]> delegate,
            final ShardStore store,
            final String topic,
            final String clusterId,
            final Uuid topicId,
            final ShardId shard) {
        return (GuardedConsumer<byte[], byte[]>) Proxy.newProxyInstance(
                KafkaClientArtifactWorkerSmoke.class.getClassLoader(),
                new Class<?>[] {GuardedConsumer.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("commitSync") && method.getParameterCount() == 1) {
                        awaitWorkerAckProcessCrashCut(store, topic, clusterId, topicId, shard);
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static void awaitWorkerAckProcessCrashCut(
            final ShardStore store, final String topic, final String clusterId, final Uuid topicId, final ShardId shard)
            throws Exception {
        writeKafkaWorkerStateDump(
                "ack-crash-wait", "WORKER_ACK_PROCESS_CRASH_READY", store, topic, clusterId, topicId, shard, false);
        final String gatePath = System.getenv("NEREUS_DELAY_KAFKA_WORKER_ACK_CRASH_GATE");
        final String pidPath = System.getenv("NEREUS_DELAY_KAFKA_WORKER_ACK_CRASH_PID_FILE");
        if (gatePath == null || gatePath.isBlank() || pidPath == null || pidPath.isBlank()) {
            throw new IllegalArgumentException(
                    "ack-crash-wait requires NEREUS_DELAY_KAFKA_WORKER_ACK_CRASH_GATE and PID_FILE");
        }
        final Path gate = Path.of(gatePath);
        final Path pid = Path.of(pidPath);
        Files.createDirectories(gate.toAbsolutePath().getParent());
        Files.writeString(
                pid,
                Long.toString(ProcessHandle.current().pid()) + "\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        Files.writeString(
                gate,
                "worker-store-durable-before-kafka-ack\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("Kafka Worker ACK process-crash cut reached: pid="
                + ProcessHandle.current().pid() + ", storeWriteBatchDurable=true, kafkaCommitSyncStarted=false");
        while (Files.exists(gate)) {
            Thread.sleep(100);
        }
    }

    /**
     * Captures the actual RocksDB identity and source-position projection at a
     * Kafka Worker crash boundary. The file is written and fsync-forced while
     * the Store is open; it is therefore evidence about the durable local
     * authority, not a shell-side copy of the log marker.
     */
    private static void writeKafkaWorkerStateDump(
            final String mode,
            final String phase,
            final ShardStore store,
            final String topic,
            final String clusterId,
            final Uuid topicId,
            final ShardId shard,
            final boolean sourceAckCommitted)
            throws Exception {
        final String ackDirectory = System.getenv("NEREUS_DELAY_KAFKA_WORKER_ACK_PROCESS_CRASH_STATE_DUMP_DIR");
        final String workerDirectory = System.getenv("NEREUS_DELAY_KAFKA_WORKER_PROCESS_CRASH_STATE_DUMP_DIR");
        final boolean ackMode = mode.equals("ack-crash-wait")
                || (ackDirectory != null
                        && !ackDirectory.isBlank()
                        && (workerDirectory == null || workerDirectory.isBlank()));
        final String directoryValue = ackMode ? ackDirectory : workerDirectory;
        if (directoryValue == null || directoryValue.isBlank()) {
            return;
        }
        final Path directory = Path.of(directoryValue).toAbsolutePath().normalize();
        Files.createDirectories(directory);
        final com.nereusstream.delay.protocol.SourcePosition applied = store.appliedShardLogPosition();
        final Long appliedOffset = applied instanceof com.nereusstream.delay.protocol.KafkaSourcePosition position
                ? position.offset()
                : null;
        final var metadata = store.metadata();
        final String fileName = phase.endsWith("READY") ? "before-process-crash.json" : "after-fresh-process.json";
        final String json = "{\n"
                + " \"schema\": \"nereus-delay-chaos-durable-state-dump\",\n"
                + " \"cell\": " + jsonString(ackMode ? "kafka-worker-ack-process-crash" : "kafka-worker-process-crash")
                + ",\n"
                + " \"phase\": " + jsonString(phase) + ",\n"
                + " \"process_pid\": " + ProcessHandle.current().pid() + ",\n"
                + " \"store_root\": " + jsonString(store.dbPath().toString()) + ",\n"
                + " \"topic\": " + jsonString(topic) + ",\n"
                + " \"cluster_id\": " + jsonString(clusterId) + ",\n"
                + " \"topic_id\": " + jsonString(topicId.toString()) + ",\n"
                + " \"route_uuid\": "
                + jsonString(shard.routeIncarnation().uuid().toString()) + ",\n"
                + " \"partition\": " + shard.partition() + ",\n"
                + " \"store_incarnation\": " + jsonString(Bytes.hex(metadata.storeIncarnation())) + ",\n"
                + " \"db_identity\": " + jsonString(Bytes.hex(metadata.dbIdentity())) + ",\n"
                + " \"applied_source_position\": "
                + jsonNullable(applied == null ? null : Bytes.hex(applied.canonicalBytes())) + ",\n"
                + " \"applied_offset\": " + jsonNullable(appliedOffset) + ",\n"
                + " \"shard_mutation_sequence\": " + store.shardMutationSequence() + ",\n"
                + " \"store_write_batch_durable\": true,\n"
                + " \"source_ack_committed\": " + sourceAckCommitted + ",\n"
                + " \"durable_store_read\": true,\n"
                + " \"dump_forced\": true\n"
                + "}\n";
        final Path target = directory.resolve(fileName);
        try (var channel = java.nio.channels.FileChannel.open(
                target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(json.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            channel.force(true);
        }
    }

    private static String jsonString(final String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String jsonNullable(final String value) {
        return value == null ? "null" : jsonString(value);
    }

    private static String jsonNullable(final Long value) {
        return value == null ? "null" : Long.toString(value);
    }

    private static WorkerAssignment publishAssignment(
            final WorkerAssignmentAuthority authority,
            final SourceAssignment sourceAssignment,
            final boolean realOxia) {
        final WorkerAssignmentCoordinator coordinator = new WorkerAssignmentCoordinator(
                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
        final long now = System.currentTimeMillis();
        final WorkerPlacementPolicy.WorkerCandidate candidate = new WorkerPlacementPolicy.WorkerCandidate(
                "kafka-worker",
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
                Bytes.sha256(Bytes.utf8("kafka-worker-capacity-envelope")),
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
        System.out.println("Kafka Worker assignment publication/acceptance passed: revision="
                + publication.revision() + ", worker=" + accepted.workerId() + ", authority="
                + (realOxia ? "real Oxia session-bound" : "in-memory"));
        return accepted;
    }

    private static CapacityVector capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[CapacityDimension.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVector(values);
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
                "nereus-delay-kafka-worker-" + UUID.randomUUID(),
                Duration.ofSeconds(15),
                "nereus-delay-kafka-worker/" + UUID.randomUUID());
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void recoverFirstRecord(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard,
            final SourceAssignment assignment,
            final OxiaOwnerLeaseStore authority,
            final com.nereusstream.delay.ownership.OwnedDelayShard ownedShard,
            final KeyPair verificationKey,
            final CompatibleControlSnapshot controlSnapshot,
            final WorkClassExecutionRegistry workClasses,
            final com.nereusstream.delay.protocol.SourcePosition persistedPosition) {
        final String groupId = "nereus-delay-worker-recovery-" + UUID.randomUUID();
        final long startOffsetInclusive = recoveryStartOffset(persistedPosition, shard, clusterId, topicId);
        try (KafkaClientArtifactRecoverySourceCursor nativeCursor = new KafkaClientArtifactRecoverySourceCursor(
                recoveryConsumer(bootstrap, groupId, clusterId, topic, topicId, shard),
                assignment,
                topic,
                startOffsetInclusive,
                Duration.ofMillis(250))) {
            final SourceReplayCursor<SourceReplayEntry> cursor = SourceReplayCursor.of(nativeCursor);
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
            if (!recovery.complete() || turn.outcomes().size() > 1) {
                throw new IllegalStateException(
                        "Kafka Worker recovery did not complete within one bounded source turn");
            }
            if (persistedPosition == null && turn.outcomes().size() != 1) {
                throw new IllegalStateException("Kafka Worker fresh recovery did not apply exactly one source record");
            }
            if (persistedPosition != null
                    && turn.outcomes().isEmpty()
                    && !sameSourcePosition(persistedPosition, ownedShard.lastCatchupPosition())) {
                throw new IllegalStateException(
                        "Kafka Worker recovery changed the durable source position while reactivating");
            }
        }
    }

    private static long recoveryStartOffset(
            final com.nereusstream.delay.protocol.SourcePosition persistedPosition,
            final ShardId shard,
            final String clusterId,
            final UUID topicId) {
        if (persistedPosition == null) {
            return 0;
        }
        if (!(persistedPosition instanceof KafkaSourcePosition kafka)
                || !kafka.shardId().equals(shard)
                || !kafka.authenticatedClusterId().equals(clusterId)
                || !kafka.nativeTopicUuid().equals(topicId)
                || kafka.offset() < 0) {
            throw new IllegalStateException("Kafka Worker durable recovery position has a different source identity");
        }
        return Math.addExact(kafka.offset(), 1);
    }

    private static KafkaSourcePosition durableAckRecoveryPosition(
            final boolean crashRecoveryResume,
            final com.nereusstream.delay.protocol.SourcePosition persistedPosition,
            final ShardId shard,
            final String clusterId,
            final UUID topicId) {
        if (!crashRecoveryResume
                || !(persistedPosition instanceof KafkaSourcePosition kafka)
                || kafka.offset() != 1
                || !kafka.shardId().equals(shard)
                || !kafka.authenticatedClusterId().equals(clusterId)
                || !kafka.nativeTopicUuid().equals(topicId)) {
            return null;
        }
        return kafka;
    }

    private static void acknowledgeDurableKafkaRecord(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard,
            final String workerGroup,
            final KafkaSourcePosition expected,
            final ShardStore store,
            final Admin admin)
            throws Exception {
        final GuardedConsumer<byte[], byte[]> consumer =
                workerConsumer(bootstrap, workerGroup, clusterId, topic, topicId, shard);
        try (KafkaClientArtifactSourceRecordConsumer source = new KafkaClientArtifactSourceRecordConsumer(
                consumer, clusterId, topicId, shard, topic, Duration.ofMillis(250))) {
            final TopicPartition topicPartition = new TopicPartition(topic, shard.partition());
            consumer.seek(topicPartition, expected.offset());
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            SourceRecordConsumer.PolledSourceRecord observed = null;
            while (System.nanoTime() < deadline) {
                final Optional<SourceRecordConsumer.PolledSourceRecord> polled = source.poll();
                if (polled.isPresent()) {
                    observed = polled.get();
                    break;
                }
            }
            if (observed == null
                    || !(observed.entry().position() instanceof KafkaSourcePosition actual)
                    || !sameSourcePosition(expected, actual)) {
                throw new IllegalStateException(
                        "Kafka Worker durable ACK retry did not fetch the exact source position");
            }
            final SourceAcknowledgement.AcknowledgementResult result =
                    observed.acknowledgement().acknowledge(observed.entry(), null);
            if (result.disposition() != SourceAcknowledgement.Disposition.ACKED) {
                throw new IllegalStateException(
                        "Kafka Worker durable ACK retry was not ACKED: " + result.disposition(), result.failure());
            }
            if (!sameSourcePosition(expected, store.appliedShardLogPosition())) {
                throw new IllegalStateException("Kafka Worker durable ACK retry changed the applied Store position");
            }
            requireCommittedOffset(admin, workerGroup, topic, shard.partition(), Math.addExact(expected.offset(), 1));
        }
    }

    private static boolean sameSourcePosition(
            final com.nereusstream.delay.protocol.SourcePosition expected,
            final com.nereusstream.delay.protocol.SourcePosition actual) {
        return actual != null && Arrays.equals(expected.canonicalBytes(), actual.canonicalBytes());
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
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS
                    && result.status() != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN
                    && result.status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                    .ACK_DEFINITIVELY_NOT_ACKED) {
                throw new IllegalStateException(
                        "Kafka Worker source turn failed: " + result.status(), result.failure());
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
                KafkaClientArtifactWorkerSmoke.class.getClassLoader(),
                new Class<?>[] {GuardedConsumer.class},
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
        configuration.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 2_000_000);
        configuration.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 4_000_000);
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return configuration;
    }

    private static void produce(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final Uuid topicId,
            final PreparedCommand command)
            throws Exception {
        produceAndPosition(bootstrap, clusterId, topic, topicId, command);
    }

    private static KafkaSourcePosition produceAndPosition(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final Uuid topicId,
            final PreparedCommand command)
            throws Exception {
        final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(
                producerConfiguration(bootstrap, "nereus-delay-worker-smoke"),
                new ByteArraySerializer(),
                new ByteArraySerializer());
        final KafkaClientArtifactProduceTransport transport =
                new KafkaClientArtifactProduceTransport((GuardedProducer<byte[], byte[]>) producer);
        try {
            final KafkaProduceResult result = transport
                    .produce(new KafkaProduceRequest(
                            clusterId,
                            topic,
                            toUuid(topicId),
                            0,
                            command.commandId(),
                            com.nereusstream.delay.protocol.CommandCodec.encodeManagedFrame(command)))
                    .toCompletableFuture()
                    .get(15, TimeUnit.SECONDS);
            if (result.disposition() != KafkaProduceResult.Disposition.PERSISTED
                    || !clusterId.equals(result.authenticatedClusterId())
                    || !toUuid(topicId).equals(result.nativeTopicUuid())
                    || result.partition() != 0) {
                throw new IllegalStateException("guarded Kafka Worker producer did not persist: " + result.disposition()
                        + "/" + result.stableCode());
            }
            final KafkaSourcePosition producedPosition = new KafkaSourcePosition(
                    command.shardId(),
                    clusterId,
                    toUuid(topicId),
                    result.offset(),
                    result.leaderEpoch(),
                    result.brokerLogAppendTimeEpochMs());
            return readBackSourcePosition(bootstrap, clusterId, topic, topicId, command, producedPosition);
        } finally {
            transport.close();
        }
    }

    /** Completes the optional Produce position with the exact guarded Fetch position. */
    private static KafkaSourcePosition readBackSourcePosition(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final Uuid topicId,
            final PreparedCommand command,
            final KafkaSourcePosition producedPosition) {
        final ConsumerResourceGuard guard = new ConsumerResourceGuard(clusterId, topic, topicId, 0);
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                configuration(bootstrap, "kafka-worker-position-readback-" + UUID.randomUUID()),
                clusterId,
                topic,
                toUuid(topicId),
                0);
        final TopicPartition partition = new TopicPartition(topic, 0);
        final byte[] expectedValue = com.nereusstream.delay.protocol.CommandCodec.encodeManagedFrame(command);
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
                        throw new IllegalStateException(
                                "Kafka Worker source readback value changed at the produced offset");
                    }
                    if (record.timestamp() != producedPosition.brokerLogAppendTimeEpochMs()) {
                        throw new IllegalStateException(
                                "Kafka Worker source readback broker append time changed at the produced offset: "
                                        + "produced=" + producedPosition.brokerLogAppendTimeEpochMs() + ", fetched="
                                        + record.timestamp());
                    }
                    final Integer fetchedLeaderEpoch = record.leaderEpoch().orElse(null);
                    if (producedPosition.leaderEpoch() != null
                            && !producedPosition.leaderEpoch().equals(fetchedLeaderEpoch)) {
                        throw new IllegalStateException(
                                "Kafka Worker source readback leader epoch changed at the produced offset: "
                                        + "produced=" + producedPosition.leaderEpoch() + ", fetched="
                                        + fetchedLeaderEpoch);
                    }
                    return new KafkaSourcePosition(
                            command.shardId(),
                            clusterId,
                            toUuid(topicId),
                            record.offset(),
                            fetchedLeaderEpoch,
                            record.timestamp());
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

    private static PreparedCommand command(
            final ShardId shard, final String identity, final byte[] payload, final long delayMs) {
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("destination-" + identity),
                1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)),
                ProfileKind.DESTINATION);
        final RetryPolicyRef retryPolicy = new RetryPolicyRef(
                Bytes.utf8("retry-" + identity), 1, Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + delayMs;
        // Broker failover and a real Worker replay can consume several seconds
        // before the due/Claim/physical-publish turn starts. Keep the smoke's
        // materialization window bounded but large enough that the fault
        // injection tests the recovery path instead of expiring the fixture.
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination,
                retryPolicy,
                deliverAt,
                deliverAt + 60_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                payload,
                Bytes.utf8("source-" + identity),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        return PreparedCommand.schedule(shard, intent, deliverAt + 90_000);
    }

    private static ScheduleResolver scheduleResolver(
            final String clusterId, final UUID destinationTopicId, final String destinationPhysicalTopic) {
        if (destinationPhysicalTopic != null) {
            return new ScheduleResolver() {
                @Override
                public ResolvedSchedule resolveSchedule(
                        final ShardId shard,
                        final DelayMessageId message,
                        final CanonicalScheduleIntent intent,
                        final com.nereusstream.delay.protocol.SourcePosition source) {
                    final byte[] tuple = canonicalLaneTuple(
                            clusterId,
                            destinationTopicId,
                            destinationPhysicalTopic,
                            intent.profile(),
                            capabilityProfile());
                    return new ResolvedSchedule(DestinationLaneId.derive(tuple), tuple, intent.inlinePayload(), null);
                }

                @Override
                public ResolvedPrepare resolvePrepare(
                        final ShardId shard,
                        final DelayMessageId message,
                        final com.nereusstream.delay.protocol.PrepareLargeScheduleBody body,
                        final com.nereusstream.delay.protocol.SourcePosition source) {
                    final byte[] tuple = canonicalLaneTuple(
                            clusterId,
                            destinationTopicId,
                            destinationPhysicalTopic,
                            body.intentWithoutPayload().profile(),
                            capabilityProfile());
                    return new ResolvedPrepare(DestinationLaneId.derive(tuple), tuple);
                }
            };
        }
        final byte[] tuple = Bytes.utf8("kafka-worker-canonical-lane-tuple");
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        return new ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(
                    final ShardId shard,
                    final com.nereusstream.delay.protocol.DelayMessageId message,
                    final CanonicalScheduleIntent intent,
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

    /** Runs the source-ordered Admission, physical K2 publish and Outcome path. */
    static void runSourceAppliedPhysicalPublish(
            final WorkerShardRuntime runtime,
            final DelayShard delayShard,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final PreparedCommand physicalCommand,
            final KafkaSourcePosition physicalSchedulePosition,
            final String bootstrap,
            final String clusterId)
            throws Exception {
        final var message = delayShard.getMessage(physicalCommand.delayMessageId());
        if (message == null) {
            throw new IllegalStateException("source-applied physical Schedule message is missing");
        }
        runSourceAppliedPhysicalPublish(
                runtime,
                delayShard,
                ownedShard,
                ownerIdentity,
                authority,
                store,
                workClasses,
                verificationKey,
                bridge,
                physicalCommand.delayMessageId(),
                physicalSchedulePosition,
                message.payload(),
                bootstrap,
                clusterId);
    }

    static void runSourceAppliedPhysicalPublish(
            final WorkerShardRuntime runtime,
            final DelayShard delayShard,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final com.nereusstream.delay.protocol.DelayMessageId physicalMessageId,
            final KafkaSourcePosition physicalSchedulePosition,
            final byte[] expectedPayload,
            final String bootstrap,
            final String clusterId)
            throws Exception {
        runSourceAppliedPhysicalPublish(
                runtime,
                delayShard,
                ownedShard,
                ownerIdentity,
                authority,
                store,
                workClasses,
                verificationKey,
                bridge,
                physicalMessageId,
                physicalSchedulePosition,
                expectedPayload,
                bootstrap,
                clusterId,
                1_000_000);
    }

    static void runSourceAppliedPhysicalPublish(
            final WorkerShardRuntime runtime,
            final DelayShard delayShard,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final com.nereusstream.delay.protocol.DelayMessageId physicalMessageId,
            final KafkaSourcePosition physicalSchedulePosition,
            final byte[] expectedPayload,
            final String bootstrap,
            final String clusterId,
            final long maxClaimBytes)
            throws Exception {
        runSourceAppliedPhysicalPublish(
                runtime,
                delayShard,
                ownedShard,
                ownerIdentity,
                authority,
                store,
                workClasses,
                verificationKey,
                bridge,
                physicalMessageId,
                physicalSchedulePosition,
                expectedPayload,
                bootstrap,
                clusterId,
                maxClaimBytes,
                null);
    }

    static void runSourceAppliedPhysicalPublish(
            final WorkerShardRuntime runtime,
            final DelayShard delayShard,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final com.nereusstream.delay.protocol.DelayMessageId physicalMessageId,
            final KafkaSourcePosition physicalSchedulePosition,
            final byte[] expectedPayload,
            final String bootstrap,
            final String clusterId,
            final long maxClaimBytes,
            final ClaimExecutionAdmission sharedClaimAdmission)
            throws Exception {
        final var message = delayShard.getMessage(physicalMessageId);
        if (message == null
                || message.status() != MessageStatus.SCHEDULED
                || !message.laneId().equals(bridge.laneId())) {
            throw new IllegalStateException(
                    "source-applied physical Schedule did not create the expected SCHEDULED message");
        }
        delayShard.activateLaneReadiness(
                bridge.laneId(),
                bridge.laneIncarnation(),
                bridge.channel(),
                bridge.readyCertificate(),
                bridge.evidenceCursors());
        final var lane = delayShard.getLane(bridge.laneId());
        if (lane == null || !lane.schedulable()) {
            throw new IllegalStateException("source-applied physical Lane did not become schedulable");
        }
        bindActiveOwnerPublishGraph(
                runtime,
                ownedShard,
                ownerIdentity,
                authority,
                store,
                workClasses,
                verificationKey,
                bridge,
                maxClaimBytes,
                sharedClaimAdmission);
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
            final TrustedUtcIntervalEvidence dueEvidence =
                    evidence(dueEarliest, dueEarliest + 500, "kafka-worker-due-clock");
            dueClaimPublish = runtime.runDueClaimPublishPhysicalTurn(
                    dueEvidence,
                    new SchedulerBudget(1, schedulerBudgetBytes, TimeUnit.SECONDS.toNanos(2)),
                    message.expireAtEpochMs() - 1,
                    claimCharge(payload.length),
                    System::currentTimeMillis,
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)),
                    16,
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)),
                    16,
                    ignored -> Optional.of(payload));
            if (dueClaimPublish.dueClaimPublishTurn().claimResult().isPresent()) {
                break;
            }
        }
        final var dueClaim = dueClaimPublish.dueClaimPublishTurn();
        final var claimResult = dueClaim.claimResult()
                .orElseThrow(
                        () -> new IllegalStateException("provider-driven Worker turns did not return a Claim result"));
        if (claimResult.kind() != ClaimHandoffWorkClassExecutor.ResultKind.CLAIMED) {
            throw new IllegalStateException("provider-driven Worker Claim was not admitted: " + claimResult.kind()
                    + ", permitRejection=" + claimResult.permitRejection()
                    + ", prerequisiteRejection=" + claimResult.prerequisiteRejection());
        }
        final var admissionSubmission = dueClaim.publishSubmission()
                .orElseThrow(
                        () -> new IllegalStateException("provider-driven Worker turn did not queue Publish Admission"));
        final var admissionResult = admissionSubmission
                .result()
                .orElseThrow(() -> new IllegalStateException("provider-driven Publish Admission has no result"));
        if (admissionResult.kind()
                        != com.nereusstream.delay.ownership.PublishAdmissionWorkClassExecutor.ResultKind.ENQUEUED
                || !(admissionResult.sourcePosition() instanceof KafkaSourcePosition admissionPosition)
                || admissionPosition.compareTo(physicalSchedulePosition) <= 0) {
            throw new IllegalStateException(
                    "Kafka Worker provider-driven Publish Admission was not source-bound: " + admissionResult.kind());
        }
        final PublishAdmissionBody admissionBody =
                PublishAdmissionBody.decode(admissionResult.mutation().canonicalBody());
        final byte[] publishAttemptId = admissionBody.publishAttemptId();
        final WorkerShardRuntime.SourceBoundPhysicalPublishTurn physicalTurn = dueClaimPublish
                .physicalTurn()
                .orElseThrow(
                        () -> new IllegalStateException("provider-driven Worker turn did not start physical publish"));
        if (physicalTurn.status() != WorkerShardRuntime.SourceBoundPhysicalPublishStatus.PHYSICAL_SUBMITTED) {
            throw new IllegalStateException("source-applied PUBLISHING did not submit physical publish: "
                    + physicalTurn.status() + "/" + physicalTurn.failure());
        }
        final WorkerPhysicalPublishExecutor.Submission submission =
                physicalTurn.physicalSubmission().orElseThrow();
        waitForPhysicalCompletion(submission);
        final DestinationPublishResult physicalResult =
                submission.physicalResult().orElseThrow();
        if (physicalResult.disposition() != DestinationPublishResult.Disposition.PUBLISHED
                || physicalResult.evidence() == null) {
            throw new IllegalStateException("source-applied physical publish did not return typed PUBLISHED evidence: "
                    + physicalResult.disposition() + "/" + physicalResult.stableCode());
        }

        SourceReplayMutation outcomeRecord = null;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            workClasses.runTurn(new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)));
            final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult turn = runtime.runSourceTurn(
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)), System::currentTimeMillis);
            if (turn.status() == com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                if (turn.entry() instanceof SourceReplayMutation mutation
                        && mutation.mutation().type() == SystemMutationType.PUBLISH_OUTCOME) {
                    outcomeRecord = mutation;
                    break;
                }
                continue;
            }
            if (turn.status() != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && turn.status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                    .WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException(
                        "Kafka Worker Publish Outcome source turn failed: " + turn.status(), turn.failure());
            }
            TimeUnit.MILLISECONDS.sleep(25);
        }
        if (outcomeRecord == null) {
            throw new IllegalStateException("source-applied PUBLISH_OUTCOME did not become visible before deadline");
        }
        final PublishOutcomeBody outcome =
                PublishOutcomeBody.decode(outcomeRecord.mutation().canonicalBody());
        if (outcome.sideEffect() != 1
                || outcome.stableCode() != StableCode.OK
                || !Arrays.equals(outcome.publishAttemptId(), publishAttemptId)) {
            throw new IllegalStateException("Kafka Worker Publish Outcome was not a definitive PUBLISHED result");
        }
        final PublishEvidence publishEvidence = PublishEvidence.decode(outcome.evidence());
        if (publishEvidence.evidenceKind() != PublishEvidenceKind.KAFKA_TRANSACTIONAL_RECEIPT
                || publishEvidence.verificationStatus() != EvidenceVerificationStatus.VERIFIED_PUBLISHED) {
            throw new IllegalStateException("Kafka Worker Publish Outcome carried the wrong evidence branch");
        }
        publishEvidence.requireBusinessMutation(publishAttemptId, true);
        if (!(outcomeRecord.position() instanceof KafkaSourcePosition outcomePosition)) {
            throw new IllegalStateException("source-applied typed Publish Outcome has a non-Kafka source position");
        }
        final var finalMessage = delayShard.getMessage(physicalMessageId);
        final var openAttempt = delayShard.findOpenPublishAttempt(publishAttemptId);
        if (finalMessage == null || finalMessage.status() != MessageStatus.PUBLISHED || openAttempt != null) {
            final var appliedResult =
                    delayShard.getSystemMutationResult(outcomeRecord.mutation().systemMutationId());
            throw new IllegalStateException("source-applied typed Publish Outcome did not close the PUBLISHED attempt: "
                    + "messageStatus=" + (finalMessage == null ? "missing" : finalMessage.status())
                    + ", openAttemptState=" + (openAttempt == null ? "none" : openAttempt.state())
                    + ", applyStatus=" + (appliedResult == null ? "missing" : appliedResult.applyStatus())
                    + ", stableCode=" + (appliedResult == null ? "missing" : appliedResult.stableCode()));
        }
        requirePayload(
                bootstrap,
                clusterId,
                bridge.destinationPhysicalTopic(),
                bridge.destinationTopicId(),
                bridge.destinationPartition(),
                payload);
        bridge.requireDestinationResponseLossResolved(physicalResult);
        System.out.println("Kafka Worker source-applied physical publish passed: Admission source offset="
                + admissionPosition.offset() + ", typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset="
                + branchNumber(publishEvidence, 2) + ", Outcome source offset=" + outcomePosition.offset()
                + ", exact payload readback");
    }

    static void bindActiveOwnerPublishGraph(
            final WorkerShardRuntime runtime,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge) {
        bindActiveOwnerPublishGraph(
                runtime,
                ownedShard,
                ownerIdentity,
                authority,
                store,
                workClasses,
                verificationKey,
                bridge,
                1_000_000,
                null);
    }

    static void bindActiveOwnerPublishGraph(
            final WorkerShardRuntime runtime,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final long maxClaimBytes) {
        bindActiveOwnerPublishGraph(
                runtime,
                ownedShard,
                ownerIdentity,
                authority,
                store,
                workClasses,
                verificationKey,
                bridge,
                maxClaimBytes,
                null);
    }

    static void bindActiveOwnerPublishGraph(
            final WorkerShardRuntime runtime,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final long maxClaimBytes,
            final ClaimExecutionAdmission sharedClaimAdmission) {
        if (maxClaimBytes <= 0) {
            throw new IllegalArgumentException("maxClaimBytes must be positive");
        }
        final WorkerSchedulingRuntime scheduling = WorkerSchedulingRuntime.openForActiveOwnerFromTypedLanes(
                workClasses, ownedShard, authority, store, ownerIdentity, List.of(bridge.laneId()), 8);
        final ClaimExecutionAdmission permits =
                sharedClaimAdmission == null ? new ClaimExecutionAdmission(1, maxClaimBytes) : sharedClaimAdmission;
        permits.registerShard(new ClaimExecutionAdmission.ShardSpec(runtime.shardId(), 1, maxClaimBytes));
        permits.registerLane(new ClaimExecutionAdmission.LaneSpec(
                runtime.shardId(), bridge.laneId(), bridge.laneIncarnation(), 0, 0, 1, maxClaimBytes));
        permits.openReady(runtime.shardId(), bridge.laneId(), bridge.laneIncarnation());
        final ClaimHandoffWorkClassExecutor claimExecutor = new ClaimHandoffWorkClassExecutor(
                workClasses,
                ownedShard,
                authority,
                scheduling.scheduler(),
                permits,
                ignored -> ClaimHandoffWorkClassExecutor.PrerequisiteDecision.available());
        final PublishAdmissionWorkClassExecutor publishExecutor = new PublishAdmissionWorkClassExecutor(
                workClasses,
                ownedShard,
                authority,
                permits,
                bridge.appender(),
                ignored -> PublishAdmissionWorkClassExecutor.PrerequisiteDecision.available());
        final WorkerCommandRuntime commandRuntime =
                new WorkerCommandRuntime(workClasses, store.sharedResources(), claimExecutor, publishExecutor);
        final WorkerPublishPreparationCoordinator preparation =
                new WorkerPublishPreparationCoordinator(ownedShard, authority, System::currentTimeMillis, request -> {
                    final long expiry = Math.min(
                            request.claim().materialization().expireAtEpochMs(),
                            request.readyCertificate().validUntilEpochMs());
                    final long retryUntil = expiry - 1;
                    final long earliest = Math.max(
                            Math.max(
                                    System.currentTimeMillis(),
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
                            request.channel(),
                            request.readyCertificate(),
                            evidence(earliest, latest, "kafka-worker-provider-preparation"),
                            retryUntil,
                            1,
                            verificationKey.getPrivate(),
                            System::currentTimeMillis));
                });
        runtime.bindActiveOwnerPublishGraph(scheduling, commandRuntime, preparation);
    }

    static void waitForPhysicalCompletion(final WorkerPhysicalPublishExecutor.Submission submission) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (submission.state() == WorkerPhysicalPublishExecutor.SubmissionState.PENDING
                && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(25);
        }
        if (submission.state() != WorkerPhysicalPublishExecutor.SubmissionState.OUTCOME_HANDOFF_QUEUED) {
            final Throwable failure = submission.failure().orElse(null);
            throw new IllegalStateException(
                    "Kafka Worker physical submission did not reach Outcome handoff: " + submission.state() + "/"
                            + failure,
                    failure);
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
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey)
            throws Exception {
        return createPhysicalPublishBridge(
                bootstrap,
                clusterId,
                sourcePhysicalTopic,
                sourceTopicId,
                shard,
                physicalSchedulePosition,
                destinationPhysicalTopic,
                destinationTopicId,
                receiptPhysicalTopic,
                receiptTopicId,
                store,
                ownedShard,
                ownerIdentity,
                authority,
                workClasses,
                verificationKey,
                destinationProfile("worker-physical-publish"),
                capabilityProfile(),
                null,
                null,
                1_000_000);
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
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final DestinationLaneId requestedLaneId,
            final byte[] requestedLaneIncarnation,
            final long maxPhysicalBytes)
            throws Exception {
        return createPhysicalPublishBridge(
                bootstrap,
                clusterId,
                sourcePhysicalTopic,
                sourceTopicId,
                shard,
                physicalSchedulePosition,
                destinationPhysicalTopic,
                destinationTopicId,
                receiptPhysicalTopic,
                receiptTopicId,
                store,
                ownedShard,
                ownerIdentity,
                authority,
                workClasses,
                verificationKey,
                destinationProfile,
                capabilityProfile,
                requestedLaneId,
                requestedLaneIncarnation,
                maxPhysicalBytes,
                null,
                0);
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
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final DestinationLaneId requestedLaneId,
            final byte[] requestedLaneIncarnation,
            final long maxPhysicalBytes,
            final DestinationPhysicalAdmission sharedPhysicalAdmission,
            final int destinationPartition)
            throws Exception {
        return createPhysicalPublishBridge(
                bootstrap,
                clusterId,
                sourcePhysicalTopic,
                sourceTopicId,
                shard,
                physicalSchedulePosition,
                destinationPhysicalTopic,
                destinationTopicId,
                receiptPhysicalTopic,
                receiptTopicId,
                store,
                ownedShard,
                ownerIdentity,
                authority,
                workClasses,
                verificationKey,
                destinationProfile,
                capabilityProfile,
                requestedLaneId,
                requestedLaneIncarnation,
                maxPhysicalBytes,
                sharedPhysicalAdmission,
                destinationPartition,
                null);
    }

    /**
     * Creates a Kafka destination bridge while allowing the source Shard Log
     * append authority to be supplied by another guarded adapter. The target
     * transaction remains Kafka-native; only the common Delay Shard mutation
     * append is externalized for cross-adapter Worker graphs.
     */
    static PhysicalPublishBridge createPhysicalPublishBridge(
            final String bootstrap,
            final String clusterId,
            final String sourcePhysicalTopic,
            final UUID sourceTopicId,
            final ShardId shard,
            final com.nereusstream.delay.protocol.SourcePosition physicalSchedulePosition,
            final String destinationPhysicalTopic,
            final UUID destinationTopicId,
            final String receiptPhysicalTopic,
            final UUID receiptTopicId,
            final ShardStore store,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final DestinationLaneId requestedLaneId,
            final byte[] requestedLaneIncarnation,
            final long maxPhysicalBytes,
            final DestinationPhysicalAdmission sharedPhysicalAdmission,
            final int destinationPartition,
            final ShardLogMutationAppender suppliedAppender)
            throws Exception {
        final ProfileRef exactDestinationProfile = Objects.requireNonNull(destinationProfile, "destinationProfile");
        final ProfileRef exactCapabilityProfile = Objects.requireNonNull(capabilityProfile, "capabilityProfile");
        if (maxPhysicalBytes <= 0) {
            throw new IllegalArgumentException("maxPhysicalBytes must be positive");
        }
        if (destinationPartition < 0) {
            throw new IllegalArgumentException("destinationPartition must be non-negative");
        }
        final DestinationLaneId laneId;
        if (requestedLaneId == null) {
            final byte[] laneTuple = canonicalLaneTuple(
                    clusterId,
                    destinationTopicId,
                    destinationPhysicalTopic,
                    exactDestinationProfile,
                    exactCapabilityProfile,
                    destinationPartition);
            laneId = DestinationLaneId.derive(laneTuple);
        } else {
            laneId = requestedLaneId;
        }
        final byte[] laneIncarnation = requestedLaneIncarnation == null
                ? LaneRecord.initial(laneId, physicalSchedulePosition).laneIncarnation()
                : Bytes.copy(requestedLaneIncarnation);
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        final BrokerResourceIdentity target =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(clusterId, destinationTopicId));
        final BrokerResourceIdentity evidenceResource =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(clusterId, receiptTopicId));
        final KafkaTargetResource targetResource =
                new KafkaTargetResource(clusterId, destinationTopicId, destinationPartition);
        final KafkaReceiptResource receiptResource = new KafkaReceiptResource(
                clusterId, receiptTopicId, shard.routeIncarnation(), shard.partition(), 0, 1, 1, shard.partition());
        final String transactionalIdentity = "nereus-delay-kafka-worker-" + UUID.randomUUID();
        final byte[] transactionalIdentitySha256 = Bytes.sha256(Bytes.utf8(transactionalIdentity));
        final KafkaReceiptJournal journal = new KafkaReceiptJournal(shard, receiptResource);
        final boolean destinationResponseLossExpected = hasWorkerDestinationResponseLoss();
        final AtomicBoolean destinationResponseLossObserved = new AtomicBoolean();
        final KafkaProducer<byte[], byte[]> destinationProducer = new KafkaProducer<>(
                transactionalProducerConfiguration(bootstrap, transactionalIdentity, Math.toIntExact(maxPhysicalBytes)),
                new ByteArraySerializer(),
                new ByteArraySerializer());
        destinationProducer.initTransactions();
        final GuardedTransactionalProducer<byte[], byte[]> guardedDestinationProducer =
                (GuardedTransactionalProducer<byte[], byte[]>) destinationProducer;
        final KafkaClientArtifactTransactionalDestinationTransport transport =
                new KafkaClientArtifactTransactionalDestinationTransport(
                        destinationResponseLossExpected
                                ? destinationResponseLossProducer(
                                        guardedDestinationProducer, destinationResponseLossObserved)
                                : guardedDestinationProducer,
                        new KafkaClientArtifactTransactionalReceiptEvidenceProvider(
                                configuration(bootstrap, "kafka-worker-k2-evidence"), 1, Duration.ofMillis(250)));
        final KafkaTransactionalDestinationAdapter adapter = new KafkaTransactionalDestinationAdapter(
                targetResource,
                receiptResource,
                destinationPhysicalTopic,
                receiptPhysicalTopic,
                journal,
                laneId,
                laneIncarnation,
                transactionalIdentitySha256,
                transport);
        final DestinationPhysicalAdmission physicalAdmission = sharedPhysicalAdmission == null
                ? new DestinationPhysicalAdmission(1, maxPhysicalBytes)
                : sharedPhysicalAdmission;
        if (sharedPhysicalAdmission == null) {
            physicalAdmission.registerTargetCluster(clusterId, 1, maxPhysicalBytes);
        }
        physicalAdmission.registerLane(new DestinationPhysicalAdmission.LaneSpec(
                laneId, laneIncarnation, clusterId, 1, 1, 1, maxPhysicalBytes, 1, maxPhysicalBytes));
        physicalAdmission.openReady(laneId);
        final ShardLogMutationAppender appender;
        if (suppliedAppender == null) {
            final KafkaProducer<byte[], byte[]> mutationProducer = new KafkaProducer<>(
                    producerConfiguration(bootstrap, "nereus-delay-kafka-worker-mutation"),
                    new ByteArraySerializer(),
                    new ByteArraySerializer());
            appender = new KafkaClientArtifactShardLogMutationAppender(
                    (GuardedProducer<byte[], byte[]>) mutationProducer,
                    shard,
                    clusterId,
                    sourcePhysicalTopic,
                    sourceTopicId,
                    Duration.ofSeconds(20));
        } else {
            appender = suppliedAppender;
        }
        final AuthorIdentity author = AuthorIdentity.owner(
                ownerIdentity.deploymentId(),
                ownerIdentity.workerRunId(),
                ownerIdentity.ownerEpoch(),
                ownerIdentity.leaseFencingDigest());
        final WorkerPublishOutcomeMutationFactory outcomeFactory = new WorkerPublishOutcomeMutationFactory(
                (attempt, request, result) -> {
                    final PublishAdmissionBody admissionBody = PublishAdmissionBody.decode(attempt.admissionBytes());
                    final long retryDeadline =
                            attempt.hasRetryWindow() ? attempt.retryDeadlineEpochMs() : request.deliverAtEpochMs();
                    return new WorkerPublishOutcomeMutationFactory.OutcomeContext(
                            retryDeadline,
                            0,
                            admissionBody.chargeVector().canonicalBytes(),
                            evidence(
                                    result.brokerPersistenceTimeEpochMs(),
                                    result.brokerPersistenceTimeEpochMs(),
                                    "kafka-worker-publish-observed"),
                            retryDecision(
                                    admissionBody.decisionTime().latestEpochMs(), retryDeadline, attempt.attemptNo()));
                },
                author.canonicalBytes(),
                1,
                verificationKey.getPrivate());
        final OutcomeWorkClassExecutor outcomes =
                new OutcomeWorkClassExecutor(workClasses, ownedShard, authority, appender);
        final WorkerPhysicalPublishExecutor executor = new WorkerPhysicalPublishExecutor(
                adapter,
                physicalAdmission,
                workClasses,
                Runnable::run,
                outcomes,
                (attempt, request, ownerClock) -> WorkerPhysicalPublishExecutor.Decision.allowed(),
                outcomeFactory,
                ownedShard::fence);
        final byte[] attestationDigest = Bytes.sha256(
                Bytes.utf8("kafka-worker-channel-attestation"),
                target.canonicalBytes(),
                evidenceResource.canonicalBytes());
        final long now = Math.max(1, System.currentTimeMillis());
        final TrustedUtcIntervalEvidence issuedAt = evidence(Math.max(0, now - 1), now, "kafka-worker-channel-issued");
        final ChannelResourceIdentity channel = channel(
                laneId,
                laneIncarnation,
                target,
                evidenceResource,
                destinationPartition,
                transactionalIdentity,
                attestationDigest,
                issuedAt,
                exactDestinationProfile);
        final long validUntil = Math.addExact(now, 60_000);
        final EvidenceCursor cursor = EvidenceCursor.kafka(
                laneId.bytes(),
                laneIncarnation,
                uuidBytes(receiptTopicId),
                receiptResource.receiptPartition(),
                1,
                0,
                1,
                1);
        final ReadyCertificate readyCertificate = readyCertificate(
                ownerIdentity,
                store.metadata().storeIncarnation(),
                laneId,
                laneIncarnation,
                channel,
                target,
                cursor,
                issuedAt,
                validUntil);
        return new PhysicalPublishBridge(
                executor,
                appender,
                laneId,
                laneIncarnation,
                exactDestinationProfile,
                exactCapabilityProfile,
                target,
                channel,
                readyCertificate,
                List.of(cursor),
                destinationPhysicalTopic,
                destinationTopicId,
                destinationResponseLossExpected,
                destinationResponseLossObserved);
    }

    private static boolean hasWorkerDestinationResponseLoss() {
        return "1".equals(System.getenv("NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS"));
    }

    @SuppressWarnings("unchecked")
    private static GuardedTransactionalProducer<byte[], byte[]> destinationResponseLossProducer(
            final GuardedTransactionalProducer<byte[], byte[]> delegate, final AtomicBoolean responseLossObserved) {
        return (GuardedTransactionalProducer<byte[], byte[]>) Proxy.newProxyInstance(
                GuardedTransactionalProducer.class.getClassLoader(),
                new Class<?>[] {GuardedTransactionalProducer.class},
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

    private static ChannelResourceIdentity channel(
            final DestinationLaneId laneId,
            final byte[] laneIncarnation,
            final BrokerResourceIdentity target,
            final BrokerResourceIdentity evidenceResource,
            final long physicalPartition,
            final String transactionalIdentity,
            final byte[] attestationDigest,
            final TrustedUtcIntervalEvidence issuedAt,
            final ProfileRef destinationProfile) {
        final byte[] producer = Bytes.utf8(transactionalIdentity);
        final byte[] binding = Bytes.sha256(
                Bytes.utf8("kafka-worker-channel-binding"),
                target.canonicalBytes(),
                evidenceResource.canonicalBytes(),
                laneId.bytes(),
                laneIncarnation);
        final byte[] fingerprint = Bytes.sha256(
                Bytes.utf8("kafka-worker-channel-fingerprint"),
                producer,
                target.canonicalBytes(),
                evidenceResource.canonicalBytes());
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, AdapterKind.KAFKA.wireValue());
            CanonicalProtobuf.uint32(output, 2, ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT.wireValue());
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
        final CredentialUseLease lease = new CredentialUseLease(
                Objects.requireNonNull(destinationProfile, "destinationProfile"),
                CredentialUseKind.DESTINATION_CHANNEL,
                CredentialUseLease.destinationChannelHolderScope(prefix),
                1,
                binding,
                fingerprint,
                issuedAt,
                Math.addExact(issuedAt.latestEpochMs(), 60_000),
                1);
        return new ChannelResourceIdentity(
                AdapterKind.KAFKA,
                ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT,
                laneId.bytes(),
                laneIncarnation,
                target,
                physicalPartition,
                1,
                0,
                producer,
                Bytes.sha256(producer),
                evidenceResource,
                1L,
                attestationDigest,
                1,
                binding,
                fingerprint,
                lease);
    }

    private static ReadyCertificate readyCertificate(
            final OwnerIdentity owner,
            final byte[] storeIncarnation,
            final DestinationLaneId laneId,
            final byte[] laneIncarnation,
            final ChannelResourceIdentity channel,
            final BrokerResourceIdentity target,
            final EvidenceCursor cursor,
            final TrustedUtcIntervalEvidence issuedAt,
            final long validUntil) {
        final byte[] barrier = ActivationBarrier.kafka(target, (int) channel.physicalPartition(), 0, 0)
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
            CanonicalProtobuf.bytes(output, 16, Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate\0"), prefix));
        });
        return ReadyCertificate.decode(encoded);
    }

    private static byte[] canonicalLaneTuple(
            final String clusterId,
            final UUID topicId,
            final String physicalTopic,
            final ProfileRef destination,
            final ProfileRef capability) {
        return canonicalLaneTuple(clusterId, topicId, physicalTopic, destination, capability, 0);
    }

    static byte[] canonicalLaneTuple(
            final String clusterId,
            final UUID topicId,
            final String physicalTopic,
            final ProfileRef destination,
            final ProfileRef capability,
            final int physicalPartition) {
        if (physicalTopic == null || physicalTopic.isBlank()) {
            throw new IllegalArgumentException("Kafka physical topic must be nonblank");
        }
        if (physicalPartition < 0) {
            throw new IllegalArgumentException("Kafka physical partition must be non-negative");
        }
        final byte[] topicUuid = uuidBytes(topicId);
        return Bytes.concat(
                Bytes.sha256(Bytes.utf8("kafka-worker-tenant-routing-scope")),
                Bytes.u8(AdapterKind.KAFKA.wireValue()),
                Bytes.lp32(Bytes.utf8(clusterId)),
                Bytes.u8(1),
                topicUuid,
                Bytes.lp32(topicUuid),
                Bytes.u32be(physicalPartition),
                Bytes.lp32(destination.profileId()),
                Bytes.u64beBits(destination.version()),
                destination.semanticHash(),
                Bytes.lp32(capability.profileId()),
                Bytes.u64beBits(capability.version()),
                capability.semanticHash(),
                Bytes.u8(1),
                Bytes.sha256(Bytes.utf8("kafka-worker-ordering-domain")));
    }

    private static ProfileRef destinationProfile(final String identity) {
        return new ProfileRef(
                Bytes.utf8("destination-" + identity),
                1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)),
                ProfileKind.DESTINATION);
    }

    static ProfileRef capabilityProfile() {
        return new ProfileRef(
                Bytes.utf8("kafka-worker-capability"),
                1,
                Bytes.sha256(Bytes.utf8("kafka-worker-capability-semantic")),
                ProfileKind.DELIVERY_CAPABILITY);
    }

    private static byte[] zeroCharge() {
        return zeroChargeVector().canonicalBytes();
    }

    private static PublishAdmissionBody.ChargeVector zeroChargeVector() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static byte[] claimCharge(final long payloadBytes) {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 1, payloadBytes, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                .canonicalBytes();
    }

    private static byte[] retryDecision(
            final long firstAttemptAt, final long retryDeadline, final int completedAttemptNo) {
        final RetryPolicyRef policy = new RetryPolicyRef(
                Bytes.utf8("kafka-worker-retry"), 1, Bytes.sha256(Bytes.utf8("kafka-worker-retry-semantic")));
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

    private static TrustedUtcIntervalEvidence evidence(final long earliest, final long latest, final String identity) {
        return new TrustedUtcIntervalEvidence(
                earliest,
                latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8(identity),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8(identity + "-proof")),
                0,
                null);
    }

    private static void waitUntil(final long epochMs) throws Exception {
        while (System.currentTimeMillis() < epochMs) {
            TimeUnit.MILLISECONDS.sleep(Math.min(50, Math.max(1, epochMs - System.currentTimeMillis())));
        }
    }

    private static DelayMessageId messageId(final PreparedCommand command) {
        return command.delayMessageId();
    }

    private static long branchNumber(final PublishEvidence publishEvidence, final int number) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(publishEvidence.branch());
        while (reader.hasRemaining()) {
            final CanonicalProtobuf.Reader.Field field = reader.next();
            if (field.number() == number) {
                return field.unsignedValue();
            }
        }
        throw new IllegalStateException("Kafka transactional receipt branch is missing field " + number);
    }

    private static void requirePayload(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final byte[] expectedPayload) {
        requirePayload(bootstrap, clusterId, topic, topicId, 0, expectedPayload);
    }

    private static void requirePayload(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final int destinationPartition,
            final byte[] expectedPayload) {
        final ConsumerResourceGuard guard = new ConsumerResourceGuard(
                clusterId,
                topic,
                new Uuid(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits()),
                destinationPartition);
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                configuration(bootstrap, "kafka-worker-destination-readback-" + destinationPartition),
                clusterId,
                topic,
                topicId,
                destinationPartition);
        final TopicPartition partition = new TopicPartition(topic, destinationPartition);
        try {
            consumer.assign(List.of(partition));
            consumer.seek(partition, 0);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(Duration.ofMillis(250));
                final GuardedFetchEvidence fetchEvidence =
                        KafkaClientArtifactFetchEvidence.requireBatch(records, guard);
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
        return ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static void writeField(
            final java.io.ByteArrayOutputStream output, final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
        } else {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        }
    }

    private static Map<String, Object> producerConfiguration(final String bootstrap, final String clientId) {
        return producerConfiguration(bootstrap, clientId, 1_048_576);
    }

    private static Map<String, Object> producerConfiguration(
            final String bootstrap, final String clientId, final int maxRequestBytes) {
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

    private static Map<String, Object> transactionalProducerConfiguration(
            final String bootstrap, final String transactionalIdentity) {
        return transactionalProducerConfiguration(bootstrap, transactionalIdentity, 1_048_576);
    }

    private static Map<String, Object> transactionalProducerConfiguration(
            final String bootstrap, final String transactionalIdentity, final int maxRequestBytes) {
        final Map<String, Object> configuration =
                producerConfiguration(bootstrap, "nereus-delay-kafka-worker-k2", maxRequestBytes);
        configuration.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalIdentity);
        return configuration;
    }

    static final class PhysicalPublishBridge implements AutoCloseable {
        private final WorkerPhysicalPublishExecutor executor;
        private final ShardLogMutationAppender appender;
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final ProfileRef destinationProfile;
        private final ProfileRef capabilityProfile;
        private final BrokerResourceIdentity targetResource;
        private final ChannelResourceIdentity channel;
        private final ReadyCertificate readyCertificate;
        private final List<EvidenceCursor> evidenceCursors;
        private final String destinationPhysicalTopic;
        private final UUID destinationTopicId;
        private final boolean destinationResponseLossExpected;
        private final AtomicBoolean destinationResponseLossObserved;

        private PhysicalPublishBridge(
                final WorkerPhysicalPublishExecutor executor,
                final ShardLogMutationAppender appender,
                final DestinationLaneId laneId,
                final byte[] laneIncarnation,
                final ProfileRef destinationProfile,
                final ProfileRef capabilityProfile,
                final BrokerResourceIdentity targetResource,
                final ChannelResourceIdentity channel,
                final ReadyCertificate readyCertificate,
                final List<EvidenceCursor> evidenceCursors,
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

        ShardLogMutationAppender appender() {
            return appender;
        }

        DestinationLaneId laneId() {
            return laneId;
        }

        byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }

        private ProfileRef destinationProfile() {
            return destinationProfile;
        }

        private ProfileRef capabilityProfile() {
            return capabilityProfile;
        }

        private BrokerResourceIdentity targetResource() {
            return targetResource;
        }

        ChannelResourceIdentity channel() {
            return channel;
        }

        ReadyCertificate readyCertificate() {
            return readyCertificate;
        }

        List<EvidenceCursor> evidenceCursors() {
            return evidenceCursors;
        }

        String destinationPhysicalTopic() {
            return destinationPhysicalTopic;
        }

        UUID destinationTopicId() {
            return destinationTopicId;
        }

        int destinationPartition() {
            return (int) channel.physicalPartition();
        }

        void requireDestinationResponseLossResolved(final DestinationPublishResult result) {
            if (!destinationResponseLossExpected) {
                return;
            }
            if (!destinationResponseLossObserved.get()) {
                throw new IllegalStateException(
                        "Kafka Worker destination response-loss proxy did not discard a committed EndTxn response");
            }
            final PublishEvidence evidence = PublishEvidence.decode(result.evidence());
            if (evidence.evidenceKind() != PublishEvidenceKind.KAFKA_TRANSACTIONAL_RECEIPT
                    || evidence.verificationStatus() != EvidenceVerificationStatus.VERIFIED_PUBLISHED) {
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
            if (appender instanceof AutoCloseable resource) {
                try {
                    resource.close();
                } catch (Exception closeFailure) {
                    final RuntimeException runtimeFailure = closeFailure instanceof RuntimeException
                            ? (RuntimeException) closeFailure
                            : new IllegalStateException("Kafka Worker mutation appender close failed", closeFailure);
                    if (failure == null) {
                        failure = runtimeFailure;
                    } else {
                        failure.addSuppressed(runtimeFailure);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
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
            throw new IllegalStateException("Kafka Worker group offset mismatch: expected=" + expected + ", actual="
                    + (offset == null ? "missing" : offset.offset()));
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

    private static void requireExistingTopic(final Admin admin, final String topic) throws Exception {
        Exception lastFailure = null;
        for (int attempt = 0; attempt < 12; attempt++) {
            try {
                if (describe(admin, topic) != null) {
                    return;
                }
            } catch (Exception failure) {
                lastFailure = failure;
            }
            TimeUnit.MILLISECONDS.sleep(500);
        }
        throw new IllegalStateException(
                "resume topic metadata did not converge without creation: " + topic, lastFailure);
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
}
