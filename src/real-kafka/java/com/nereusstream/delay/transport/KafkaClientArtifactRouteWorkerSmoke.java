package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.OwnedDelayShard;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OwnerRecoveryCoordinator;
import com.nereusstream.delay.ownership.OwnerRecoveryTurn;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.ownership.OxiaSyncWorkerAssignmentBackend;
import com.nereusstream.delay.ownership.ReplayTurnBudget;
import com.nereusstream.delay.ownership.RouteWorkerAssignmentCoordinator;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.ownership.SourceAcknowledgement;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayCursor;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.WorkerAssignment;
import com.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import com.nereusstream.delay.ownership.WorkerAssignmentCoordinator;
import com.nereusstream.delay.ownership.WorkerShardFleetRuntime;
import com.nereusstream.delay.ownership.WorkerShardRuntime;
import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CompatibleControlSnapshot;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.IngressCredentialBindingRef;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.KafkaIngressRouteResource;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteLifecycle;
import com.nereusstream.delay.protocol.RoutePartitionPolicy;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.RoutingHashVersion;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.route.OxiaRouteAuthoritySession;
import com.nereusstream.delay.route.OxiaSignedRouteSnapshotProvider;
import com.nereusstream.delay.route.OxiaSignedRouteSnapshotPublisher;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.ScheduleResolver;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.RouteSelectionHint;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.GuardedConsumerRecords;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Real Kafka proof from guarded Fetch evidence to a signed Route and source
 * assignment. The Oxia authority is deliberately required: this smoke does
 * not substitute an in-memory Route or assignment authority for the durable
 * publication/acceptance boundary.
 */
public final class KafkaClientArtifactRouteWorkerSmoke {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(250);

    private KafkaClientArtifactRouteWorkerSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <route-topic>");
        }
        final String oxiaEndpoint = configuredRequired("NEREUS_DELAY_OXIA_ENDPOINT");
        if ("2".equals(configured("NEREUS_DELAY_KAFKA_ROUTE_WORKER_SHARDS", "1"))) {
            runMultiShard(arguments[0], arguments[1], oxiaEndpoint);
            return;
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
            final RouteIncarnation routeIncarnation = RouteIncarnation.random();
            final ShardId shard = new ShardId(routeIncarnation, 0);
            final PreparedCommand beforeRoute = command(shard, "route-before");
            final PreparedCommand afterRoute = command(shard, "route-after");
            final PreparedCommand afterFailover = command(shard, "route-after-failover");
            append(bootstrap, clusterId, topic, topicId, beforeRoute, 0);

            final GuardedFetchEvidence fetchEvidence = fetchEvidence(bootstrap, clusterId, topic, nativeTopicId, shard);
            final long barrierOffset = Math.addExact(fetchEvidence.lastRecordOffset(), 1);
            if (fetchEvidence.firstRecordOffset() != 0
                    || fetchEvidence.lastRecordOffset() != 0
                    || fetchEvidence.lastStableOffset() < barrierOffset) {
                throw new IllegalStateException(
                        "Kafka Fetch proof did not establish the one-record LSO barrier: " + fetchEvidence);
            }

            final KeyPair signingKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final RouteSelectionHint hint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
            final AuthenticatedTenantContext tenant =
                    new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
            final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
            final String routePrefix = "nereus-delay/kafka-route-worker/" + UUID.randomUUID();
            final String assignmentPrefix = "nereus-delay/kafka-route-worker-assignment/" + UUID.randomUUID();
            try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                            oxiaEndpoint,
                            namespace,
                            "nereus-delay-kafka-route-publisher-" + UUID.randomUUID(),
                            Duration.ofSeconds(15),
                            routePrefix);
                    OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                            oxiaEndpoint,
                            namespace,
                            "nereus-delay-kafka-route-provider-" + UUID.randomUUID(),
                            Duration.ofSeconds(15),
                            routePrefix);
                    OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle =
                            OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                    oxiaEndpoint,
                                    namespace,
                                    "nereus-delay-kafka-route-assignment-" + UUID.randomUUID(),
                                    Duration.ofSeconds(15),
                                    assignmentPrefix)) {
                final RouteSnapshot snapshot =
                        routeSnapshot(clusterId, topic, nativeTopicId, routeIncarnation, fetchEvidence, signingKeys);
                final OxiaSignedRouteSnapshotPublisher publisher =
                        new OxiaSignedRouteSnapshotPublisher(publisherSession, routePrefix, signingKeys.getPublic());
                final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                        providerSession, routePrefix, signingKeys.getPublic(), System::currentTimeMillis);
                publisher.publish(hint, snapshot, 0);
                provider.refresh().toCompletableFuture().join();

                final WorkerAssignmentAuthority assignmentAuthority =
                        new OxiaSyncWorkerAssignmentBackend(assignmentHandle, assignmentPrefix);
                final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(
                        provider,
                        new WorkerAssignmentCoordinator(
                                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)),
                                assignmentAuthority));
                final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement =
                        coordinator.placeActive(tenant, hint, placementRequest(System.currentTimeMillis()));
                final WorkerAssignment accepted = coordinator.requireAccepted(
                        tenant,
                        placement.publication().revision(),
                        placement.publication().assignment());
                requireRouteAssignment(accepted, snapshot, clusterId, nativeTopicId, barrierOffset);

                final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(assignmentHandle.backend());
                final OwnerLease lease = ownerAuthority
                        .acquire(
                                accepted.sourceAssignment(),
                                "kafka-route-worker",
                                assignmentHandle.sessionIdentity(),
                                System.currentTimeMillis(),
                                60_000)
                        .orElseThrow();
                final WorkClassExecutionRegistry workClasses = workClasses();
                final KeyPair verificationKey =
                        KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
                final CompatibleControlSnapshot controlSnapshot = controlSnapshot(shard);
                final Path root = Files.createTempDirectory("nereus-delay-kafka-route-worker-");
                boolean drained = false;
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
                                        Bytes.sha256(Bytes.utf8("kafka-route-worker-fencing"))));
                        recoverRouteRecord(
                                accepted,
                                ownerAuthority,
                                ownedShard,
                                firstRouteRecord(bootstrap, clusterId, topic, nativeTopicId, shard, beforeRoute),
                                verificationKey,
                                controlSnapshot,
                                workClasses);
                        if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                || !(ownedShard.lastCatchupPosition()
                                        instanceof com.nereusstream.delay.protocol.KafkaSourcePosition recovered)
                                || recovered.offset() != 0) {
                            throw new IllegalStateException(
                                    "Kafka Route Worker recovery did not apply the pre-Route record");
                        }
                        ackFirstRouteRecord(bootstrap, clusterId, topic, nativeTopicId, shard, beforeRoute, admin);

                        append(bootstrap, clusterId, topic, topicId, afterRoute, barrierOffset);
                        final String workerGroup = "nereus-delay-route-worker-" + UUID.randomUUID();
                        final GuardedConsumer<byte[], byte[]> workerConsumer =
                                workerConsumer(bootstrap, workerGroup, clusterId, topic, nativeTopicId, shard);
                        try {
                            final WorkerShardRuntime runtime = KafkaClientArtifactWorkerSourceFactory.create(
                                    workerConsumer,
                                    topic,
                                    POLL_TIMEOUT,
                                    accepted.sourceAssignment(),
                                    workClasses,
                                    ownedShard,
                                    store,
                                    resources,
                                    ownerAuthority,
                                    verificationKey.getPublic());
                            final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result =
                                    runUntilApplied(runtime);
                            if (result.status()
                                    != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                            .APPLIED_AND_ACKED) {
                                throw new IllegalStateException(
                                        "Kafka Route Worker active source did not ACK: " + result.status(),
                                        result.failure());
                            }
                            if (!(store.appliedShardLogPosition()
                                            instanceof com.nereusstream.delay.protocol.KafkaSourcePosition applied)
                                    || applied.offset() != barrierOffset
                                    || !applied.shardId().equals(shard)
                                    || !applied.authenticatedClusterId().equals(clusterId)
                                    || !applied.nativeTopicUuid().equals(nativeTopicId)) {
                                throw new IllegalStateException(
                                        "Kafka Route Worker Store did not persist the post-barrier position");
                            }
                            requireCommittedOffset(admin, workerGroup, topic, 0, barrierOffset + 1);
                            final boolean acceptedRouteFailover =
                                    configured("NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_GATE", "")
                                                    .length()
                                            > 0;
                            if (acceptedRouteFailover) {
                                awaitFailoverGate(
                                        Path.of(configuredRequired("NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_GATE")),
                                        placement.publication().revision(),
                                        barrierOffset);
                                append(bootstrap, clusterId, topic, topicId, afterFailover, barrierOffset + 1);
                                final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult
                                        failoverResult = runUntilApplied(runtime);
                                if (failoverResult.status()
                                        != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                                .APPLIED_AND_ACKED) {
                                    throw new IllegalStateException(
                                            "Kafka Route Worker did not recover after accepted-route failover: "
                                                    + failoverResult.status(),
                                            failoverResult.failure());
                                }
                                if (!(store.appliedShardLogPosition()
                                                instanceof
                                                com.nereusstream.delay.protocol.KafkaSourcePosition failoverPosition)
                                        || failoverPosition.offset() != barrierOffset + 1
                                        || !failoverPosition.shardId().equals(shard)
                                        || !failoverPosition
                                                .authenticatedClusterId()
                                                .equals(clusterId)
                                        || !failoverPosition.nativeTopicUuid().equals(nativeTopicId)) {
                                    throw new IllegalStateException(
                                            "Kafka Route Worker Store did not persist the post-failover position");
                                }
                                requireCommittedOffset(admin, workerGroup, topic, 0, barrierOffset + 2);
                            }
                            final Path checkpointPath = root.resolve("route-worker-final-checkpoint");
                            final byte[] checkpointId = java.util.Arrays.copyOf(
                                    Bytes.sha256(Bytes.utf8("kafka-route-worker-final-checkpoint")), 16);
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
                                    || !ownerAuthority.current(shard).isEmpty()) {
                                throw new IllegalStateException(
                                        "Kafka Route Worker drain did not publish the final checkpoint "
                                                + "or release the owner lease");
                            }
                            runtime.close();
                            if (!assignmentAuthority.withdraw(placement.publication())) {
                                throw new IllegalStateException(
                                        "Kafka Route Worker assignment was not withdrawn exactly");
                            }
                            drained = true;
                            provider.close();
                            System.out.println("Kafka signed Route -> guarded Fetch barrier -> Oxia Worker assignment "
                                    + "-> RocksDB apply/checkpoint smoke passed: fetch=v"
                                    + fetchEvidence.requestVersion() + ", lso=" + fetchEvidence.lastStableOffset()
                                    + ", routeRevision=" + placement.routeRevision() + ", assignmentRevision="
                                    + placement.publication().revision() + ", barrierOffset=" + barrierOffset
                                    + ", sourceOffset=" + (acceptedRouteFailover ? barrierOffset + 1 : barrierOffset)
                                    + ", commitSync ACK"
                                    + (acceptedRouteFailover ? ", accepted-route broker failover" : "")
                                    + ", final checkpoint");
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
    }

    /**
     * Real multi-shard Route/Assignment/Owner/Worker proof. The Route is
     * published once with two signed Kafka partition barriers; each barrier
     * then crosses its own Oxia Assignment CAS and Owner Lease before two
     * native guarded source consumers are admitted to one fair Worker fleet.
     */
    private static void runMultiShard(final String bootstrap, final String topicPrefix, final String oxiaEndpoint)
            throws Exception {
        final int shardCount = 2;
        final String topic = topicPrefix + "-" + UUID.randomUUID();
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                10_000);

        try (Admin admin = Admin.create(adminConfiguration)) {
            ensureTopic(admin, topic, shardCount);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final UUID nativeTopicId = toUuid(topicId);
            final RouteIncarnation routeIncarnation = RouteIncarnation.random();
            final List<RouteShardProbe> probes = new ArrayList<>(shardCount);
            for (int partition = 0; partition < shardCount; partition++) {
                final ShardId shard = new ShardId(routeIncarnation, partition);
                final PreparedCommand beforeRoute = command(shard, "route-multi-before-" + partition);
                final PreparedCommand afterRoute = command(shard, "route-multi-after-" + partition);
                append(bootstrap, clusterId, topic, topicId, beforeRoute, partition, 0);
                final GuardedFetchEvidence evidence = fetchEvidence(bootstrap, clusterId, topic, nativeTopicId, shard);
                final long barrierOffset = Math.addExact(evidence.lastRecordOffset(), 1);
                if (evidence.firstRecordOffset() != 0
                        || evidence.lastRecordOffset() != 0
                        || evidence.lastStableOffset() < barrierOffset) {
                    throw new IllegalStateException("Kafka multi-shard Fetch proof did not establish partition "
                            + partition + " LSO barrier: " + evidence);
                }
                probes.add(new RouteShardProbe(shard, beforeRoute, afterRoute, evidence, barrierOffset));
            }

            final KeyPair signingKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final RouteSelectionHint hint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
            final AuthenticatedTenantContext tenant =
                    new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
            final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
            final String routePrefix = "nereus-delay/kafka-route-worker-multi/" + UUID.randomUUID();
            final String assignmentPrefix = "nereus-delay/kafka-route-worker-multi-assignment/" + UUID.randomUUID();
            try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                            oxiaEndpoint,
                            namespace,
                            "nereus-delay-kafka-route-multi-publisher-" + UUID.randomUUID(),
                            Duration.ofSeconds(15),
                            routePrefix);
                    OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                            oxiaEndpoint,
                            namespace,
                            "nereus-delay-kafka-route-multi-provider-" + UUID.randomUUID(),
                            Duration.ofSeconds(15),
                            routePrefix);
                    OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle =
                            OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                    oxiaEndpoint,
                                    namespace,
                                    "nereus-delay-kafka-route-multi-assignment-" + UUID.randomUUID(),
                                    Duration.ofSeconds(15),
                                    assignmentPrefix)) {
                final RouteSnapshot snapshot =
                        multiRouteSnapshot(clusterId, topic, nativeTopicId, routeIncarnation, probes, signingKeys);
                final OxiaSignedRouteSnapshotPublisher publisher =
                        new OxiaSignedRouteSnapshotPublisher(publisherSession, routePrefix, signingKeys.getPublic());
                final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                        providerSession, routePrefix, signingKeys.getPublic(), System::currentTimeMillis);
                final long routeRevision = publisher.publish(hint, snapshot, 0).revision();
                provider.refresh().toCompletableFuture().join();

                final WorkerAssignmentAuthority assignmentAuthority =
                        new OxiaSyncWorkerAssignmentBackend(assignmentHandle, assignmentPrefix);
                final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(
                        provider,
                        new WorkerAssignmentCoordinator(
                                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)),
                                assignmentAuthority));
                final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(assignmentHandle.backend());
                final List<MultiShardAdmission> admissions = new ArrayList<>(shardCount);
                final Set<String> assignedWorkers = new HashSet<>();
                for (RouteShardProbe probe : probes) {
                    final String expectedWorker =
                            "kafka-route-worker-" + (char) ('a' + probe.shard().partition());
                    final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement = coordinator.placeActive(
                            tenant,
                            hint,
                            placementRequest(
                                    System.currentTimeMillis(), probe.shard().partition(), expectedWorker));
                    final WorkerAssignment accepted = coordinator.requireAccepted(
                            tenant,
                            placement.publication().revision(),
                            placement.publication().assignment());
                    requireRouteAssignment(accepted, snapshot, clusterId, nativeTopicId, probe.barrierOffset());
                    if (!expectedWorker.equals(accepted.workerId())) {
                        throw new IllegalStateException("Kafka multi-shard placement selected unexpected Worker: "
                                + "partition=" + probe.shard().partition() + ", expected=" + expectedWorker
                                + ", actual=" + accepted.workerId());
                    }
                    final OwnerLease lease = ownerAuthority
                            .acquire(
                                    accepted.sourceAssignment(),
                                    expectedWorker,
                                    assignmentHandle.sessionIdentity(),
                                    System.currentTimeMillis(),
                                    60_000)
                            .orElseThrow();
                    if (!assignedWorkers.add(accepted.workerId())) {
                        throw new IllegalStateException(
                                "Kafka multi-shard placement reused a Worker identity: " + accepted.workerId());
                    }
                    admissions.add(new MultiShardAdmission(probe, placement.publication(), accepted, lease));
                }
                if (assignedWorkers.size() != shardCount) {
                    throw new IllegalStateException("Kafka multi-shard placement did not span two Worker identities");
                }

                final WorkClassExecutionRegistry workClasses = workClasses();
                final KeyPair verificationKey =
                        KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
                final Path root = Files.createTempDirectory("nereus-delay-kafka-route-worker-multi-");
                final List<ShardStore> stores = new ArrayList<>(shardCount);
                final List<WorkerShardRuntime> runtimes = new ArrayList<>(shardCount);
                final List<String> workerGroups = new ArrayList<>(shardCount);
                WorkerShardFleetRuntime fleet = null;
                boolean drained = false;
                try {
                    final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
                    final SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                    try {
                        resources.bindWorkClassExecutionRegistry(workClasses);
                        for (int index = 0; index < admissions.size(); index++) {
                            final MultiShardAdmission admission = admissions.get(index);
                            final ShardId shard = admission.probe().shard();
                            final ShardStore store = ShardStore.open(storeConfig, shard, resources);
                            stores.add(store);
                            final CompatibleControlSnapshot controlSnapshot = controlSnapshot(shard);
                            store.recordControlSnapshot(controlSnapshot);
                            final DelayShard delayShard =
                                    new DelayShard(store, DelayShardConfig.defaults(), null, null, scheduleResolver());
                            final OwnedDelayShard ownedShard = new OwnedDelayShard(
                                    delayShard,
                                    admission.lease(),
                                    new OwnerIdentity(
                                            bytes(16, 70 + index),
                                            bytes(16, 90 + index),
                                            admission.lease().ownerEpoch(),
                                            Bytes.sha256(Bytes.utf8("kafka-route-worker-multi-fencing-" + index))));
                            recoverRouteRecord(
                                    admission.assignment(),
                                    ownerAuthority,
                                    ownedShard,
                                    firstRouteRecord(
                                            bootstrap,
                                            clusterId,
                                            topic,
                                            nativeTopicId,
                                            shard,
                                            admission.probe().beforeRoute()),
                                    verificationKey,
                                    controlSnapshot,
                                    workClasses);
                            if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                    || !(ownedShard.lastCatchupPosition()
                                            instanceof com.nereusstream.delay.protocol.KafkaSourcePosition recovered)
                                    || recovered.offset() != 0
                                    || !recovered.shardId().equals(shard)) {
                                throw new IllegalStateException("Kafka multi-shard recovery did not apply partition "
                                        + shard.partition() + " pre-Route record");
                            }
                            ackFirstRouteRecord(
                                    bootstrap,
                                    clusterId,
                                    topic,
                                    nativeTopicId,
                                    shard,
                                    admission.probe().beforeRoute(),
                                    admin);
                            append(
                                    bootstrap,
                                    clusterId,
                                    topic,
                                    topicId,
                                    admission.probe().afterRoute(),
                                    shard.partition(),
                                    admission.probe().barrierOffset());
                            final String workerGroup =
                                    "nereus-delay-route-worker-multi-" + shard.partition() + "-" + UUID.randomUUID();
                            workerGroups.add(workerGroup);
                            final GuardedConsumer<byte[], byte[]> workerConsumer =
                                    workerConsumer(bootstrap, workerGroup, clusterId, topic, nativeTopicId, shard);
                            runtimes.add(KafkaClientArtifactWorkerSourceFactory.create(
                                    workerConsumer,
                                    topic,
                                    POLL_TIMEOUT,
                                    admission.assignment().sourceAssignment(),
                                    workClasses,
                                    ownedShard,
                                    store,
                                    resources,
                                    ownerAuthority,
                                    verificationKey.getPublic()));
                        }
                        fleet = new WorkerShardFleetRuntime(workClasses, resources, runtimes);
                        final Set<ShardId> pending = new HashSet<>(fleet.shardIds());
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                        while (!pending.isEmpty() && System.nanoTime() < deadline) {
                            final WorkerShardFleetRuntime.SourceTurn turn = fleet.runNextSourceTurn(
                                    new SchedulerBudget(1, 1_000_000, TimeUnit.SECONDS.toNanos(2)),
                                    System::currentTimeMillis);
                            if (turn.result().status()
                                    == com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                            .APPLIED_AND_ACKED) {
                                pending.remove(turn.shardId());
                            } else if (turn.result().status()
                                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                                    .WAITING_FOR_SOURCE
                                    && turn.result().status()
                                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                                    .WAITING_FOR_WORK_CLASS) {
                                throw new IllegalStateException(
                                        "Kafka multi-shard Worker source turn failed: shard=" + turn.shardId()
                                                + ", status=" + turn.result().status(),
                                        turn.result().failure());
                            }
                        }
                        if (!pending.isEmpty()) {
                            throw new IllegalStateException(
                                    "Kafka multi-shard Worker source apply timed out: " + pending);
                        }
                        for (int index = 0; index < admissions.size(); index++) {
                            final MultiShardAdmission admission = admissions.get(index);
                            final ShardId shard = admission.probe().shard();
                            final ShardStore store = stores.get(index);
                            if (!(store.appliedShardLogPosition()
                                            instanceof com.nereusstream.delay.protocol.KafkaSourcePosition applied)
                                    || applied.offset() != admission.probe().barrierOffset()
                                    || !applied.shardId().equals(shard)
                                    || !applied.authenticatedClusterId().equals(clusterId)
                                    || !applied.nativeTopicUuid().equals(nativeTopicId)) {
                                throw new IllegalStateException("Kafka multi-shard Store did not persist partition "
                                        + shard.partition() + " post-barrier position");
                            }
                            requireCommittedOffset(
                                    admin,
                                    workerGroups.get(index),
                                    topic,
                                    shard.partition(),
                                    admission.probe().barrierOffset() + 1);
                        }
                        for (int index = 0; index < runtimes.size(); index++) {
                            final MultiShardAdmission admission = admissions.get(index);
                            final ShardId shard = admission.probe().shard();
                            final Path checkpointPath =
                                    root.resolve("route-worker-multi-final-checkpoint-" + shard.partition());
                            final byte[] checkpointId = java.util.Arrays.copyOf(
                                    Bytes.sha256(Bytes.utf8(
                                            "kafka-route-worker-multi-final-checkpoint-" + shard.partition())),
                                    16);
                            final var drain = runtimes.get(index)
                                    .drain(
                                            new com.nereusstream.delay.ownership.OwnerDrainCoordinator.DrainRequest(
                                                    System.currentTimeMillis() + 30_000,
                                                    0,
                                                    checkpointPath,
                                                    checkpointId),
                                            System::currentTimeMillis,
                                            () -> {});
                            if (drain.pendingCheckpointTask() != null
                                    || drain.finalCheckpointPath() == null
                                    || !Files.isDirectory(checkpointPath)
                                    || CheckpointFileInventory.collect(checkpointPath)
                                            .isEmpty()
                                    || !ownerAuthority.current(shard).isEmpty()) {
                                throw new IllegalStateException("Kafka multi-shard drain did not complete partition "
                                        + shard.partition() + " checkpoint/lease release");
                            }
                        }
                        fleet.close();
                        for (MultiShardAdmission admission : admissions) {
                            if (!assignmentAuthority.withdraw(admission.publication())) {
                                throw new IllegalStateException(
                                        "Kafka multi-shard assignment was not withdrawn exactly: "
                                                + admission.probe().shard());
                            }
                        }
                        drained = true;
                        provider.close();
                        System.out.println("Kafka signed Route -> two guarded Fetch barriers -> Oxia multi-shard "
                                + "Assignment/Owner -> one Worker fleet -> RocksDB apply/ACK/checkpoint smoke "
                                + "passed: fetchPartitions=" + shardCount + ", routeRevision="
                                + routeRevision + ", assignmentRevisions="
                                + admissions.stream()
                                        .map(admission ->
                                                admission.publication().revision())
                                        .toList()
                                + ", workers=" + assignedWorkers + ", sourceBarriers="
                                + probes.stream()
                                        .map(RouteShardProbe::barrierOffset)
                                        .toList());
                    } finally {
                        if (fleet != null && !drained) {
                            try {
                                fleet.close();
                            } catch (RuntimeException | Error ignored) {
                                // Preserve the primary multi-shard failure; the
                                // process-scoped Oxia session still fences the
                                // temporary authority on teardown.
                            }
                        }
                        for (ShardStore store : stores) {
                            try {
                                store.close();
                            } catch (RuntimeException | Error ignored) {
                                // The normal successful path closes Stores via
                                // owner drain; retries remain bounded to this
                                // short-lived E2E process.
                            }
                        }
                        resources.close();
                    }
                } finally {
                    deleteTree(root);
                }
            }
        }
    }

    private static GuardedFetchEvidence fetchEvidence(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard) {
        final String groupId = "nereus-delay-route-barrier-" + UUID.randomUUID();
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, groupId), clusterId, topic, topicId, shard.partition());
        final TopicPartition topicPartition = new TopicPartition(topic, shard.partition());
        final ConsumerResourceGuard guard = new ConsumerResourceGuard(
                clusterId,
                topic,
                new Uuid(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits()),
                shard.partition());
        try {
            consumer.assign(List.of(topicPartition));
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(POLL_TIMEOUT);
                final GuardedFetchEvidence evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, guard);
                if (evidence != null) {
                    return evidence;
                }
            }
            throw new IllegalStateException("Kafka guarded Fetch evidence did not become visible");
        } finally {
            consumer.close();
        }
    }

    private static SourceReplayEntry firstRouteRecord(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard,
            final PreparedCommand expected) {
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, "nereus-delay-route-recovery-" + UUID.randomUUID()),
                clusterId,
                topic,
                topicId,
                shard.partition());
        try (KafkaClientArtifactRecoverySourceCursor cursor = new KafkaClientArtifactRecoverySourceCursor(
                consumer,
                new SourceAssignment(
                        shard,
                        Bytes.sha256(Bytes.utf8("route-recovery-assignment")),
                        1,
                        new KafkaActivationBarrier(shard, clusterId, topicId, 1)),
                topic,
                0,
                POLL_TIMEOUT)) {
            if (!cursor.hasNext()) {
                throw new IllegalStateException("Kafka Route recovery record did not become visible");
            }
            final SourceReplayEntry entry = cursor.next();
            if (!(entry instanceof SourceReplayRecord record)
                    || !expected.equals(record.command())
                    || !(record.position() instanceof com.nereusstream.delay.protocol.KafkaSourcePosition position)
                    || position.offset() != 0) {
                throw new IllegalStateException("Kafka Route recovery returned an unexpected pre-Route record");
            }
            return entry;
        }
    }

    private static void recoverRouteRecord(
            final WorkerAssignment accepted,
            final OxiaOwnerLeaseStore authority,
            final OwnedDelayShard ownedShard,
            final SourceReplayEntry entry,
            final KeyPair verificationKey,
            final CompatibleControlSnapshot controlSnapshot,
            final WorkClassExecutionRegistry workClasses) {
        final SourceReplayCursor<SourceReplayEntry> cursor =
                SourceReplayCursor.of(List.of(entry).iterator());
        final OwnerRecoveryCoordinator recovery = new OwnerRecoveryCoordinator(
                ownedShard,
                authority,
                accepted.sourceAssignment(),
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
            throw new IllegalStateException("Kafka Route Worker recovery did not apply exactly one record");
        }
    }

    private static void ackFirstRouteRecord(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard,
            final PreparedCommand expected,
            final Admin admin)
            throws Exception {
        final String groupId = "nereus-delay-route-first-ack-" + UUID.randomUUID();
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, groupId), clusterId, topic, topicId, shard.partition());
        final KafkaClientArtifactSourceRecordConsumer source =
                new KafkaClientArtifactSourceRecordConsumer(consumer, clusterId, topicId, shard, topic, POLL_TIMEOUT);
        try {
            consumer.seek(new TopicPartition(topic, shard.partition()), 0);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                final Optional<SourceRecordConsumer.PolledSourceRecord> polled = source.poll();
                if (polled.isEmpty()) {
                    continue;
                }
                if (!(polled.get().entry() instanceof SourceReplayRecord record)
                        || !expected.equals(record.command())
                        || !(record.position() instanceof com.nereusstream.delay.protocol.KafkaSourcePosition position)
                        || position.offset() != 0) {
                    throw new IllegalStateException("Kafka Route first ACK returned an unexpected record");
                }
                requireAcked(
                        polled.get().acknowledgement().acknowledge(polled.get().entry(), null));
                requireCommittedOffset(admin, groupId, topic, shard.partition(), 1);
                return;
            }
            throw new IllegalStateException("Kafka Route first ACK record did not become visible");
        } finally {
            source.close();
        }
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
                        "Kafka Route Worker source turn failed: " + result.status(), result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Kafka Route Worker source record did not become visible");
    }

    private static void awaitFailoverGate(final Path gate, final long assignmentRevision, final long sourceOffset)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        System.out.println("Kafka Route Worker ready for accepted-route failover: assignmentRevision="
                + assignmentRevision + ", sourceOffset=" + sourceOffset + ", gate=" + gate);
        while (!Files.exists(gate) && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(50);
        }
        if (!Files.exists(gate)) {
            throw new IllegalStateException("accepted-route failover gate was not released: " + gate);
        }
    }

    private static GuardedConsumer<byte[], byte[]> workerConsumer(
            final String bootstrap,
            final String groupId,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard) {
        return KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, groupId), clusterId, topic, topicId, shard.partition());
    }

    private static ScheduleResolver scheduleResolver() {
        final byte[] tuple = Bytes.utf8("kafka-route-worker-canonical-lane-tuple");
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
        final java.util.EnumMap<WorkClass, WorkClassPolicy> policies = new java.util.EnumMap<>(WorkClass.class);
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

    private static SourceObservation pollAfterBarrier(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard,
            final long barrierOffset,
            final PreparedCommand expected) {
        final String groupId = "nereus-delay-route-source-" + UUID.randomUUID();
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, groupId), clusterId, topic, topicId, shard.partition());
        final KafkaClientArtifactSourceRecordConsumer source =
                new KafkaClientArtifactSourceRecordConsumer(consumer, clusterId, topicId, shard, topic, POLL_TIMEOUT);
        try {
            consumer.seek(new TopicPartition(topic, shard.partition()), barrierOffset);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                final Optional<SourceRecordConsumer.PolledSourceRecord> polled = source.poll();
                if (polled.isEmpty()) {
                    continue;
                }
                if (!(polled.get().entry() instanceof SourceReplayRecord replay)
                        || !expected.equals(replay.command())
                        || !(replay.position() instanceof com.nereusstream.delay.protocol.KafkaSourcePosition position)
                        || position.offset() != barrierOffset) {
                    throw new IllegalStateException("Kafka source did not start at the signed Route barrier");
                }
                requireAcked(
                        polled.get().acknowledgement().acknowledge(polled.get().entry(), null));
                return new SourceObservation(groupId, polled.get(), position);
            }
            throw new IllegalStateException("Kafka source record after Route barrier did not become visible");
        } catch (RuntimeException | Error failure) {
            source.close();
            throw failure;
        } finally {
            source.close();
        }
    }

    private static RouteSnapshot multiRouteSnapshot(
            final String clusterId,
            final String topic,
            final UUID topicId,
            final RouteIncarnation incarnation,
            final List<RouteShardProbe> probes,
            final KeyPair signingKeys) {
        final long now = System.currentTimeMillis();
        final BrokerResourceIdentity broker =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(clusterId, topicId));
        final List<RoutePartitionPolicy> policies = probes.stream()
                .map(probe -> {
                    final GuardedFetchEvidence evidence = probe.fetchEvidence();
                    final byte[] guardDigest = Bytes.sha256(
                            Bytes.utf8("nereus-delay-kafka-route-fetch-proof\0"), evidence.fetchResponseBodySha256());
                    return new RoutePartitionPolicy(
                            probe.shard().partition(),
                            ActivationBarrier.kafka(
                                    broker,
                                    probe.shard().partition(),
                                    probe.barrierOffset(),
                                    evidence.lastStableOffset()),
                            zeroQuota(),
                            1,
                            guardDigest);
                })
                .toList();
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                now - 100,
                now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("kafka-route-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("kafka-route-issued-at")),
                0,
                null);
        return RouteSnapshot.create(
                incarnation,
                bytes(32, 1),
                bytes(32, 2),
                RouteLifecycle.ACTIVE_FOR_NEW,
                now + 30_000,
                new KafkaIngressRouteResource(clusterId, topic, topicId, probes.size()),
                RoutingHashVersion.ROUTING_HASH,
                new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1),
                1,
                policies,
                100,
                200,
                1024,
                4096,
                10,
                8192,
                500,
                now - 1_000,
                now + 60_000,
                new IngressCredentialBindingRef(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                Bytes.sha256(Bytes.utf8("kafka-route-prerequisite")),
                issuedAt,
                1,
                signingKeys.getPrivate());
    }

    private static RouteSnapshot routeSnapshot(
            final String clusterId,
            final String topic,
            final UUID topicId,
            final RouteIncarnation incarnation,
            final GuardedFetchEvidence evidence,
            final KeyPair signingKeys) {
        final long now = System.currentTimeMillis();
        final BrokerResourceIdentity broker =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(clusterId, topicId));
        final long nextOffsetExclusive = Math.addExact(evidence.lastRecordOffset(), 1);
        final byte[] guardDigest =
                Bytes.sha256(Bytes.utf8("nereus-delay-kafka-route-fetch-proof\0"), evidence.fetchResponseBodySha256());
        final RoutePartitionPolicy policy = new RoutePartitionPolicy(
                0,
                ActivationBarrier.kafka(broker, 0, nextOffsetExclusive, evidence.lastStableOffset()),
                zeroQuota(),
                1,
                guardDigest);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                now - 100,
                now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("kafka-route-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("kafka-route-issued-at")),
                0,
                null);
        return RouteSnapshot.create(
                incarnation,
                bytes(32, 1),
                bytes(32, 2),
                RouteLifecycle.ACTIVE_FOR_NEW,
                now + 30_000,
                new KafkaIngressRouteResource(clusterId, topic, topicId, 1),
                RoutingHashVersion.ROUTING_HASH,
                new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1),
                1,
                List.of(policy),
                100,
                200,
                1024,
                4096,
                10,
                8192,
                500,
                now - 1_000,
                now + 60_000,
                new IngressCredentialBindingRef(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                Bytes.sha256(Bytes.utf8("kafka-route-prerequisite")),
                issuedAt,
                1,
                signingKeys.getPrivate());
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest placementRequest(final long now) {
        return placementRequest(now, 0, "kafka-route-worker");
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest placementRequest(
            final long now, final int partition, final String workerId) {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(
                partition,
                Bytes.sha256(Bytes.utf8("kafka-route-worker-assignment-" + partition + "-" + workerId)),
                1,
                Bytes.sha256(Bytes.utf8("kafka-route-worker-capacity-" + partition + "-" + workerId)),
                1,
                List.of(new WorkerPlacementPolicy.WorkerCandidate(
                        workerId,
                        capacity(2),
                        CapacityVector.empty(),
                        0,
                        16,
                        0,
                        16,
                        WorkerLoadVector.empty(),
                        WorkerLoadVector.empty(),
                        now,
                        true,
                        0)),
                capacity(1),
                CapacityVector.empty(),
                CapacityVector.empty(),
                null,
                now,
                0,
                0);
    }

    private static void requireRouteAssignment(
            final WorkerAssignment assignment,
            final RouteSnapshot snapshot,
            final String clusterId,
            final UUID topicId,
            final long barrierOffset) {
        if (!assignment.routeBound()
                || !java.util.Arrays.equals(snapshot.snapshotDigest(), assignment.routeSnapshotDigest())
                || !(assignment.sourceAssignment().activationBarrier() instanceof KafkaActivationBarrier barrier)
                || !clusterId.equals(barrier.authenticatedClusterId())
                || !topicId.equals(barrier.nativeTopicUuid())
                || barrier.exclusiveOffset() != barrierOffset) {
            throw new IllegalStateException("Oxia Worker assignment did not retain the signed Kafka Route barrier");
        }
    }

    private static void append(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final Uuid topicId,
            final PreparedCommand command,
            final long expectedOffset)
            throws Exception {
        append(bootstrap, clusterId, topic, topicId, command, 0, expectedOffset);
    }

    private static void append(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final Uuid topicId,
            final PreparedCommand command,
            final int partition,
            final long expectedOffset)
            throws Exception {
        if (partition < 0) {
            throw new IllegalArgumentException("Kafka Route append partition must be non-negative");
        }
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(configuration, new ByteArraySerializer(), new ByteArraySerializer())) {
            final GuardedProducer<byte[], byte[]> guarded = (GuardedProducer<byte[], byte[]>) producer;
            final var metadata = guarded.sendGuarded(
                            new ProducerRecord<>(
                                    topic,
                                    partition,
                                    null,
                                    com.nereusstream.delay.protocol.CommandCodec.encodeManagedFrame(command)),
                            new org.apache.kafka.clients.producer.ProducerResourceGuard(
                                    clusterId, topic, topicId, partition))
                    .get(10, TimeUnit.SECONDS);
            if (metadata.recordMetadata().offset() != expectedOffset) {
                throw new IllegalStateException("Kafka Route smoke append offset mismatch: expected=" + expectedOffset
                        + ", actual=" + metadata.recordMetadata().offset());
            }
        }
    }

    private static Map<String, Object> consumerConfiguration(final String bootstrap, final String groupId) {
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

    private static PreparedCommand command(final ShardId shard, final String identity) {
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("destination-" + identity),
                1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)),
                ProfileKind.DESTINATION);
        final RetryPolicyRef retryPolicy = new RetryPolicyRef(
                Bytes.utf8("retry-" + identity), 1, Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final com.nereusstream.delay.protocol.CanonicalScheduleIntent intent =
                com.nereusstream.delay.protocol.CanonicalScheduleIntent.create(
                        destination,
                        retryPolicy,
                        deliverAt,
                        deliverAt + 10_000,
                        DeliveryMode.MANAGED,
                        OrderingMode.BEST_EFFORT,
                        new byte[0],
                        Bytes.utf8("source-" + identity),
                        null,
                        AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                        null,
                        null);
        return PreparedCommand.schedule(shard, intent, deliverAt + 20_000);
    }

    private static QuotaGrantRef zeroQuota() {
        return new QuotaGrantRef(
                bytes(32, 20),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    }

    private static CapacityVector capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[CapacityDimension.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVector(values);
    }

    private static void requireAcked(final SourceAcknowledgement.AcknowledgementResult result) {
        if (result.disposition() != SourceAcknowledgement.Disposition.ACKED) {
            throw new IllegalStateException(
                    "Kafka Route source record was not ACKED: " + result.disposition(), result.failure());
        }
    }

    private static void requireCommittedOffset(
            final Admin admin, final String groupId, final String topic, final int partition, final long expected)
            throws Exception {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final OffsetAndMetadata offset = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS)
                .get(topicPartition);
        if (offset == null || offset.offset() != expected) {
            throw new IllegalStateException("Kafka Route source group offset mismatch: expected=" + expected
                    + ", actual=" + (offset == null ? "missing" : offset.offset()));
        }
    }

    private static void ensureTopic(final Admin admin, final String topic) throws Exception {
        ensureTopic(admin, topic, 1);
    }

    private static void ensureTopic(final Admin admin, final String topic, final int partitions) throws Exception {
        if (partitions <= 0) {
            throw new IllegalArgumentException("Kafka Route topic partition count must be positive");
        }
        try {
            final TopicDescription existing = describe(admin, topic);
            if (existing != null) {
                if (existing.partitions().size() != partitions) {
                    throw new IllegalStateException("Kafka Route topic partition count mismatch: expected=" + partitions
                            + ", actual=" + existing.partitions().size());
                }
                return;
            }
        } catch (Exception missing) {
            // Create below.
        }
        final NewTopic newTopic = new NewTopic(topic, partitions, (short) 3);
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
        throw new IllegalStateException("Kafka Route topic metadata did not converge");
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

    private static String configuredRequired(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required for the real Route authority smoke");
        }
        return value;
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
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

    private record SourceObservation(
            String groupId,
            SourceRecordConsumer.PolledSourceRecord record,
            com.nereusstream.delay.protocol.KafkaSourcePosition position) {}

    private record RouteShardProbe(
            ShardId shard,
            PreparedCommand beforeRoute,
            PreparedCommand afterRoute,
            GuardedFetchEvidence fetchEvidence,
            long barrierOffset) {}

    private record MultiShardAdmission(
            RouteShardProbe probe,
            WorkerAssignmentAuthority.Publication publication,
            WorkerAssignment assignment,
            OwnerLease lease) {}
}
