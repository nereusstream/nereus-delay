package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PulsarSendRequest;
import com.nereusstream.delay.adapter.PulsarSendResult;
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
import com.nereusstream.delay.protocol.ActivationBarrierV1;
import com.nereusstream.delay.protocol.AdapterKindV1;
import com.nereusstream.delay.protocol.AdapterMetadataV1;
import com.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimensionV1;
import com.nereusstream.delay.protocol.CapacityVectorV1;
import com.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.IngressCredentialBindingRefV1;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentityV1;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKindV1;
import com.nereusstream.delay.protocol.ProfileRefV1;
import com.nereusstream.delay.protocol.ProtocolTupleV1;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.PulsarIngressRouteResourceV1;
import com.nereusstream.delay.protocol.PulsarMetadataV1;
import com.nereusstream.delay.protocol.PulsarPhysicalPartitionIdentityV1;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.QuotaGrantRefV1;
import com.nereusstream.delay.protocol.RetryPolicyRefV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteLifecycleV1;
import com.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import com.nereusstream.delay.protocol.RouteSnapshotV1;
import com.nereusstream.delay.protocol.RoutingHashVersionV1;
import com.nereusstream.delay.protocol.ScheduleIntentV1;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubjectV1;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.route.OxiaRouteAuthoritySession;
import com.nereusstream.delay.route.OxiaSignedRouteSnapshotProvider;
import com.nereusstream.delay.route.OxiaSignedRouteSnapshotPublisher;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.V1ScheduleResolver;
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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;

/**
 * Real P1 proof from guarded SUBSCRIBE evidence to a signed Route and source
 * assignment. The source connection is kept alive across Route publication so
 * the signed connection-generation fence is checked at the post-barrier ACK.
 */
public final class PulsarClientArtifactRouteWorkerSmoke {
    private static final String CLUSTER = "standalone";
    private static final byte[] INCARNATION = digest(67);
    private static final long CREATION_TIMESTAMP = 4001L;
    private static final Duration RECEIVE_TIMEOUT = Duration.ofMillis(250);

    private PulsarClientArtifactRouteWorkerSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <route-topic>");
        }
        final String oxiaEndpoint = configuredRequired("NEREUS_DELAY_OXIA_ENDPOINT");
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String requestedShards = configured("NEREUS_DELAY_PULSAR_ROUTE_WORKER_SHARDS", "1");
        if (!"1".equals(requestedShards) && !"2".equals(requestedShards)) {
            throw new IllegalArgumentException("NEREUS_DELAY_PULSAR_ROUTE_WORKER_SHARDS must be 1 or 2");
        }
        if ("2".equals(requestedShards)) {
            runMultiShard(serviceUrl, adminUrl, arguments[2], oxiaEndpoint);
            return;
        }
        final String topicBaseName = arguments[2] + "-" + UUID.randomUUID();
        final String physicalTopicName = topicBaseName + "-partition-0";
        final String physicalTopic = "persistent://public/default/" + physicalTopicName;
        final String physicalTopicBase = "persistent://public/default/" + topicBaseName;
        final HttpClient admin = HttpClient.newHttpClient();
        createTopic(admin, adminUrl, topicBaseName, 1);
        try {
            final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
            final RouteIncarnation routeIncarnation = RouteIncarnation.random();
            final ShardId shard = new ShardId(routeIncarnation, 0);
            final PreparedCommand beforeRoute = command(shard, "route-before");
            final PreparedCommand afterRoute = command(shard, "route-after");
            final KeyPair signingKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.PULSAR, Bytes.utf8("primary"));
            final AuthenticatedTenantContext tenant =
                    new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
            final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
            final String routePrefix = "nereus-delay/pulsar-route-worker/" + UUID.randomUUID();
            final String assignmentPrefix = "nereus-delay/pulsar-route-worker-assignment/" + UUID.randomUUID();

            try (PulsarClient client =
                    PulsarClient.builder().serviceUrl(serviceUrl).build()) {
                final GuardedConsumer<byte[]> nativeConsumer = PulsarClientArtifactSourceConsumerFactory.create(
                        client, guard, physicalTopic, "nereus-delay-route-source-" + UUID.randomUUID());
                final PulsarClientArtifactSourceRecordConsumer source = new PulsarClientArtifactSourceRecordConsumer(
                        nativeConsumer, guard, shard, physicalTopic, RECEIVE_TIMEOUT);
                boolean firstAcked = false;
                boolean runtimeOwnsConsumer = false;
                boolean runtimeDrained = false;
                try {
                    send(client, guard, physicalTopic, beforeRoute, "route-before-producer");
                    final SourceRecordConsumer.PolledSourceRecord first = poll(source);
                    final PulsarSourcePosition firstPosition = requireCommand(first, beforeRoute, "before Route");
                    final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof barrierProof =
                            PulsarClientArtifactRecoverySourcePositioner.awaitStableProof(
                                    nativeConsumer, guard, physicalTopic, shard.partition(), Duration.ofSeconds(5));

                    final RouteSnapshotV1 snapshot = routeSnapshot(
                            physicalTopicBase,
                            physicalTopic,
                            routeIncarnation,
                            firstPosition,
                            barrierProof,
                            signingKeys);
                    try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                                    oxiaEndpoint,
                                    namespace,
                                    "nereus-delay-pulsar-route-publisher-" + UUID.randomUUID(),
                                    Duration.ofSeconds(15),
                                    routePrefix);
                            OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                                    oxiaEndpoint,
                                    namespace,
                                    "nereus-delay-pulsar-route-provider-" + UUID.randomUUID(),
                                    Duration.ofSeconds(15),
                                    routePrefix);
                            OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle =
                                    OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                            oxiaEndpoint,
                                            namespace,
                                            "nereus-delay-pulsar-route-assignment-" + UUID.randomUUID(),
                                            Duration.ofSeconds(15),
                                            assignmentPrefix)) {
                        final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(
                                publisherSession, routePrefix, signingKeys.getPublic());
                        final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                                providerSession, routePrefix, signingKeys.getPublic(), System::currentTimeMillis);
                        publisher.publish(hint, snapshot, 0);
                        provider.refresh().toCompletableFuture().join();

                        final WorkerAssignmentAuthority assignmentAuthority =
                                new OxiaSyncWorkerAssignmentBackend(assignmentHandle, assignmentPrefix);
                        final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(
                                provider,
                                new WorkerAssignmentCoordinator(
                                        new WorkerPlacementPolicy(
                                                new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)),
                                        assignmentAuthority));
                        final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement =
                                coordinator.placeActive(tenant, hint, placementRequest(System.currentTimeMillis()));
                        final WorkerAssignment accepted = coordinator.requireAccepted(
                                tenant,
                                placement.publication().revision(),
                                placement.publication().assignment());
                        requireRouteAssignment(accepted, snapshot, firstPosition, barrierProof);

                        final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(assignmentHandle.backend());
                        final OwnerLease lease = ownerAuthority
                                .acquire(
                                        accepted.sourceAssignment(),
                                        "pulsar-route-worker",
                                        assignmentHandle.sessionIdentity(),
                                        System.currentTimeMillis(),
                                        60_000)
                                .orElseThrow();
                        final WorkClassExecutionRegistry workClasses = workClasses();
                        final KeyPair verificationKey =
                                KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
                        final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshot(shard);
                        final Path root = Files.createTempDirectory("nereus-delay-pulsar-route-worker-");
                        try {
                            final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
                            try (SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                                    ShardStore store = ShardStore.open(storeConfig, shard, resources)) {
                                resources.bindWorkClassExecutionRegistry(workClasses);
                                store.recordControlSnapshot(controlSnapshot);
                                final DelayShard delayShard = new DelayShard(
                                        store, DelayShardConfig.defaults(), null, null, scheduleResolver());
                                final OwnedDelayShard ownedShard = new OwnedDelayShard(
                                        delayShard,
                                        lease,
                                        new OwnerIdentityV1(
                                                bytes(16, 70),
                                                bytes(16, 71),
                                                lease.ownerEpoch(),
                                                Bytes.sha256(Bytes.utf8("pulsar-route-worker-fencing"))));
                                recoverRouteRecord(
                                        accepted,
                                        ownerAuthority,
                                        ownedShard,
                                        first.entry(),
                                        verificationKey,
                                        controlSnapshot,
                                        workClasses);
                                if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                        || !(ownedShard.lastCatchupPosition() instanceof PulsarSourcePosition recovered)
                                        || !recovered.equals(firstPosition)) {
                                    throw new IllegalStateException(
                                            "Pulsar Route Worker recovery did not apply the pre-Route record");
                                }
                                requireAcked(first.acknowledgement().acknowledge(first.entry(), null));
                                firstAcked = true;

                                send(client, guard, physicalTopic, afterRoute, "route-after-producer");
                                if (nativeConsumer.connectionGeneration() != barrierProof.connectionGeneration()) {
                                    throw new IllegalStateException(
                                            "Pulsar Route Worker connection generation changed before active apply");
                                }
                                final WorkerShardRuntime runtime = PulsarClientArtifactWorkerSourceFactory.create(
                                        nativeConsumer,
                                        guard,
                                        physicalTopic,
                                        RECEIVE_TIMEOUT,
                                        accepted.sourceAssignment(),
                                        workClasses,
                                        ownedShard,
                                        store,
                                        resources,
                                        ownerAuthority,
                                        verificationKey.getPublic());
                                runtimeOwnsConsumer = true;
                                final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result =
                                        runUntilApplied(runtime);
                                if (result.status()
                                                != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                                        .APPLIED_AND_ACKED
                                        || !(result.entry() instanceof SourceReplayRecord activeRecord)
                                        || !activeRecord.command().equals(afterRoute)
                                        || !(activeRecord.position() instanceof PulsarSourcePosition secondPosition)
                                        || secondPosition.compareWithinShard(firstPosition) <= 0) {
                                    throw new IllegalStateException(
                                            "Pulsar Route Worker active source did not apply and ACK "
                                                    + "the post-barrier record");
                                }
                                if (!(store.appliedShardLogPosition() instanceof PulsarSourcePosition applied)
                                        || !applied.equals(secondPosition)) {
                                    throw new IllegalStateException(
                                            "Pulsar Route Worker Store did not persist the post-barrier position");
                                }
                                final Path checkpointPath = root.resolve("route-worker-final-checkpoint");
                                final byte[] checkpointId = Arrays.copyOf(
                                        Bytes.sha256(Bytes.utf8("pulsar-route-worker-final-checkpoint")), 16);
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
                                            "Pulsar Route Worker drain did not publish the final checkpoint "
                                                    + "or release the owner lease");
                                }
                                runtimeDrained = true;
                                runtime.close();
                                if (!assignmentAuthority.withdraw(placement.publication())) {
                                    throw new IllegalStateException(
                                            "Pulsar Route Worker assignment was not withdrawn exactly");
                                }
                                provider.close();
                                System.out.println("Pulsar signed Route -> guarded SUBSCRIBE barrier -> Oxia Worker "
                                        + "assignment -> RocksDB apply/checkpoint smoke passed: generation="
                                        + barrierProof.connectionGeneration() + ", barrier="
                                        + firstPosition.ledgerId() + "/" + firstPosition.entryId()
                                        + ", routeRevision=" + placement.routeRevision() + ", assignmentRevision="
                                        + placement.publication().revision() + ", source=" + secondPosition.ledgerId()
                                        + "/" + secondPosition.entryId() + ", ACK, final checkpoint");
                            }
                        } finally {
                            deleteTree(root);
                        }
                    }
                } finally {
                    if (runtimeOwnsConsumer) {
                        if (!runtimeDrained) {
                            closeNative(nativeConsumer);
                        }
                    } else if (firstAcked) {
                        source.close();
                    } else {
                        closeNative(nativeConsumer);
                    }
                }
            }
        } finally {
            deleteTopicIfPresent(admin, adminUrl, topicBaseName);
        }
    }

    /**
     * Real multi-shard Route/Assignment/Owner/Worker proof. One signed Route
     * carries two independent guarded SUBSCRIBE barriers; each partition then
     * crosses its own Oxia Assignment CAS and Owner Lease before two native
     * source runtimes are admitted to one fair Worker fleet.
     */
    private static void runMultiShard(
            final String serviceUrl, final String adminUrl, final String topicPrefix, final String oxiaEndpoint)
            throws Exception {
        final int shardCount = 2;
        final String topicBaseName = topicPrefix + "-" + UUID.randomUUID();
        final String physicalTopicBase = "persistent://public/default/" + topicBaseName;
        final HttpClient admin = HttpClient.newHttpClient();
        createTopic(admin, adminUrl, topicBaseName, shardCount);
        try {
            final RouteIncarnation routeIncarnation = RouteIncarnation.random();
            final KeyPair signingKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.PULSAR, Bytes.utf8("primary"));
            final AuthenticatedTenantContext tenant =
                    new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
            final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
            final String routePrefix = "nereus-delay/pulsar-route-worker-multi/" + UUID.randomUUID();
            final String assignmentPrefix = "nereus-delay/pulsar-route-worker-multi-assignment/" + UUID.randomUUID();

            try (PulsarClient client =
                    PulsarClient.builder().serviceUrl(serviceUrl).build()) {
                final List<RouteShardProbe> probes = new ArrayList<>(shardCount);
                for (int partition = 0; partition < shardCount; partition++) {
                    final String physicalTopic = physicalTopicBase + "-partition-" + partition;
                    final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
                    GuardedConsumer<byte[]> nativeConsumer = null;
                    try {
                        nativeConsumer = PulsarClientArtifactSourceConsumerFactory.create(
                                client,
                                guard,
                                physicalTopic,
                                "nereus-delay-route-source-multi-" + partition + "-" + UUID.randomUUID());
                        final PulsarClientArtifactSourceRecordConsumer source =
                                new PulsarClientArtifactSourceRecordConsumer(
                                        nativeConsumer,
                                        guard,
                                        new ShardId(routeIncarnation, partition),
                                        physicalTopic,
                                        RECEIVE_TIMEOUT);
                        final ShardId shard = new ShardId(routeIncarnation, partition);
                        final PreparedCommand beforeRoute = command(shard, "route-multi-before-" + partition);
                        final PreparedCommand afterRoute = command(shard, "route-multi-after-" + partition);
                        send(
                                client,
                                guard,
                                physicalTopic,
                                beforeRoute,
                                "route-multi-before-producer-" + partition,
                                partition);
                        final SourceRecordConsumer.PolledSourceRecord first = poll(source);
                        final PulsarSourcePosition firstPosition =
                                requireCommand(first, beforeRoute, "before Route partition " + partition);
                        final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof barrierProof =
                                PulsarClientArtifactRecoverySourcePositioner.awaitStableProof(
                                        nativeConsumer, guard, physicalTopic, partition, Duration.ofSeconds(5));
                        probes.add(new RouteShardProbe(
                                shard,
                                guard,
                                physicalTopic,
                                beforeRoute,
                                afterRoute,
                                first,
                                firstPosition,
                                barrierProof,
                                nativeConsumer));
                    } catch (RuntimeException | Error failure) {
                        if (nativeConsumer != null) {
                            closeNativeQuietly(nativeConsumer);
                        }
                        throw failure;
                    }
                }

                final RouteSnapshotV1 snapshot =
                        multiRouteSnapshot(physicalTopicBase, routeIncarnation, probes, signingKeys);
                final boolean[] runtimeOwned = new boolean[shardCount];
                final boolean[] runtimeDrained = new boolean[shardCount];
                try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                                oxiaEndpoint,
                                namespace,
                                "nereus-delay-pulsar-route-multi-publisher-" + UUID.randomUUID(),
                                Duration.ofSeconds(15),
                                routePrefix);
                        OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                                oxiaEndpoint,
                                namespace,
                                "nereus-delay-pulsar-route-multi-provider-" + UUID.randomUUID(),
                                Duration.ofSeconds(15),
                                routePrefix);
                        OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle =
                                OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                        oxiaEndpoint,
                                        namespace,
                                        "nereus-delay-pulsar-route-multi-assignment-" + UUID.randomUUID(),
                                        Duration.ofSeconds(15),
                                        assignmentPrefix)) {
                    final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(
                            publisherSession, routePrefix, signingKeys.getPublic());
                    final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                            providerSession, routePrefix, signingKeys.getPublic(), System::currentTimeMillis);
                    final long routeRevision =
                            publisher.publish(hint, snapshot, 0).revision();
                    provider.refresh().toCompletableFuture().join();

                    final WorkerAssignmentAuthority assignmentAuthority =
                            new OxiaSyncWorkerAssignmentBackend(assignmentHandle, assignmentPrefix);
                    final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(
                            provider,
                            new WorkerAssignmentCoordinator(
                                    new WorkerPlacementPolicy(
                                            new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)),
                                    assignmentAuthority));
                    final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(assignmentHandle.backend());
                    final List<MultiShardAdmission> admissions = new ArrayList<>(shardCount);
                    final Set<String> assignedWorkers = new HashSet<>();
                    for (RouteShardProbe probe : probes) {
                        final int partition = probe.shard().partition();
                        final String expectedWorker = "pulsar-route-worker-" + (char) ('a' + partition);
                        final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement = coordinator.placeActive(
                                tenant, hint, placementRequest(System.currentTimeMillis(), partition, expectedWorker));
                        final WorkerAssignment accepted = coordinator.requireAccepted(
                                tenant,
                                placement.publication().revision(),
                                placement.publication().assignment());
                        requireRouteAssignment(accepted, snapshot, probe);
                        if (!expectedWorker.equals(accepted.workerId())) {
                            throw new IllegalStateException("Pulsar multi-shard placement selected unexpected Worker: "
                                    + "partition=" + partition + ", expected=" + expectedWorker + ", actual="
                                    + accepted.workerId());
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
                                    "Pulsar multi-shard placement reused a Worker identity: " + accepted.workerId());
                        }
                        admissions.add(new MultiShardAdmission(probe, placement.publication(), accepted, lease));
                    }
                    if (assignedWorkers.size() != shardCount) {
                        throw new IllegalStateException(
                                "Pulsar multi-shard placement did not span two Worker identities");
                    }

                    final WorkClassExecutionRegistry workClasses = workClasses();
                    final KeyPair verificationKey =
                            KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
                    final Path root = Files.createTempDirectory("nereus-delay-pulsar-route-worker-multi-");
                    final List<ShardStore> stores = new ArrayList<>(shardCount);
                    final List<WorkerShardRuntime> runtimes = new ArrayList<>(shardCount);
                    WorkerShardFleetRuntime fleet = null;
                    boolean drained = false;
                    try {
                        final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
                        final SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                        try {
                            resources.bindWorkClassExecutionRegistry(workClasses);
                            for (int index = 0; index < admissions.size(); index++) {
                                final MultiShardAdmission admission = admissions.get(index);
                                final RouteShardProbe probe = admission.probe();
                                final ShardId shard = probe.shard();
                                final ShardStore store = ShardStore.open(storeConfig, shard, resources);
                                stores.add(store);
                                final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshot(shard);
                                store.recordControlSnapshot(controlSnapshot);
                                final DelayShard delayShard = new DelayShard(
                                        store, DelayShardConfig.defaults(), null, null, scheduleResolver());
                                final OwnedDelayShard ownedShard = new OwnedDelayShard(
                                        delayShard,
                                        admission.lease(),
                                        new OwnerIdentityV1(
                                                bytes(16, 70 + index),
                                                bytes(16, 90 + index),
                                                admission.lease().ownerEpoch(),
                                                Bytes.sha256(
                                                        Bytes.utf8("pulsar-route-worker-multi-fencing-" + index))));
                                recoverRouteRecord(
                                        admission.assignment(),
                                        ownerAuthority,
                                        ownedShard,
                                        probe.firstRecord().entry(),
                                        verificationKey,
                                        controlSnapshot,
                                        workClasses);
                                if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                        || !(ownedShard.lastCatchupPosition() instanceof PulsarSourcePosition recovered)
                                        || !recovered.equals(probe.firstPosition())) {
                                    throw new IllegalStateException(
                                            "Pulsar multi-shard recovery did not apply partition " + shard.partition()
                                                    + " pre-Route record");
                                }
                                requireAcked(probe.firstRecord()
                                        .acknowledgement()
                                        .acknowledge(probe.firstRecord().entry(), null));
                                send(
                                        client,
                                        probe.guard(),
                                        probe.physicalTopic(),
                                        probe.afterRoute(),
                                        "route-multi-after-producer-" + index,
                                        shard.partition());
                                if (probe.nativeConsumer().connectionGeneration()
                                        != probe.barrierProof().connectionGeneration()) {
                                    throw new IllegalStateException(
                                            "Pulsar multi-shard source connection generation changed "
                                                    + "before active apply: partition=" + shard.partition());
                                }
                                runtimes.add(PulsarClientArtifactWorkerSourceFactory.create(
                                        probe.nativeConsumer(),
                                        probe.guard(),
                                        probe.physicalTopic(),
                                        RECEIVE_TIMEOUT,
                                        admission.assignment().sourceAssignment(),
                                        workClasses,
                                        ownedShard,
                                        store,
                                        resources,
                                        ownerAuthority,
                                        verificationKey.getPublic()));
                                runtimeOwned[index] = true;
                            }

                            fleet = new WorkerShardFleetRuntime(workClasses, resources, runtimes);
                            final Set<ShardId> pending = new HashSet<>(fleet.shardIds());
                            final Map<Integer, com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult>
                                    appliedTurns = new HashMap<>();
                            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                            while (!pending.isEmpty() && System.nanoTime() < deadline) {
                                final WorkerShardFleetRuntime.SourceTurn turn = fleet.runNextSourceTurn(
                                        new SchedulerBudget(1, 1_000_000, TimeUnit.SECONDS.toNanos(2)),
                                        System::currentTimeMillis);
                                if (turn.result().status()
                                        == com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                                .APPLIED_AND_ACKED) {
                                    if (!(turn.result().entry() instanceof SourceReplayRecord record)
                                            || !record.command()
                                                    .equals(probes.get(turn.shardId()
                                                                    .partition())
                                                            .afterRoute())
                                            || !(record.position() instanceof PulsarSourcePosition)) {
                                        throw new IllegalStateException(
                                                "Pulsar multi-shard Worker applied an unexpected "
                                                        + "post-barrier record: shard=" + turn.shardId());
                                    }
                                    appliedTurns.put(turn.shardId().partition(), turn.result());
                                    pending.remove(turn.shardId());
                                } else if (turn.result().status()
                                                != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                                        .WAITING_FOR_SOURCE
                                        && turn.result().status()
                                                != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                                        .WAITING_FOR_WORK_CLASS) {
                                    throw new IllegalStateException(
                                            "Pulsar multi-shard Worker source turn failed: shard=" + turn.shardId()
                                                    + ", status="
                                                    + turn.result().status(),
                                            turn.result().failure());
                                }
                            }
                            if (!pending.isEmpty()) {
                                throw new IllegalStateException(
                                        "Pulsar multi-shard Worker source apply timed out: " + pending);
                            }
                            for (int index = 0; index < admissions.size(); index++) {
                                final MultiShardAdmission admission = admissions.get(index);
                                final ShardId shard = admission.probe().shard();
                                final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult turn =
                                        appliedTurns.get(index);
                                if (turn == null
                                        || !(turn.entry() instanceof SourceReplayRecord record)
                                        || !(record.position() instanceof PulsarSourcePosition appliedPosition)
                                        || !(stores.get(index).appliedShardLogPosition()
                                                instanceof PulsarSourcePosition applied)
                                        || !applied.equals(appliedPosition)
                                        || !applied.shardId().equals(shard)
                                        || !applied.physicalTopic()
                                                .equals(admission.probe().physicalTopic())) {
                                    throw new IllegalStateException(
                                            "Pulsar multi-shard Store did not persist partition " + shard.partition()
                                                    + " post-barrier position");
                                }
                            }
                            for (int index = 0; index < runtimes.size(); index++) {
                                final MultiShardAdmission admission = admissions.get(index);
                                final ShardId shard = admission.probe().shard();
                                final Path checkpointPath =
                                        root.resolve("route-worker-multi-final-checkpoint-" + shard.partition());
                                final byte[] checkpointId = Arrays.copyOf(
                                        Bytes.sha256(Bytes.utf8(
                                                "pulsar-route-worker-multi-final-checkpoint-" + shard.partition())),
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
                                    throw new IllegalStateException(
                                            "Pulsar multi-shard drain did not complete partition " + shard.partition()
                                                    + " checkpoint/lease release");
                                }
                                runtimeDrained[index] = true;
                            }
                            fleet.close();
                            for (MultiShardAdmission admission : admissions) {
                                if (!assignmentAuthority.withdraw(admission.publication())) {
                                    throw new IllegalStateException(
                                            "Pulsar multi-shard assignment was not withdrawn exactly: "
                                                    + admission.probe().shard());
                                }
                            }
                            drained = true;
                            provider.close();
                            System.out.println("Pulsar signed Route -> two guarded SUBSCRIBE barriers -> Oxia "
                                    + "multi-shard Assignment/Owner -> one Worker fleet -> RocksDB "
                                    + "apply/ACK/checkpoint "
                                    + "smoke passed: subscribePartitions=" + shardCount + ", routeRevision="
                                    + routeRevision + ", assignmentRevisions="
                                    + admissions.stream()
                                            .map(admission ->
                                                    admission.publication().revision())
                                            .toList()
                                    + ", workers=" + assignedWorkers + ", sourceBarriers="
                                    + probes.stream()
                                            .map(probe -> probe.firstPosition().ledgerId() + "/"
                                                    + probe.firstPosition().entryId())
                                            .toList());
                        } finally {
                            if (fleet != null && !drained) {
                                try {
                                    fleet.close();
                                } catch (RuntimeException | Error ignored) {
                                    // Preserve the primary multi-shard failure;
                                    // the short-lived Oxia session fences the
                                    // temporary authority on teardown.
                                }
                            }
                            for (ShardStore store : stores) {
                                try {
                                    store.close();
                                } catch (RuntimeException | Error ignored) {
                                    // Preserve the primary failure while the
                                    // process-scoped Store is being torn down.
                                }
                            }
                            resources.close();
                        }
                    } finally {
                        deleteTree(root);
                    }
                } finally {
                    for (int index = 0; index < probes.size(); index++) {
                        if (runtimeOwned[index] && runtimeDrained[index]) {
                            continue;
                        }
                        closeNativeQuietly(probes.get(index).nativeConsumer());
                    }
                }
            }
        } finally {
            deleteTopicIfPresent(admin, adminUrl, topicBaseName);
        }
    }

    private static PulsarSourcePosition requireCommand(
            final SourceRecordConsumer.PolledSourceRecord polled, final PreparedCommand expected, final String phase) {
        if (!(polled.entry() instanceof SourceReplayRecord replay)
                || !expected.equals(replay.command())
                || !(replay.position() instanceof PulsarSourcePosition position)) {
            throw new IllegalStateException("Pulsar source returned an unexpected command " + phase);
        }
        return position;
    }

    private static SourceRecordConsumer.PolledSourceRecord poll(final PulsarClientArtifactSourceRecordConsumer source) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            final Optional<SourceRecordConsumer.PolledSourceRecord> polled = source.poll();
            if (polled.isPresent()) {
                return polled.get();
            }
        }
        throw new IllegalStateException("Pulsar Route source record did not become visible");
    }

    private static void recoverRouteRecord(
            final WorkerAssignment accepted,
            final OxiaOwnerLeaseStore authority,
            final OwnedDelayShard ownedShard,
            final SourceReplayEntry entry,
            final KeyPair verificationKey,
            final CompatibleControlSnapshotV1 controlSnapshot,
            final WorkClassExecutionRegistry workClasses) {
        final SourceReplayCursor<SourceReplayEntry> cursor =
                SourceReplayCursor.of(List.of(entry).iterator());
        final OwnerRecoveryCoordinator recovery = new OwnerRecoveryCoordinator(
                ownedShard,
                authority,
                accepted.sourceAssignment(),
                SourceReplaySuccessor.strictPulsarBatchMember(),
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
            throw new IllegalStateException("Pulsar Route Worker recovery did not apply exactly one record");
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
                        "Pulsar Route Worker source turn failed: " + result.status(), result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Pulsar Route Worker source record did not become visible");
    }

    private static V1ScheduleResolver scheduleResolver() {
        final byte[] tuple = Bytes.utf8("pulsar-route-worker-canonical-lane-tuple-v1");
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        return new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(
                    final ShardId shard,
                    final com.nereusstream.delay.protocol.DelayMessageId message,
                    final ScheduleIntentV1 intent,
                    final com.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedSchedule(lane, tuple, intent.inlinePayload(), null);
            }

            @Override
            public ResolvedPrepare resolvePrepare(
                    final ShardId shard,
                    final com.nereusstream.delay.protocol.DelayMessageId message,
                    final com.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1 body,
                    final com.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedPrepare(lane, tuple);
            }
        };
    }

    private static CompatibleControlSnapshotV1 controlSnapshot(final ShardId shard) {
        return new CompatibleControlSnapshotV1(
                new ShardSubjectV1(shard),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRefV1(bytes(32, 50), 1, bytes(32, 51), ProfileKindV1.DESTINATION)),
                new QuotaGrantRefV1(
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

    private static RouteSnapshotV1 multiRouteSnapshot(
            final String physicalTopicBase,
            final RouteIncarnation incarnation,
            final List<RouteShardProbe> probes,
            final KeyPair signingKeys) {
        final long now = System.currentTimeMillis();
        final List<PulsarPhysicalPartitionIdentityV1> physicalPartitions = probes.stream()
                .map(probe -> new PulsarPhysicalPartitionIdentityV1(
                        probe.shard().partition(), probe.physicalTopic(), INCARNATION, CREATION_TIMESTAMP))
                .toList();
        final PulsarIngressRouteResourceV1 ingress =
                new PulsarIngressRouteResourceV1(CLUSTER, physicalTopicBase, physicalPartitions);
        final List<RoutePartitionPolicyV1> policies = probes.stream()
                .map(probe -> {
                    final BrokerResourceIdentityV1 broker =
                            BrokerResourceIdentityV1.pulsar(new PulsarBrokerResourceIdentityV1(
                                    CLUSTER, INCARNATION, probe.physicalTopic(), CREATION_TIMESTAMP));
                    final PulsarSourcePosition position = probe.firstPosition();
                    final var proof = probe.barrierProof();
                    final ActivationBarrierV1 barrier = ActivationBarrierV1.pulsar(
                            broker,
                            probe.shard().partition(),
                            position.ledgerId(),
                            position.entryId(),
                            position.normalizedBatchIndex(),
                            position.batchSize(),
                            proof.connectionGeneration(),
                            proof.attestationDigest());
                    return new RoutePartitionPolicyV1(
                            probe.shard().partition(),
                            barrier,
                            zeroQuota(),
                            proof.connectionGeneration(),
                            proof.attestationDigest());
                })
                .toList();
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                now - 100,
                now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("pulsar-route-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("pulsar-route-issued-at")),
                0,
                null);
        return RouteSnapshotV1.create(
                incarnation,
                bytes(32, 1),
                bytes(32, 2),
                RouteLifecycleV1.ACTIVE_FOR_NEW,
                now + 30_000,
                ingress,
                RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1),
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
                new IngressCredentialBindingRefV1(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                Bytes.sha256(Bytes.utf8("pulsar-route-prerequisite")),
                issuedAt,
                1,
                signingKeys.getPrivate());
    }

    private static RouteSnapshotV1 routeSnapshot(
            final String physicalTopicBase,
            final String physicalTopic,
            final RouteIncarnation incarnation,
            final PulsarSourcePosition position,
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof,
            final KeyPair signingKeys) {
        final long now = System.currentTimeMillis();
        final BrokerResourceIdentityV1 broker = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1(CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP));
        final ActivationBarrierV1 barrier = ActivationBarrierV1.pulsar(
                broker,
                0,
                position.ledgerId(),
                position.entryId(),
                position.normalizedBatchIndex(),
                position.batchSize(),
                proof.connectionGeneration(),
                proof.attestationDigest());
        final RoutePartitionPolicyV1 policy = new RoutePartitionPolicyV1(
                0, barrier, zeroQuota(), proof.connectionGeneration(), proof.attestationDigest());
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                now - 100,
                now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("pulsar-route-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("pulsar-route-issued-at")),
                0,
                null);
        final PulsarIngressRouteResourceV1 ingress = new PulsarIngressRouteResourceV1(
                CLUSTER,
                physicalTopicBase,
                List.of(new PulsarPhysicalPartitionIdentityV1(0, physicalTopic, INCARNATION, CREATION_TIMESTAMP)));
        return RouteSnapshotV1.create(
                incarnation,
                bytes(32, 1),
                bytes(32, 2),
                RouteLifecycleV1.ACTIVE_FOR_NEW,
                now + 30_000,
                ingress,
                RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1),
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
                new IngressCredentialBindingRefV1(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                Bytes.sha256(Bytes.utf8("pulsar-route-prerequisite")),
                issuedAt,
                1,
                signingKeys.getPrivate());
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest placementRequest(final long now) {
        return placementRequest(now, 0, "pulsar-route-worker");
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest placementRequest(
            final long now, final int partition, final String workerId) {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(
                partition,
                Bytes.sha256(Bytes.utf8("pulsar-route-worker-assignment-" + partition + "-" + workerId)),
                1,
                Bytes.sha256(Bytes.utf8("pulsar-route-worker-capacity-" + partition + "-" + workerId)),
                1,
                List.of(new WorkerPlacementPolicy.WorkerCandidate(
                        workerId,
                        capacity(2),
                        CapacityVectorV1.empty(),
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
                CapacityVectorV1.empty(),
                CapacityVectorV1.empty(),
                null,
                now,
                0,
                0);
    }

    private static void requireRouteAssignment(
            final WorkerAssignment assignment, final RouteSnapshotV1 snapshot, final RouteShardProbe probe) {
        if (!assignment.routeBound()
                || !Arrays.equals(snapshot.snapshotDigest(), assignment.routeSnapshotDigest())
                || !(assignment.sourceAssignment().activationBarrier() instanceof PulsarActivationBarrier barrier)
                || !Arrays.equals(barrier.brokerResourceIncarnation(), INCARNATION)
                || !barrier.shardId().equals(probe.shard())
                || !barrier.physicalTopic().equals(probe.physicalTopic())
                || barrier.ledgerId() != probe.firstPosition().ledgerId()
                || barrier.entryId() != probe.firstPosition().entryId()
                || barrier.normalizedLastBatchIndex() != probe.firstPosition().normalizedBatchIndex()
                || barrier.batchSize() != probe.firstPosition().batchSize()
                || barrier.guardedSourceConnectionGeneration()
                        != probe.barrierProof().connectionGeneration()
                || !Arrays.equals(
                        barrier.resourceGuardAttestationDigest(),
                        probe.barrierProof().attestationDigest())) {
            throw new IllegalStateException(
                    "Oxia multi-shard Worker assignment did not retain the signed Pulsar " + "Route barrier");
        }
    }

    private static void requireRouteAssignment(
            final WorkerAssignment assignment,
            final RouteSnapshotV1 snapshot,
            final PulsarSourcePosition firstPosition,
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof) {
        if (!assignment.routeBound()
                || !Arrays.equals(snapshot.snapshotDigest(), assignment.routeSnapshotDigest())
                || !(assignment.sourceAssignment().activationBarrier() instanceof PulsarActivationBarrier barrier)
                || !Arrays.equals(barrier.brokerResourceIncarnation(), INCARNATION)
                || !barrier.physicalTopic().equals(firstPosition.physicalTopic())
                || barrier.ledgerId() != firstPosition.ledgerId()
                || barrier.entryId() != firstPosition.entryId()
                || barrier.normalizedLastBatchIndex() != firstPosition.normalizedBatchIndex()
                || barrier.batchSize() != firstPosition.batchSize()
                || barrier.guardedSourceConnectionGeneration() != proof.connectionGeneration()
                || !Arrays.equals(barrier.resourceGuardAttestationDigest(), proof.attestationDigest())) {
            throw new IllegalStateException("Oxia Worker assignment did not retain the signed Pulsar Route barrier");
        }
    }

    private static void send(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final PreparedCommand command,
            final String producerName)
            throws Exception {
        send(client, guard, physicalTopic, command, producerName, 0);
    }

    private static void send(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final PreparedCommand command,
            final String producerName,
            final int partition)
            throws Exception {
        final PulsarClientArtifactSendTransport transport = new PulsarClientArtifactSendTransport(
                PulsarClientArtifactProducerFactory.create(
                        client, CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, producerName),
                CLUSTER,
                INCARNATION,
                physicalTopic,
                CREATION_TIMESTAMP,
                partition);
        try {
            final PulsarSendResult result = transport
                    .send(new PulsarSendRequest(
                            CLUSTER,
                            INCARNATION,
                            physicalTopic,
                            CREATION_TIMESTAMP,
                            partition,
                            command.commandId(),
                            com.nereusstream.delay.protocol.CommandCodec.encodeFrameV1(command)))
                    .toCompletableFuture()
                    .get(15, TimeUnit.SECONDS);
            if (result.disposition() != PulsarSendResult.Disposition.PERSISTED) {
                throw new IllegalStateException(
                        "guarded Pulsar Route producer did not persist: " + result.disposition());
            }
        } finally {
            transport.close();
        }
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        final ProfileRefV1 destination = new ProfileRefV1(
                Bytes.utf8("destination-" + identity),
                1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)),
                ProfileKindV1.DESTINATION);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(
                Bytes.utf8("retry-" + identity), 1, Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final com.nereusstream.delay.protocol.ScheduleIntentV1 intent =
                com.nereusstream.delay.protocol.ScheduleIntentV1.create(
                        destination,
                        retryPolicy,
                        deliverAt,
                        deliverAt + 10_000,
                        DeliveryMode.MANAGED,
                        OrderingMode.BEST_EFFORT,
                        new byte[0],
                        Bytes.utf8("source-" + identity),
                        null,
                        AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of())),
                        null,
                        null);
        return PreparedCommand.scheduleV1(shard, intent, deliverAt + 20_000);
    }

    private static QuotaGrantRefV1 zeroQuota() {
        return new QuotaGrantRefV1(
                bytes(32, 20),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    }

    private static CapacityVectorV1 capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimensionV1.COUNT];
        values[CapacityDimensionV1.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVectorV1(values);
    }

    private static void requireAcked(final SourceAcknowledgement.AcknowledgementResult result) {
        if (result.disposition() != SourceAcknowledgement.Disposition.ACKED) {
            throw new IllegalStateException(
                    "Pulsar Route source record was not ACKED: " + result.disposition(), result.failure());
        }
    }

    private static void createTopic(
            final HttpClient client, final String adminUrl, final String topicBase, final int partitions)
            throws Exception {
        if (partitions <= 0) {
            throw new IllegalArgumentException("Pulsar Route topic must contain at least one partition");
        }
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topicBase + "/partitions";
        for (int attempt = 0; attempt < 40; attempt++) {
            final HttpResponse<String> response = request(client, path, "PUT", Integer.toString(partitions));
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                break;
            }
            if (response.statusCode() != 409 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw failure("create Route partitioned topic", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
            if (attempt == 39) {
                throw failure("create Route partitioned topic", response);
            }
        }
        for (int partition = 0; partition < partitions; partition++) {
            final String physicalTopic = topicBase + "-partition-" + partition;
            final String guardPath =
                    adminUrl + "/admin/v2/persistent/public/default/" + physicalTopic + "/resourceGuard";
            final String guardBody = "{\"nereus.resource.guard.version\":\"1\","
                    + "\"nereus.resource.incarnation\":\""
                    + Base64.getUrlEncoder().withoutPadding().encodeToString(INCARNATION) + "\","
                    + "\"nereus.resource.created-at\":\""
                    + Long.toUnsignedString(CREATION_TIMESTAMP) + "\"}";
            for (int attempt = 0; attempt < 40; attempt++) {
                final HttpResponse<String> response = request(client, guardPath, "PUT", guardBody);
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    break;
                }
                if (response.statusCode() != 409 && response.statusCode() != 412 && response.statusCode() != 503) {
                    throw failure("stamp Route physical topic resource guard", response);
                }
                TimeUnit.MILLISECONDS.sleep(250);
                if (attempt == 39) {
                    throw failure("stamp Route physical topic resource guard", response);
                }
            }
        }
    }

    private static void deleteTopicIfPresent(final HttpClient client, final String adminUrl, final String topic) {
        try {
            final HttpResponse<String> response = request(
                    client,
                    adminUrl + "/admin/v2/persistent/public/default/" + topic + "/partitions?force=true",
                    "DELETE",
                    "");
            if (response.statusCode() >= 300 && response.statusCode() != 404) {
                System.err.println("Pulsar Route smoke cleanup could not delete topic: " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("Pulsar Route smoke cleanup failed: " + failure.getMessage());
        }
    }

    private static HttpResponse<String> request(
            final HttpClient client, final String path, final String method, final String body) throws Exception {
        final HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(path)).header("Content-Type", "application/json");
        final HttpRequest request = "DELETE".equals(method)
                ? builder.DELETE().build()
                : builder.PUT(HttpRequest.BodyPublishers.ofString(body)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static IllegalStateException failure(final String operation, final HttpResponse<String> response) {
        return new IllegalStateException(
                operation + " failed with HTTP " + response.statusCode() + ": " + response.body());
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

    private static void closeNative(final GuardedConsumer<byte[]> consumer) {
        try {
            consumer.close();
        } catch (PulsarClientException failure) {
            throw new IllegalStateException("Pulsar Route Worker native consumer close failed", failure);
        }
    }

    private static void closeNativeQuietly(final GuardedConsumer<byte[]> consumer) {
        try {
            consumer.close();
        } catch (PulsarClientException | RuntimeException ignored) {
            // Teardown must not hide the primary multi-shard assertion.
        }
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

    private record RouteShardProbe(
            ShardId shard,
            TopicResourceGuard guard,
            String physicalTopic,
            PreparedCommand beforeRoute,
            PreparedCommand afterRoute,
            SourceRecordConsumer.PolledSourceRecord firstRecord,
            PulsarSourcePosition firstPosition,
            PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof barrierProof,
            GuardedConsumer<byte[]> nativeConsumer) {}

    private record MultiShardAdmission(
            RouteShardProbe probe,
            WorkerAssignmentAuthority.Publication publication,
            WorkerAssignment assignment,
            OwnerLease lease) {}

    private static byte[] digest(final int seed) {
        final byte[] result = new byte[32];
        Arrays.fill(result, (byte) seed);
        return result;
    }
}
