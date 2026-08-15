package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.PulsarSendRequest;
import io.nereusstream.delay.adapter.PulsarSendResult;
import io.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.nereusstream.delay.ownership.OxiaSyncWorkerAssignmentBackend;
import io.nereusstream.delay.ownership.OwnerLease;
import io.nereusstream.delay.ownership.OwnerRecoveryCoordinator;
import io.nereusstream.delay.ownership.OwnerRecoveryTurn;
import io.nereusstream.delay.ownership.OwnedDelayShard;
import io.nereusstream.delay.ownership.RouteWorkerAssignmentCoordinator;
import io.nereusstream.delay.ownership.ReplayTurnBudget;
import io.nereusstream.delay.ownership.SourceAcknowledgement;
import io.nereusstream.delay.ownership.SourceRecordConsumer;
import io.nereusstream.delay.ownership.SourceReplayRecord;
import io.nereusstream.delay.ownership.SourceReplayCursor;
import io.nereusstream.delay.ownership.SourceReplayEntry;
import io.nereusstream.delay.ownership.SourceReplaySuccessor;
import io.nereusstream.delay.ownership.ShardLifecycleState;
import io.nereusstream.delay.ownership.WorkerAssignment;
import io.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import io.nereusstream.delay.ownership.WorkerAssignmentCoordinator;
import io.nereusstream.delay.ownership.WorkerShardRuntime;
import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CapacityDimensionV1;
import io.nereusstream.delay.protocol.CapacityVectorV1;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.IngressCredentialBindingRefV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PulsarIngressRouteResourceV1;
import io.nereusstream.delay.protocol.PulsarMetadataV1;
import io.nereusstream.delay.protocol.PulsarPhysicalPartitionIdentityV1;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteLifecycleV1;
import io.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.RoutingHashVersionV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.route.OxiaRouteAuthoritySession;
import io.nereusstream.delay.route.OxiaSignedRouteSnapshotProvider;
import io.nereusstream.delay.route.OxiaSignedRouteSnapshotPublisher;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.V1ScheduleResolver;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.store.CheckpointFileInventory;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import io.nereusstream.delay.store.WorkerLoadVector;
import io.nereusstream.delay.store.WorkerPlacementPolicy;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TopicResourceGuard;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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

    private PulsarClientArtifactRouteWorkerSmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <route-topic>");
        }
        final String oxiaEndpoint = configuredRequired("NEREUS_DELAY_OXIA_ENDPOINT");
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String topicBaseName = arguments[2] + "-" + UUID.randomUUID();
        final String physicalTopicName = topicBaseName + "-partition-0";
        final String physicalTopic = "persistent://public/default/" + physicalTopicName;
        final String physicalTopicBase = "persistent://public/default/" + topicBaseName;
        final HttpClient admin = HttpClient.newHttpClient();
        createTopic(admin, adminUrl, topicBaseName, physicalTopicName);
        try {
            final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION,
                    CREATION_TIMESTAMP);
            final RouteIncarnation routeIncarnation = RouteIncarnation.random();
            final ShardId shard = new ShardId(routeIncarnation, 0);
            final PreparedCommand beforeRoute = command(shard, "route-before");
            final PreparedCommand afterRoute = command(shard, "route-after");
            final KeyPair signingKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.PULSAR, Bytes.utf8("primary"));
            final AuthenticatedTenantContext tenant = new AuthenticatedTenantContext(
                    bytes(32, 1), bytes(32, 2), bytes(32, 3));
            final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
            final String routePrefix = "nereus-delay/pulsar-route-worker/" + UUID.randomUUID();
            final String assignmentPrefix = "nereus-delay/pulsar-route-worker-assignment/" + UUID.randomUUID();

            try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
                final GuardedConsumer<byte[]> nativeConsumer = PulsarClientArtifactSourceConsumerFactory.create(
                        client, guard, physicalTopic, "nereus-delay-route-source-" + UUID.randomUUID());
                final PulsarClientArtifactSourceRecordConsumer source =
                        new PulsarClientArtifactSourceRecordConsumer(nativeConsumer, guard, shard, physicalTopic,
                                RECEIVE_TIMEOUT);
                boolean firstAcked = false;
                boolean runtimeOwnsConsumer = false;
                boolean runtimeDrained = false;
                try {
                    send(client, guard, physicalTopic, beforeRoute, "route-before-producer");
                    final SourceRecordConsumer.PolledSourceRecord first = poll(source);
                    final PulsarSourcePosition firstPosition = requireCommand(first, beforeRoute, "before Route");
                    final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof barrierProof =
                            PulsarClientArtifactRecoverySourcePositioner.awaitStableProof(nativeConsumer, guard,
                                    physicalTopic, shard.partition(), Duration.ofSeconds(5));

                    final RouteSnapshotV1 snapshot = routeSnapshot(physicalTopicBase, physicalTopic,
                            routeIncarnation, firstPosition, barrierProof, signingKeys);
                    try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                                 oxiaEndpoint, namespace, "nereus-delay-pulsar-route-publisher-" + UUID.randomUUID(),
                                 Duration.ofSeconds(15), routePrefix);
                         OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                                 oxiaEndpoint, namespace, "nereus-delay-pulsar-route-provider-" + UUID.randomUUID(),
                                 Duration.ofSeconds(15), routePrefix);
                         OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle =
                                 OxiaSyncOwnerLeaseBackend.connectUnchecked(oxiaEndpoint, namespace,
                                         "nereus-delay-pulsar-route-assignment-" + UUID.randomUUID(),
                                         Duration.ofSeconds(15), assignmentPrefix)) {
                        final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(
                                publisherSession, routePrefix, signingKeys.getPublic());
                        final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                                providerSession, routePrefix, signingKeys.getPublic(), System::currentTimeMillis);
                        publisher.publish(hint, snapshot, 0);
                        provider.refresh().toCompletableFuture().join();

                        final WorkerAssignmentAuthority assignmentAuthority = new OxiaSyncWorkerAssignmentBackend(
                                assignmentHandle, assignmentPrefix);
                        final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(
                                provider, new WorkerAssignmentCoordinator(new WorkerPlacementPolicy(
                                        new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)),
                                        assignmentAuthority));
                        final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement =
                                coordinator.placeActive(tenant, hint, placementRequest(System.currentTimeMillis()));
                        final WorkerAssignment accepted = coordinator.requireAccepted(tenant,
                                placement.publication().revision(), placement.publication().assignment());
                        requireRouteAssignment(accepted, snapshot, firstPosition, barrierProof);

                        final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(assignmentHandle.backend());
                        final OwnerLease lease = ownerAuthority.acquire(accepted.sourceAssignment(),
                                "pulsar-route-worker", assignmentHandle.sessionIdentity(),
                                System.currentTimeMillis(), 60_000).orElseThrow();
                        final WorkClassExecutionRegistry workClasses = workClasses();
                        final KeyPair verificationKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
                        final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshot(shard);
                        final Path root = Files.createTempDirectory("nereus-delay-pulsar-route-worker-");
                        try {
                            final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
                            try (SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                                 ShardStore store = ShardStore.open(storeConfig, shard, resources)) {
                                resources.bindWorkClassExecutionRegistry(workClasses);
                                store.recordControlSnapshot(controlSnapshot);
                                final DelayShard delayShard = new DelayShard(store, DelayShardConfig.defaults(), null,
                                        null, scheduleResolver());
                                final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease,
                                        new OwnerIdentityV1(bytes(16, 70), bytes(16, 71), lease.ownerEpoch(),
                                                Bytes.sha256(Bytes.utf8("pulsar-route-worker-fencing"))));
                                recoverRouteRecord(accepted, ownerAuthority, ownedShard, first.entry(), verificationKey,
                                        controlSnapshot, workClasses);
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
                                        nativeConsumer, guard, physicalTopic, RECEIVE_TIMEOUT,
                                        accepted.sourceAssignment(), workClasses, ownedShard, store, resources,
                                        ownerAuthority, verificationKey.getPublic());
                                runtimeOwnsConsumer = true;
                                final io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result =
                                        runUntilApplied(runtime);
                                if (result.status() != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                        .APPLIED_AND_ACKED
                                        || !(result.entry() instanceof SourceReplayRecord activeRecord)
                                        || !activeRecord.command().equals(afterRoute)
                                        || !(activeRecord.position() instanceof PulsarSourcePosition secondPosition)
                                        || secondPosition.compareWithinShard(firstPosition) <= 0) {
                                    throw new IllegalStateException(
                                            "Pulsar Route Worker active source did not apply and ACK the post-barrier record");
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
                                        new io.nereusstream.delay.ownership.OwnerDrainCoordinator.DrainRequest(
                                                System.currentTimeMillis() + 30_000, 0, checkpointPath, checkpointId),
                                        System::currentTimeMillis, () -> { });
                                if (drain.pendingCheckpointTask() != null || drain.finalCheckpointPath() == null
                                        || !Files.isDirectory(checkpointPath)
                                        || CheckpointFileInventory.collect(checkpointPath).isEmpty()
                                        || !ownerAuthority.current(shard).isEmpty()) {
                                    throw new IllegalStateException(
                                            "Pulsar Route Worker drain did not publish the final checkpoint or release the owner lease");
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

    private static PulsarSourcePosition requireCommand(final SourceRecordConsumer.PolledSourceRecord polled,
                                                       final PreparedCommand expected, final String phase) {
        if (!(polled.entry() instanceof SourceReplayRecord replay)
                || !expected.equals(replay.command())
                || !(replay.position() instanceof PulsarSourcePosition position)) {
            throw new IllegalStateException("Pulsar source returned an unexpected command " + phase);
        }
        return position;
    }

    private static SourceRecordConsumer.PolledSourceRecord poll(
            final PulsarClientArtifactSourceRecordConsumer source) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            final Optional<SourceRecordConsumer.PolledSourceRecord> polled = source.poll();
            if (polled.isPresent()) {
                return polled.get();
            }
        }
        throw new IllegalStateException("Pulsar Route source record did not become visible");
    }

    private static void recoverRouteRecord(final WorkerAssignment accepted,
                                           final OxiaOwnerLeaseStore authority,
                                           final OwnedDelayShard ownedShard,
                                           final SourceReplayEntry entry,
                                           final KeyPair verificationKey,
                                           final CompatibleControlSnapshotV1 controlSnapshot,
                                           final WorkClassExecutionRegistry workClasses) {
        final SourceReplayCursor<SourceReplayEntry> cursor = SourceReplayCursor.of(List.of(entry).iterator());
        final OwnerRecoveryCoordinator recovery = new OwnerRecoveryCoordinator(ownedShard, authority,
                accepted.sourceAssignment(), SourceReplaySuccessor.strictPulsarBatchMember(), cursor,
                verificationKey.getPublic(), controlSnapshot, System::currentTimeMillis,
                new ReplayTurnBudget(1, 1_000_000, TimeUnit.SECONDS.toNanos(10)), workClasses);
        OwnerRecoveryTurn turn;
        do {
            turn = recovery.runTurn();
        } while (!turn.complete());
        if (turn.outcomes().size() != 1 || !recovery.complete()) {
            throw new IllegalStateException("Pulsar Route Worker recovery did not apply exactly one record");
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
            if (result.status()
                    != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status()
                    != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException("Pulsar Route Worker source turn failed: " + result.status(),
                        result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Pulsar Route Worker source record did not become visible");
    }

    private static V1ScheduleResolver scheduleResolver() {
        final byte[] tuple = Bytes.utf8("pulsar-route-worker-canonical-lane-tuple-v1");
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        return new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(final ShardId shard,
                                                     final io.nereusstream.delay.protocol.DelayMessageId message,
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
        final java.util.EnumMap<WorkClass, WorkClassPolicy> policies =
                new java.util.EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, 8, 1_000_000, 1, 1_000_000, 1_000_000,
                    protectedClass ? 1 : 0, protectedClass ? 1 : 0, workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies,
                TimeUnit.SECONDS.toNanos(5), TimeUnit.SECONDS.toNanos(30), 16, 8_000_000), System::nanoTime);
    }

    private static RouteSnapshotV1 routeSnapshot(final String physicalTopicBase, final String physicalTopic,
                                                  final RouteIncarnation incarnation,
                                                  final PulsarSourcePosition position,
                                                  final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof,
                                                  final KeyPair signingKeys) {
        final long now = System.currentTimeMillis();
        final BrokerResourceIdentityV1 broker = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1(CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP));
        final ActivationBarrierV1 barrier = ActivationBarrierV1.pulsar(broker, 0,
                position.ledgerId(), position.entryId(), position.normalizedBatchIndex(), position.batchSize(),
                proof.connectionGeneration(), proof.attestationDigest());
        final RoutePartitionPolicyV1 policy = new RoutePartitionPolicyV1(0, barrier, zeroQuota(),
                proof.connectionGeneration(), proof.attestationDigest());
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(now - 100, now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("pulsar-route-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("pulsar-route-issued-at")), 0, null);
        final PulsarIngressRouteResourceV1 ingress = new PulsarIngressRouteResourceV1(CLUSTER, physicalTopicBase,
                List.of(new PulsarPhysicalPartitionIdentityV1(0, physicalTopic, INCARNATION,
                        CREATION_TIMESTAMP)));
        return RouteSnapshotV1.create(incarnation, bytes(32, 1), bytes(32, 2), RouteLifecycleV1.ACTIVE_FOR_NEW,
                now + 30_000, ingress, RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1), 1, List.of(policy),
                100, 200, 1024, 4096, 10, 8192, 500, now - 1_000, now + 60_000,
                new IngressCredentialBindingRefV1(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                Bytes.sha256(Bytes.utf8("pulsar-route-prerequisite")), issuedAt, 1, signingKeys.getPrivate());
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest placementRequest(final long now) {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(0,
                Bytes.sha256(Bytes.utf8("pulsar-route-worker-assignment")), 1,
                Bytes.sha256(Bytes.utf8("pulsar-route-worker-capacity")), 1,
                List.of(new WorkerPlacementPolicy.WorkerCandidate("pulsar-route-worker", capacity(2),
                        CapacityVectorV1.empty(), 0, 16, 0, 16, WorkerLoadVector.empty(), WorkerLoadVector.empty(),
                        now, true, 0)), capacity(1), CapacityVectorV1.empty(), CapacityVectorV1.empty(), null,
                now, 0, 0);
    }

    private static void requireRouteAssignment(final WorkerAssignment assignment, final RouteSnapshotV1 snapshot,
                                               final PulsarSourcePosition firstPosition,
                                               final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof) {
        if (!assignment.routeBound() || !Arrays.equals(snapshot.snapshotDigest(), assignment.routeSnapshotDigest())
                || !(assignment.sourceAssignment().activationBarrier() instanceof PulsarActivationBarrier barrier)
                || !Arrays.equals(barrier.brokerResourceIncarnation(), INCARNATION)
                || !barrier.physicalTopic().equals(firstPosition.physicalTopic())
                || barrier.ledgerId() != firstPosition.ledgerId() || barrier.entryId() != firstPosition.entryId()
                || barrier.normalizedLastBatchIndex() != firstPosition.normalizedBatchIndex()
                || barrier.batchSize() != firstPosition.batchSize()
                || barrier.guardedSourceConnectionGeneration() != proof.connectionGeneration()
                || !Arrays.equals(barrier.resourceGuardAttestationDigest(), proof.attestationDigest())) {
            throw new IllegalStateException("Oxia Worker assignment did not retain the signed Pulsar Route barrier");
        }
    }

    private static void send(final PulsarClient client, final TopicResourceGuard guard, final String physicalTopic,
                             final PreparedCommand command, final String producerName) throws Exception {
        final PulsarClientArtifactSendTransport transport = new PulsarClientArtifactSendTransport(
                PulsarClientArtifactProducerFactory.create(client, CLUSTER, INCARNATION, physicalTopic,
                        CREATION_TIMESTAMP, producerName), CLUSTER, INCARNATION, physicalTopic,
                CREATION_TIMESTAMP, 0);
        try {
            final PulsarSendResult result = transport.send(new PulsarSendRequest(CLUSTER, INCARNATION, physicalTopic,
                    CREATION_TIMESTAMP, 0, command.commandId(),
                    io.nereusstream.delay.protocol.CommandCodec.encodeFrameV1(command)))
                    .toCompletableFuture().get(15, TimeUnit.SECONDS);
            if (result.disposition() != PulsarSendResult.Disposition.PERSISTED) {
                throw new IllegalStateException("guarded Pulsar Route producer did not persist: "
                        + result.disposition());
            }
        } finally {
            transport.close();
        }
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("destination-" + identity), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)), ProfileKindV1.DESTINATION);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(Bytes.utf8("retry-" + identity), 1,
                Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final io.nereusstream.delay.protocol.ScheduleIntentV1 intent =
                io.nereusstream.delay.protocol.ScheduleIntentV1.create(destination, retryPolicy, deliverAt,
                        deliverAt + 10_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0],
                        Bytes.utf8("source-" + identity), null,
                        AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of())), null, null);
        return PreparedCommand.scheduleV1(shard, intent, deliverAt + 20_000);
    }

    private static QuotaGrantRefV1 zeroQuota() {
        return new QuotaGrantRefV1(bytes(32, 20), 1, new PublishAdmissionBody.ChargeVector(
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    }

    private static CapacityVectorV1 capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimensionV1.COUNT];
        values[CapacityDimensionV1.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVectorV1(values);
    }

    private static void requireAcked(final SourceAcknowledgement.AcknowledgementResult result) {
        if (result.disposition() != SourceAcknowledgement.Disposition.ACKED) {
            throw new IllegalStateException("Pulsar Route source record was not ACKED: " + result.disposition(),
                    result.failure());
        }
    }

    private static void createTopic(final HttpClient client, final String adminUrl, final String topicBase,
                                    final String physicalTopic)
            throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topicBase + "/partitions";
        for (int attempt = 0; attempt < 40; attempt++) {
            final HttpResponse<String> response = request(client, path, "PUT", "1");
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                break;
            }
            if (response.statusCode() != 409 && response.statusCode() != 412
                    && response.statusCode() != 503) {
                throw failure("create Route partitioned topic", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
            if (attempt == 39) {
                throw failure("create Route partitioned topic", response);
            }
        }
        final String guardPath = adminUrl + "/admin/v2/persistent/public/default/" + physicalTopic + "/resourceGuard";
        final String guardBody = "{\"nereus.resource.guard.version\":\"1\","
                + "\"nereus.resource.incarnation\":\""
                + Base64.getUrlEncoder().withoutPadding().encodeToString(INCARNATION) + "\","
                + "\"nereus.resource.created-at\":\""
                + Long.toUnsignedString(CREATION_TIMESTAMP) + "\"}";
        for (int attempt = 0; attempt < 40; attempt++) {
            final HttpResponse<String> response = request(client, guardPath, "PUT", guardBody);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return;
            }
            if (response.statusCode() != 409 && response.statusCode() != 412
                    && response.statusCode() != 503) {
                throw failure("stamp Route physical topic resource guard", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
            if (attempt == 39) {
                throw failure("stamp Route physical topic resource guard", response);
            }
        }
    }

    private static void deleteTopicIfPresent(final HttpClient client, final String adminUrl, final String topic) {
        try {
            final HttpResponse<String> response = request(client,
                    adminUrl + "/admin/v2/persistent/public/default/" + topic + "/partitions?force=true",
                    "DELETE", "");
            if (response.statusCode() >= 300 && response.statusCode() != 404) {
                System.err.println("Pulsar Route smoke cleanup could not delete topic: " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("Pulsar Route smoke cleanup failed: " + failure.getMessage());
        }
    }

    private static HttpResponse<String> request(final HttpClient client, final String path, final String method,
                                                final String body) throws Exception {
        final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(path))
                .header("Content-Type", "application/json");
        final HttpRequest request = "DELETE".equals(method)
                ? builder.DELETE().build()
                : builder.PUT(HttpRequest.BodyPublishers.ofString(body)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static IllegalStateException failure(final String operation, final HttpResponse<String> response) {
        return new IllegalStateException(operation + " failed with HTTP " + response.statusCode()
                + ": " + response.body());
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

    private static byte[] digest(final int seed) {
        final byte[] result = new byte[32];
        Arrays.fill(result, (byte) seed);
        return result;
    }
}
