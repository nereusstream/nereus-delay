package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import io.nereusstream.delay.adapter.DestinationPublishResult;
import io.nereusstream.delay.adapter.PinnedPulsarDestinationAdapter;
import io.nereusstream.delay.adapter.PulsarDestinationRequest;
import io.nereusstream.delay.adapter.PulsarSendRequest;
import io.nereusstream.delay.adapter.PulsarSendAckEvidence;
import io.nereusstream.delay.adapter.PulsarSendResult;
import io.nereusstream.delay.adapter.PulsarTargetResource;
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
import io.nereusstream.delay.ownership.PublishAdmissionWorkClassExecutor;
import io.nereusstream.delay.ownership.ReplayTurnBudget;
import io.nereusstream.delay.ownership.ShardLogMutationAppender;
import io.nereusstream.delay.ownership.ShardLifecycleState;
import io.nereusstream.delay.ownership.SourceAssignment;
import io.nereusstream.delay.ownership.SourceReplayMutation;
import io.nereusstream.delay.ownership.SourceReplayCursor;
import io.nereusstream.delay.ownership.SourceReplayEntry;
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
import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.AuthorIdentity;
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
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PublishEvidenceKindV1;
import io.nereusstream.delay.protocol.PublishEvidenceV1;
import io.nereusstream.delay.protocol.PublishOutcomeBody;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PulsarMetadataV1;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.ReadyCertificateV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.V1ScheduleResolver;
import io.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import io.nereusstream.delay.scheduler.SchedulerBudget;
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
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.GuardedMessageId;
import org.apache.pulsar.client.api.GuardedSendSuccessEvidence;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
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
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Real Pulsar recovery, active Worker apply and synchronous ACK smoke. */
public final class PulsarClientArtifactWorkerSmoke {
    private static final String CLUSTER = "standalone";
    private static final byte[] INCARNATION = digest(43);
    private static final long CREATION_TIMESTAMP = 2001L;
    private static final byte[] DESTINATION_INCARNATION = digest(17);
    private static final long DESTINATION_CREATION_TIMESTAMP = 1001L;
    private static final long LEASE_DURATION_MS = 60_000;
    private static final long DUE_DISCOVERY_MAX_BYTES = 900_000;

    private PulsarClientArtifactWorkerSmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3 && arguments.length != 4 && arguments.length != 5) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <topic> "
                    + "[run|prepare|resume] [destination-topic]");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String mode = arguments.length >= 4 ? arguments[3] : "run";
        final String destinationTopic = arguments.length == 5 && !arguments[4].isBlank() ? arguments[4] : null;
        if (!mode.equals("run") && !mode.equals("prepare") && !mode.equals("resume")) {
            throw new IllegalArgumentException("unknown Worker smoke mode: " + mode);
        }
        final String topic = mode.equals("run") ? arguments[2] + "-worker-" + UUID.randomUUID() : arguments[2];
        final String physicalTopic = "persistent://public/default/" + topic;
        final String destinationPhysicalTopic = destinationTopic == null ? null
                : "persistent://public/default/" + destinationTopic;
        final HttpClient admin = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        createTopic(admin, adminUrl, topic, mode.equals("resume"));
        if (destinationTopic != null && !mode.equals("prepare")) {
            createTopic(admin, adminUrl, destinationTopic, false, DESTINATION_INCARNATION,
                    DESTINATION_CREATION_TIMESTAMP);
        }
        try {
            final var clientBuilder = PulsarClient.builder().serviceUrl(serviceUrl);
            final String listenerName = System.getenv("NEREUS_DELAY_PULSAR_LISTENER_NAME");
            if (listenerName != null && !listenerName.isBlank()) {
                clientBuilder.listenerName(listenerName);
            }
            try (PulsarClient client = clientBuilder.build()) {
                if (mode.equals("prepare")) {
                    prepareWorkerRecord(client, physicalTopic);
                } else {
                    runWorker(client, physicalTopic, !mode.equals("resume"), destinationPhysicalTopic);
                }
            }
        } finally {
            if (!mode.equals("prepare")) {
                deleteTopicIfPresent(admin, adminUrl, topic);
                if (destinationTopic != null) {
                    deleteTopicIfPresent(admin, adminUrl, destinationTopic);
                }
            }
        }
    }

    private static void prepareWorkerRecord(final PulsarClient client, final String physicalTopic) throws Exception {
        final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
        final ShardId shard = restartShard(physicalTopic);
        send(client, guard, physicalTopic, command(shard, "worker-restart-prepared"),
                "worker-restart-preparation-producer");
        System.out.println("Pulsar Worker restart preparation passed: one guarded record persisted before broker restart");
    }

    private static void runWorker(final PulsarClient client, final String physicalTopic,
                                  final boolean seedRecovery, final String destinationPhysicalTopic)
            throws Exception {
        final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
        final ShardId shard = seedRecovery ? new ShardId(RouteIncarnation.random(), 0) : restartShard(physicalTopic);
        if (seedRecovery) {
            final PreparedCommand recoveryCommand = command(shard, "worker-recovery");
            send(client, guard, physicalTopic, recoveryCommand, "worker-recovery-producer");
        }

        final OxiaSyncOwnerLeaseBackend.ClientHandle oxia = connectOxiaIfConfigured();
        try {
            final String subscription = "nereus-delay-worker-" + UUID.randomUUID();
            final GuardedConsumer<byte[]> rawNativeConsumer = PulsarClientArtifactSourceConsumerFactory.create(
                    client, guard, physicalTopic, subscription);
            final AtomicBoolean sourceAckResponseLossObserved = new AtomicBoolean();
            final GuardedConsumer<byte[]> nativeConsumer = hasSourceAckResponseLoss()
                    ? responseLossConsumer(rawNativeConsumer, sourceAckResponseLossObserved) : rawNativeConsumer;
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof =
                    PulsarClientArtifactRecoverySourcePositioner.seekAfter(nativeConsumer, guard, physicalTopic, shard,
                            Optional.empty(), Duration.ofSeconds(5));
            final SourceAssignment sourceAssignment = new SourceAssignment(shard,
                    Bytes.sha256(Bytes.utf8("pulsar-worker-assignment")), 1,
                    PulsarActivationBarrier.empty(shard, INCARNATION, physicalTopic,
                            proof.connectionGeneration(), proof.attestationDigest()));
            final WorkerAssignmentAuthority assignmentAuthority = oxia == null
                    ? new InMemoryWorkerAssignmentAuthority()
                    : new OxiaSyncWorkerAssignmentBackend(oxia,
                            "nereus-delay/pulsar-worker-placement/" + UUID.randomUUID());
            final WorkerAssignment assignmentProjection = publishAssignment(assignmentAuthority,
                    sourceAssignment, oxia != null);
            final SourceAssignment assignment = assignmentProjection.sourceAssignment();
            final OxiaOwnerLeaseStore authority = oxia == null
                    ? new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore())
                    : new OxiaOwnerLeaseStore(oxia.backend());
            final long ownerNow = System.currentTimeMillis();
            final byte[] sessionIdentity = oxia == null
                    ? Bytes.sha256(Bytes.utf8("pulsar-worker-session")) : oxia.sessionIdentity();
            final OwnerLease lease = authority.acquire(assignment, "pulsar-worker", sessionIdentity,
                    ownerNow, LEASE_DURATION_MS).orElseThrow();
            final WorkClassExecutionRegistry workClasses = workClasses();
            final KeyPair verificationKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshot(shard);
            final Path root = Files.createTempDirectory("nereus-delay-pulsar-worker-");
            boolean runtimeDrained = false;
            try {
                final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
                try (SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                     ShardStore store = ShardStore.open(storeConfig, shard, resources)) {
                    resources.bindWorkClassExecutionRegistry(workClasses);
                    store.recordControlSnapshot(controlSnapshot);
                    final DelayShard delayShard = new DelayShard(store, DelayShardConfig.defaults(), null, null,
                            scheduleResolver(destinationPhysicalTopic));
                    final OwnerIdentityV1 ownerIdentity = new OwnerIdentityV1(bytes(16, 70), bytes(16, 71),
                            lease.ownerEpoch(), Bytes.sha256(Bytes.utf8("pulsar-worker-fencing")));
                    final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease, ownerIdentity);

                    try (PulsarClientArtifactRecoverySourceCursor recovery =
                                 new PulsarClientArtifactRecoverySourceCursor(nativeConsumer, guard, assignment,
                                         physicalTopic, Duration.ofMillis(250))) {
                        final SourceReplayCursor<SourceReplayEntry> cursor = SourceReplayCursor.of(recovery);
                        final OwnerRecoveryCoordinator coordinator = new OwnerRecoveryCoordinator(ownedShard, authority,
                                assignment, SourceReplaySuccessor.strictPulsarBatchMember(), cursor,
                                verificationKey.getPublic(), controlSnapshot, System::currentTimeMillis,
                                new ReplayTurnBudget(1, 1_000_000, TimeUnit.SECONDS.toNanos(10)), workClasses);
                        OwnerRecoveryTurn turn;
                        do {
                            turn = coordinator.runTurn();
                        } while (!turn.complete());
                        if (turn.outcomes().size() != 1 || !coordinator.complete()
                                || ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                || !(ownedShard.lastCatchupPosition() instanceof PulsarSourcePosition recovered)) {
                            throw new IllegalStateException("Pulsar Worker recovery did not activate at one exact record");
                        }
                        if (!recovered.shardId().equals(shard)
                                || !Arrays.equals(recovered.brokerResourceIncarnation(), INCARNATION)
                                || !recovered.physicalTopic().equals(physicalTopic)) {
                            throw new IllegalStateException("Pulsar Worker recovery position identity changed");
                        }

                        final PreparedCommand activeCommand = command(shard, "worker-active");
                        send(client, guard, physicalTopic, activeCommand, "worker-active-producer");
                        final PreparedCommand physicalCommand = destinationPhysicalTopic == null ? null
                                : command(shard, "worker-physical-publish", Bytes.utf8(
                                "pulsar-worker-source-applied-payload"), 2_000);
                        final PulsarSourcePosition physicalSchedulePosition = physicalCommand == null ? null
                                : sendAndPosition(client, guard, physicalTopic, physicalCommand,
                                "worker-physical-schedule-producer");
                        final PhysicalPublishBridge physicalBridge = physicalCommand == null ? null
                                : createPhysicalPublishBridge(client, nativeConsumer, physicalTopic, shard,
                                physicalSchedulePosition, destinationPhysicalTopic, store, ownedShard, ownerIdentity,
                                authority, workClasses, verificationKey);
                        try (physicalBridge) {
                            final WorkerShardRuntime runtime = PulsarClientArtifactWorkerSourceFactory.create(
                                    nativeConsumer, guard, physicalTopic, Duration.ofMillis(250), assignment,
                                    workClasses, ownedShard, store, resources, authority,
                                    verificationKey.getPublic(), null, null, null, null,
                                    physicalBridge == null ? null : physicalBridge.executor());
                            try {
                            final io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result =
                                    runUntilApplied(runtime);
                            if (!(result.entry() instanceof SourceReplayRecord record)
                                    || !record.command().equals(activeCommand)
                                    || !(record.position() instanceof PulsarSourcePosition activePosition)) {
                                throw new IllegalStateException(
                                        "Pulsar Worker active turn did not apply the second record");
                            }
                            final var applied = store.appliedShardLogPosition();
                            if (!(applied instanceof PulsarSourcePosition appliedPosition)
                                    || !appliedPosition.equals(activePosition)
                                    || activePosition.compareTo(recovered) <= 0) {
                                throw new IllegalStateException(
                                        "Pulsar Worker Store did not persist the active position");
                            }
                            if (physicalBridge != null) {
                                final io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult physicalSchedule =
                                        runUntilApplied(runtime);
                                if (!(physicalSchedule.entry() instanceof SourceReplayRecord physicalRecord)
                                        || !physicalRecord.command().equals(physicalCommand)
                                        || !(physicalSchedule.entry().position() instanceof PulsarSourcePosition appliedPhysical)) {
                                    throw new IllegalStateException(
                                            "Pulsar Worker physical Schedule was not source-applied");
                                }
                                if (!appliedPhysical.equals(physicalSchedulePosition)) {
                                    throw new IllegalStateException(
                                            "Pulsar Worker physical Schedule Source Position changed across apply");
                                }
                                runSourceAppliedPhysicalPublish(runtime, delayShard, ownedShard, ownerIdentity, authority,
                                        store, workClasses, verificationKey, physicalBridge, physicalCommand,
                                        physicalSchedulePosition, client);
                            }
                            final Path checkpointPath = root.resolve("worker-final-checkpoint");
                            final byte[] checkpointId = Arrays.copyOf(
                                    Bytes.sha256(Bytes.utf8("pulsar-worker-final-checkpoint")), 16);
                            final var drain = runtime.drain(
                                    new io.nereusstream.delay.ownership.OwnerDrainCoordinator.DrainRequest(
                                            System.currentTimeMillis() + 30_000, 0, checkpointPath, checkpointId),
                                    System::currentTimeMillis, () -> { });
                            if (drain.pendingCheckpointTask() != null || drain.finalCheckpointPath() == null
                                    || !Files.isDirectory(checkpointPath)
                                    || CheckpointFileInventory.collect(checkpointPath).isEmpty()
                                    || !authority.current(shard).isEmpty()) {
                                throw new IllegalStateException(
                                        "Pulsar Worker drain did not publish the final checkpoint and release the exact owner lease");
                            }
                            runtimeDrained = true;
                            System.out.println("Pulsar Worker vertical smoke passed: assignment recovery ledger/entry="
                                    + recovered.ledgerId() + "/" + recovered.entryId()
                                    + ", active apply ledger/entry=" + activePosition.ledgerId() + "/"
                                    + activePosition.entryId() + ", guarded SUBSCRIBE, RocksDB WriteBatch, ACK, "
                                    + "and final checkpoint");
                            if (oxia != null) {
                                System.out.println("Pulsar Worker authority smoke passed: real Oxia session-bound lease");
                            }
                            if (hasSourceAckResponseLoss()) {
                                if (!sourceAckResponseLossObserved.get()) {
                                    throw new IllegalStateException(
                                            "Pulsar Worker source ACK response-loss wrapper did not lose a response");
                                }
                                System.out.println("Pulsar Worker source ACK response-loss smoke passed: real ACK "
                                        + "was accepted before the local response was discarded, and the same "
                                        + "source record was ACKed on the next bounded Worker turn");
                            }
                            } finally {
                                if (!runtimeDrained) {
                                    closeNative(nativeConsumer);
                                }
                            }
                        }
                    }
                }
            } finally {
                deleteTree(root);
                if (!runtimeDrained) {
                    closeNative(nativeConsumer);
                }
            }
        } finally {
            if (oxia != null) {
                oxia.close();
            }
        }
    }

    /**
     * Exercises the source-ordered Admission to physical destination bridge.
     * The Claim and certificate are deliberately supplied by this bounded
     * smoke authority; the source log remains authoritative for Admission and
     * Outcome application.
     */
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
            final PulsarSourcePosition physicalSchedulePosition,
            final PulsarClient client) throws Exception {
        runSourceAppliedPhysicalPublish(runtime, delayShard, ownedShard, ownerIdentity, authority, store,
                workClasses, verificationKey, bridge, physicalCommand.delayMessageId(), physicalSchedulePosition, client,
                null, 1_000_000);
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
            final DelayMessageId physicalMessageId,
            final PulsarSourcePosition physicalSchedulePosition,
            final PulsarClient client,
            final byte[] payloadOverride,
            final long workClassBytes) throws Exception {
        if (workClassBytes <= 0 || (payloadOverride != null && payloadOverride.length > workClassBytes)) {
            throw new IllegalArgumentException("Pulsar physical publish payload exceeds the admitted work-class bytes");
        }
        final var message = delayShard.getMessage(physicalMessageId);
        if (message == null || message.status() != MessageStatus.SCHEDULED
                || !message.laneId().equals(bridge.laneId())) {
            throw new IllegalStateException("source-applied physical Schedule did not create the expected SCHEDULED message");
        }
        delayShard.activateLaneReadiness(bridge.laneId(), bridge.laneIncarnation(), bridge.channel(),
                bridge.readyCertificate(), bridge.evidenceCursors());
        final var lane = delayShard.getLane(bridge.laneId());
        if (lane == null || !lane.schedulable()) {
            throw new IllegalStateException("source-applied physical Lane did not become schedulable");
        }
        bindActiveOwnerPublishGraph(runtime, ownedShard, ownerIdentity, authority, store, workClasses,
                verificationKey, bridge, workClassBytes);
        waitUntil(message.deliverAtEpochMs());

        final byte[] payload = payloadOverride == null ? message.payload() : Bytes.copy(payloadOverride);
        WorkerShardRuntime.DueClaimPublishPhysicalTurn dueClaimPublish = null;
        // A large payload can exceed the Lane's initial DRR quantum. Spend a
        // bounded number of ordinary due turns to accumulate the exact head
        // credit; keep every submitted due task inside its WorkClass cap.
        for (int schedulerTurn = 0; schedulerTurn < 32; schedulerTurn++) {
            final long dueEarliest = Math.max(System.currentTimeMillis(), message.deliverAtEpochMs());
            final TrustedUtcIntervalEvidence dueEvidence = evidence(dueEarliest, dueEarliest + 500,
                    "pulsar-worker-due-clock");
            final long dueTaskRequestBytes = Math.addExact(44L, dueEvidence.canonicalBytes().length);
            final long dueDiscoveryBytes = Math.min(Math.max(DUE_DISCOVERY_MAX_BYTES, (long) payload.length),
                    Math.subtractExact(workClassBytes, dueTaskRequestBytes));
            if (dueDiscoveryBytes <= 0) {
                throw new IllegalArgumentException(
                        "Pulsar physical publish work-class bytes cannot contain due discovery request");
            }
            dueClaimPublish = runtime.runDueClaimPublishPhysicalTurn(dueEvidence,
                    new SchedulerBudget(1, dueDiscoveryBytes, TimeUnit.SECONDS.toNanos(2)),
                    message.expireAtEpochMs() - 1, claimCharge(payload.length), System::currentTimeMillis,
                    new SchedulerBudget(1, workClassBytes, TimeUnit.SECONDS.toNanos(2)), 16,
                    new SchedulerBudget(1, workClassBytes, TimeUnit.SECONDS.toNanos(2)), 16,
                    ignored -> Optional.of(payload));
            if (dueClaimPublish.dueClaimPublishTurn().claimResult().isPresent()) {
                break;
            }
        }
        final var dueClaim = dueClaimPublish.dueClaimPublishTurn();
        final var claimResult = dueClaim.claimResult().orElseThrow(
                () -> new IllegalStateException("provider-driven Worker turn did not return a Claim result"));
        if (claimResult.kind() != ClaimHandoffWorkClassExecutor.ResultKind.CLAIMED) {
            throw new IllegalStateException("provider-driven Worker Claim was not admitted: " + claimResult.kind());
        }
        final var admissionSubmission = dueClaim.publishSubmission().orElseThrow(
                () -> new IllegalStateException("provider-driven Worker turn did not queue Publish Admission"));
        final var admissionResult = admissionSubmission.result().orElseThrow(
                () -> new IllegalStateException("provider-driven Publish Admission has no result"));
        final WorkerShardRuntime.SourceBoundPhysicalPublishTurn physicalTurn =
                dueClaimPublish.physicalTurn().orElseThrow(
                        () -> new IllegalStateException("provider-driven Worker turn did not start physical publish"));
        final PulsarSourcePosition admissionPosition;
        if (admissionResult.kind()
                == io.nereusstream.delay.ownership.PublishAdmissionWorkClassExecutor.ResultKind.ENQUEUED) {
            if (bridge.admissionResponseLoss()) {
                throw new IllegalStateException(
                        "Pulsar Worker admission response-loss smoke did not produce UNKNOWN admission");
            }
            if (!(admissionResult.sourcePosition() instanceof PulsarSourcePosition persistedAdmissionPosition)
                    || persistedAdmissionPosition.compareTo(physicalSchedulePosition) <= 0) {
                throw new IllegalStateException("Pulsar Worker provider-driven Publish Admission was not source-bound: "
                        + admissionResult.kind());
            }
            admissionPosition = persistedAdmissionPosition;
        } else if (admissionResult.kind()
                == io.nereusstream.delay.ownership.PublishAdmissionWorkClassExecutor.ResultKind.UNKNOWN) {
            final var recoveredAttempt = physicalTurn.attempt().orElseThrow(
                    () -> new IllegalStateException("UNKNOWN Publish Admission did not recover a PUBLISHING attempt: "
                            + physicalTurn.status() + "/" + physicalTurn.failure()));
            final SourcePosition recoveredPosition = SourcePositionCodec.decode(recoveredAttempt.sourcePosition());
            if (!(recoveredPosition instanceof PulsarSourcePosition recoveredAdmissionPosition)
                    || recoveredAdmissionPosition.compareTo(physicalSchedulePosition) <= 0) {
                throw new IllegalStateException("Pulsar Worker recovered UNKNOWN Publish Admission was not source-bound");
            }
            admissionPosition = recoveredAdmissionPosition;
            System.out.println("Pulsar Worker recovered UNKNOWN Publish Admission from exact source mutation: "
                    + admissionPosition);
            if (bridge.admissionResponseLoss()) {
                if (!bridge.admissionResponseLossObserved()) {
                    throw new IllegalStateException(
                            "Pulsar Worker admission response-loss wrapper did not discard a persisted response");
                }
                System.out.println("Pulsar Worker Publish Admission response-loss smoke passed: the real Shard Log "
                        + "mutation was persisted, its local append response was discarded, and exact source replay "
                        + "recovered the PUBLISHING admission");
            }
        } else {
            throw new IllegalStateException("Pulsar Worker provider-driven Publish Admission was not source-bound: "
                    + admissionResult.kind());
        }
        final PublishAdmissionBody admissionBody = PublishAdmissionBody.decode(
                admissionResult.mutation().canonicalBody());
        final byte[] publishAttemptId = admissionBody.publishAttemptId();
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
                throw new IllegalStateException("Pulsar Worker Publish Outcome source turn failed: "
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
            throw new IllegalStateException("Pulsar Worker Publish Outcome was not a definitive PUBLISHED result");
        }
        final PublishEvidenceV1 evidence = PublishEvidenceV1.decode(outcome.evidence());
        if (evidence.evidenceKind() != PublishEvidenceKindV1.PULSAR_SEND_ACK
                || evidence.verificationStatus() != EvidenceVerificationStatusV1.VERIFIED_PUBLISHED) {
            throw new IllegalStateException("Pulsar Worker Publish Outcome carried the wrong evidence branch");
        }
        evidence.requireBusinessMutation(publishAttemptId, true);
        if (!(outcomeRecord.position() instanceof PulsarSourcePosition outcomePosition)) {
            throw new IllegalStateException("source-applied typed Publish Outcome has a non-Pulsar source position");
        }
        final var finalMessage = delayShard.getMessage(physicalMessageId);
        if (finalMessage == null || finalMessage.status() != MessageStatus.PUBLISHED
                || delayShard.findOpenPublishAttempt(publishAttemptId) != null) {
            throw new IllegalStateException("source-applied typed Publish Outcome did not close the PUBLISHED attempt");
        }
        requirePayload(client, bridge.destinationPhysicalTopic(), payload);
        if (bridge.destinationResponseLoss()) {
            if (!bridge.destinationResponseEvidenceResolved()) {
                throw new IllegalStateException(
                        "Pulsar Worker destination response-loss provider did not resolve evidence");
            }
            System.out.println("Pulsar Worker destination response-loss smoke passed: real SEND persisted the "
                    + "exact payload, the local response was discarded, and typed PULSAR_SEND_ACK evidence "
                    + "resolved the source-applied PUBLISHED Outcome");
        }
        System.out.println("Pulsar Worker source-applied physical publish passed: Admission source ledger="
                + admissionPosition.ledgerId() + "/" + admissionPosition.entryId()
                + ", typed PULSAR_SEND_ACK target ledger/entry=" + branchNumber(evidence, 3) + "/"
                + branchNumber(evidence, 4) + ", Outcome source ledger=" + outcomePosition.ledgerId()
                + "/" + outcomePosition.entryId() + ", exact payload readback");
    }

    private static void bindActiveOwnerPublishGraph(
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

    private static void bindActiveOwnerPublishGraph(
            final WorkerShardRuntime runtime,
            final OwnedDelayShard ownedShard,
            final OwnerIdentityV1 ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final long workClassBytes) {
        final WorkerSchedulingRuntime scheduling = WorkerSchedulingRuntime.openForActiveOwnerFromTypedLanes(
                workClasses, ownedShard, authority, store, ownerIdentity, List.of(bridge.laneId()), 8);
        final ClaimExecutionAdmission permits = new ClaimExecutionAdmission(1, workClassBytes);
        permits.registerShard(new ClaimExecutionAdmission.ShardSpec(runtime.shardId(), 1, workClassBytes));
        permits.registerLane(new ClaimExecutionAdmission.LaneSpec(runtime.shardId(), bridge.laneId(),
                bridge.laneIncarnation(), 0, 0, 1, workClassBytes));
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
                            evidence(earliest, latest, "pulsar-worker-provider-preparation"), retryUntil, 1,
                            verificationKey.getPrivate(), System::currentTimeMillis));
                });
        runtime.bindActiveOwnerPublishGraph(scheduling, commandRuntime, preparation);
    }

    private static void waitForPhysicalCompletion(final WorkerPhysicalPublishExecutor.Submission submission)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (submission.state() == WorkerPhysicalPublishExecutor.SubmissionState.PENDING
                && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(25);
        }
        if (submission.state() != WorkerPhysicalPublishExecutor.SubmissionState.OUTCOME_HANDOFF_QUEUED) {
            throw new IllegalStateException("Pulsar Worker physical submission did not reach Outcome handoff: "
                    + submission.state() + "/" + submission.failure());
        }
    }

    private static Optional<PulsarClientArtifactDestinationTransport.ResolvedPublish> resolveDestinationResponseLoss(
            final PulsarDestinationRequest request, final byte[] preparedPublishHash,
            final byte[] producerNameHash, final AtomicReference<GuardedMessageId> responseLostMessage,
            final AtomicBoolean responseEvidenceResolved) {
        final GuardedMessageId messageId = responseLostMessage.get();
        if (messageId == null) {
            return Optional.empty();
        }
        final TopicResourceGuard expectedGuard = new TopicResourceGuard(request.authenticatedClusterId(),
                request.resourceIncarnation(), request.physicalTopicCreationTimestamp());
        if (!expectedGuard.equals(messageId.resourceGuard()) || !request.physicalTopic().equals(messageId.physicalTopic())
                || request.partition() != messageId.partition() || !(messageId instanceof MessageIdAdv advanced)
                || advanced.getLedgerId() < 0 || advanced.getEntryId() < 0
                || advanced.getPartitionIndex() != request.partition()) {
            return Optional.empty();
        }
        final GuardedSendSuccessEvidence evidence = messageId.responseEvidence();
        final TopicResourceGuardAttestation expectedAttestation = new TopicResourceGuardAttestation(
                expectedGuard, request.physicalTopic(), request.partition());
        if (evidence == null || !expectedAttestation.equals(evidence.attestation())
                || evidence.ledgerId() != advanced.getLedgerId() || evidence.entryId() != advanced.getEntryId()
                || evidence.brokerEntryTimestamp() != messageId.brokerEntryTimestamp()) {
            return Optional.empty();
        }
        final int rawBatchIndex = advanced.getBatchIndex();
        final int rawBatchSize = advanced.getBatchSize();
        final int normalizedBatchIndex = rawBatchIndex < 0 ? 0 : rawBatchIndex;
        if (rawBatchIndex >= 0 && (rawBatchSize <= 0
                || Integer.compareUnsigned(rawBatchIndex, rawBatchSize) >= 0)) {
            return Optional.empty();
        }
        final PublishEvidenceV1 typed = PulsarSendAckEvidence.published(request, preparedPublishHash,
                producerNameHash, advanced.getLedgerId(), advanced.getEntryId(), normalizedBatchIndex,
                evidence.brokerEntryTimestamp(), evidence.sequenceId(), evidence.authenticatedResponseCommandSha256());
        typed.requireBusinessMutation(request.publishAttemptId(), true);
        responseEvidenceResolved.set(true);
        return Optional.of(new PulsarClientArtifactDestinationTransport.ResolvedPublish(
                typed, evidence.brokerEntryTimestamp()));
    }

    @SuppressWarnings("unchecked")
    private static Producer<byte[]> responseLossProducer(final Producer<byte[]> delegate,
                                                          final AtomicReference<GuardedMessageId> responseLostMessage) {
        return (Producer<byte[]>) Proxy.newProxyInstance(
                PulsarClientArtifactWorkerSmoke.class.getClassLoader(), new Class<?>[]{Producer.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("newMessage") && method.getParameterCount() == 0) {
                        final TypedMessageBuilder<byte[]> builder = (TypedMessageBuilder<byte[]>) invoke(
                                delegate, method, arguments);
                        return responseLossBuilder(builder, responseLostMessage);
                    }
                    return invoke(delegate, method, arguments);
                });
    }

    @SuppressWarnings("unchecked")
    private static TypedMessageBuilder<byte[]> responseLossBuilder(
            final TypedMessageBuilder<byte[]> delegate, final AtomicReference<GuardedMessageId> responseLostMessage) {
        return (TypedMessageBuilder<byte[]>) Proxy.newProxyInstance(
                PulsarClientArtifactWorkerSmoke.class.getClassLoader(), new Class<?>[]{TypedMessageBuilder.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("value") && method.getParameterCount() == 1) {
                        invoke(delegate, method, arguments);
                        return proxy;
                    }
                    if (method.getName().equals("sendAsync") && method.getParameterCount() == 0) {
                        final CompletableFuture<MessageId> sent = (CompletableFuture<MessageId>) invoke(
                                delegate, method, arguments);
                        return sent.thenCompose(messageId -> {
                            if (!(messageId instanceof GuardedMessageId guarded)) {
                                return CompletableFuture.failedFuture(new IllegalStateException(
                                        "Pulsar Worker response-loss wrapper observed an unguarded MessageId"));
                            }
                            responseLostMessage.set(guarded);
                            return CompletableFuture.failedFuture(new IllegalStateException(
                                    "simulated committed Pulsar Worker destination response loss"));
                        });
                    }
                    return invoke(delegate, method, arguments);
                });
    }

    private static boolean hasWorkerDestinationResponseLoss() {
        return "1".equals(System.getenv("NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS"));
    }

    private static boolean hasWorkerAdmissionResponseLoss() {
        return "1".equals(System.getenv("NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS"));
    }

    /**
     * Injects one client-side committed-response loss after the real guarded
     * Shard Log producer has returned PERSISTED. The next mutation, normally
     * PUBLISH_OUTCOME, is left untouched so this remains a bounded admission
     * recovery exercise rather than a general source-write failure mode.
     */
    private static final class AdmissionResponseLossMutationAppender
            implements ShardLogMutationAppender, AutoCloseable {
        private final PulsarClientArtifactShardLogMutationAppender delegate;
        private final AtomicBoolean responseLossObserved;

        private AdmissionResponseLossMutationAppender(
                final PulsarClientArtifactShardLogMutationAppender delegate,
                final AtomicBoolean responseLossObserved) {
            this.delegate = delegate;
            this.responseLossObserved = responseLossObserved;
        }

        @Override
        public AppendOutcome append(final SystemMutation mutation) {
            final AppendOutcome result = delegate.append(mutation);
            if (result.disposition() == AppendDisposition.PERSISTED
                    && responseLossObserved.compareAndSet(false, true)) {
                return AppendOutcome.unknown();
            }
            return result;
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    static PhysicalPublishBridge createPhysicalPublishBridge(
            final PulsarClient client,
            final GuardedConsumer<?> nativeConsumer,
            final String sourcePhysicalTopic,
            final ShardId shard,
            final PulsarSourcePosition physicalSchedulePosition,
            final String destinationPhysicalTopic,
            final ShardStore store,
            final OwnedDelayShard ownedShard,
            final OwnerIdentityV1 ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey) throws Exception {
        return createPhysicalPublishBridge(client, nativeConsumer, sourcePhysicalTopic, shard,
                physicalSchedulePosition, destinationPhysicalTopic, store, ownedShard, ownerIdentity, authority,
                workClasses, verificationKey, destinationProfile("worker-physical-publish"), capabilityProfile(),
                null, null, 1_000_000);
    }

    static PhysicalPublishBridge createPhysicalPublishBridge(
            final PulsarClient client,
            final GuardedConsumer<?> nativeConsumer,
            final String sourcePhysicalTopic,
            final ShardId shard,
            final PulsarSourcePosition physicalSchedulePosition,
            final String destinationPhysicalTopic,
            final ShardStore store,
            final OwnedDelayShard ownedShard,
            final OwnerIdentityV1 ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final ProfileRefV1 destinationProfile,
            final ProfileRefV1 capabilityProfile,
            final long workClassBytes) throws Exception {
        return createPhysicalPublishBridge(client, nativeConsumer, sourcePhysicalTopic, shard,
                physicalSchedulePosition, destinationPhysicalTopic, store, ownedShard, ownerIdentity, authority,
                workClasses, verificationKey, destinationProfile, capabilityProfile, null, null, workClassBytes);
    }

    static PhysicalPublishBridge createPhysicalPublishBridge(
            final PulsarClient client,
            final GuardedConsumer<?> nativeConsumer,
            final String sourcePhysicalTopic,
            final ShardId shard,
            final PulsarSourcePosition physicalSchedulePosition,
            final String destinationPhysicalTopic,
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
            final long workClassBytes) throws Exception {
        if (workClassBytes <= 0) {
            throw new IllegalArgumentException("Pulsar physical publish work-class bytes must be positive");
        }
        if ((requestedLaneId == null) != (requestedLaneIncarnation == null)) {
            throw new IllegalArgumentException("Pulsar physical publish lane identity must be supplied as a pair");
        }
        final byte[] laneTuple = canonicalLaneTuple(destinationPhysicalTopic, destinationProfile, capabilityProfile);
        final DestinationLaneId laneId = requestedLaneId == null
                ? DestinationLaneId.derive(laneTuple) : requestedLaneId;
        final byte[] laneIncarnation = requestedLaneIncarnation == null
                ? LaneRecord.initial(laneId, physicalSchedulePosition).laneIncarnation()
                : Bytes.copy(requestedLaneIncarnation);
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        final io.nereusstream.delay.protocol.BrokerResourceIdentityV1 target =
                destinationResource(destinationPhysicalTopic);
        final TopicResourceGuard destinationGuard = new TopicResourceGuard(CLUSTER, DESTINATION_INCARNATION,
                DESTINATION_CREATION_TIMESTAMP);
        final byte[] attestationDigest = PulsarClientArtifactSourceRecordConsumer.attestationDigest(
                new TopicResourceGuardAttestation(destinationGuard, destinationPhysicalTopic, 0));
        final long now = Math.max(1, System.currentTimeMillis());
        final TrustedUtcIntervalEvidence issuedAt = evidence(Math.max(0, now - 1), now,
                "pulsar-worker-channel-issued");
        final ChannelResourceIdentityV1 channel = channel(laneId, laneIncarnation, target,
                destinationPhysicalTopic, attestationDigest, issuedAt, destinationProfile);
        final long validUntil = Math.addExact(now, 60_000);
        final ReadyCertificateV1 readyCertificate = readyCertificate(
                ownerIdentity, store.metadata().storeIncarnation(), laneId, laneIncarnation, channel, target,
                destinationPhysicalTopic, attestationDigest, issuedAt, validUntil, destinationProfile);
        final DestinationPhysicalAdmission physicalAdmission = new DestinationPhysicalAdmission(1, workClassBytes);
        physicalAdmission.registerTargetCluster(CLUSTER, 1, workClassBytes);
        physicalAdmission.registerLane(new DestinationPhysicalAdmission.LaneSpec(laneId, laneIncarnation, CLUSTER,
                1, 1, 1, workClassBytes, 1, workClassBytes));
        physicalAdmission.openReady(laneId);
        final String producerName = "pulsar-worker-destination-" + UUID.randomUUID();
        final byte[] producerNameHash = Bytes.sha256(Bytes.utf8(producerName));
        final boolean destinationResponseLoss = hasWorkerDestinationResponseLoss();
        final AtomicReference<GuardedMessageId> responseLostMessage = new AtomicReference<>();
        final AtomicBoolean responseEvidenceResolved = new AtomicBoolean();
        final Producer<byte[]> rawProducer = PulsarClientArtifactProducerFactory.create(client, CLUSTER,
                DESTINATION_INCARNATION, destinationPhysicalTopic, DESTINATION_CREATION_TIMESTAMP, producerName);
        final Producer<byte[]> producer = destinationResponseLoss
                ? responseLossProducer(rawProducer, responseLostMessage) : rawProducer;
        final PulsarClientArtifactDestinationTransport.PublishEvidenceProvider evidenceProvider = destinationResponseLoss
                ? (request, preparedHash, failure) -> resolveDestinationResponseLoss(request, preparedHash,
                producerNameHash, responseLostMessage, responseEvidenceResolved)
                : null;
        final io.nereusstream.delay.transport.PulsarClientArtifactDestinationTransport transport =
                new PulsarClientArtifactDestinationTransport(
                        producer,
                        CLUSTER, DESTINATION_INCARNATION, destinationPhysicalTopic,
                        DESTINATION_CREATION_TIMESTAMP, 0, producerNameHash, evidenceProvider);
        final PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(
                new PulsarTargetResource(CLUSTER, DESTINATION_INCARNATION, destinationPhysicalTopic,
                        DESTINATION_CREATION_TIMESTAMP, 0), transport);
        final String mutationProducerName = "pulsar-worker-mutation-" + UUID.randomUUID();
        final PulsarClientArtifactShardLogMutationAppender realAppender = new PulsarClientArtifactShardLogMutationAppender(
                PulsarClientArtifactProducerFactory.create(client, CLUSTER, INCARNATION, sourcePhysicalTopic,
                        CREATION_TIMESTAMP, mutationProducerName), nativeConsumer, shard, CLUSTER, INCARNATION,
                sourcePhysicalTopic, CREATION_TIMESTAMP, Duration.ofSeconds(20));
        final boolean admissionResponseLoss = hasWorkerAdmissionResponseLoss();
        final AtomicBoolean admissionResponseLossObserved = new AtomicBoolean();
        final ShardLogMutationAppender appender;
        final AutoCloseable appenderResource;
        if (admissionResponseLoss) {
            final AdmissionResponseLossMutationAppender wrapper = new AdmissionResponseLossMutationAppender(
                    realAppender, admissionResponseLossObserved);
            appender = wrapper;
            appenderResource = wrapper;
        } else {
            appender = realAppender;
            appenderResource = realAppender;
        }
        final AuthorIdentity author = AuthorIdentity.owner(ownerIdentity.deploymentId(), ownerIdentity.workerRunId(),
                ownerIdentity.ownerEpoch(), ownerIdentity.leaseFencingDigest());
        final WorkerPublishOutcomeMutationFactory outcomeFactory = new WorkerPublishOutcomeMutationFactory(
                (attempt, request, result) -> {
                    final PublishAdmissionBody admission = PublishAdmissionBody.decode(attempt.admissionBytes());
                    final long retryDeadline = attempt.hasRetryWindow() ? attempt.retryDeadlineEpochMs()
                            : request.deliverAtEpochMs();
                    return new WorkerPublishOutcomeMutationFactory.OutcomeContext(retryDeadline, 0,
                            admission.chargeVector().canonicalBytes(),
                            evidence(result.brokerPersistenceTimeEpochMs(), result.brokerPersistenceTimeEpochMs(),
                                    "pulsar-worker-publish-observed"),
                            retryDecision(admission.decisionTime().latestEpochMs(), retryDeadline,
                                    attempt.attemptNo()));
                }, author.canonicalBytes(), 1, verificationKey.getPrivate());
        final OutcomeWorkClassExecutor outcomes = new OutcomeWorkClassExecutor(workClasses, ownedShard, authority,
                appender);
        final WorkerPhysicalPublishExecutor executor = new WorkerPhysicalPublishExecutor(adapter, physicalAdmission,
                workClasses, Runnable::run, outcomes,
                (attempt, request, ownerClock) -> WorkerPhysicalPublishExecutor.Decision.allowed(), outcomeFactory,
                ownedShard::fence);
        return new PhysicalPublishBridge(executor, appender, appenderResource, laneId, laneIncarnation,
                destinationProfile,
                capabilityProfile, target, channel, readyCertificate, List.of(
                EvidenceCursorV1.pulsar(laneId.bytes(), laneIncarnation, DESTINATION_INCARNATION, 0, 1, 0,
                        destinationPhysicalTopic, DESTINATION_CREATION_TIMESTAMP, 0, 0, 0, 1)),
                destinationPhysicalTopic, destinationResponseLoss, responseEvidenceResolved,
                admissionResponseLoss, admissionResponseLossObserved);
    }

    private static ChannelResourceIdentityV1 channel(final DestinationLaneId laneId, final byte[] laneIncarnation,
                                                      final io.nereusstream.delay.protocol.BrokerResourceIdentityV1 target,
                                                      final String physicalTopic, final byte[] attestationDigest,
                                                      final TrustedUtcIntervalEvidence issuedAt,
                                                      final ProfileRefV1 destinationProfile) {
        final byte[] producer = Bytes.utf8("pulsar-worker-destination-producer");
        final byte[] binding = Bytes.sha256(Bytes.utf8("pulsar-worker-channel-binding"), target.canonicalBytes());
        final byte[] fingerprint = Bytes.sha256(Bytes.utf8("pulsar-worker-channel-fingerprint"),
                Bytes.utf8(physicalTopic));
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, AdapterKindV1.PULSAR.wireValue());
            CanonicalProtobuf.uint32(output, 2, ChannelKindV1.PULSAR_DEDUP_PRODUCER.wireValue());
            CanonicalProtobuf.bytes(output, 3, laneId.bytes());
            CanonicalProtobuf.bytes(output, 4, laneIncarnation);
            CanonicalProtobuf.bytes(output, 5, target.canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, 0);
            CanonicalProtobuf.uint64(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, 0);
            CanonicalProtobuf.bytes(output, 9, producer);
            CanonicalProtobuf.bytes(output, 10, Bytes.sha256(producer));
            CanonicalProtobuf.bytes(output, 11, target.canonicalBytes());
            CanonicalProtobuf.uint64(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, attestationDigest);
        });
        final CredentialUseLeaseV1 lease = new CredentialUseLeaseV1(destinationProfile,
                CredentialUseKindV1.DESTINATION_CHANNEL, CredentialUseLeaseV1.destinationChannelHolderScope(prefix),
                1, binding, fingerprint, issuedAt, Math.addExact(issuedAt.latestEpochMs(), 60_000), 1);
        return new ChannelResourceIdentityV1(AdapterKindV1.PULSAR, ChannelKindV1.PULSAR_DEDUP_PRODUCER,
                laneId.bytes(), laneIncarnation, target, 0, 1, 0, producer, Bytes.sha256(producer), target, 1L,
                attestationDigest, 1, binding, fingerprint, lease);
    }

    private static ReadyCertificateV1 readyCertificate(
            final io.nereusstream.delay.protocol.OwnerIdentityV1 owner,
            final byte[] storeIncarnation, final DestinationLaneId laneId, final byte[] laneIncarnation,
            final ChannelResourceIdentityV1 channel,
            final io.nereusstream.delay.protocol.BrokerResourceIdentityV1 target,
            final String physicalTopic, final byte[] attestationDigest,
            final TrustedUtcIntervalEvidence issuedAt, final long validUntil,
            final ProfileRefV1 destinationProfile) {
        final ActivationBarrierV1 barrier = ActivationBarrierV1.pulsar(target, 0, 0, 0, 0, 1, 1,
                attestationDigest);
        final EvidenceCursorV1 cursor = EvidenceCursorV1.pulsar(laneId.bytes(), laneIncarnation,
                DESTINATION_INCARNATION, 0, 1, 0, physicalTopic, DESTINATION_CREATION_TIMESTAMP, 0, 0, 0, 1);
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, storeIncarnation);
            CanonicalProtobuf.bytes(output, 4, laneId.bytes());
            CanonicalProtobuf.bytes(output, 5, laneIncarnation);
            CanonicalProtobuf.bytes(output, 6, channel.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, barrier.canonicalBytes());
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

    static byte[] canonicalLaneTuple(final String physicalTopic, final ProfileRefV1 destination,
                                     final ProfileRefV1 capability) {
        return Bytes.concat(bytes(32, 61), Bytes.u8(AdapterKindV1.PULSAR.wireValue()),
                Bytes.lp32(Bytes.utf8(CLUSTER)), Bytes.u8(2), DESTINATION_INCARNATION,
                Bytes.u64be(DESTINATION_CREATION_TIMESTAMP), Bytes.lp32(Bytes.utf8(physicalTopic)), Bytes.u32be(0),
                Bytes.lp32(destination.profileId()), Bytes.u64be(destination.version()), destination.semanticHash(),
                Bytes.lp32(capability.profileId()), Bytes.u64be(capability.version()), capability.semanticHash(),
                Bytes.u8(2), Bytes.u32be(0));
    }

    private static io.nereusstream.delay.protocol.BrokerResourceIdentityV1 destinationResource(
            final String physicalTopic) {
        return io.nereusstream.delay.protocol.BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1(CLUSTER, DESTINATION_INCARNATION, physicalTopic,
                        DESTINATION_CREATION_TIMESTAMP));
    }

    private static ProfileRefV1 destinationProfile(final String identity) {
        return new ProfileRefV1(Bytes.utf8("destination-" + identity), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)), ProfileKindV1.DESTINATION);
    }

    static ProfileRefV1 capabilityProfile() {
        return new ProfileRefV1(Bytes.utf8("pulsar-worker-capability"), 1,
                Bytes.sha256(Bytes.utf8("pulsar-worker-capability-semantic")),
                ProfileKindV1.DELIVERY_CAPABILITY);
    }

    private static byte[] claimCharge(final long payloadBytes) {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 1, payloadBytes,
                0, 0, 0, 0, 0, 0, 0, 0, 0).canonicalBytes();
    }

    private static byte[] retryDecision(final long firstAttemptAt, final long retryDeadline,
                                        final int completedAttemptNo) {
        final RetryPolicyRefV1 policy = new RetryPolicyRefV1(Bytes.utf8("pulsar-worker-retry"), 1,
                Bytes.sha256(Bytes.utf8("pulsar-worker-retry-semantic")));
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

    private static long branchNumber(final PublishEvidenceV1 evidence, final int number) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(evidence.branch());
        while (reader.hasRemaining()) {
            final CanonicalProtobuf.Reader.Field field = reader.next();
            if (field.number() == number) {
                return field.unsignedValue();
            }
        }
        throw new IllegalStateException("Pulsar SEND ACK branch is missing field " + number);
    }

    private static void requirePayload(final PulsarClient client, final String physicalTopic,
                                       final byte[] expectedPayload) throws Exception {
        final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, DESTINATION_INCARNATION,
                DESTINATION_CREATION_TIMESTAMP);
        final GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(client, guard,
                physicalTopic, "nereus-delay-p1-worker-destination-" + physicalTopic.hashCode());
        try {
            final Message<byte[]> message = consumer.receive(15, TimeUnit.SECONDS);
            if (message == null || !Arrays.equals(expectedPayload, message.getValue())) {
                throw new IllegalStateException("source-applied typed destination payload was not read back exactly");
            }
            consumer.acknowledge(message);
        } finally {
            consumer.close();
        }
    }

    private static void writeField(final java.io.ByteArrayOutputStream output,
                                   final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
        } else {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        }
    }

    static final class PhysicalPublishBridge implements AutoCloseable {
        private final WorkerPhysicalPublishExecutor executor;
        private final ShardLogMutationAppender appender;
        private final AutoCloseable appenderResource;
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final ProfileRefV1 destinationProfile;
        private final ProfileRefV1 capabilityProfile;
        private final io.nereusstream.delay.protocol.BrokerResourceIdentityV1 targetResource;
        private final ChannelResourceIdentityV1 channel;
        private final ReadyCertificateV1 readyCertificate;
        private final List<EvidenceCursorV1> evidenceCursors;
        private final String destinationPhysicalTopic;
        private final boolean destinationResponseLoss;
        private final AtomicBoolean destinationResponseEvidenceResolved;
        private final boolean admissionResponseLoss;
        private final AtomicBoolean admissionResponseLossObserved;

        private PhysicalPublishBridge(final WorkerPhysicalPublishExecutor executor,
                                      final ShardLogMutationAppender appender,
                                      final AutoCloseable appenderResource,
                                      final DestinationLaneId laneId, final byte[] laneIncarnation,
                                      final ProfileRefV1 destinationProfile, final ProfileRefV1 capabilityProfile,
                                      final io.nereusstream.delay.protocol.BrokerResourceIdentityV1 targetResource,
                                      final ChannelResourceIdentityV1 channel,
                                      final ReadyCertificateV1 readyCertificate,
                                      final List<EvidenceCursorV1> evidenceCursors,
                                      final String destinationPhysicalTopic, final boolean destinationResponseLoss,
                                      final AtomicBoolean destinationResponseEvidenceResolved,
                                      final boolean admissionResponseLoss,
                                      final AtomicBoolean admissionResponseLossObserved) {
            this.executor = executor;
            this.appender = appender;
            this.appenderResource = appenderResource;
            this.laneId = laneId;
            this.laneIncarnation = Bytes.copy(laneIncarnation);
            this.destinationProfile = destinationProfile;
            this.capabilityProfile = capabilityProfile;
            this.targetResource = targetResource;
            this.channel = channel;
            this.readyCertificate = readyCertificate;
            this.evidenceCursors = List.copyOf(evidenceCursors);
            this.destinationPhysicalTopic = destinationPhysicalTopic;
            this.destinationResponseLoss = destinationResponseLoss;
            this.destinationResponseEvidenceResolved = destinationResponseEvidenceResolved;
            this.admissionResponseLoss = admissionResponseLoss;
            this.admissionResponseLossObserved = admissionResponseLossObserved;
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

        ProfileRefV1 destinationProfile() {
            return destinationProfile;
        }

        ProfileRefV1 capabilityProfile() {
            return capabilityProfile;
        }

        io.nereusstream.delay.protocol.BrokerResourceIdentityV1 targetResource() {
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

        boolean destinationResponseLoss() {
            return destinationResponseLoss;
        }

        boolean destinationResponseEvidenceResolved() {
            return destinationResponseEvidenceResolved.get();
        }

        boolean admissionResponseLoss() {
            return admissionResponseLoss;
        }

        boolean admissionResponseLossObserved() {
            return admissionResponseLossObserved.get();
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
                appenderResource.close();
            } catch (Exception closeFailure) {
                final RuntimeException runtimeFailure = closeFailure instanceof RuntimeException
                        ? (RuntimeException) closeFailure
                        : new IllegalStateException("Pulsar Worker mutation appender close failed", closeFailure);
                if (failure == null) {
                    failure = runtimeFailure;
                } else {
                    failure.addSuppressed(runtimeFailure);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static ShardId restartShard(final String physicalTopic) {
        return new ShardId(new RouteIncarnation(Arrays.copyOf(
                Bytes.sha256(Bytes.utf8("nereus-delay-pulsar-worker-restart/" + physicalTopic)),
                RouteIncarnation.LENGTH)), 0);
    }

    private static WorkerAssignment publishAssignment(final WorkerAssignmentAuthority authority,
                                                       final SourceAssignment sourceAssignment,
                                                       final boolean realOxia) {
        final WorkerAssignmentCoordinator coordinator = new WorkerAssignmentCoordinator(
                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
        final long now = System.currentTimeMillis();
        final WorkerPlacementPolicy.WorkerCandidate candidate = new WorkerPlacementPolicy.WorkerCandidate(
                "pulsar-worker", capacity(1), CapacityVectorV1.empty(), 0, 16, 0, 16,
                WorkerLoadVector.empty(), WorkerLoadVector.empty(), now, true, 0);
        final WorkerAssignmentCoordinator.PlacementResult result = coordinator.place(sourceAssignment,
                Bytes.sha256(Bytes.utf8("pulsar-worker-capacity-envelope")), 1, List.of(candidate),
                capacity(1), CapacityVectorV1.empty(), CapacityVectorV1.empty(), null, now, 0, 0);
        final WorkerAssignmentAuthority.Publication publication = result.publication().orElseThrow();
        final WorkerAssignment accepted = coordinator.requireAccepted(sourceAssignment.shardId(),
                publication.revision(), publication.assignment());
        System.out.println("Pulsar Worker assignment publication/acceptance passed: revision="
                + publication.revision() + ", worker=" + accepted.workerId() + ", authority="
                + (realOxia ? "real Oxia session-bound" : "in-memory"));
        return accepted;
    }

    private static CapacityVectorV1 capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimensionV1.COUNT];
        values[CapacityDimensionV1.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVectorV1(values);
    }

    private static OxiaSyncOwnerLeaseBackend.ClientHandle connectOxiaIfConfigured() {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        if (endpoint == null || endpoint.isBlank()) {
            return null;
        }
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        return OxiaSyncOwnerLeaseBackend.connectUnchecked(endpoint, namespace,
                "nereus-delay-pulsar-worker-" + UUID.randomUUID(), Duration.ofSeconds(15),
                "nereus-delay-pulsar-worker/" + UUID.randomUUID());
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
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
                    != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS
                    && result.status()
                    != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN
                    && result.status()
                    != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.ACK_DEFINITIVELY_NOT_ACKED) {
                throw new IllegalStateException("Pulsar Worker source turn failed: " + result.status(),
                        result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Pulsar Worker source record did not become visible before deadline");
    }

    private static boolean hasSourceAckResponseLoss() {
        return "1".equals(System.getenv("NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS"));
    }

    @SuppressWarnings("unchecked")
    private static GuardedConsumer<byte[]> responseLossConsumer(
            final GuardedConsumer<byte[]> delegate, final AtomicBoolean responseLossObserved) {
        return (GuardedConsumer<byte[]>) Proxy.newProxyInstance(
                PulsarClientArtifactWorkerSmoke.class.getClassLoader(), new Class<?>[]{GuardedConsumer.class},
                (proxy, method, arguments) -> {
                    final Object result = invoke(delegate, method, arguments);
                    if (method.getName().equals("acknowledge") && method.getParameterCount() == 1
                            && responseLossObserved.compareAndSet(false, true)) {
                        throw new IllegalStateException("simulated committed Pulsar source ACK response loss");
                    }
                    return result;
                });
    }

    private static Object invoke(final Object target, final java.lang.reflect.Method method,
                                 final Object[] arguments) throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    private static void send(final PulsarClient client, final TopicResourceGuard guard, final String physicalTopic,
                             final PreparedCommand command, final String producerName) throws Exception {
        sendAndPosition(client, guard, physicalTopic, command, producerName);
    }

    static PulsarSourcePosition sendAndPosition(final PulsarClient client, final TopicResourceGuard guard,
                                                final String physicalTopic, final PreparedCommand command,
                                                final String producerName) throws Exception {
        final PulsarClientArtifactSendTransport transport = new PulsarClientArtifactSendTransport(
                PulsarClientArtifactProducerFactory.create(client, guard.authenticatedClusterId(),
                        guard.resourceIncarnation(), physicalTopic, guard.topicCreationTimestamp(), producerName),
                CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, 0);
        try {
            final PulsarSendResult result = transport.send(new PulsarSendRequest(CLUSTER, INCARNATION, physicalTopic,
                    CREATION_TIMESTAMP, 0, command.commandId(),
                    io.nereusstream.delay.protocol.CommandCodec.encodeFrameV1(command)))
                    .toCompletableFuture().get(15, TimeUnit.SECONDS);
            if (result.disposition() != PulsarSendResult.Disposition.PERSISTED) {
                throw new IllegalStateException("guarded Pulsar Worker producer did not persist: "
                        + result.disposition());
            }
            return new PulsarSourcePosition(command.shardId(), INCARNATION, physicalTopic, result.ledgerId(),
                    result.entryId(), result.batchIndex(), result.batchSize(),
                    result.batched() ? PulsarSourcePosition.EntryKind.BATCH : PulsarSourcePosition.EntryKind.NON_BATCH,
                    result.brokerEntryTimestampEpochMs());
        } finally {
            transport.close();
        }
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        return command(shard, identity, new byte[0], 1_000);
    }

    private static PreparedCommand command(final ShardId shard, final String identity, final byte[] payload,
                                           final long delayMs) {
        final ProfileRefV1 destination = destinationProfile(identity);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(Bytes.utf8("retry-" + identity), 1,
                Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + delayMs;
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination, retryPolicy, deliverAt,
                deliverAt + 10_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, payload,
                Bytes.utf8("source-" + identity), null,
                AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of())), null, null);
        return PreparedCommand.scheduleV1(shard, intent, deliverAt + 20_000);
    }

    private static V1ScheduleResolver scheduleResolver(final String destinationPhysicalTopic) {
        if (destinationPhysicalTopic != null) {
            return new V1ScheduleResolver() {
                @Override
                public ResolvedSchedule resolveSchedule(final ShardId shard,
                                                         final DelayMessageId message,
                                                         final ScheduleIntentV1 intent,
                                                         final io.nereusstream.delay.protocol.SourcePosition source) {
                    final byte[] tuple = canonicalLaneTuple(destinationPhysicalTopic, intent.profile(),
                            capabilityProfile());
                    return new ResolvedSchedule(DestinationLaneId.derive(tuple), tuple, intent.inlinePayload(), null);
                }

                @Override
                public ResolvedPrepare resolvePrepare(final ShardId shard,
                                                      final DelayMessageId message,
                                                      final io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1 body,
                                                      final io.nereusstream.delay.protocol.SourcePosition source) {
                    final byte[] tuple = canonicalLaneTuple(destinationPhysicalTopic,
                            body.intentWithoutPayload().profile(), capabilityProfile());
                    return new ResolvedPrepare(DestinationLaneId.derive(tuple), tuple);
                }
            };
        }
        final byte[] tuple = Bytes.utf8("pulsar-worker-canonical-lane-tuple-v1");
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
        return workClasses(1_000_000);
    }

    static WorkClassExecutionRegistry workClasses(final long workClassBytes) {
        if (workClassBytes <= 0) {
            throw new IllegalArgumentException("Pulsar Worker work-class bytes must be positive");
        }
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, 8, workClassBytes,
                    1, workClassBytes, workClassBytes, protectedClass ? 1 : 0,
                    protectedClass ? 1 : 0, workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies,
                TimeUnit.SECONDS.toNanos(5), TimeUnit.SECONDS.toNanos(30),
                16, 8_000_000), System::nanoTime);
    }

    private static void createTopic(final HttpClient client, final String adminUrl, final String topic,
                                    final boolean allowExisting)
            throws Exception {
        createTopic(client, adminUrl, topic, allowExisting, INCARNATION, CREATION_TIMESTAMP);
    }

    private static void createTopic(final HttpClient client, final String adminUrl, final String topic,
                                    final boolean allowExisting, final byte[] incarnation,
                                    final long creationTimestamp) throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic;
        final String body = "{\"nereus.resource.guard.version\":\"1\","
                + "\"nereus.resource.incarnation\":\""
                + Base64.getUrlEncoder().withoutPadding().encodeToString(incarnation) + "\","
                + "\"nereus.resource.created-at\":\""
                + Long.toUnsignedString(creationTimestamp) + "\"}";
        for (int attempt = 0; attempt < 40; attempt++) {
            final HttpResponse<String> response = request(client, path, "PUT", body);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return;
            }
            if (allowExisting && response.statusCode() == 409) {
                return;
            }
            if (response.statusCode() != 409 && response.statusCode() != 412
                    && response.statusCode() != 503) {
                throw failure("create topic", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("topic create did not converge: " + topic);
    }

    private static void deleteTopicIfPresent(final HttpClient client, final String adminUrl, final String topic) {
        try {
            final HttpResponse<String> response = request(client,
                    adminUrl + "/admin/v2/persistent/public/default/" + topic + "?force=true", "DELETE", "");
            if (response.statusCode() >= 300 && response.statusCode() != 404) {
                System.err.println("Pulsar Worker smoke cleanup could not delete topic: " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("Pulsar Worker smoke cleanup failed: " + failure.getMessage());
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

    private static void closeNative(final GuardedConsumer<byte[]> consumer) {
        try {
            consumer.close();
        } catch (PulsarClientException failure) {
            throw new IllegalStateException("Pulsar Worker native consumer close failed", failure);
        }
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static byte[] digest(final int seed) {
        final byte[] result = new byte[32];
        Arrays.fill(result, (byte) seed);
        return result;
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
