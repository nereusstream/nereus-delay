package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.ownership.SourceApplyCoordinator;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.TargetCheckpointSemanticInputs;
import com.nereusstream.delay.ownership.TargetOwnerRecoveryCoordinator;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.TargetWorkerHostRuntime;
import com.nereusstream.delay.ownership.TargetWorkerOwnerActivation;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.ControlRoleSet;
import com.nereusstream.delay.protocol.ControlTargetKind;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.RecoveryCandidateKind;
import com.nereusstream.delay.protocol.RecoveryCandidateRef;
import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.SelfRoutingId;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.TargetCloseStore;
import com.nereusstream.delay.runtime.TargetCommandStore;
import com.nereusstream.delay.runtime.TargetQuotaGrantControlVerifier;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointDownloadRequest;
import com.nereusstream.delay.store.CheckpointFileInventory;
import com.nereusstream.delay.store.CheckpointManifest;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.CheckpointPublicationCoordinator;
import com.nereusstream.delay.store.CheckpointRestoreCoordinator;
import com.nereusstream.delay.store.CheckpointRestoreWorkClassExecutor;
import com.nereusstream.delay.store.CheckpointUploadIntentStore;
import com.nereusstream.delay.store.FilesystemCheckpointDownloadAdapter;
import com.nereusstream.delay.store.FilesystemCheckpointUploadAdapter;
import com.nereusstream.delay.store.RecoveryCatalog;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
import com.nereusstream.delay.store.TargetCheckpointControlAuthority;
import com.nereusstream.delay.store.TargetCheckpointPublicationWorkClassExecutor;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.net.http.HttpClient;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicResourceGuard;

/** Real P1 source-factory to Target Store apply/ACK smoke with explicit test-only authorities. */
public final class PulsarClientArtifactTargetWorkerSourceSmoke {
    private static final String CLUSTER = PulsarClientArtifactClientBuilder.clusterId();
    private static final byte[] INCARNATION = bytes(32, 0x31);
    private static final long CREATION_TIMESTAMP = 3001L;

    private PulsarClientArtifactTargetWorkerSourceSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <source-topic-prefix>");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String topic = arguments[2] + "-target-" + UUID.randomUUID();
        final String physicalTopic = "persistent://public/default/" + topic;
        final HttpClient admin = HttpClient.newHttpClient();
        createTopic(admin, adminUrl, topic);
        try (PulsarClient client = PulsarClientArtifactClientBuilder.builder(serviceUrl).build()) {
            runTargetSourceTurn(client, physicalTopic, serviceUrl);
            System.out.println(
                    "Pulsar Target source factory: first grant at entry 0; Target Cancel applied at entry 1;");
            System.out.println(
                    "  RocksDB source frontier advanced before receipt-confirmed Pulsar ACK; guard proof verified.");
        } finally {
            deleteTopicIfPresent(admin, adminUrl, topic);
        }
    }

    private static void runTargetSourceTurn(
            final PulsarClient client,
            final String physicalTopic, final String serviceUrl)
            throws Exception {
        final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final TargetQuotaScope scope = new TargetQuotaScope(
                shard, Bytes.sha256(Bytes.utf8("pulsar-target-source-tenant")), null);
        final long sourceTime = System.currentTimeMillis();
        final KeyPair signingKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var actor = new ControlAuthorizationContext(
                Bytes.sha256(Bytes.utf8("pulsar-target-source-actor")),
                ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR),
                Bytes.sha256(Bytes.utf8("pulsar-target-source-resource-scope")));
        final RootControl rootControl = firstGrant(scope, shard, sourceTime, actor, signingKeys);
        final GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(
                client, guard, physicalTopic, "nereus-delay-target-source-" + UUID.randomUUID());
        final OwnerLease[] activeHolder = new OwnerLease[1];
        final boolean[] consumerTransferred = new boolean[1];
        final var leaseBackend = new InMemoryOwnerLeaseStore();
        final var leases = new OxiaOwnerLeaseStore(leaseBackend);
        OwnerLease acquiring = null;
        try {
            final PulsarSourcePosition rootPosition = appendFirstGrant(client, consumer, guard, physicalTopic, shard,
                    rootControl.mutation());
            final var messageId = DelayMessageId.random(shard);
            final var commandId = new CommandId(SelfRoutingId.fromLogicalUuid(
                            shard,
                            SelfRoutingId.uuidV7(sourceTime, new java.security.SecureRandom()))
                    .bytes());
            final long retryUntil = Math.addExact(sourceTime, 600_000);
            final var command = PreparedCommand.cancel(
                    shard, commandId, messageId, new MessagePrecondition(null, null), retryUntil);
            final byte[] commandFrame = com.nereusstream.delay.protocol.CommandCodec.encodeFrame(command);
            final MessageIdAdv commandMessageId = appendCommand(client, guard, physicalTopic, commandFrame);
            if (comparePosition(rootPosition, commandMessageId) >= 0) {
                throw new IllegalStateException("Pulsar Target command did not follow the first grant entry");
            }

            final var positioned = PulsarClientArtifactRecoverySourcePositioner.seekAfter(
                    consumer, guard, physicalTopic, shard, Optional.of(rootPosition), Duration.ofSeconds(5));
            final var assignment = new com.nereusstream.delay.ownership.SourceAssignment(
                    shard,
                    Bytes.sha256(Bytes.utf8("pulsar-target-source-assignment-" + UUID.randomUUID())),
                    1,
                    PulsarActivationBarrier.empty(
                            shard,
                            INCARNATION,
                            physicalTopic,
                            positioned.connectionGeneration(),
                            positioned.attestationDigest()));
            final long leaseNow = System.currentTimeMillis();
            acquiring = leases.acquire(
                            assignment,
                            "pulsar-target-source-owner-" + UUID.randomUUID(),
                            Bytes.sha256(Bytes.utf8("pulsar-target-source-session-" + UUID.randomUUID())),
                            leaseNow,
                            60_000)
                    .orElseThrow(() -> new IllegalStateException("test Pulsar Target Owner lease acquisition failed"));

            final Path storeRoot = Files.createTempDirectory("nereus-delay-pulsar-target-source-");
            try {
                runWithTargetStore(
                        client,
                        guard,
                        physicalTopic,
                        scope,
                        rootPosition,
                        assignment,
                        acquiring,
                        leases,
                        actor,
                        signingKeys,
                        rootControl,
                        command,
                        commandFrame,
                        commandMessageId,
                        consumer,
                        activeHolder,
                        consumerTransferred,
                        storeRoot, serviceUrl);
            } finally {
                deleteTree(storeRoot);
            }
        } finally {
            final OwnerLease active = activeHolder[0];
            if (active != null) {
                leases.release(active);
            } else if (acquiring != null) {
                final Optional<OwnerLease> current = leases.current(shard);
                if (current.isPresent() && current.orElseThrow().sameIdentity(acquiring)) {
                    leases.release(acquiring);
                }
            }
            if (!consumerTransferred[0]) {
                closeNative(consumer);
            }
        }
    }

    private static void runWithTargetStore(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final TargetQuotaScope scope,
            final PulsarSourcePosition rootPosition,
            final com.nereusstream.delay.ownership.SourceAssignment assignment,
            final OwnerLease acquiring,
            final OxiaOwnerLeaseStore leases,
            final ControlAuthorizationContext actor,
            final KeyPair signingKeys,
            final RootControl rootControl,
            final PreparedCommand command,
            final byte[] commandFrame,
            final MessageIdAdv commandMessageId,
            final GuardedConsumer<byte[]> consumer,
            final OwnerLease[] activeHolder,
            final boolean[] consumerTransferred,
            final Path storeRoot, final String serviceUrl)
            throws Exception {
        final var config = ShardStoreConfig.defaults(storeRoot);
        final var registration = new InMemoryControlTargetRegistrationAuthority();
        registration.register(rootControl.prepared());
        final var grants = new TargetQuotaGrantControlVerifier.Authority(
                registration,
                (version, position) -> version == 1 ? signingKeys.getPublic() : null,
                (actualScope, position) -> {
                    if (!scope.equals(actualScope)
                            || !rootPosition.sameSourceIdentity(position)
                            || !scope.shard().equals(position.shardId())) {
                        throw new IllegalStateException("test Pulsar grant source identity changed");
                    }
                },
                (body, view, position, allocation) -> {
                    if (allocation != null || !scope.equals(body.request().next().scope())) {
                        throw new IllegalStateException("test Pulsar initial Shard grant has an unexpected allocation");
                    }
                },
                actor,
                prepared -> true);
        final var workerClasses = workClasses();
        try (var resources = PulsarSmokeWorkerResources.open(config);
                var store = ShardStore.openTarget(config, scope.shard(), resources)) {
            final var initialInput = com.nereusstream.delay.protocol.TargetControlSnapshotInput.signed(scope,
                    bytes(16, 0x31), TargetQuotaGrantControlBody.decode(rootControl.mutation().canonicalBody())
                            .request().next().digest(), rootPosition,
                    Bytes.sha256(rootControl.mutation().canonicalEnvelope()),
                    List.of(com.nereusstream.delay.protocol.ProtocolTuple.currentSystemMutation()), List.of(),
                    1, signingKeys.getPrivate());
            final var prepared = TargetStoreBootstrap.prepareWithControlSnapshot(
                    store,
                    scope,
                    bytes(16, 0x31),
                    new TargetStoreBackend.WriteLimits(128, 4 << 20),
                    budget(),
                    rootControl.prepared(),
                    rootControl.mutation(),
                    rootPosition,
                    grants,
                    (metadata, root, complete) -> {
                        final var mutation = complete.quota().counters().mutation();
                        if (metadata.storeFormatVersion() != 2
                                || !scope.shard().equals(root.identity().shard())
                                || mutation.sequence() != 1
                                || !Arrays.equals(mutation.source().canonicalBytes(), rootPosition.canonicalBytes())) {
                            throw new IllegalStateException("test Pulsar StartAuthority received another root/source");
                        }
                    }, initialInput, (input, control, mutation, at) -> {
                        if (Thread.holdsLock(store)
                                || !Arrays.equals(input.canonicalBytes(), initialInput.canonicalBytes())
                                || !rootPosition.equals(at)) {
                            throw new IllegalStateException("P1 initial input lost its Source/Store-lock boundary");
                        }
                        return signingKeys.getPublic();
                    });
            final var initialized = TargetStoreBootstrap.commit(
                    prepared, ownerCommitAuthority(leases, acquiring));
            final var active = TargetWorkerOwnerActivation.activate(
                    initialized, store, assignment, acquiring, leases, System::currentTimeMillis);
            activeHolder[0] = active;
            final var backend = initialized.backend();
            final var commandStore = new TargetCommandStore(backend, scope, bytes(16, 0x31), 16, 1);
            final var closeControls = new TargetCloseStore(backend, scope, bytes(16, 0x31), 16, 1)
                    .reservationControls((reader, binding) -> Optional.empty());
            final var policy = new TargetCommandStore.Policy(
                    scope,
                    600_000,
                    600_000,
                    10_000,
                    Set.of(command.protocolTuple()),
                    new TargetCommandStore.DeliveryWindow(60_000, 1, 600_000));
            final var commandControl = new TargetSourceApplyRuntime.CommandControl(
                    policy,
                    (reader, binding, position) -> false,
                    closeControls,
                    (incoming, position) -> {
                        throw new AssertionError("Cancel cannot resolve Pulsar Schedule authority");
                    },
                    (binding, position) -> {
                        throw new AssertionError("Cancel cannot resolve Pulsar payload proof authority");
                    },
                    ownerCommitAuthority(leases, active));
            final var successor = new java.util.concurrent.atomic.AtomicReference<SourceReplaySuccessor>(
                    seekBoundarySuccessor(rootPosition, commandMessageId));
            final var sourceRuntime = new TargetSourceApplyRuntime(
                    initialized,
                    store,
                    assignment,
                    active,
                    new TargetSourceApplyRuntime.Authorities(
                            leases,
                            (previous, current) -> successor.get().isSuccessor(previous, current),
                            entry -> { throw new AssertionError("unexpected Pulsar quota grant after activation"); },
                            entry -> { throw new AssertionError("unexpected Pulsar Target time fence"); },
                            entry -> { throw new AssertionError("unexpected Pulsar Target expiry control"); },
                            entry -> { throw new AssertionError("unexpected Pulsar Target Close control"); },
                            entry -> { throw new AssertionError("unexpected Pulsar membership control"); },
                            ownerCommitAuthority(leases, active),
                            ownerReadAuthority(leases, active),
                            entry -> commandControl,
                            entry -> { throw new AssertionError("unexpected P1 Native policy"); },
                            entry -> { throw new AssertionError("unexpected P1 Admission"); },
                            entry -> new TargetSourceApplyRuntime.OutcomeControl((actualScope, writer, mutation, at) -> {
                                throw new AssertionError("expired Outcome must reject before historical authority");
                            }, ownerCommitAuthority(leases, active))),
                    new TargetSourceApplyRuntime.Limits(2048, 16L << 20, 60_000_000_000L, 16, 1),
                    System::nanoTime);
            final var maintenance = new TargetWorkerShardRuntime.Maintenance(
                    closeControls,
                    new com.nereusstream.delay.ownership.TargetReservationClosureWorkClassExecutor.Limits(
                            4096, 250_000, 60_000_000_000L),
                    new com.nereusstream.delay.ownership.TargetReservationExpiryWorkClassExecutor.Limits(
                            2048, 100_000, 60_000_000_000L),
                    ownerCommitAuthority(leases, active),
                    delta -> { throw new AssertionError("unexpected Pulsar Close materialization"); },
                    delta -> { throw new AssertionError("unexpected Pulsar reservation expiry"); },
                    delta -> { throw new AssertionError("unexpected Pulsar Close cursor update"); },
                    System::currentTimeMillis);
            final var worker = PulsarClientArtifactTargetWorkerSourceFactory.create(
                    consumer,
                    guard,
                    physicalTopic,
                    Duration.ofMillis(250),
                    assignment,
                    workerClasses,
                    store,
                    resources,
                    sourceRuntime,
                    maintenance);
            consumerTransferred[0] = true;
            final var checkpoint = captureInitialCheckpoint(client, guard, store, worker, workerClasses, resources,
                    scope, leases, active, signingKeys, registration, actor, storeRoot);
            PulsarSourcePosition recoveryTip = null;
            TargetWorkerHostRuntime host = null;
            java.util.concurrent.ThreadPoolExecutor observerExecutor = null;
            try {
                worker.configureMessageExpiryMaintenance(
                        mutation -> { throw new AssertionError("Cancel fixture cannot append Message expiry"); },
                        () -> expiryRequest(active, signingKeys));
                host = TargetWorkerHostRuntime.start(workerClasses, resources, List.of(worker),
                        new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                        Duration.ofSeconds(1), failure -> {});
                final long beforeBacklog = store.latestSequenceNumber();
                observerExecutor = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                    new java.util.concurrent.ArrayBlockingQueue<>(1),
                    new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
                final var observerCache = new com.nereusstream.delay.ownership.TargetSourceBacklogCache(
                        1, TimeUnit.SECONDS.toNanos(15), observerExecutor, System::nanoTime);
                com.nereusstream.delay.ownership.TargetSourceBacklog.Snapshot backlog;
                final var rejectObservation = new java.util.concurrent.atomic.AtomicBoolean();
                final var failedObserver =
                        new java.util.concurrent.atomic.AtomicReference<PulsarClientArtifactTargetSourceBacklog.Query>();
                host.configureSourceObservations(observerCache,
                        (actualWorker, cut) -> {
                            final boolean rejected = rejectObservation.get();
                            final var query = PulsarClientArtifactTargetSourceBacklog.open(client,
                                    rejected ? new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP + 1)
                                            : guard, cut,
                                new com.nereusstream.delay.ownership.TargetSourceBacklog.Limits(
                                        8, 2L << 20, 1 << 20, TimeUnit.SECONDS.toNanos(10)), () -> {
                                    if (!leases.current(scope.shard()).orElseThrow().sameIdentity(active)) {
                                        throw new IllegalStateException(
                                                "P1 backlog fixture lost Owner/history protection");
                                    }
                                });
                            if (rejected) { failedObserver.set(query); }
                            return query;
                        }, System::currentTimeMillis);
                // The existing maintenance scheduler must queue this observation without a manual refresh.
                final long observerDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (true) {
                    try {
                        backlog = observerCache.get(worker.shardId());
                        break;
                    } catch (IllegalStateException unavailable) {
                        if (System.nanoTime() >= observerDeadline || host.firstMaintenanceFailure() != null) {
                            throw new IllegalStateException("P1 Host Source observer did not settle",
                                    host.firstMaintenanceFailure() == null
                                            ? unavailable : host.firstMaintenanceFailure());
                        }
                        Thread.sleep(1);
                    }
                }
                if (observerCache.get(worker.shardId()) != backlog
                        || !observerCache.cleanupComplete(worker.shardId())) {
                    throw new IllegalStateException("P1 Host cache repeated or retained its successful observation");
                }
                if (backlog.records() != 1 || backlog.bytes() != commandFrame.length
                        || store.latestSequenceNumber() != beforeBacklog) {
                    throw new IllegalStateException("P1 backlog did not count the exact unapplied frame read-only");
                }
                System.out.println("P1 Source backlog: one actual unapplied frame; primary cursor/Store unchanged.");
                final SourceApplyCoordinator.TurnResult result = runUntilApplied(host, worker.shardId());
                final boolean exactCommand = result.entry() instanceof SourceReplayRecord replay
                        && replay.command().equals(command);
                final boolean exactPosition = result.entry() instanceof SourceReplayRecord replay
                        && replay.position() instanceof PulsarSourcePosition position
                        && position.shardId().equals(scope.shard())
                        && position.physicalTopic().equals(physicalTopic)
                        && Arrays.equals(position.brokerResourceIncarnation(), guard.resourceIncarnation())
                        && position.ledgerId() == commandMessageId.getLedgerId()
                        && position.entryId() == commandMessageId.getEntryId();
                final StableCode appliedCode = result.appliedOutcome() == null
                        ? null
                        : result.appliedOutcome().commandResult().stableCode();
                if (result.status() != SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED
                        || !exactCommand
                        || !exactPosition
                        || appliedCode != StableCode.NOT_FOUND) {
                    throw new IllegalStateException(
                            "real Pulsar Target Worker did not apply and ACK the exact command: status="
                                    + result.status() + ", exactCommand=" + exactCommand + ", exactPosition="
                                    + exactPosition + ", appliedCode=" + appliedCode + ", failure="
                                    + result.failure());
                }
                final var applied = store.appliedShardLogPosition();
                if (!(applied instanceof PulsarSourcePosition pulsar)
                        || !exactPosition
                        || !pulsar.equals(result.entry().position())
                        || store.shardMutationSequence() != 2) {
                    throw new IllegalStateException("Pulsar Target Store did not durably advance before ACK");
                }
                verifyRejectedOutcomeHistory(client, consumer, guard, physicalTopic, host, worker, store, scope,
                        leases, active, signingKeys, successor);
                final var semanticInputs = verifyControlHistory(client, guard, store, worker, scope, leases,
                        active, signingKeys);
                verifyScheduledSemanticCandidate(client, guard, host, worker, store, scope, leases, active, signingKeys,
                        registration, actor, semanticInputs, storeRoot, observerExecutor);
                recoveryTip = (PulsarSourcePosition) store.appliedShardLogPosition();
                // The service owns this exact client. After the last primary ACK, test a real failed SUBSCRIBE
                // and service shutdown; no timeout handler may close a shared primary client automatically.
                rejectObservation.set(true);
                final long failedOpenDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (failedObserver.get() == null
                        || !hasGuardMismatch(failedObserver.get().cleanupFailure())
                        || observerCache.state(worker.shardId()).status()
                                != com.nereusstream.delay.ownership.TargetSourceBacklogCache.Status.CLEANUP_PENDING) {
                    if (System.nanoTime() >= failedOpenDeadline || host.firstMaintenanceFailure() != null) {
                        throw new IllegalStateException("P1 Broker did not reject the mismatched observer guard",
                                host.firstMaintenanceFailure());
                    }
                    Thread.sleep(1);
                }
                final var retained = failedObserver.get();
                final var pendingDrain = host.drainAll(
                        new com.nereusstream.delay.ownership.TargetOwnerDrainCoordinator.Request(
                                Math.addExact(System.currentTimeMillis(), 60_000),
                                new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10))),
                        new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)), System::currentTimeMillis);
                if (pendingDrain.complete() || pendingDrain.shards().size() != 1
                        || pendingDrain.shards().getFirst().status()
                                != TargetWorkerHostRuntime.Status.PENDING_SOURCE_OBSERVATION
                        || host.pendingSourceObservation(worker.shardId()).orElseThrow() != retained
                        || retained.cleanupComplete() || store.isCloseStarted()
                        || !leases.current(scope.shard()).orElseThrow().sameIdentity(active)) {
                    throw new IllegalStateException("P1 failed observer released its Query, Store or Owner early");
                }
                retained.closeClientBackendAfterFailedOpen().get(10, TimeUnit.SECONDS);
                final long cleanupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!observerCache.cleanupComplete(worker.shardId())) {
                    if (System.nanoTime() >= cleanupDeadline) {
                        throw new IllegalStateException("P1 backend shutdown did not settle the retained observer");
                    }
                    Thread.sleep(1);
                }
                System.out.println("P1 observer guarded SUBSCRIBE rejected by Broker; Host retained exact Query "
                        + "and Owner until original client backend shutdown completed.");
            } finally {
                try {
                    if (host != null) {
                        final long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                        TargetWorkerHostRuntime.Result drained;
                        do {
                            drained = host.drainAll(
                                    new com.nereusstream.delay.ownership.TargetOwnerDrainCoordinator.Request(
                                            Math.addExact(System.currentTimeMillis(), 60_000),
                                            new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10))),
                                    new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                                    System::currentTimeMillis);
                            if (!drained.complete()) { Thread.sleep(1); }
                        } while (!drained.complete() && System.nanoTime() < drainDeadline);
                        if (!drained.complete() || drained.shards().size() != 1
                                || drained.shards().getFirst().status() != TargetWorkerHostRuntime.Status.RELEASED
                                || host.firstMaintenanceFailure() != null
                                || leases.current(scope.shard()).isPresent() || !store.isCloseStarted()) {
                            throw new IllegalStateException("P1 Host did not close Store/Source and release Owner: "
                                    + drained.shards(), host.firstMaintenanceFailure());
                        }
                        activeHolder[0] = null;
                        System.out.println("P1 Host Source observer: existing tick refreshed; cleanup preceded Owner "
                                + "drain; Store/Source closed and lease released.");
                    } else if (worker.pendingSourceEntry().isEmpty()) {
                        worker.pauseNewTurns();
                        worker.closeSource();
                    }
                } finally {
                    if (observerExecutor != null) {
                        observerExecutor.shutdown();
                        if (!observerExecutor.awaitTermination(15, TimeUnit.SECONDS)) {
                            throw new IllegalStateException(
                                    "P1 Source observer executor did not terminate after drain");
                        }
                    }
                }
            }
            if (recoveryTip == null) { throw new IllegalStateException("P1 original Source tip was not recorded"); }
            try (PulsarClient recoveryClient = PulsarClientArtifactClientBuilder.builder(serviceUrl).build()) {
                checkpoint.historyClient().set(recoveryClient);
                recoverPublishedCheckpoint(recoveryClient, guard, physicalTopic, checkpoint, scope, leases, command,
                        recoveryTip, storeRoot);
            } finally { checkpoint.historyClient().set(null); }
        }
    }

    /** Actual receipt-confirmed native cut and Host schedule; namespace/pin permissions remain fixtures. */
    private static void verifyScheduledSemanticCandidate(PulsarClient client, TopicResourceGuard resource,
            TargetWorkerHostRuntime host,
            TargetWorkerShardRuntime worker, ShardStore store, TargetQuotaScope scope, OxiaOwnerLeaseStore leases,
            OwnerLease active, KeyPair keys, InMemoryControlTargetRegistrationAuthority registration,
            ControlAuthorizationContext actor,
            com.nereusstream.delay.ownership.TargetCheckpointControlInputs.Result inputs, Path root,
            java.util.concurrent.ThreadPoolExecutor historyExecutor) {
        final var physical = new CheckpointManifestLimits(1_000, 256L << 20, 256L << 20,
                1_024, 4 << 20, 1_000, 1_024);
        final var quota = new TargetCheckpointRootVerifier.QuotaAuditLimits(4096, 32L << 20);
        final var ledger = new TargetCheckpointRootVerifier.LedgerAuditLimits(4096, 32L << 20, 16_384, 32L << 20);
        final var semanticLimits = new TargetCheckpointSemanticSnapshot.Limits(64, 1 << 20, 1 << 20);
        final var dependencies = inputs.cut().dependencies();
        final var queuedHistoryEntered = new java.util.concurrent.CountDownLatch(1);
        final var releaseQueuedHistory = new java.util.concurrent.CountDownLatch(1);
        final var historyGuardCalled = new java.util.concurrent.atomic.AtomicBoolean();
        historyExecutor.execute(() -> {
            queuedHistoryEntered.countDown();
            try { releaseQueuedHistory.await(); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        });
        try {
            if (!queuedHistoryEntered.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("queued history gate did not start");
            }
            final var queuedHistory = PulsarClientArtifactTargetAdmissionHistory.readControlAsync(client, resource,
                    dependencies.controlReferences().getFirst(), Duration.ofSeconds(10),
                    () -> historyGuardCalled.set(true), historyExecutor);
            com.nereusstream.delay.ownership.CheckpointInputFuture.requestStop(queuedHistory);
            try {
                queuedHistory.get(5, TimeUnit.SECONDS);
                throw new AssertionError("queued history stop reported success");
            } catch (java.util.concurrent.ExecutionException stopped) {
                if (!stopped.getCause().getMessage().contains("stopped before I/O")) { throw stopped; }
            }
            if (historyGuardCalled.get()
                    || !com.nereusstream.delay.ownership.CheckpointInputFuture.ioExited(queuedHistory)) {
                throw new IllegalStateException("queued P1 history opened I/O or lacked no-start proof");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failed) {
            throw new IllegalStateException("queued P1 history stop failed", failed);
        } finally {
            releaseQueuedHistory.countDown();
        }
        final long now = System.currentTimeMillis();
        final var pending = new CheckpointUploadIntent(new ShardSubject(scope.shard()),
                dependencies.root().root().recoveryLineage(), bytes(16, 0x71),
                new OwnerIdentity(bytes(8, 0x72), bytes(8, 0x73), active.ownerEpoch(), active.leaseToken()),
                store.metadata().storeIncarnation(), bytes(32, 0x74), 1, null, null,
                new com.nereusstream.delay.protocol.ProfileRef(Bytes.utf8("scheduled-semantic-fixture"), 1,
                        bytes(32, 0x75), com.nereusstream.delay.protocol.ProfileKind.OBJECT_STORE),
                new TrustedUtcIntervalEvidence(now - 1, now, TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        bytes(8, 0x76), 1, 1, 1, bytes(32, 0x77), 0, null), now + 60_000,
                CheckpointUploadState.PENDING_UPLOAD, 1, null, null);
        final var intents = new CheckpointUploadIntentStore();
        final var registrationRequested = new java.util.concurrent.atomic.AtomicBoolean();
        final var releaseRegistration = new java.util.concurrent.CompletableFuture<CheckpointUploadIntent>();
        final var catalog = new RecoveryCatalog();
        final var uploaded = new java.util.concurrent.atomic.AtomicBoolean();
        final var adapter = new FilesystemCheckpointUploadAdapter(root.resolve("scheduled-semantic-objects"),
                "p1-scheduled-fixture", physical);
        final var plan = new TargetCheckpointCandidateWorkClassExecutor.PublicationPlan(
                new CheckpointPublicationCoordinator(store.sharedResources(), intents, physical, catalog), 0,
                file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("scheduled/" + file.name()), Bytes.utf8("immutable-1"), null),
                (manifest, folded, semantic) -> {
                    final var controls = new TargetCheckpointControlAuthority(registration,
                            (version, at) -> version == 1 ? keys.getPublic() : null, (input, first) -> {
                                final var original = PulsarClientArtifactTargetAdmissionHistory.readControl(client,
                                        resource, folded.dependencies().controlReferences().getFirst(),
                                        Duration.ofSeconds(10), () -> {});
                                if (!Arrays.equals(original.canonicalEnvelope(), input.image().canonicalEnvelope())) {
                                    throw new IllegalStateException("scheduled image differs from original P1 grant");
                                }
                            }, (registered, input) -> new TargetCheckpointControlAuthority.Authorization(actor,
                                    prepared -> true), budget(), () -> {});
                    controls.authenticateSnapshot(folded.dependencies(), semantic, semanticLimits,
                            (input, registered, mutation, at) -> keys.getPublic());
                    semantic.authenticateOutcomes(folded.dependencies(), semanticLimits, budget(), (input, first) -> {
                        final var reference = folded.dependencies().outcomeReferences().stream()
                                .filter(ref -> ref.source().equals(input.source())).findFirst().orElseThrow();
                        final var original = PulsarClientArtifactTargetAdmissionHistory.readOutcome(client, resource,
                                reference, Duration.ofSeconds(10), () -> {});
                        if (!Arrays.equals(original.canonicalEnvelope(), input.image().canonicalEnvelope())) {
                            throw new IllegalStateException("scheduled image differs from original P1 Outcome");
                        }
                        return keys.getPublic();
                    }, () -> {});
                }, request -> {
                    // Receipt/Source ownership cannot advance between candidate and Catalog completion.
                    try {
                        worker.runSourceTurn(new SchedulerBudget(1, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                                System::currentTimeMillis);
                        throw new AssertionError("Source was admitted during scheduled publication");
                    } catch (IllegalStateException protectedCut) {
                        if (!protectedCut.getMessage().contains("checkpoint cut is still pending")) {
                            throw protectedCut;
                        }
                    }
                    final var result = adapter.upload(request);
                    uploaded.set(true);
                    return result;
                }, (initialization, image) -> ownerCommitAuthority(leases, active)
                        .acquire(store.metadata(), scope, null));
        final var refuse = new java.util.concurrent.atomic.AtomicBoolean(false);
        final var checkpointClock = new java.util.concurrent.atomic.AtomicLong(now);
        final var historyRequested = new java.util.concurrent.atomic.AtomicBoolean();
        final var releaseHistory = new java.util.concurrent.CompletableFuture<Void>();
        final com.nereusstream.delay.ownership.TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory
                factory = (selected, claim, cut) -> {
                    cut.requireCurrent();
                    if (selected != worker || !cut.position().equals(dependencies.root().source())) {
                        throw new IllegalStateException("scheduled semantic request received another native cut");
                    }
                    final var controls = new com.nereusstream.delay.ownership.TargetCheckpointControlInputs(
                            selected, PulsarClientArtifactTargetWorkerSourceSmoke::budget, ledger, ref -> {
                                historyRequested.set(true);
                                return PulsarClientArtifactTargetAdmissionHistory.readControlAsync(client, resource,
                                        ref, Duration.ofSeconds(10), cut::requireCurrent, historyExecutor)
                                        .thenCombine(releaseHistory, (frame, ignored) -> frame);
                            }, new com.nereusstream.delay.ownership.TargetCheckpointControlInputs.Limits(64, 1 << 20),
                            System::currentTimeMillis);
                    final var outcomes = new com.nereusstream.delay.ownership.TargetCheckpointOutcomeInputs(
                            selected, PulsarClientArtifactTargetWorkerSourceSmoke::budget, ledger,
                            ref -> PulsarClientArtifactTargetAdmissionHistory.readOutcomeAsync(client, resource,
                                    ref, Duration.ofSeconds(10), cut::requireCurrent, historyExecutor),
                            new com.nereusstream.delay.ownership.TargetCheckpointOutcomeInputs.Limits(64, 1 << 20),
                            System::currentTimeMillis);
                    final var admissions = new com.nereusstream.delay.ownership.TargetCheckpointAdmissionInputs(
                            selected, PulsarClientArtifactTargetWorkerSourceSmoke::budget,
                            ref -> PulsarClientArtifactTargetAdmissionHistory.readAdmissionAsync(client, resource,
                                    ref, Duration.ofSeconds(10), cut::requireCurrent, historyExecutor),
                            new com.nereusstream.delay.ownership.TargetCheckpointAdmissionInputs.Limits(64, 1 << 20),
                            System::currentTimeMillis);
                    final var preparation = new TargetCheckpointSemanticInputs.Preparation(
                            selected, PulsarClientArtifactTargetWorkerSourceSmoke::budget, System::currentTimeMillis,
                            cut::requireCurrent,
                            new com.nereusstream.delay.ownership.TargetCheckpointRegistryInputs.Providers(
                                    use -> { throw new AssertionError("empty checkpoint has no Profile use"); },
                                    use -> { throw new AssertionError("empty checkpoint has no Retry use"); },
                                    ref -> { throw new AssertionError("empty checkpoint has no Trust Set use"); }),
                            new com.nereusstream.delay.ownership.TargetCheckpointRegistryInputs.Limits(64, 1 << 20),
                            semanticLimits, (control, outcome, retained, complete) -> {
                                if (outcome.inputs().size() != 1 || !retained.isEmpty()) {
                                    throw new IllegalStateException("scheduled checkpoint lost original Outcome");
                                }
                                return new TargetCheckpointSemanticInputs.OriginalAuthorities(
                                        (input, first) -> { throw new AssertionError("no retained Admission"); },
                                        (actualScope, writer, mutation, at) -> {
                                            throw new AssertionError("no retained Admission issuer");
                                        }, (input, first) -> keys.getPublic(), (input, first) -> keys.getPublic());
                            });
                    final var collection = TargetCheckpointSemanticInputs.authenticated(
                            controls, outcomes, admissions, preparation);
                    return new com.nereusstream.delay.ownership.TargetCheckpointCandidateSchedule
                            .CollectedSemanticRequest(
                            new com.nereusstream.delay.ownership.TargetCheckpointCandidateSchedule.Request(intents,
                                    System::currentTimeMillis, root.resolve("scheduled-semantic-image"), pending,
                                    physical, quota, ledger), collection, semanticLimits, () -> {
                                        if (refuse.get()) {
                                            throw new IllegalStateException("scheduled semantic protection unavailable");
                                        }
                                    }, plan, proposed -> {
                                        if (!pending.equals(proposed)
                                                || registrationRequested.getAndSet(true)) {
                                            throw new IllegalStateException("checkpoint registration identity changed");
                                        }
                                        return releaseRegistration;
                                    });
                };
        final var undersized = com.nereusstream.delay.ownership.TargetCheckpointCandidateSchedule.collectedSemantic(
                host, new com.nereusstream.delay.store.CheckpointScheduler(1_000, 0, 1), factory,
                new SchedulerBudget(1, 1, TimeUnit.SECONDS.toNanos(10)));
        undersized.register(worker, now);
        final long beforeBudgetRefusal = store.latestSequenceNumber();
        try {
            undersized.claimDueAndSubmit(now + 1_000, 1);
            throw new AssertionError("unserviceable checkpoint task was admitted");
        } catch (IllegalArgumentException refused) {
            if (!refused.getMessage().contains("cannot be served")) { throw refused; }
        }
        if (undersized.pendingTask(scope.shard()).isPresent() || worker.pendingCheckpointTask().isPresent()
                || registrationRequested.get() || historyRequested.get()
                || store.latestSequenceNumber() != beforeBudgetRefusal
                || Files.exists(root.resolve("scheduled-semantic-image"))) {
            throw new IllegalStateException("checkpoint budget refusal queued history or changed Store/files");
        }
        undersized.unregister(scope.shard());
        refuse.set(true);
        final var scheduled = host.configurePeriodicCollectedSemanticCheckpoints(
                new com.nereusstream.delay.store.CheckpointScheduler(1_000, 0, 1), factory,
                checkpointClock::get, new SchedulerBudget(1, 4L << 20, TimeUnit.SECONDS.toNanos(10)));
        final long sourceSequence = store.shardMutationSequence();
        final var source = store.appliedShardLogPosition();
        final long due = now + 1_000;
        final long beforeRefusal = store.latestSequenceNumber();
        try {
            scheduled.claimDueAndSubmit(due, 1);
            throw new AssertionError("unprotected semantic candidate was queued");
        } catch (IllegalStateException refused) {
            if (!refused.getMessage().contains("protection unavailable")) { throw refused; }
        }
        if (store.latestSequenceNumber() != beforeRefusal || scheduled.pendingTask(scope.shard()).isPresent()
                || Files.exists(root.resolve("scheduled-semantic-image"))) {
            throw new IllegalStateException("semantic factory refusal leaked claim or changed Store/files");
        }
        refuse.set(false);
        final long retried = due + 1_000;
        checkpointClock.set(retried);
        final long registrationDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!registrationRequested.get()) {
            if (System.nanoTime() >= registrationDeadline || host.firstMaintenanceFailure() != null) {
                throw new IllegalStateException("periodic P1 checkpoint did not start intent registration",
                        host.firstMaintenanceFailure());
            }
            try { Thread.sleep(10); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while awaiting checkpoint registration", interrupted);
            }
        }
        if (historyRequested.get() || intents.current(pending).isPresent()
                || scheduled.pendingTask(scope.shard()).isEmpty() || store.latestSequenceNumber() != beforeRefusal
                || Files.exists(root.resolve("scheduled-semantic-image"))) {
            throw new IllegalStateException("pending registration read history or changed Store/files");
        }
        try {
            worker.runSourceTurn(new SchedulerBudget(1, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                    System::currentTimeMillis);
            throw new AssertionError("Source advanced while checkpoint intent was waiting");
        } catch (IllegalStateException protectedCut) {
            if (!protectedCut.getMessage().contains("checkpoint cut is still pending")) { throw protectedCut; }
        }
        intents.create(pending);
        releaseRegistration.complete(pending);
        final long historyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!historyRequested.get()) {
            if (System.nanoTime() >= historyDeadline || host.firstMaintenanceFailure() != null) {
                throw new IllegalStateException("periodic P1 checkpoint did not start async history",
                        host.firstMaintenanceFailure());
            }
            try { Thread.sleep(10); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while awaiting checkpoint history", interrupted);
            }
        }
        if (scheduled.pendingTask(scope.shard()).isEmpty() || store.latestSequenceNumber() != beforeRefusal
                || Files.exists(root.resolve("scheduled-semantic-image"))) {
            throw new IllegalStateException("pending history lost its claim or wrote a checkpoint");
        }
        try {
            worker.runSourceTurn(new SchedulerBudget(1, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                    System::currentTimeMillis);
            throw new AssertionError("Source advanced while checkpoint history was waiting");
        } catch (IllegalStateException protectedCut) {
            if (!protectedCut.getMessage().contains("checkpoint cut is still pending")) { throw protectedCut; }
        }
        releaseHistory.complete(null);
        final long publicationDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (catalog.manifest(pending.checkpointId()).isEmpty() || scheduled.pendingTask(scope.shard()).isPresent()) {
            if (System.nanoTime() >= publicationDeadline || host.firstMaintenanceFailure() != null) {
                throw new IllegalStateException("periodic P1 checkpoint did not settle",
                        host.firstMaintenanceFailure());
            }
            try { Thread.sleep(10); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while awaiting periodic checkpoint", interrupted);
            }
        }
        final Path publishedPath = root.resolve("scheduled-semantic-image");
        final var proof = TargetCheckpointRootVerifier.auditPersistedSemanticSnapshot(publishedPath,
                scope.shard(), physical, quota, ledger, semanticLimits);
        if (!source.equals(proof.root().source()) || sourceSequence != proof.root().mutationSequence()
                || !source.equals(store.appliedShardLogPosition()) || store.shardMutationSequence() != sourceSequence
                || scheduled.pendingTask(scope.shard()).isPresent()
                || intents.currentPublishedFor(pending).orElseThrow().state() != CheckpointUploadState.PUBLISHED
                || !uploaded.get() || catalog.currentInitialization().isEmpty()
                || catalog.manifest(pending.checkpointId()).isEmpty()) {
            throw new IllegalStateException("scheduled semantic publication changed Source or lacked Catalog binding");
        }
        if (!leases.current(scope.shard()).orElseThrow().sameIdentity(active)) {
            throw new IllegalStateException("scheduled semantic candidate lost its Owner");
        }
        System.out.println("P1 existing maintenance ticks collect original Control/nonempty Outcome asynchronously "
                + "then claim/settle native ACK "
                + "semantic candidate/full auth/Catalog init/File upload/PUBLISHED CAS, "
                + "blocked new Source during I/O, then settled/resumed.");
    }

    /** Actual P1 grant cut and File image; global Catalog history/permissions remain explicit test fixtures. */
    private static PublishedCheckpoint captureInitialCheckpoint(PulsarClient client, TopicResourceGuard resource,
            ShardStore store, TargetWorkerShardRuntime worker, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources, TargetQuotaScope scope, OxiaOwnerLeaseStore leases, OwnerLease active,
            KeyPair keys, InMemoryControlTargetRegistrationAuthority registration, ControlAuthorizationContext actor,
            Path root) throws Exception {
        final var collected = verifyControlHistory(client, resource, store, worker, scope, leases, active, keys);
        final var dependencies = collected.cut().dependencies();
        final var physical = new CheckpointManifestLimits(1_000, 256L << 20, 256L << 20,
                1_024, 4 << 20, 1_000, 1_024);
        final var quota = new TargetCheckpointRootVerifier.QuotaAuditLimits(4096, 32L << 20);
        final var ledgerLimits = new TargetCheckpointRootVerifier.LedgerAuditLimits(4096, 32L << 20, 16_384, 32L << 20);
        final var semanticLimits = new TargetCheckpointSemanticSnapshot.Limits(64, 1 << 20, 1 << 20);
        final var snapshot = TargetCheckpointSemanticSnapshot.collect(dependencies, List.of(),
                new com.nereusstream.delay.runtime.InMemoryProfileCatalog(),
                new com.nereusstream.delay.runtime.InMemoryRetryPolicyCatalog(),
                new com.nereusstream.delay.runtime.InMemoryPayloadProofTrustSetCatalog(), budget(), semanticLimits,
                () -> {}).withControlInputs(dependencies, collected.inputs(), budget(), semanticLimits,
                        (input, first) -> keys.getPublic(), () -> {});
        final byte[] checkpointId = bytes(16, 0x61);
        final long now = System.currentTimeMillis();
        final var owner = new OwnerIdentity(bytes(8, 0x62), bytes(8, 0x63), active.ownerEpoch(), active.leaseToken());
        final var profile = new com.nereusstream.delay.protocol.ProfileRef(Bytes.utf8("p1-checkpoint-fixture"), 1,
                bytes(32, 0x64), com.nereusstream.delay.protocol.ProfileKind.OBJECT_STORE);
        final var evidence = new TrustedUtcIntervalEvidence(now - 1, now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 0x65), 1, 1, 1,
                bytes(32, 0x66), 0, null);
        final var pending = new CheckpointUploadIntent(new ShardSubject(scope.shard()),
                dependencies.root().root().recoveryLineage(), checkpointId, owner, store.metadata().storeIncarnation(),
                bytes(32, 0x67), 1, null, null, profile, evidence, now + 60_000,
                CheckpointUploadState.PENDING_UPLOAD, 1, null, null);
        final var intents = new CheckpointUploadIntentStore();
        intents.create(pending);
        final var candidate = new TargetCheckpointCandidateWorkClassExecutor(classes, store, leases, intents,
                System::currentTimeMillis);
        final var placed = candidate.submitSemantic(new TargetCheckpointCandidateWorkClassExecutor.SemanticRequest(
                new TargetCheckpointCandidateWorkClassExecutor.Request(root.resolve("recovery-image"), pending,
                        active, physical, quota, ledgerLimits), snapshot, semanticLimits, () -> {}));
        classes.runTurn(new SchedulerBudget(1, placed.task().bytes(), TimeUnit.SECONDS.toNanos(10)));
        final var candidateResult = placed.outcome().orElseThrow();
        if (candidateResult.failure() != null) {
            throw new IllegalStateException("P1 image", candidateResult.failure());
        }
        final var files = CheckpointFileInventory.collect(candidateResult.checkpointPath(), physical).stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("p1-image/" + file.name()), Bytes.utf8("immutable-1"), null)).toList();
        final var historyClient = new java.util.concurrent.atomic.AtomicReference<PulsarClient>(client);
        final var authentications = new java.util.concurrent.atomic.AtomicInteger();
        final var descriptor = new TargetCheckpointRootVerifier.CandidateImage(candidateResult.checkpointPath(),
                pending, 0, files, physical, quota, ledgerLimits, semanticLimits,
                (candidateManifest, ledger, semantic) -> {
                    authentications.incrementAndGet();
                    if (!semantic.profiles().isEmpty() || !semantic.admissions().isEmpty()
                            || !semantic.outcomes().isEmpty() || ledger.root().mutationSequence() != 1) {
                        throw new IllegalStateException("P1 fixture accepts only its initial grant image");
                    }
                    final var controls = new TargetCheckpointControlAuthority(registration,
                            (version, at) -> version == 1 ? keys.getPublic() : null,
                            (input, first) -> {
                                final var original = PulsarClientArtifactTargetAdmissionHistory.readControl(
                                        java.util.Objects.requireNonNull(historyClient.get(), "owned history client"),
                                        resource, ledger.dependencies().controlReferences().getFirst(),
                                        Duration.ofSeconds(10), () -> {});
                                if (!Arrays.equals(original.canonicalEnvelope(), input.image().canonicalEnvelope())) {
                                    throw new IllegalStateException("P1 image no longer matches original Broker grant");
                                }
                            }, (registered, input) -> new TargetCheckpointControlAuthority.Authorization(actor,
                                    prepared -> true), budget(), () -> {});
                    controls.authenticateSnapshot(ledger.dependencies(), semantic, semanticLimits,
                            (input, registered, mutation, at) -> keys.getPublic());
                }, () -> {});
        final var catalog = new RecoveryCatalog();
        final Path objects = root.resolve("recovery-objects");
        final var publisher = new TargetCheckpointPublicationWorkClassExecutor(classes,
                new CheckpointPublicationCoordinator(resources, intents, physical, catalog));
        final var publication = publisher.submitInitialCandidate(descriptor, () -> {
            if (authentications.get() == 0) { throw new AssertionError("publication clock sampled before image auth"); }
            return System.currentTimeMillis();
        }, new FilesystemCheckpointUploadAdapter(objects, "p1-fixture", physical), (marker, authenticated) ->
                ownerCommitAuthority(leases, active).acquire(store.metadata(), scope, null));
        if (authentications.get() != 0 || publication.outcome().isPresent()
                || publication.authenticatedImage().isPresent()) {
            throw new IllegalStateException("P1 candidate publication performed work during queue admission");
        }
        classes.runTurn(new SchedulerBudget(1, publication.task().bytes(), TimeUnit.SECONDS.toNanos(10)));
        final var published = publication.outcome().orElseThrow();
        if (published.failure() != null) { throw new IllegalStateException("P1 upload", published.failure()); }
        final var image = publication.authenticatedImage().orElseThrow();
        return new PublishedCheckpoint(image, published.published().uploadIntent().publishedManifest(),
                catalog, physical, objects, ledgerLimits, historyClient);
    }

    /** New Owner/native delivery pair/installed Store share one exact published image and Source cut. */
    private static void recoverPublishedCheckpoint(PulsarClient client, TopicResourceGuard resource, String topic,
            PublishedCheckpoint checkpoint, TargetQuotaScope scope, OxiaOwnerLeaseStore leases, PreparedCommand command,
            PulsarSourcePosition tip, Path root) throws Exception {
        final var opening = client.newConsumer(Schema.BYTES).topic(topic)
                .subscriptionName("target-installed-recovery-" + UUID.randomUUID())
                .subscriptionType(org.apache.pulsar.client.api.SubscriptionType.Exclusive)
                .subscriptionMode(org.apache.pulsar.client.api.SubscriptionMode.NonDurable)
                .subscriptionInitialPosition(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(1).autoUpdatePartitions(false).startMessageIdInclusive().resourceGuard(resource)
                .subscribeAsync();
        final var owned = new com.nereusstream.delay.ownership.OwnedSourceCursor<>(opening,
                org.apache.pulsar.client.api.Consumer::closeAsync);
        PulsarClientArtifactRecoverySourceCursor cursor = null;
        OwnerLease acquired = null;
        RecoveryPin pin = null;
        final var manifest = checkpoint.image().manifest();
        try {
            final var nativeConsumer = opening.get(10, TimeUnit.SECONDS);
            if (!(nativeConsumer instanceof GuardedConsumer<?>)) {
                throw new IllegalStateException("P1 installed recovery lacks guarded Source");
            }
            @SuppressWarnings("unchecked")
            final var consumer = (GuardedConsumer<byte[]>) nativeConsumer;
            final var after = (PulsarSourcePosition) manifest.appliedShardLogPosition();
            final var positioned = PulsarClientArtifactRecoverySourcePositioner.seekAfter(consumer, resource, topic,
                    scope.shard(), Optional.of(after), Duration.ofSeconds(10));
            final var assignment = new com.nereusstream.delay.ownership.SourceAssignment(scope.shard(),
                    Bytes.sha256(Bytes.utf8("p1-installed-recovery-" + UUID.randomUUID())), 2,
                    new PulsarActivationBarrier(scope.shard(), resource.resourceIncarnation(), topic, tip.ledgerId(),
                            tip.entryId(), tip.normalizedBatchIndex(), tip.batchSize(),
                            positioned.connectionGeneration(), positioned.attestationDigest(), false));
            acquired = leases.acquire(assignment, "p1-installed-recovery", bytes(32, 0x69),
                    System.currentTimeMillis(), 60_000).orElseThrow();
            if (Long.compareUnsigned(acquired.ownerEpoch(), manifest.createdBy().ownerEpoch()) <= 0) {
                throw new IllegalStateException("P1 installed recovery did not acquire a newer Owner epoch");
            }
            final var floor = checkpoint.catalog().advanceFloor(manifest.checkpointId(), 2, manifest.evidenceCursors());
            pin = checkpoint.catalog().createRecoveryPin(new RecoveryPin(bytes(16, 0x6a),
                    new ShardSubject(scope.shard()), new OwnerIdentity(bytes(8, 0x6b), bytes(8, 0x6c),
                            acquired.ownerEpoch(), acquired.leaseToken()),
                    new RecoveryCandidateRef(RecoveryCandidateKind.CATALOG_CHECKPOINT, manifest.recoveryLineageId(),
                            manifest.checkpointId(), manifest.manifestSha256(), null), floor, 3, bytes(32, 0x6d)));
            cursor = PulsarClientArtifactRecoverySourceCursor.forTarget(consumer, resource, assignment, topic,
                    Duration.ofSeconds(10), 2 << 20, () -> {});
            final var successor = cursor.successorAfter(after);
            final var replay = cursor.replayCursor();
            final var config = ShardStoreConfig.defaults(root.resolve("recovered-store"));
            final var classes = workClasses();
            try (var resources = PulsarSmokeWorkerResources.open(config)) {
                final var restore = new CheckpointRestoreWorkClassExecutor(classes, new CheckpointRestoreCoordinator(
                        config, scope.shard(), resources,
                        new FilesystemCheckpointDownloadAdapter(checkpoint.objects(), checkpoint.limits()),
                        checkpoint.catalog(), checkpoint.limits()));
                final var submitted = restore.submit(new CheckpointRestoreWorkClassExecutor.RestoreRequest(
                        new CheckpointDownloadRequest(manifest, checkpoint.resource()), pin,
                        checkpoint.image().restoreAuthentication(budget())));
                classes.runTurn(new SchedulerBudget(1, submitted.task().bytes(), TimeUnit.SECONDS.toNanos(10)));
                final var installed = submitted.outcome().orElseThrow();
                if (installed.failure() != null) { throw new IllegalStateException("P1 install", installed.failure()); }
                try (var store = installed.restored()) {
                    if (store.metadata().storeIncarnationUuid().equals(manifest.sourceStoreIncarnation())) {
                        throw new IllegalStateException("P1 restore reused its source Store incarnation");
                    }
                    final var owner = acquired;
                    final TargetStoreBackend.CommitAuthority commits = (metadata, actualScope, mutation) ->
                            new TargetStoreBackend.CommitGuard() {
                                @Override
                                public void requireCurrent() {
                                    final var current = leases.current(scope.shard()).orElseThrow();
                                    if (!owner.sameIdentity(current) || !current.validAt(System.currentTimeMillis())) {
                                        throw new IllegalStateException("P1 recovery Owner changed");
                                    }
                                    replay.requireCurrent();
                                }
                                @Override
                                public void close() {}
                            };
                    final var reopened = TargetStoreBootstrap.reopen(store, scope,
                            new TargetStoreBackend.WriteLimits(128, 4 << 20), budget(),
                            (metadata, actualScope) -> commits.acquire(metadata, actualScope, null),
                            checkpoint.ledgerLimits());
                    final var closes = new TargetCloseStore(reopened.backend(), scope,
                            manifest.recoveryLineageId(), 16, 1)
                            .reservationControls((reader, binding) -> Optional.empty());
                    final var policy = new TargetCommandStore.Policy(scope, 600_000, 600_000, 10_000,
                            Set.of(command.protocolTuple()), new TargetCommandStore.DeliveryWindow(60_000, 1, 600_000));
                    final var commands = new TargetSourceApplyRuntime.CommandControl(policy,
                            (reader, binding, position) -> false, closes,
                            (incoming, position) -> { throw new AssertionError("recovery Cancel cannot Schedule"); },
                            (binding, position) -> { throw new AssertionError("recovery Cancel has no payload"); },
                            commits);
                    final var authorities = new TargetSourceApplyRuntime.Authorities(leases, successor,
                            entry -> { throw new AssertionError("unexpected recovery grant"); },
                            entry -> { throw new AssertionError("unexpected recovery Fence"); },
                            entry -> { throw new AssertionError("unexpected recovery expiry"); },
                            entry -> { throw new AssertionError("unexpected recovery Close"); },
                            entry -> { throw new AssertionError("unexpected recovery membership"); }, commits,
                            (metadata, actualScope) -> commits.acquire(metadata, actualScope, null), entry -> commands,
                            entry -> { throw new AssertionError("unexpected recovery Native policy"); },
                            entry -> { throw new AssertionError("unexpected recovery Admission"); },
                            entry -> new TargetSourceApplyRuntime.OutcomeControl(
                                    (actualScope, writer, mutation, at) -> {
                                        throw new AssertionError("expired recovered Outcome must reject "
                                                + "before history");
                                    }, commits));
                    final var recovery = new TargetOwnerRecoveryCoordinator(store, reopened, assignment, owner,
                            authorities, new TargetSourceApplyRuntime.Limits(2048, 16L << 20,
                                    TimeUnit.SECONDS.toNanos(60), 16, 1), replay, checkpoint.catalog(), pin, () -> {},
                            new com.nereusstream.delay.ownership.ReplayTurnBudget(1, 4L << 20,
                                    TimeUnit.SECONDS.toNanos(10)), classes, System::currentTimeMillis, System::nanoTime,
                            PulsarClientArtifactTargetWorkerSourceSmoke::budget, checkpoint.ledgerLimits());
                    // The subscription resumes inclusively: the exact grant anchor is a physical redelivery,
                    // not a missing/extra business operation. Preserve it and let the existing replay path prove it.
                    final var duplicate = recovery.runTurn();
                    if (duplicate.complete() || duplicate.outcomes().size() != 1
                            || duplicate.outcomes().getFirst().systemMutationResult() == null
                            || duplicate.outcomes().getFirst().systemMutationResult().stableCode() != StableCode.OK
                            || !after.equals(store.appliedShardLogPosition()) || store.shardMutationSequence() != 1) {
                        throw new IllegalStateException("P1 exact resume anchor changed checkpoint business state");
                    }
                    final var first = recovery.runTurn();
                    if (first.complete() || first.outcomes().size() != 1
                            || first.outcomes().getFirst().commandResult() == null
                            || first.outcomes().getFirst().commandResult().stableCode() != StableCode.NOT_FOUND
                            || store.shardMutationSequence() != 2) {
                        throw new IllegalStateException("P1 installed Store did not first-apply its real Cancel");
                    }
                    final var second = recovery.runTurn();
                    if (!second.complete() || second.outcomes().size() != 1
                            || second.outcomes().getFirst().systemMutationResult() == null
                            || second.outcomes().getFirst().systemMutationResult().stableCode()
                                    != StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED
                            || !tip.equals(store.appliedShardLogPosition()) || store.shardMutationSequence() != 3
                            || store.recoveryMetadata().installState().phase()
                                    != com.nereusstream.delay.protocol.RecoveryInstallPhase.OPEN
                            || recovery.activeLease().state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                            || replay.hasNext()) {
                        throw new IllegalStateException("P1 installed Store did not complete exact real "
                                + "Source recovery");
                    }
                    System.out.println("P1 exact published grant image -> File download/new Store incarnation -> "
                            + "guarded no-ACK exact resume redelivery/first Cancel/rejected Outcome "
                            + "-> OPEN/new Owner ACTIVE.");
                }
                if (resources.registeredPhysicalUsageSources() != 0) {
                    throw new IllegalStateException("P1 restored Store physical usage registration leaked");
                }
            }
        } finally {
            if (cursor != null) { cursor.close(); }
            else { owned.requestClose().get(10, TimeUnit.SECONDS); }
            if (acquired != null) {
                final var current = leases.current(scope.shard()).orElseThrow();
                if (!current.sameIdentity(acquired) || !leases.release(current)) {
                    throw new IllegalStateException("P1 recovery Owner release was not confirmed");
                }
            }
            if (pin != null) { checkpoint.catalog().releaseRecoveryPin(pin); }
        }
        if (leases.current(scope.shard()).isPresent() || checkpoint.catalog().activeRecoveryPin().isPresent()) {
            throw new IllegalStateException("P1 recovery Owner/Pin cleanup incomplete");
        }
    }

    private record PublishedCheckpoint(TargetCheckpointRootVerifier.AuthenticatedImage image,
            CheckpointResource resource, RecoveryCatalog catalog, CheckpointManifestLimits limits, Path objects,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            java.util.concurrent.atomic.AtomicReference<PulsarClient> historyClient) {}

    private static void verifyRejectedOutcomeHistory(PulsarClient client, GuardedConsumer<byte[]> primary,
            TopicResourceGuard resource, String topic, TargetWorkerHostRuntime host, TargetWorkerShardRuntime worker,
            ShardStore store, TargetQuotaScope scope, OxiaOwnerLeaseStore leases, OwnerLease active, KeyPair keys,
            java.util.concurrent.atomic.AtomicReference<SourceReplaySuccessor> successor) throws Exception {
        final byte[] placeholder = com.nereusstream.delay.protocol.CanonicalProtobuf.message(out ->
                com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 1, new byte[] {1}));
        final var evidence = new com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence(1, 2,
                com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("p1-expired-outcome-fixture"), 1, 1, 1, bytes(32, 0x71), 0, null);
        final byte[] body = com.nereusstream.delay.protocol.PublishOutcomeBody.encodeInitial(scope.shard(), 1,
                bytes(32, 0x72), 3, 4, StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, null,
                placeholder, evidence, placeholder);
        final var decoded = com.nereusstream.delay.protocol.PublishOutcomeBody.decode(body);
        final var mutation = SystemMutation.signed(scope.shard(), SystemMutationType.PUBLISH_OUTCOME, 1,
                decoded.initialLogicalOperationIdentity(), body,
                AuthorIdentity.owner(Bytes.utf8("p1-expired-owner-deployment"), Bytes.utf8("p1-expired-owner-run"),
                        active.ownerEpoch(), Bytes.sha256(active.leaseToken())).canonicalBytes(), 1, keys.getPrivate());
        final var previous = store.appliedShardLogPosition();
        final var appended = appendFirstGrant(client, primary, resource, topic, scope.shard(), mutation);
        final var priorSuccessor = successor.get();
        successor.set((before, after) -> before.equals(previous) && after instanceof PulsarSourcePosition actual
                && actual.ledgerId() == appended.ledgerId() && actual.entryId() == appended.entryId()
                && actual.normalizedBatchIndex() == appended.normalizedBatchIndex()
                || priorSuccessor.isSuccessor(before, after));
        final var turn = runUntilApplied(host, scope.shard());
        if (turn.appliedOutcome().systemMutationResult().applyStatus()
                        != com.nereusstream.delay.runtime.ApplyStatus.REJECTED
                || turn.appliedOutcome().systemMutationResult().stableCode()
                        != StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED) {
            throw new IllegalStateException("P1 expired Outcome did not retain its rejected first Source result");
        }
        final long beforeHistory = store.latestSequenceNumber();
        final Runnable protection = () -> {
            if (Thread.holdsLock(store) || store.latestSequenceNumber() != beforeHistory
                    || !leases.current(scope.shard()).orElseThrow().sameIdentity(active)) {
                throw new IllegalStateException("P1 Outcome history lost actual Worker/Owner cut");
            }
        };
        final var executor = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(1),
                new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
        try {
            final var collector = new com.nereusstream.delay.ownership.TargetCheckpointOutcomeInputs(worker,
                    PulsarClientArtifactTargetWorkerSourceSmoke::budget,
                    new com.nereusstream.delay.store.TargetCheckpointRootVerifier.LedgerAuditLimits(
                            4096, 32L << 20, 16_384, 32L << 20),
                    ref -> PulsarClientArtifactTargetAdmissionHistory.readOutcomeAsync(client, resource, ref,
                            Duration.ofSeconds(10), protection, executor),
                    new com.nereusstream.delay.ownership.TargetCheckpointOutcomeInputs.Limits(8, 2L << 20),
                    System::currentTimeMillis);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (true) {
                final var observed = collector.runTurn();
                if (observed.failure() != null || System.nanoTime() >= deadline) {
                    throw new IllegalStateException("P1 Outcome history did not complete", observed.failure());
                }
                if (observed.status() == com.nereusstream.delay.ownership.TargetCheckpointOutcomeInputs.Status.COMPLETE) {
                    break;
                }
                Thread.sleep(1);
            }
            final var collected = collector.result().orElseThrow();
            if (collected.inputs().size() != 1 || store.latestSequenceNumber() != beforeHistory
                    || !Arrays.equals(collected.inputs().getFirst().image().canonicalEnvelope(),
                            mutation.canonicalEnvelope())) {
                throw new IllegalStateException("P1 original rejected Outcome history changed frame or Store");
            }
            collected.cut().dependencies().requireOutcomeImages(collected.inputs(), 2L << 20, budget(),
                    (input, first) -> { protection.run(); return null; }, protection);
            verifyTargetRecoveryCursor(client, resource, topic, store, mutation, protection);
            if (store.latestSequenceNumber() != beforeHistory) {
                throw new IllegalStateException("P1 no-ACK recovery cursor changed primary Store");
            }
            System.out.println("P1 real rejected Outcome Source/apply/ACK and bounded independent history matched "
                    + "one actual Worker cut; primary cursor/Store unchanged by history.");
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                throw new IllegalStateException("P1 Outcome history executor did not terminate");
            }
        }
    }

    private static void verifyTargetRecoveryCursor(PulsarClient client, TopicResourceGuard resource, String topic,
            ShardStore store, SystemMutation expectedOutcome, Runnable protection) throws Exception {
        protection.run();
        final var opening = client.newConsumer(Schema.BYTES).topic(topic)
                .subscriptionName("target-recovery-fixture-" + UUID.randomUUID())
                .subscriptionType(org.apache.pulsar.client.api.SubscriptionType.Exclusive)
                .subscriptionMode(org.apache.pulsar.client.api.SubscriptionMode.NonDurable)
                .subscriptionInitialPosition(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(1).autoUpdatePartitions(false).startMessageIdInclusive().resourceGuard(resource)
                .subscribeAsync();
        final var owned = new com.nereusstream.delay.ownership.OwnedSourceCursor<>(opening,
                org.apache.pulsar.client.api.Consumer::closeAsync);
        PulsarClientArtifactRecoverySourceCursor cursor = null;
        try {
            final var nativeConsumer = opening.get(10, TimeUnit.SECONDS);
            if (!(nativeConsumer instanceof GuardedConsumer<?>)) {
                throw new IllegalStateException("P1 recovery fixture lacks guarded consumer");
            }
            @SuppressWarnings("unchecked")
            final var consumer = (GuardedConsumer<byte[]>) nativeConsumer;
            final var positioned = PulsarClientArtifactRecoverySourcePositioner.seekAfter(consumer, resource, topic,
                    store.shardId(), Optional.empty(), Duration.ofSeconds(10));
            final var end = (PulsarSourcePosition) store.appliedShardLogPosition();
            final var barrier = new PulsarActivationBarrier(store.shardId(), resource.resourceIncarnation(), topic,
                    end.ledgerId(), end.entryId(), end.normalizedBatchIndex(), end.batchSize(),
                    positioned.connectionGeneration(), positioned.attestationDigest(), false);
            final var assignment = new com.nereusstream.delay.ownership.SourceAssignment(store.shardId(),
                    Bytes.sha256(Bytes.utf8("p1-native-recovery-fixture")), 1, barrier);
            cursor = PulsarClientArtifactRecoverySourceCursor.forTarget(consumer, resource, assignment, topic,
                    Duration.ofSeconds(10), 2 << 20, protection);
            final var replay = cursor.replayCursor();
            int read = 0;
            boolean targetControl = false;
            boolean outcome = false;
            while (replay.hasNext()) {
                cursor.requireCurrent();
                final var entry = replay.next();
                if (entry instanceof SourceReplayMutation system) {
                    if (read == 0) {
                        targetControl = com.nereusstream.delay.protocol.TargetQuotaGrantControlBody
                                .decode(system.mutation().canonicalBody()).request().next().scope().shard()
                                .equals(store.shardId());
                    }
                    if (entry.position().equals(end)) {
                        outcome = Arrays.equals(system.mutation().canonicalEnvelope(), expectedOutcome.canonicalEnvelope());
                    }
                }
                read++;
            }
            cursor.requireCurrent();
            if (read != 3 || !targetControl || !outcome) {
                throw new IllegalStateException("P1 Target recovery did not read complete grant/Command/Outcome cut");
            }
            cursor.close();
            try {
                replay.hasNext();
                throw new AssertionError("closed native Source was accepted by cached recovery cursor");
            } catch (IllegalStateException expectedClosed) {
                if (!expectedClosed.getMessage().contains("closed")) { throw expectedClosed; }
            }
            System.out.println("Target guarded look-ahead rejected the closed native recovery Source "
                    + "after barrier.");
            System.out.println("P1 guarded non-durable Target no-ACK recovery cursor read exact real "
                    + "Control/Command/Outcome through post-seek attested barrier; primary Store unchanged.");
        } finally {
            if (cursor != null) { cursor.close(); }
            else {
                owned.requestClose().get(10, TimeUnit.SECONDS);
            }
        }
    }

    /** Actual P1 history/resource/Store correlation; issuer, Owner and retention/pin authorities are fixtures. */
    private static com.nereusstream.delay.ownership.TargetCheckpointControlInputs.Result verifyControlHistory(
            PulsarClient client, TopicResourceGuard resource, ShardStore store,
            TargetWorkerShardRuntime worker, TargetQuotaScope scope, OxiaOwnerLeaseStore leases, OwnerLease active,
            KeyPair signingKeys) throws Exception {
        final long before = store.latestSequenceNumber();
        final Runnable historyGuard = () -> {
            final var current = leases.current(scope.shard()).orElseThrow();
            if (Thread.holdsLock(store) || !active.sameIdentity(current)
                    || current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                    || !current.validAt(System.currentTimeMillis())
                    || store.latestSequenceNumber() != before) {
                throw new IllegalStateException("P1 Control history lost its fixture Owner/Store cut");
            }
        };
        final var executor = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(1),
                new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
        final com.nereusstream.delay.ownership.TargetCheckpointControlInputs.Result collected;
        try {
            final var collector = new com.nereusstream.delay.ownership.TargetCheckpointControlInputs(worker,
                    PulsarClientArtifactTargetWorkerSourceSmoke::budget,
                    new com.nereusstream.delay.store.TargetCheckpointRootVerifier.LedgerAuditLimits(
                            4096, 32L << 20, 16_384, 32L << 20),
                    ref -> PulsarClientArtifactTargetAdmissionHistory.readControlAsync(client, resource, ref,
                            Duration.ofSeconds(10), historyGuard, executor),
                    new com.nereusstream.delay.ownership.TargetCheckpointControlInputs.Limits(64, 1 << 20),
                    System::currentTimeMillis);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
            while (true) {
                final var turn = collector.runTurn();
                if (turn.status() == com.nereusstream.delay.ownership.TargetCheckpointControlInputs.Status.FAILED) {
                    throw new IllegalStateException("P1 async Control collection failed", turn.failure());
                }
                if (turn.status() == com.nereusstream.delay.ownership.TargetCheckpointControlInputs.Status.COMPLETE) {
                    collected = collector.result().orElseThrow();
                    break;
                }
                if (System.nanoTime() >= deadline) { throw new IllegalStateException("P1 Control inputs deadline"); }
                Thread.sleep(5);
            }
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(25, TimeUnit.SECONDS)) {
                throw new IllegalStateException("P1 caller-owned history executor did not terminate");
            }
        }
        final var dependencies = collected.cut().dependencies();
        final var inputs = collected.inputs();
        if (dependencies.requireControlImages(inputs, 1 << 20, budget(),
                (input, first) -> { historyGuard.run(); return signingKeys.getPublic(); }, historyGuard).size() != 1
                || store.latestSequenceNumber() != before) {
            throw new IllegalStateException("P1 Control history omitted the first grant or changed Store state");
        }
        System.out.println("P1 async Control history matched one actual Worker cut; Store unchanged; "
                + "issuer/Owner/retention/pins remain explicit fixtures.");
        return collected;
    }

    private static PulsarSourcePosition appendFirstGrant(
            final PulsarClient client,
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final ShardId shard,
            final SystemMutation mutation)
            throws Exception {
        final Path windowRoot = Files.createTempDirectory("nereus-delay-p1-system-writer-window-");
        try (var resources = PulsarSmokeWorkerResources.open(ShardStoreConfig.defaults(windowRoot))) {
            final var writerBudget = new com.nereusstream.delay.ownership.SystemWriterAppendBudget(
                    new com.nereusstream.delay.ownership.SystemWriterAppendBudget.Limits(2, 2L << 20, 1L << 20, 1),
                    workClasses(), resources);
            final PulsarSourcePosition appended;
            try (var appender = new PulsarClientArtifactShardLogMutationAppender(
                    PulsarClientArtifactProducerFactory.create(
                            client, CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, "target-source-grant"),
                    consumer,
                    shard,
                    CLUSTER,
                    INCARNATION,
                    physicalTopic,
                    CREATION_TIMESTAMP,
                    Duration.ofSeconds(15), writerBudget.registerWriter(shard))) {
                final var outcome = appender.append(mutation);
                if (outcome.disposition()
                                != com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendDisposition.PERSISTED
                        || !(outcome.sourcePosition() instanceof PulsarSourcePosition position)
                        || !Arrays.equals(position.brokerResourceIncarnation(), guard.resourceIncarnation())
                        || !physicalTopic.equals(position.physicalTopic())) {
                    throw new IllegalStateException("Pulsar first Target grant was not durably appended");
                }
                appended = position;
            }
            if (!writerBudget.usage().equals(new com.nereusstream.delay.ownership.SystemWriterAppendBudget.Usage(
                    0, 0, 0))) {
                throw new IllegalStateException("closed P1 system writer retained an append-window charge");
            }
            System.out.println("P1 system-writer append window settled after exact producer close (fixture resources).");
            return appended;
        } finally {
            Files.deleteIfExists(windowRoot);
        }
    }

    private static MessageIdAdv appendCommand(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final byte[] commandFrame)
            throws Exception {
        try (Producer<byte[]> producer = PulsarClientArtifactProducerFactory.create(
                client, CLUSTER, guard.resourceIncarnation(), physicalTopic, guard.topicCreationTimestamp(),
                "target-source-command")) {
            final var messageId = producer.newMessage().value(commandFrame).send();
            if (!(messageId instanceof MessageIdAdv advanced)
                    || advanced.getLedgerId() < 0
                    || advanced.getEntryId() < 0
                    || advanced.getFirstChunkMessageId() != null) {
                throw new IllegalStateException("Pulsar Target command did not return an exact non-chunked id");
            }
            return advanced;
        }
    }

    private static int comparePosition(final PulsarSourcePosition first, final MessageIdAdv second) {
        final int ledger = Long.compareUnsigned(first.ledgerId(), second.getLedgerId());
        return ledger == 0 ? Long.compareUnsigned(first.entryId(), second.getEntryId()) : ledger;
    }

    private static RootControl firstGrant(
            final TargetQuotaScope scope,
            final ShardId shard,
            final long sourceTime,
            final ControlAuthorizationContext actor,
            final KeyPair keys) {
        final long retryUntil = Math.addExact(sourceTime, 600_000);
        final long[] amounts = new long[CapacityDimension.COUNT];
        for (CapacityDimension dimension : CapacityDimension.values()) {
            final int wire = dimension.wireValue();
            if (wire <= 15 || (wire >= 51 && wire <= 55)) {
                amounts[wire - 1] = 1L << 40;
            }
        }
        final var accounting = new TargetQuotaAccounting(
                Bytes.sha256(Bytes.utf8("pulsar-target-source-schema")), 64, 32, 32, 1);
        final var limit = new TargetQuotaUsage(
                new CapacityVector(amounts), 1L << 20, 1L << 20, 1L << 20, 1L << 20);
        final var grant = new TargetQuotaGrant(
                scope,
                Bytes.sha256(Bytes.utf8("pulsar-target-source-grant")),
                1,
                accounting,
                limit,
                1,
                Bytes.sha256(Bytes.utf8("pulsar-target-source-policy")));
        final var request = new TargetQuotaGrantControlRequest(grant, null, null);
        final byte[] operationId = Bytes.sha256(Bytes.utf8("pulsar-target-source-root-grant-operation"));
        final var ref = new ControlRef(
                operationId,
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final var body = new TargetQuotaGrantControlBody(shard, retryUntil, ref, request);
        final var author = new ControlAuthor(
                actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash());
        final var mutation = SystemMutation.signed(
                shard,
                SystemMutationType.APPLY_SHARD_CONTROL,
                retryUntil,
                body.logicalIdentity(),
                body.canonicalBytes(),
                AuthorIdentity.control(
                                actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash())
                        .canonicalBytes(),
                1,
                keys.getPrivate());
        final var target = new ControlTargetRef(
                0,
                ControlTargetKind.SHARD,
                new ShardSubject(shard),
                mutation.systemMutationId(),
                mutation.mutationHash());
        final var prepared = PreparedControlOperation.prepare(
                operationId,
                request.operationKind(),
                author,
                request.operationRequest(),
                List.of(target),
                1,
                retryUntil,
                1,
                keys.getPrivate());
        return new RootControl(prepared, mutation);
    }

    private static TargetStoreBackend.CommitAuthority ownerCommitAuthority(
            final OxiaOwnerLeaseStore leases, final OwnerLease expected) {
        return (metadata, scope, mutation) -> new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                final OwnerLease current = leases.current(expected.shardId())
                        .orElseThrow(() -> new IllegalStateException("test Pulsar Owner lease disappeared"));
                if (!expected.sameIdentity(current)
                        || current.state() != expected.state()
                        || !current.validAt(System.currentTimeMillis())) {
                    throw new IllegalStateException("test Pulsar Owner lease changed before Target commit");
                }
            }

            @Override
            public void close() {}
        };
    }

    private static TargetStoreBackend.ReadAuthority ownerReadAuthority(
            final OxiaOwnerLeaseStore leases, final OwnerLease expected) {
        return (metadata, scope) -> ownerCommitAuthority(leases, expected).acquire(metadata, scope, null);
    }

    private static SourceApplyCoordinator.TurnResult runUntilApplied(
            final TargetWorkerHostRuntime host, final ShardId shard) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        SourceApplyCoordinator.TurnResult result;
        do {
            final var selected = host.runNextSourceTurn(
                    new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)), System::currentTimeMillis);
            if (!selected.shardId().equals(shard)) {
                throw new IllegalStateException("P1 Host selected another Source Shard");
            }
            result = selected.result();
            if (result.status() == SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                return result;
            }
            if (result.status() != SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status() != SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException(
                        "real Pulsar Target Worker source turn failed: " + result.status(), result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("real Pulsar Target source record did not apply before the deadline");
    }

    /** The post-seek fixture accepts only its exact first command; later batch members stay strict. */
    private static SourceReplaySuccessor seekBoundarySuccessor(
            final PulsarSourcePosition seekFloor, final MessageIdAdv expectedFirstMessage) {
        final var batchSuccessor = SourceReplaySuccessor.strictPulsarBatchMember();
        return (previous, current) -> previous instanceof PulsarSourcePosition previousPulsar
                && current instanceof PulsarSourcePosition currentPulsar
                && Arrays.equals(previousPulsar.canonicalBytes(), seekFloor.canonicalBytes())
                && currentPulsar.entryKind() == PulsarSourcePosition.EntryKind.NON_BATCH
                && currentPulsar.ledgerId() == expectedFirstMessage.getLedgerId()
                && currentPulsar.entryId() == expectedFirstMessage.getEntryId()
                || batchSuccessor.isSuccessor(previous, current);
    }

    private static boolean hasGuardMismatch(final Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof PulsarClientException.ResourceIncarnationMismatchException) { return true; }
        }
        return false;
    }

    /** Explicit test clock/Owner/signing fixtures; this does not certify a production clock. */
    private static TargetWorkerShardRuntime.MessageExpiryRequest expiryRequest(
            final OwnerLease active, final KeyPair keys) {
        final long now = System.currentTimeMillis();
        final var evidence = new com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence(
                Math.max(0, now - 1), now,
                com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("p1-source-host-fixture-clock"), 1, 1, Math.max(0, System.nanoTime()),
                Bytes.sha256(Bytes.utf8("p1-source-host-fixture-clock-evidence")), 0, null);
        return new TargetWorkerShardRuntime.MessageExpiryRequest(budget(), evidence, Math.addExact(now, 600_000),
                new com.nereusstream.delay.protocol.OwnerIdentity(Bytes.utf8("p1-source-host-fixture-deployment"),
                        Bytes.utf8("p1-source-host-fixture-run"), active.ownerEpoch(),
                        Bytes.sha256(active.leaseToken())),
                1, keys.getPrivate());
    }

    private static WorkClassExecutionRegistry workClasses() {
        final var policies = new EnumMap<WorkClass, WorkClassPolicy>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = workClass != WorkClass.QUERY && workClass != WorkClass.CHECKPOINT;
            final long bytes = workClass == WorkClass.CHECKPOINT ? 4L << 20 : 1_000_000;
            policies.put(
                    workClass,
                    new WorkClassPolicy(
                            1,
                            1,
                            bytes,
                            1,
                            bytes,
                            1_000,
                            protectedClass ? 1 : 0,
                            protectedClass ? 1 : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100, 16, 2_000_000), () -> 0);
    }

    private static BoundedReadBudget budget() {
        return new BoundedReadBudget(2048, 32L << 20, 60_000_000_000L, System::nanoTime);
    }

    private static void createTopic(final HttpClient client, final String adminUrl, final String topic)
            throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic;
        final String body = "{\"nereus.resource.guard.version\":\"1\","
                + "\"nereus.resource.incarnation\":\""
                + Base64.getUrlEncoder().withoutPadding().encodeToString(INCARNATION)
                + "\",\"nereus.resource.created-at\":\""
                + Long.toUnsignedString(CREATION_TIMESTAMP)
                + "\"}";
        for (int attempt = 0; attempt < 40; attempt++) {
            final HttpResponse<String> response = PulsarClientArtifactAdminHttp.request(client, path, "PUT", body);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return;
            }
            if (response.statusCode() != 409 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw new IllegalStateException(
                        "Pulsar Target source topic create failed with HTTP "
                                + response.statusCode() + ": " + response.body());
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("Pulsar Target source topic create did not converge: " + topic);
    }

    private static void deleteTopicIfPresent(final HttpClient client, final String adminUrl, final String topic) {
        try {
            final HttpResponse<String> response = PulsarClientArtifactAdminHttp.request(
                    client,
                    adminUrl + "/admin/v2/persistent/public/default/" + topic + "?force=true",
                    "DELETE",
                    "");
            if (response.statusCode() >= 300 && response.statusCode() != 404) {
                System.err.println("Pulsar Target source smoke cleanup could not delete topic: "
                        + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("Pulsar Target source smoke cleanup failed: " + failure.getMessage());
        }
    }

    private static void closeNative(final GuardedConsumer<byte[]> consumer) {
        try {
            consumer.close();
        } catch (PulsarClientException failure) {
            throw new IllegalStateException("Pulsar Target source consumer close failed", failure);
        }
    }

    private static void deleteTree(final Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static byte[] bytes(final int size, final int value) {
        final byte[] result = new byte[size];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private record RootControl(PreparedControlOperation prepared, SystemMutation mutation) {}
}
