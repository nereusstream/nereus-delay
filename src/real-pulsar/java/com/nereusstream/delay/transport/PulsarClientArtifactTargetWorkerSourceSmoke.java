package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.ownership.SourceApplyCoordinator;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.TargetWorkerOwnerActivation;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
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
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
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
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
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
            runTargetSourceTurn(client, physicalTopic);
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
            final String physicalTopic)
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
                        storeRoot);
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
            final Path storeRoot)
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
            final var prepared = TargetStoreBootstrap.prepare(
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
            final var sourceRuntime = new TargetSourceApplyRuntime(
                    initialized,
                    store,
                    assignment,
                    active,
                    new TargetSourceApplyRuntime.Authorities(
                            leases,
                            seekBoundarySuccessor(rootPosition, commandMessageId),
                            entry -> { throw new AssertionError("unexpected Pulsar quota grant after activation"); },
                            entry -> { throw new AssertionError("unexpected Pulsar Target time fence"); },
                            entry -> { throw new AssertionError("unexpected Pulsar Target expiry control"); },
                            entry -> { throw new AssertionError("unexpected Pulsar Target Close control"); },
                            entry -> { throw new AssertionError("unexpected Pulsar membership control"); },
                            ownerCommitAuthority(leases, active),
                            ownerReadAuthority(leases, active),
                            entry -> commandControl),
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
            try {
                final SourceApplyCoordinator.TurnResult result = runUntilApplied(worker);
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
            } finally {
                if (worker.pendingSourceEntry().isEmpty()) {
                    worker.pauseNewTurns();
                    final OwnerLease draining = leases.transition(active, ShardLifecycleState.DRAINING)
                            .orElseThrow(() -> new IllegalStateException("test Pulsar Owner could not enter DRAINING"));
                    try {
                        worker.closeSource();
                    } finally {
                        if (!leases.release(draining)) {
                            throw new IllegalStateException("test Pulsar Owner lease release was not observed");
                        }
                        activeHolder[0] = null;
                    }
                }
            }
        }
    }

    private static PulsarSourcePosition appendFirstGrant(
            final PulsarClient client,
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final ShardId shard,
            final SystemMutation mutation)
            throws Exception {
        try (var appender = new PulsarClientArtifactShardLogMutationAppender(
                PulsarClientArtifactProducerFactory.create(
                        client, CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, "target-source-grant"),
                consumer,
                shard,
                CLUSTER,
                INCARNATION,
                physicalTopic,
                CREATION_TIMESTAMP,
                Duration.ofSeconds(15))) {
            final var outcome = appender.append(mutation);
            if (outcome.disposition()
                            != com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendDisposition.PERSISTED
                    || !(outcome.sourcePosition() instanceof PulsarSourcePosition position)
                    || !Arrays.equals(position.brokerResourceIncarnation(), guard.resourceIncarnation())
                    || !physicalTopic.equals(position.physicalTopic())) {
                throw new IllegalStateException("Pulsar first Target grant was not durably appended");
            }
            return position;
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

    private static SourceApplyCoordinator.TurnResult runUntilApplied(final TargetWorkerShardRuntime worker) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        SourceApplyCoordinator.TurnResult result;
        do {
            result = worker.runSourceTurn(
                    new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)), System::currentTimeMillis);
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

    private static WorkClassExecutionRegistry workClasses() {
        final var policies = new EnumMap<WorkClass, WorkClassPolicy>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = workClass != WorkClass.QUERY && workClass != WorkClass.CHECKPOINT;
            policies.put(
                    workClass,
                    new WorkClassPolicy(
                            1,
                            1,
                            1_000_000,
                            1,
                            1_000_000,
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
