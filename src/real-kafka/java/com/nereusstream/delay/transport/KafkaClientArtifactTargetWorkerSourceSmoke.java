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
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedControlOperation;
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
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Real K1 source-factory to Target Store apply/ACK smoke with an explicit test-only bootstrap authority. */
public final class KafkaClientArtifactTargetWorkerSourceSmoke {
    private static final byte[] LINEAGE = bytes(16, 0x31);

    private KafkaClientArtifactTargetWorkerSourceSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <source-topic-prefix>");
        }
        final String bootstrap = arguments[0];
        final String topic = arguments[1] + "-target-" + UUID.randomUUID();
        final String groupId = "nereus-delay-target-source-" + UUID.randomUUID();
        final Map<String, Object> adminConfig = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        try (Admin admin = Admin.create(adminConfig)) {
            try {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = admin.describeTopics(List.of(topic))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS)
                    .get(topic)
                    .topicId();
            if (topicId == null || topicId.equals(Uuid.ZERO_UUID)) {
                throw new IllegalStateException("Kafka did not return the exact source TopicId");
            }

            final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
            final TargetQuotaScope scope = new TargetQuotaScope(
                    shard, Bytes.sha256(Bytes.utf8("target-source-tenant")), null);
            final long sourceTime = System.currentTimeMillis();
            final var signingKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final var actor = new ControlAuthorizationContext(
                    Bytes.sha256(Bytes.utf8("target-source-actor")),
                    ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR),
                    Bytes.sha256(Bytes.utf8("target-source-resource-scope")));
            final RootControl rootControl = firstGrant(scope, shard, sourceTime, actor, signingKeys);
            final var rootMutation = rootControl.mutation();
            final var messageId = DelayMessageId.random(shard);
            final var commandId = new CommandId(SelfRoutingId.fromLogicalUuid(
                            shard,
                            SelfRoutingId.uuidV7(sourceTime, new java.security.SecureRandom()))
                    .bytes());
            final long retryUntil = Math.addExact(sourceTime, 600_000);
            final var command = PreparedCommand.cancel(
                    shard, commandId, messageId, new MessagePrecondition(null, null), retryUntil);
            final var commandFrame = com.nereusstream.delay.protocol.CommandCodec.encodeFrame(command);
            final var rootMetadata = produce(
                    bootstrap,
                    topic,
                    0,
                    sourceTime,
                    rootMutation.systemMutationId(),
                    rootMutation.encodeFrame());
            final var commandMetadata = produce(
                    bootstrap,
                    topic,
                    shard.partition(),
                    sourceTime + 1,
                    command.commandId().bytes(),
                    commandFrame);
            if (rootMetadata.offset() != 0 || commandMetadata.offset() != 1) {
                throw new IllegalStateException("fresh Kafka Target source did not start at offsets 0 and 1");
            }

            final var source = new KafkaSourcePosition(
                    shard,
                    clusterId,
                    toUuid(topicId),
                    rootMetadata.offset(),
                    null,
                    rootMetadata.timestamp());
            final var assignment = new com.nereusstream.delay.ownership.SourceAssignment(
                    shard,
                    Bytes.sha256(Bytes.utf8("target-source-assignment-" + UUID.randomUUID())),
                    1,
                    new KafkaActivationBarrier(shard, clusterId, toUuid(topicId), 1));
            final var leaseBackend = new InMemoryOwnerLeaseStore();
            final var leases = new OxiaOwnerLeaseStore(leaseBackend);
            final long leaseNow = System.currentTimeMillis();
            final var acquiring = leases.acquire(
                            assignment,
                            "target-source-owner-" + UUID.randomUUID(),
                            Bytes.sha256(Bytes.utf8("target-source-session-" + UUID.randomUUID())),
                            leaseNow,
                            60_000)
                    .orElseThrow(() -> new IllegalStateException("test Owner lease acquisition failed"));

            final Path storeRoot = Files.createTempDirectory("nereus-delay-kafka-target-source-");
            try {
                runTargetSourceTurn(
                        admin,
                        bootstrap,
                        topic,
                        groupId,
                        clusterId,
                        toUuid(topicId),
                        scope,
                        source,
                        assignment,
                        acquiring,
                        leases,
                        actor,
                        signingKeys,
                        rootControl,
                        command,
                        commandMetadata.offset(),
                        storeRoot);
            } finally {
                deleteTree(storeRoot);
            }
            System.out.println(
                    "Kafka Target source factory: first grant at offset 0; Cancel at offset 1 applied before an "
                            + "injected pre-commit ACK UNKNOWN;");
            System.out.println(
                    "  replacement Owner reopened RocksDB, replayed the exact Broker record without a second "
                            + "Store write, then committed Kafka group offset 2; TopicId/partition guard verified.");
            } finally {
                try {
                    admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
                } catch (Exception cleanupFailure) {
                    System.err.println("Kafka Target source smoke could not delete its unique test topic: "
                            + cleanupFailure);
                }
            }
        }
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
                Bytes.sha256(Bytes.utf8("target-source-schema")), 64, 32, 32, 1);
        final var limit = new TargetQuotaUsage(new CapacityVector(amounts), 1L << 20, 1L << 20, 1L << 20, 1L << 20);
        final var grant = new TargetQuotaGrant(
                scope,
                Bytes.sha256(Bytes.utf8("target-source-grant")),
                1,
                accounting,
                limit,
                1,
                Bytes.sha256(Bytes.utf8("target-source-policy")));
        final var request = new TargetQuotaGrantControlRequest(grant, null, null);
        final byte[] operationId = Bytes.sha256(Bytes.utf8("target-source-root-grant-operation"));
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

    private static void runTargetSourceTurn(
            final Admin admin,
            final String bootstrap,
            final String topic,
            final String groupId,
            final String clusterId,
            final UUID topicId,
            final TargetQuotaScope scope,
            final KafkaSourcePosition source,
            final com.nereusstream.delay.ownership.SourceAssignment assignment,
            final OwnerLease acquiring,
            final OxiaOwnerLeaseStore leases,
            final ControlAuthorizationContext actor,
            final KeyPair signingKeys,
            final RootControl rootControl,
            final PreparedCommand command,
            final long commandOffset,
            final Path storeRoot) throws Exception {
        final var config = ShardStoreConfig.defaults(storeRoot);
        final var registration = new InMemoryControlTargetRegistrationAuthority();
        registration.register(rootControl.prepared());
        final var grants = new TargetQuotaGrantControlVerifier.Authority(
                registration,
                (version, position) -> version == 1 ? signingKeys.getPublic() : null,
                (actualScope, position) -> {
                    if (!scope.equals(actualScope)
                            || !source.sameSourceIdentity(position)
                            || !scope.shard().equals(position.shardId())) {
                        throw new IllegalStateException("test grant source identity changed");
                    }
                },
                (body, view, position, allocation) -> {
                    if (allocation != null || !scope.equals(body.request().next().scope())) {
                        throw new IllegalStateException("test initial Shard grant has an unexpected allocation");
                    }
                },
                actor,
                prepared -> true);
        final var workerClasses = workClasses();
        final OwnerLease[] activeHolder = new OwnerLease[1];
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, scope.shard(), resources)) {
            final var prepared = TargetStoreBootstrap.prepare(
                    store,
                    scope,
                    LINEAGE,
                    new TargetStoreBackend.WriteLimits(128, 4 << 20),
                    budget(),
                    rootControl.prepared(),
                    rootControl.mutation(),
                    source,
                    grants,
                    (metadata, root, complete) -> {
                        final var stamp = complete.quota().counters().mutation();
                        if (metadata.storeFormatVersion() != 2
                                || !scope.shard().equals(root.identity().shard())
                                || stamp.sequence() != 1
                                || !Arrays.equals(stamp.source().canonicalBytes(), source.canonicalBytes())) {
                            throw new IllegalStateException("test StartAuthority received another root/source batch");
                        }
                    });
            final var initialized = TargetStoreBootstrap.commit(
                    prepared, ownerCommitAuthority(leases, acquiring));
            final var active = TargetWorkerOwnerActivation.activate(
                    initialized, store, assignment, acquiring, leases, System::currentTimeMillis);
            activeHolder[0] = active;
            final var backend = initialized.backend();
            final var commandStore = new TargetCommandStore(backend, scope, LINEAGE, 16, 1);
            final var closeControls = new TargetCloseStore(backend, scope, LINEAGE, 16, 1)
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
                        throw new AssertionError("Cancel cannot resolve Schedule authority");
                    },
                    (binding, position) -> {
                        throw new AssertionError("Cancel cannot resolve payload proof authority");
                    },
                    ownerCommitAuthority(leases, active));
            final var sourceRuntime = new TargetSourceApplyRuntime(
                    initialized,
                    store,
                    assignment,
                    active,
                    new TargetSourceApplyRuntime.Authorities(
                            leases,
                            SourceReplaySuccessor.strictKafka(),
                            entry -> { throw new AssertionError("unexpected quota grant after activation"); },
                            entry -> { throw new AssertionError("unexpected Target time fence"); },
                            entry -> { throw new AssertionError("unexpected Target expiry control"); },
                            entry -> { throw new AssertionError("unexpected Target Close control"); },
                            entry -> { throw new AssertionError("unexpected membership control"); },
                            ownerCommitAuthority(leases, active),
                            ownerReadAuthority(leases, active),
                            entry -> commandControl),
                    new TargetSourceApplyRuntime.Limits(2048, 16L << 20, 60_000_000_000L, 16, 1),
                    System::nanoTime);
            final var consumer = newSourceConsumer(bootstrap, groupId, clusterId, topic, topicId, scope.shard());
            final var ackUnknownInjected = new AtomicBoolean();
            final var guardedConsumer = failBeforeFirstCommit(consumer, ackUnknownInjected);
            final var maintenance = new TargetWorkerShardRuntime.Maintenance(
                    closeControls,
                    new com.nereusstream.delay.ownership.TargetReservationClosureWorkClassExecutor.Limits(
                            4096, 250_000, 60_000_000_000L),
                    new com.nereusstream.delay.ownership.TargetReservationExpiryWorkClassExecutor.Limits(
                            2048, 100_000, 60_000_000_000L),
                    ownerCommitAuthority(leases, active),
                    delta -> { throw new AssertionError("unexpected Close materialization"); },
                    delta -> { throw new AssertionError("unexpected reservation expiry"); },
                    delta -> { throw new AssertionError("unexpected Close cursor update"); },
                    System::currentTimeMillis);
            final var worker = KafkaClientArtifactTargetWorkerSourceFactory.create(
                    guardedConsumer,
                    topic,
                    Duration.ofSeconds(5),
                    assignment,
                    workerClasses,
                    store,
                    resources,
                    sourceRuntime,
                    maintenance);
            try {
                final SourceApplyCoordinator.TurnResult result = runUntilAckUnknown(worker);
                if (result.status() == SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN) {
                    activeHolder[0] = active;
                }
                final boolean exactCommand = result.entry() instanceof SourceReplayRecord replay
                        && replay.command().equals(command);
                final boolean exactPosition = result.entry() instanceof SourceReplayRecord replay
                        && replay.position() instanceof KafkaSourcePosition replayPosition
                        && replayPosition.offset() == commandOffset
                        && clusterId.equals(replayPosition.authenticatedClusterId())
                        && topicId.equals(replayPosition.nativeTopicUuid());
                if (result.status() != SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN
                        || !exactCommand
                        || !exactPosition
                        || result.appliedOutcome() != null
                        || !ackUnknownInjected.get()) {
                    throw new IllegalStateException(
                            "real Kafka Target Worker did not apply the exact command before ACK UNKNOWN: status="
                                    + result.status() + ", exactCommand=" + exactCommand + ", exactPosition="
                                    + exactPosition + ", appliedOutcome=" + result.appliedOutcome() + ", ackInjected="
                                    + ackUnknownInjected.get() + ", entry=" + result.entry() + ", failure="
                                    + result.failure());
                }
                final var applied = store.appliedShardLogPosition();
                if (!(applied instanceof KafkaSourcePosition kafka) || kafka.offset() != commandOffset
                        || store.shardMutationSequence() != 2) {
                    throw new IllegalStateException("Target Store did not durably advance before uncertain source ACK");
                }
                final var committedOffsets = admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
                        .get(10, TimeUnit.SECONDS);
                final var committed = committedOffsets.get(new TopicPartition(topic, scope.shard().partition()));
                if (committed != null) {
                    throw new IllegalStateException("Kafka group offset advanced before the injected ACK commit");
                }
            } finally {
                if (worker.pendingSourceEntry().isPresent()) {
                    consumer.close();
                    if (!leases.release(active)) {
                        throw new IllegalStateException("test Owner loss was not observed after ACK UNKNOWN");
                    }
                    activeHolder[0] = null;
                } else {
                    worker.pauseNewTurns();
                    final OwnerLease draining = leases.transition(active, ShardLifecycleState.DRAINING)
                            .orElseThrow(() -> new IllegalStateException("test Owner could not enter DRAINING"));
                    try {
                        worker.closeSource();
                    } finally {
                        if (!leases.release(draining)) {
                            throw new IllegalStateException("test Owner lease release was not observed");
                        }
                        activeHolder[0] = null;
                    }
                }
            }
        } finally {
            final OwnerLease active = activeHolder[0];
            if (active != null) {
                leases.release(active);
            } else if (leases.current(scope.shard()).filter(current -> current.sameIdentity(acquiring)).isPresent()) {
                leases.release(acquiring);
            }
        }
        runTargetSourceReplayAfterUnknown(
                admin,
                bootstrap,
                topic,
                groupId,
                clusterId,
                topicId,
                scope,
                assignment,
                leases,
                command,
                commandOffset,
                config);
    }

    private static void runTargetSourceReplayAfterUnknown(
            final Admin admin,
            final String bootstrap,
            final String topic,
            final String groupId,
            final String clusterId,
            final UUID topicId,
            final TargetQuotaScope scope,
            final com.nereusstream.delay.ownership.SourceAssignment assignment,
            final OxiaOwnerLeaseStore leases,
            final PreparedCommand command,
            final long commandOffset,
            final ShardStoreConfig config)
            throws Exception {
        final OwnerLease replayAcquiring = leases.acquire(
                        assignment,
                        "target-source-replay-owner-" + UUID.randomUUID(),
                        Bytes.sha256(Bytes.utf8("target-source-replay-session-" + UUID.randomUUID())),
                        System.currentTimeMillis(),
                        60_000)
                .orElseThrow(() -> new IllegalStateException("replacement Target Owner lease acquisition failed"));
        final OwnerLease[] ownerHolder = {replayAcquiring};
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, scope.shard(), resources)) {
            final var reopened = TargetStoreBootstrap.reopen(
                    store,
                    scope,
                    new TargetStoreBackend.WriteLimits(128, 4 << 20),
                    budget(),
                    ownerReadAuthority(leases, replayAcquiring));
            final var active = TargetWorkerOwnerActivation.activate(
                    reopened, store, assignment, replayAcquiring, leases, System::currentTimeMillis);
            ownerHolder[0] = active;
            final var replacementWorkClasses = workClasses();
            final var lineage = reopened.root().recoveryLineage();
            final var closeControls = new TargetCloseStore(reopened.backend(), scope, lineage, 16, 1)
                    .reservationControls((reader, binding) -> Optional.empty());
            final var runtime = new TargetSourceApplyRuntime(
                    reopened,
                    store,
                    assignment,
                    active,
                    new TargetSourceApplyRuntime.Authorities(
                            leases,
                            SourceReplaySuccessor.strictKafka(),
                            entry -> { throw new AssertionError("replay resolved a quota grant"); },
                            entry -> { throw new AssertionError("replay resolved a time fence"); },
                            entry -> { throw new AssertionError("replay resolved expiry control"); },
                            entry -> { throw new AssertionError("replay resolved a Close control"); },
                            entry -> { throw new AssertionError("replay resolved membership control"); },
                            ownerCommitAuthority(leases, active),
                            ownerReadAuthority(leases, active),
                            entry -> { throw new AssertionError("exact replay resolved new command authority"); }),
                    new TargetSourceApplyRuntime.Limits(2048, 16L << 20, 60_000_000_000L, 16, 1),
                    System::nanoTime);
            final var consumer = newSourceConsumer(bootstrap, groupId, clusterId, topic, topicId, scope.shard());
            final var maintenance = new TargetWorkerShardRuntime.Maintenance(
                    closeControls,
                    new com.nereusstream.delay.ownership.TargetReservationClosureWorkClassExecutor.Limits(
                            4096, 250_000, 60_000_000_000L),
                    new com.nereusstream.delay.ownership.TargetReservationExpiryWorkClassExecutor.Limits(
                            2048, 100_000, 60_000_000_000L),
                    ownerCommitAuthority(leases, active),
                    delta -> { throw new AssertionError("unexpected Close materialization"); },
                    delta -> { throw new AssertionError("unexpected reservation expiry"); },
                    delta -> { throw new AssertionError("unexpected Close cursor update"); },
                    System::currentTimeMillis);
            final var worker = KafkaClientArtifactTargetWorkerSourceFactory.create(
                    consumer,
                    topic,
                    Duration.ofSeconds(5),
                    assignment,
                    replacementWorkClasses,
                    store,
                    resources,
                    runtime,
                    maintenance);
            try {
                final long beforeReplaySequence = store.latestSequenceNumber();
                final long beforeReplayMutation = store.shardMutationSequence();
                final SourceApplyCoordinator.TurnResult result = runUntilApplied(worker);
                final boolean exactCommand = result.entry() instanceof SourceReplayRecord replay
                        && replay.command().equals(command);
                final boolean exactPosition = result.entry() instanceof SourceReplayRecord replay
                        && replay.position() instanceof KafkaSourcePosition replayPosition
                        && replayPosition.offset() == commandOffset
                        && clusterId.equals(replayPosition.authenticatedClusterId())
                        && topicId.equals(replayPosition.nativeTopicUuid());
                final StableCode appliedCode = result.appliedOutcome() == null
                        ? null
                        : result.appliedOutcome().commandResult().stableCode();
                if (result.status() != SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED
                        || !exactCommand
                        || !exactPosition
                        || appliedCode != StableCode.NOT_FOUND) {
                    throw new IllegalStateException(
                            "replacement Target Worker did not ACK the exact replay: status=" + result.status()
                                    + ", exactCommand=" + exactCommand + ", exactPosition=" + exactPosition
                                    + ", appliedCode=" + appliedCode + ", entry=" + result.entry() + ", failure="
                                    + result.failure());
                }
                if (store.latestSequenceNumber() != beforeReplaySequence
                        || store.shardMutationSequence() != beforeReplayMutation
                        || !active.sameIdentity(leases.current(scope.shard()).orElseThrow())) {
                    throw new IllegalStateException("Target replay rewrote the Store or lost replacement Owner");
                }
                final var committedOffsets = admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
                        .get(10, TimeUnit.SECONDS);
                final var committed = committedOffsets.get(new TopicPartition(topic, scope.shard().partition()));
                if (committed == null || committed.offset() != commandOffset + 1) {
                    throw new IllegalStateException("Kafka source group did not commit the replayed offset");
                }
            } finally {
                if (worker.pendingSourceEntry().isPresent()) {
                    consumer.close();
                    if (!leases.release(active)) {
                        throw new IllegalStateException("replacement Target Owner release was not observed");
                    }
                    ownerHolder[0] = null;
                } else {
                    worker.pauseNewTurns();
                    final OwnerLease draining = leases.transition(active, ShardLifecycleState.DRAINING)
                            .orElseThrow(() -> new IllegalStateException("replacement Owner could not enter DRAINING"));
                    try {
                        worker.closeSource();
                    } finally {
                        if (!leases.release(draining)) {
                            throw new IllegalStateException("replacement Target Owner release was not observed");
                        }
                        ownerHolder[0] = null;
                    }
                }
            }
        } finally {
            final OwnerLease current = ownerHolder[0];
            if (current != null) {
                leases.release(current);
            }
        }
    }

    private static GuardedConsumer<byte[], byte[]> newSourceConsumer(
            final String bootstrap,
            final String groupId,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard) {
        return KafkaClientArtifactSourceConsumerFactory.create(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        bootstrap,
                        ConsumerConfig.GROUP_ID_CONFIG,
                        groupId,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        ByteArrayDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        ByteArrayDeserializer.class.getName(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest"),
                clusterId,
                topic,
                topicId,
                shard.partition());
    }

    @SuppressWarnings("unchecked")
    private static GuardedConsumer<byte[], byte[]> failBeforeFirstCommit(
            final GuardedConsumer<byte[], byte[]> delegate, final AtomicBoolean observed) {
        final var injected = new AtomicBoolean();
        return (GuardedConsumer<byte[], byte[]>) Proxy.newProxyInstance(
                KafkaClientArtifactTargetWorkerSourceSmoke.class.getClassLoader(),
                new Class<?>[] {GuardedConsumer.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("commitSync")
                            && method.getParameterCount() == 1
                            && injected.compareAndSet(false, true)) {
                        observed.set(true);
                        throw new IllegalStateException("injected before Kafka source commitSync");
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static TargetStoreBackend.CommitAuthority ownerCommitAuthority(
            final OxiaOwnerLeaseStore leases, final OwnerLease expected) {
        return (metadata, scope, mutation) -> new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                final OwnerLease current = leases.current(expected.shardId())
                        .orElseThrow(() -> new IllegalStateException("test Owner lease disappeared"));
                if (!expected.sameIdentity(current)
                        || current.state() != expected.state()
                        || !current.validAt(System.currentTimeMillis())) {
                    throw new IllegalStateException("test Owner lease changed before Target commit");
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
                        "real Kafka Target Worker source turn failed: " + result.status(), result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("real Kafka Target source record did not apply before the deadline");
    }

    private static SourceApplyCoordinator.TurnResult runUntilAckUnknown(final TargetWorkerShardRuntime worker) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        SourceApplyCoordinator.TurnResult result;
        do {
            result = worker.runSourceTurn(
                    new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)), System::currentTimeMillis);
            if (result.status() == SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN) {
                return result;
            }
            if (result.status() != SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status() != SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException(
                        "real Kafka Target Worker did not reach the ACK uncertainty cut: " + result.status(),
                        result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("real Kafka Target source did not reach ACK UNKNOWN before the deadline");
    }

    private record RootControl(PreparedControlOperation prepared, SystemMutation mutation) {}

    private static org.apache.kafka.clients.producer.RecordMetadata produce(
            final String bootstrap,
            final String topic,
            final int partition,
            final long timestamp,
            final byte[] key,
            final byte[] value) throws Exception {
        final Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName(),
                ProducerConfig.ACKS_CONFIG,
                "all");
        try (var producer = new KafkaProducer<byte[], byte[]>(config)) {
            return producer.send(new ProducerRecord<>(topic, partition, timestamp, key, value))
                    .get(30, TimeUnit.SECONDS);
        }
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
        return new WorkClassExecutionRegistry(
                new WorkClassRuntimeConfig(policies, 100, 100, 16, 2_000_000), () -> 0);
    }

    private static BoundedReadBudget budget() {
        return new BoundedReadBudget(2048, 32L << 20, 60_000_000_000L, System::nanoTime);
    }

    private static UUID toUuid(final Uuid uuid) {
        return new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    private static byte[] bytes(final int size, final int value) {
        final byte[] result = new byte[size];
        Arrays.fill(result, (byte) value);
        return result;
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
}
