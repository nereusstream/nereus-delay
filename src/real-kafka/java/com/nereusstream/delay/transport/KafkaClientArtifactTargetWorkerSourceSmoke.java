package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.ownership.SourceApplyCoordinator;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.TargetOwnerDrainCoordinator;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.TargetWorkerHostRuntime;
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
import com.nereusstream.delay.protocol.OwnerIdentity;
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
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetStoreBackendFailureSmokeBridge;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.producer.GuardedProducer;
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
        if (arguments.length != 2
                && arguments.length != 3
                && arguments.length != 6
                && arguments.length != 7
                && arguments.length != 10) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <source-topic-prefix> "
                    + "[no-injection | store-write-response-unknown | target-expire-not-found | "
                    + "target-expire-not-found-ack-loss | target-expire-not-found-ack-loss-reopen | "
                    + "target-expire-not-found-network-ack-loss | target-expire-scheduled-message | "
                    + "target-expire-scheduled-message-network-ack-loss | "
                    + "network-response-loss "
                    + "<hold-file> <release-file> <dropped-response-file>] "
                    + "[network-expiry-response-loss <hold-file> <release-file> <dropped-response-file> "
                    + "target-expire-not-found-network-ack-loss | "
                    + "target-expire-scheduled-message-network-ack-loss] "
                    + "[network-response-loss-process-crash <phase> <hold-file> <release-file> "
                    + "<dropped-response-file> <state-file> <store-root> <ready-file>]");
        }
        final String bootstrap = arguments[0];
        final String scenario = arguments.length == 3 ? arguments[2] : arguments.length == 7 ? arguments[6] : "";
        final boolean targetExpiryAckResponseLossReopen =
                "target-expire-not-found-ack-loss-reopen".equals(scenario);
        final boolean targetExpiryAckResponseLoss = targetExpiryAckResponseLossReopen
                || "target-expire-not-found-ack-loss".equals(scenario);
        final boolean targetExpiryNotFound =
                targetExpiryAckResponseLoss
                        || "target-expire-not-found".equals(scenario)
                        || "target-expire-not-found-network-ack-loss".equals(scenario);
        final boolean targetScheduledMessageExpiry = "target-expire-scheduled-message".equals(scenario)
                || "target-expire-scheduled-message-network-ack-loss".equals(scenario);
        final boolean targetExpiryNetworkAckResponseLoss = arguments.length == 7
                && "network-expiry-response-loss".equals(arguments[2]);
        final boolean networkExpiryScenario = "target-expire-not-found-network-ack-loss".equals(scenario)
                || "target-expire-scheduled-message-network-ack-loss".equals(scenario);
        final AckInjection ackInjection = targetExpiryAckResponseLossReopen
                ? AckInjection.withExpiryAckResponseLossReopen()
                : targetExpiryAckResponseLoss ? AckInjection.withExpiryAckResponseLoss()
                : targetExpiryNetworkAckResponseLoss
                        ? AckInjection.from(arguments)
                        : targetExpiryNotFound || targetScheduledMessageExpiry
                                ? AckInjection.acked()
                                : AckInjection.from(arguments);
        if (targetExpiryNetworkAckResponseLoss != networkExpiryScenario) {
            throw new IllegalArgumentException("expiry network ACK loss requires its dedicated Target scenario");
        }
        if (ackInjection.crashPhase() == CrashPhase.RESUME) {
            replayAfterProcessCrash(bootstrap, ackInjection);
            return;
        }
        final String topic = arguments[1] + "-target-" + UUID.randomUUID();
        final String groupId = "nereus-delay-target-source-" + UUID.randomUUID();
        if (ackInjection.networkResponseLoss()) {
            final boolean gateReady = targetExpiryNetworkAckResponseLoss
                    ? !Files.exists(ackInjection.holdFile())
                    : Files.exists(ackInjection.holdFile());
            if (!gateReady
                    || Files.exists(ackInjection.releaseFile())
                    || Files.exists(ackInjection.droppedResponseFile())) {
                throw new IllegalArgumentException(targetExpiryNetworkAckResponseLoss
                        ? "expiry network ACK response-loss gate must start open and unobserved"
                        : "network ACK response-loss gate must start held and unobserved");
            }
        }
        final Map<String, Object> adminConfig = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        try (Admin admin = Admin.create(adminConfig)) {
            try {
            final NewTopic sourceTopic = new NewTopic(topic, 2, (short) 1);
            sourceTopic.configs(Map.of("message.timestamp.type", "LogAppendTime"));
            admin.createTopics(List.of(sourceTopic)).all().get(30, TimeUnit.SECONDS);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = admin.describeTopics(List.of(topic))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS)
                    .get(topic)
                    .topicId();
            if (topicId == null || topicId.equals(Uuid.ZERO_UUID)) {
                throw new IllegalStateException("Kafka did not return the exact source TopicId");
            }
            if (targetScheduledMessageExpiry) {
                KafkaClientArtifactTargetScheduledExpirySmoke.run(
                        admin,
                        bootstrap,
                        topic,
                        clusterId,
                        topicId,
                        ackInjection.mode(),
                        ackInjection.holdFile(),
                        ackInjection.releaseFile(),
                        ackInjection.droppedResponseFile());
                return;
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

            final boolean exerciseSibling = ackInjection.mode() == AckMode.NETWORK_RESPONSE_LOSS
                    && ackInjection.crashPhase() == CrashPhase.NONE;
            final ShardId siblingShard = new ShardId(shard.routeIncarnation(), 1);
            final TargetQuotaScope siblingScope = new TargetQuotaScope(
                    siblingShard, Bytes.sha256(Bytes.utf8("target-source-tenant")), null);
            final RootControl siblingRootControl;
            final PreparedCommand siblingCommand;
            final org.apache.kafka.clients.producer.RecordMetadata siblingRootMetadata;
            final org.apache.kafka.clients.producer.RecordMetadata siblingCommandMetadata;
            if (exerciseSibling) {
                siblingRootControl = firstGrant(siblingScope, siblingShard, sourceTime, actor, signingKeys);
                final long siblingCommandTime = Math.addExact(sourceTime, 2);
                final var siblingRoutingId =
                        SelfRoutingId.uuidV7(siblingCommandTime, new java.security.SecureRandom());
                final var siblingCommandId =
                        new CommandId(SelfRoutingId.fromLogicalUuid(siblingShard, siblingRoutingId).bytes());
                siblingCommand = PreparedCommand.cancel(
                        siblingShard,
                        siblingCommandId,
                        DelayMessageId.random(siblingShard),
                        new MessagePrecondition(null, null),
                        Math.addExact(siblingCommandTime, 600_000));
                siblingRootMetadata = produce(
                        bootstrap,
                        topic,
                        siblingShard.partition(),
                        siblingCommandTime,
                        siblingRootControl.mutation().systemMutationId(),
                        siblingRootControl.mutation().encodeFrame());
                siblingCommandMetadata = produce(
                        bootstrap,
                        topic,
                        siblingShard.partition(),
                        Math.addExact(siblingCommandTime, 1),
                        siblingCommand.commandId().bytes(),
                        com.nereusstream.delay.protocol.CommandCodec.encodeFrame(siblingCommand));
                if (siblingRootMetadata.offset() != 0 || siblingCommandMetadata.offset() != 1) {
                    throw new IllegalStateException("fresh Kafka sibling Shard did not start at offsets 0 and 1");
                }
            } else {
                siblingRootControl = null;
                siblingCommand = null;
                siblingRootMetadata = null;
                siblingCommandMetadata = null;
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
            final OxiaSyncOwnerLeaseBackend.ClientHandle ownerClient = openProcessCrashOwnerClient(ackInjection);
            try (ownerClient) {
                final OxiaOwnerLeaseStore leases = ownerClient == null
                        ? new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore())
                        : new OxiaOwnerLeaseStore(ownerClient.backend());
                final byte[] ownerSessionIdentity = ownerClient == null
                        ? Bytes.sha256(Bytes.utf8("target-source-session-" + UUID.randomUUID()))
                        : ownerClient.sessionIdentity();
                final String ownerLeasePrefix = ownerClient == null || ackInjection.crashPhase() == CrashPhase.NONE
                        ? null
                        : processCrashOwnerLeasePrefix(ackInjection);
                final long leaseNow = System.currentTimeMillis();
                final var acquiring = leases.acquire(
                                assignment,
                                "target-source-owner-" + UUID.randomUUID(),
                                ownerSessionIdentity,
                                leaseNow,
                                60_000)
                        .orElseThrow(() -> new IllegalStateException("test Owner lease acquisition failed"));

                final boolean preserveStoreAfterTurn = ackInjection.crashPhase() == CrashPhase.PREPARE
                        || ackInjection.expiryAckResponseLossReopen();
                final Path storeRoot = ackInjection.crashPhase() == CrashPhase.PREPARE
                        ? ackInjection.storeRoot()
                        : Files.createTempDirectory("nereus-delay-kafka-target-source-");
                final CheckedAction siblingProgress = exerciseSibling
                        ? () -> {
                            final var siblingSource = new KafkaSourcePosition(
                                    siblingShard,
                                    clusterId,
                                    toUuid(topicId),
                                    siblingRootMetadata.offset(),
                                    null,
                                    siblingRootMetadata.timestamp());
                            final var siblingAssignment = new com.nereusstream.delay.ownership.SourceAssignment(
                                    siblingShard,
                                    Bytes.sha256(Bytes.utf8("target-source-assignment-" + UUID.randomUUID())),
                                    1,
                                    new KafkaActivationBarrier(siblingShard, clusterId, toUuid(topicId), 1));
                            final var siblingAcquiring = leases.acquire(
                                            siblingAssignment,
                                            "target-source-sibling-owner-" + UUID.randomUUID(),
                                            ownerSessionIdentity,
                                            System.currentTimeMillis(),
                                            60_000)
                                    .orElseThrow(() -> new IllegalStateException(
                                            "test sibling Owner lease acquisition failed"));
                            final Path siblingStoreRoot = Files.createTempDirectory(
                                    "nereus-delay-kafka-target-source-sibling-");
                            try {
                                runTargetSourceTurn(
                                        admin,
                                        bootstrap,
                                        topic,
                                        groupId,
                                        clusterId,
                                        toUuid(topicId),
                                        siblingScope,
                                        siblingSource,
                                        siblingAssignment,
                                        siblingAcquiring,
                                        leases,
                                        ownerSessionIdentity,
                                        null,
                                        actor,
                                        signingKeys,
                                        siblingRootControl,
                                        siblingCommand,
                                        siblingCommandMetadata.offset(),
                                        siblingStoreRoot,
                                        AckInjection.acked(),
                                        null,
                                        false,
                                        false);
                            } finally {
                                leases.current(siblingShard).ifPresent(leases::release);
                                deleteTree(siblingStoreRoot);
                            }
                            System.out.println(
                                    "Kafka Target sibling Shard progressed while the first Shard retained "
                                            + "ACK_UNKNOWN: partition=1, brokerOffset=2, status=APPLIED_AND_ACKED.");
                        }
                        : null;
                try {
                    if (preserveStoreAfterTurn) {
                        Files.createDirectories(storeRoot);
                    }
                    final TargetExpiryFixture completedExpiryFixture = runTargetSourceTurn(
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
                            ownerSessionIdentity,
                            ownerLeasePrefix,
                            actor,
                            signingKeys,
                            rootControl,
                            command,
                            commandMetadata.offset(),
                            storeRoot,
                            ackInjection,
                            siblingProgress,
                            ownerClient != null && exerciseSibling,
                            targetExpiryNotFound);
                    if (ackInjection.expiryAckResponseLossReopen()) {
                        if (completedExpiryFixture == null) {
                            throw new IllegalStateException("expiry ACK response-loss replay has no expiry fixture");
                        }
                        runTargetSourceExpiryReplayAfterUnknown(
                                admin,
                                bootstrap,
                                topic,
                                groupId,
                                clusterId,
                                toUuid(topicId),
                                scope,
                                assignment,
                                leases,
                                ownerSessionIdentity,
                                completedExpiryFixture.mutation(),
                                commandMetadata.offset() + 1,
                                commandMetadata.offset() + 2,
                                storeRoot);
                    }
                } finally {
                    if (ackInjection.crashPhase() != CrashPhase.PREPARE) {
                        deleteTree(storeRoot);
                    }
                }
                final String failureBoundary = switch (ackInjection.mode()) {
                    case NO_INJECTION -> "Broker OffsetCommit response reached the Kafka client";
                    case CLIENT_DELEGATE_RESPONSE_LOSS ->
                        "simulated lost commitSync response after the Kafka client returned success";
                    case EXPIRY_ACK_RESPONSE_LOSS ->
                        "in-process wrapper lost expiry commitSync response after "
                                + "the Kafka client returned success";
                    case EXPIRY_ACK_RESPONSE_LOSS_REOPEN ->
                        "in-process wrapper lost expiry commitSync response after "
                                + "the Kafka client returned success, followed by replacement Owner replay";
                    case STORE_WRITE_RESPONSE_UNKNOWN ->
                        "RocksDB applied the source batch but its native write response was lost before source ACK";
                    case NETWORK_RESPONSE_LOSS ->
                        "Broker OffsetCommit response was received by the TCP proxy and withheld before reaching "
                                + "the Kafka client";
                    case EXPIRY_NETWORK_RESPONSE_LOSS ->
                        "Target expiry Broker OffsetCommit response was received by the TCP proxy and withheld "
                                + "before reaching the Kafka client";
                };
                if (ackInjection.storeWriteResponseUnknown()) {
                    System.out.println("Kafka Target source factory: first grant at offset 0; Cancel at offset 1 "
                            + "was written by RocksDB before the native response failed; Broker offset remained "
                            + "uncommitted and the Worker retained/fenced the exact entry.");
                    System.out.println("  replacement Owner reopened RocksDB, replayed the exact Broker record "
                            + "without another Store write, and confirmed group offset 2; TopicId/partition guard "
                            + "verified.");
                } else if (ackInjection.expiryAckResponseLoss()) {
                    System.out.println("Kafka Target source factory: first grant at offset 0; Cancel at offset 1 "
                            + "was applied and ACKed; EXPIRE_GENERATION applied with NOT_FOUND; "
                            + failureBoundary + ".");
                    if (ackInjection.expiryAckResponseLossReopen()) {
                        System.out.println("  a replacement Owner reopened the same Store at offset "
                                + (commandMetadata.offset() + 1) + "; its assignment resumed from the Broker's "
                                + "committed frontier, then ACKed the exact duplicate expiry record; "
                                + "TopicId/partition guard verified.");
                    } else {
                        System.out.println("  the Worker retained the exact applied source entry and retried only "
                                + "the ACK, without another Store write; TopicId/partition guard verified.");
                    }
                } else {
                    System.out.println("Kafka Target source factory: first grant at offset 0; Cancel at offset 1 "
                            + "applied; " + failureBoundary + ";");
                    System.out.println("  replacement Owner reopened RocksDB, replayed the exact Broker record "
                            + "without a second Store write, and confirmed Kafka group offset 2; TopicId/partition "
                            + "guard verified.");
                }
            }
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

    static RootControl firstGrant(
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

    private static TargetExpiryFixture runTargetSourceTurn(
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
            final byte[] ownerSessionIdentity,
            final String ownerLeasePrefix,
            final ControlAuthorizationContext actor,
            final KeyPair signingKeys,
            final RootControl rootControl,
            final PreparedCommand command,
            final long commandOffset,
            final Path storeRoot,
            final AckInjection ackInjection,
            final CheckedAction siblingProgress,
            final boolean loseOwnerBeforeSibling,
            final boolean targetExpiryNotFound) throws Exception {
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
        final AtomicBoolean activeLeaseReleased = new AtomicBoolean();
        final TargetExpiryFixture[] expiryFixtureAfterTurn = new TargetExpiryFixture[1];
        try (var resources = KafkaSmokeWorkerResources.open(config);
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
            final TargetExpiryFixture expiryFixture = targetExpiryNotFound
                    ? targetExpiryFixture(scope.shard(), command.delayMessageId(), active, signingKeys)
                    : null;
            expiryFixtureAfterTurn[0] = expiryFixture;
            final java.util.concurrent.atomic.AtomicInteger expiryAuthorityResolutions =
                    new java.util.concurrent.atomic.AtomicInteger();
            if (expiryFixture != null) {
                final KafkaSourcePosition firstExpiry = appendExpiryMutation(
                        bootstrap, clusterId, topic, topicId, scope.shard(), expiryFixture.mutation());
                final KafkaSourcePosition duplicateExpiry = appendExpiryMutation(
                        bootstrap, clusterId, topic, topicId, scope.shard(), expiryFixture.mutation());
                if (firstExpiry.offset() != commandOffset + 1
                        || duplicateExpiry.offset() != commandOffset + 2) {
                    throw new IllegalStateException("Kafka Target expiry source records are not consecutive");
                }
            }
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
                            entry -> {
                                if (expiryFixture == null || !expiryFixture.mutation().equals(entry.mutation())) {
                                    throw new AssertionError("unexpected Target expiry control");
                                }
                                expiryAuthorityResolutions.incrementAndGet();
                                final var expiryAuthor = expiryFixture.author();
                                return new TargetSourceApplyRuntime.ExpiryControl(
                                        (actualScope, author, mutation, position) -> {
                                            if (!scope.equals(actualScope)
                                                    || !Arrays.equals(
                                                            expiryAuthor.canonicalBytes(), author.canonicalBytes())
                                                    || !expiryFixture.mutation().equals(mutation)
                                                    || !source.sameSourceIdentity(position)) {
                                                throw new IllegalStateException(
                                                        "Target expiry authority received another signed source entry");
                                            }
                                            return new com.nereusstream.delay.runtime.TargetExpireGenerationVerifier
                                                    .Authorization(
                                                            signingKeys.getPublic(),
                                                            1,
                                                            (proofScope, proofAuthor, proofSource, evidence) ->
                                                                    scope.equals(proofScope)
                                                                            && Arrays.equals(
                                                                                    expiryAuthor.canonicalBytes(),
                                                                                    proofAuthor.canonicalBytes())
                                                                            && source.sameSourceIdentity(proofSource)
                                                                            && Arrays.equals(
                                                                                    expiryFixture.evidence()
                                                                                            .canonicalBytes(),
                                                                                    evidence.canonicalBytes()));
                                        },
                                        ownerCommitAuthority(leases, active));
                            },
                            entry -> { throw new AssertionError("unexpected Target Close control"); },
                            entry -> { throw new AssertionError("unexpected membership control"); },
                            ownerCommitAuthority(leases, active),
                            ownerReadAuthority(leases, active),
                            entry -> commandControl),
                    new TargetSourceApplyRuntime.Limits(2048, 16L << 20, 60_000_000_000L, 16, 1),
                    System::nanoTime);
            final var consumer = newSourceConsumer(
                    bootstrap, groupId, clusterId, topic, topicId, scope.shard(), ackInjection.mode());
            final var ackResponseLost = new AtomicBoolean();
            final var guardedConsumer = switch (ackInjection.mode()) {
                case CLIENT_DELEGATE_RESPONSE_LOSS -> loseCommitSyncResponse(consumer, ackResponseLost, 1);
                case EXPIRY_ACK_RESPONSE_LOSS, EXPIRY_ACK_RESPONSE_LOSS_REOPEN ->
                    loseCommitSyncResponse(consumer, ackResponseLost, 2);
                default -> consumer;
            };
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
            TargetWorkerHostRuntime host = null;
            final CountDownLatch hostMaintenanceTick = new CountDownLatch(1);
            try {
                final boolean expectApplyFailure = ackInjection.storeWriteResponseUnknown();
                final boolean expectAckUnknown = ackInjection.acknowledgementResponseUnknown();
                final boolean replacementOwnerExpiryReplay = ackInjection.expiryAckResponseLossReopen();
                if (!expectApplyFailure && !expectAckUnknown && !replacementOwnerExpiryReplay) {
                    worker.configureMessageExpiryMaintenance(
                            mutation -> {
                                throw new AssertionError("unexpected Target message expiry append");
                            },
                            () -> {
                                final long now = System.currentTimeMillis();
                                final var evidence = new TrustedUtcIntervalEvidence(
                                        Math.max(0, now - 1),
                                        now,
                                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                                        Bytes.utf8("kafka-target-worker-host-clock"),
                                        1,
                                        1,
                                        Math.max(0, System.nanoTime()),
                                        Bytes.sha256(Bytes.utf8("kafka-target-worker-host-clock-evidence")),
                                        0,
                                        null);
                                hostMaintenanceTick.countDown();
                                return new TargetWorkerShardRuntime.MessageExpiryRequest(
                                        budget(),
                                        evidence,
                                        Math.addExact(now, 600_000),
                                        new OwnerIdentity(
                                                Bytes.utf8("kafka-target-worker-deployment"),
                                                Bytes.utf8("kafka-target-worker-host-run"),
                                                active.ownerEpoch(),
                                                Bytes.sha256(active.leaseToken())),
                                        1,
                                        signingKeys.getPrivate());
                            });
                    host = TargetWorkerHostRuntime.start(
                            workerClasses,
                            resources,
                            List.of(worker),
                            new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                            Duration.ofHours(1),
                            failure -> {});
                    if (!hostMaintenanceTick.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("real Kafka Target Worker Host did not run maintenance");
                    }
                }
                if (expectApplyFailure) {
                    TargetStoreBackendFailureSmokeBridge.failNextCommitAfterNativeWrite(backend);
                }
                final SourceApplyCoordinator.TurnResult result = expectApplyFailure
                        ? runUntilApplyFailure(worker)
                        : expectAckUnknown
                                ? runUntilAckUnknown(worker)
                                : replacementOwnerExpiryReplay
                                        ? runUntilApplied(worker)
                                        : runUntilAppliedByHost(host);
                if (ackInjection.mode() == AckMode.NETWORK_RESPONSE_LOSS) {
                    final String dropped = Files.exists(ackInjection.droppedResponseFile())
                            ? Files.readString(ackInjection.droppedResponseFile())
                            : "";
                    if (!dropped.contains("apiKey=8")
                            || !dropped.contains("brokerResponseReceived=true forwarded=false")) {
                        throw new IllegalStateException(
                                "TCP proxy did not withhold a Broker OffsetCommit response: " + dropped);
                    }
                    ackResponseLost.set(true);
                }
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
                final StableCode resultCode = result.appliedOutcome() == null
                        ? null
                        : result.appliedOutcome().commandResult().stableCode();
                final SourceApplyCoordinator.TurnStatus expectedStatus = expectApplyFailure
                        ? SourceApplyCoordinator.TurnStatus.APPLY_FAILURE
                        : expectAckUnknown
                                ? SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN
                                : SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED;
                if (result.status() != expectedStatus
                        || !exactCommand
                        || !exactPosition
                        || ((expectApplyFailure || expectAckUnknown) && result.appliedOutcome() != null)
                        || (!expectApplyFailure && !expectAckUnknown && resultCode != StableCode.NOT_FOUND)
                        || ackResponseLost.get() != expectAckUnknown) {
                    throw new IllegalStateException(
                            "real Kafka Target Worker produced an unexpected source turn: status="
                                    + result.status() + ", exactCommand=" + exactCommand + ", exactPosition="
                                    + exactPosition + ", resultCode=" + resultCode
                                    + ", ackResponseLost=" + ackResponseLost.get() + ", entry=" + result.entry()
                                    + ", failure="
                                    + result.failure());
                }
                if (expectApplyFailure) {
                    if (!store.isWriteOutcomeUncertain()
                            || !sourceRuntime.fenced()
                            || !worker.pendingSourceEntry().filter(result.entry()::equals).isPresent()) {
                        throw new IllegalStateException(
                                "uncertain Target native write did not fence and retain the exact Broker entry");
                    }
                } else {
                    final var applied = store.appliedShardLogPosition();
                    if (!(applied instanceof KafkaSourcePosition kafka) || kafka.offset() != commandOffset
                            || store.shardMutationSequence() != 2) {
                        throw new IllegalStateException(
                                "Target Store did not durably advance before uncertain source ACK");
                    }
                }
                final var committedOffsets = admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
                        .get(10, TimeUnit.SECONDS);
                final var committed = committedOffsets.get(new TopicPartition(topic, scope.shard().partition()));
                if (expectApplyFailure ? committed != null
                        : committed == null || committed.offset() != commandOffset + 1) {
                    throw new IllegalStateException(expectApplyFailure
                            ? "Kafka Broker committed the source offset after the Target Store apply failed"
                            : "Kafka Broker did not commit the source offset before the injected response loss");
                }
                if (ackInjection.crashPhase() == CrashPhase.PREPARE) {
                    writeProcessCrashState(ackInjection, topic, groupId, clusterId, topicId, scope, assignment,
                            command, commandOffset, active.ownerEpoch(), ownerLeasePrefix);
                } else if (ackInjection.mode() == AckMode.NETWORK_RESPONSE_LOSS) {
                    Files.writeString(ackInjection.releaseFile(), "release\n");
                    if (siblingProgress != null) {
                        final var pendingBeforeSibling = worker.pendingSourceEntry().orElseThrow(() ->
                                new IllegalStateException("first Shard lost its ACK_UNKNOWN obligation"));
                        final long mutationBeforeSibling = store.shardMutationSequence();
                        if (loseOwnerBeforeSibling) {
                            if (!leases.release(active)) {
                                throw new IllegalStateException("live Oxia Owner release was not observed");
                            }
                            activeLeaseReleased.set(true);
                            final long sequenceBeforeOwnerRetry = store.latestSequenceNumber();
                            final SourceApplyCoordinator.TurnResult ownerLossTurn = worker.runSourceTurn(
                                    new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                                    System::currentTimeMillis);
                            if (ownerLossTurn.status() != SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN
                                    || !pendingBeforeSibling.equals(worker.pendingSourceEntry().orElse(null))
                                    || !sourceRuntime.fenced()
                                    || leases.current(scope.shard()).isPresent()
                                    || store.latestSequenceNumber() != sequenceBeforeOwnerRetry
                                    || store.shardMutationSequence() != mutationBeforeSibling) {
                                throw new IllegalStateException(
                                        "live Oxia Owner loss did not fence only the pending source Shard");
                            }
                            System.out.println(
                                    "Kafka Target live Oxia Owner loss fenced only partition 0; pending ACK_UNKNOWN "
                                            + "and Store mutation sequence were preserved.");
                        }
                        siblingProgress.run();
                        if (!worker.pendingSourceEntry().filter(pendingBeforeSibling::equals).isPresent()
                                || store.shardMutationSequence() != mutationBeforeSibling
                                || (loseOwnerBeforeSibling && leases.current(scope.shard()).isPresent())
                                || (!loseOwnerBeforeSibling
                                        && !active.sameIdentity(leases.current(scope.shard()).orElseThrow()))
                                || sourceRuntime.fenced() != loseOwnerBeforeSibling) {
                            throw new IllegalStateException(
                                    "sibling source progress changed the first Shard ACK_UNKNOWN state");
                        }
                    }
                }
                if (expiryFixture != null) {
                    final SourceApplyCoordinator.TurnResult firstExpiryTurn;
                    if (ackInjection.expiryAckResponseLoss()) {
                        if (ackInjection.expiryNetworkResponseLoss()) {
                            Files.writeString(ackInjection.holdFile(), "hold\n");
                        }
                        final var unknownExpiryTurn = ackInjection.expiryAckResponseLossReopen()
                                ? runUntilAckUnknown(worker)
                                : runUntilAckUnknownByHost(host);
                        if (ackInjection.expiryNetworkResponseLoss()) {
                            final String dropped = Files.exists(ackInjection.droppedResponseFile())
                                    ? Files.readString(ackInjection.droppedResponseFile())
                                    : "";
                            if (!dropped.contains("apiKey=8")
                                    || !dropped.contains("brokerResponseReceived=true forwarded=false")) {
                                throw new IllegalStateException(
                                        "TCP proxy did not withhold the Target expiry Broker ACK response: "
                                                + dropped);
                            }
                            ackResponseLost.set(true);
                        }
                        if (unknownExpiryTurn.status() != SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN
                                || !(unknownExpiryTurn.entry() instanceof com.nereusstream.delay.ownership
                                        .SourceReplayMutation unknownExpiryEntry)
                                || !expiryFixture.mutation().equals(unknownExpiryEntry.mutation())
                                || !(unknownExpiryEntry.position() instanceof KafkaSourcePosition unknownPosition)
                                || unknownPosition.offset() != commandOffset + 1
                                || !ackResponseLost.get()
                                || worker.pendingSourceEntry().filter(unknownExpiryEntry::equals).isEmpty()
                                || expiryAuthorityResolutions.get() != 1) {
                            throw new IllegalStateException(
                                    "Target expiry ACK response loss did not retain the exact applied Broker record");
                        }
                        final var sourceAfterUnknown = store.appliedShardLogPosition();
                        final long sequenceAfterUnknown = store.latestSequenceNumber();
                        final long mutationsAfterUnknown = store.shardMutationSequence();
                        if (!(sourceAfterUnknown instanceof KafkaSourcePosition appliedExpiryPosition)
                                || appliedExpiryPosition.offset() != commandOffset + 1) {
                            throw new IllegalStateException(
                                    "Target Store did not durably apply expiry before Broker ACK response loss");
                        }
                        final var committedAfterUnknown = admin.listConsumerGroupOffsets(groupId)
                                .partitionsToOffsetAndMetadata()
                                .get(10, TimeUnit.SECONDS)
                                .get(new TopicPartition(topic, scope.shard().partition()));
                        if (committedAfterUnknown == null || committedAfterUnknown.offset() != commandOffset + 2) {
                            throw new IllegalStateException(
                                    "Kafka Broker did not commit expiry offset before its response was lost");
                        }

                        if (ackInjection.expiryAckResponseLossReopen()) {
                            return expiryFixture;
                        }

                        if (ackInjection.expiryNetworkResponseLoss()) {
                            Files.writeString(ackInjection.releaseFile(), "release\n");
                        }
                        firstExpiryTurn = runUntilAppliedByHost(host);
                        if (firstExpiryTurn.status() != SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED
                                || !unknownExpiryEntry.equals(firstExpiryTurn.entry())
                                || firstExpiryTurn.appliedOutcome() == null
                                || firstExpiryTurn.appliedOutcome().systemMutationResult().stableCode()
                                        != StableCode.NOT_FOUND
                                || worker.pendingSourceEntry().isPresent()
                                || store.latestSequenceNumber() != sequenceAfterUnknown
                                || store.shardMutationSequence() != mutationsAfterUnknown
                                || expiryAuthorityResolutions.get() != 1) {
                            throw new IllegalStateException(
                                    "Target expiry ACK retry reapplied Store state or changed the pending entry");
                        }
                        final var committedAfterRetry = admin.listConsumerGroupOffsets(groupId)
                                .partitionsToOffsetAndMetadata()
                                .get(10, TimeUnit.SECONDS)
                                .get(new TopicPartition(topic, scope.shard().partition()));
                        if (committedAfterRetry == null || committedAfterRetry.offset() != commandOffset + 2) {
                            throw new IllegalStateException("Kafka expiry ACK retry changed the committed frontier");
                        }
                    } else {
                        firstExpiryTurn = runUntilAppliedByHost(host);
                    }
                    if (firstExpiryTurn.status() != SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED
                            || !(firstExpiryTurn.entry() instanceof com.nereusstream.delay.ownership
                                    .SourceReplayMutation firstExpiryEntry)
                            || !expiryFixture.mutation().equals(firstExpiryEntry.mutation())
                            || !(firstExpiryEntry.position() instanceof KafkaSourcePosition firstExpiryPosition)
                            || firstExpiryPosition.offset() != commandOffset + 1
                            || firstExpiryTurn.appliedOutcome() == null
                            || firstExpiryTurn.appliedOutcome().systemMutationResult().applyStatus()
                                    != com.nereusstream.delay.runtime.ApplyStatus.APPLIED
                            || firstExpiryTurn.appliedOutcome().systemMutationResult().stableCode()
                                    != StableCode.NOT_FOUND
                            || expiryAuthorityResolutions.get() != 1) {
                        throw new IllegalStateException(
                                "real Kafka Target EXPIRE_GENERATION first application did not apply and ACK");
                    }
                    final var duplicateExpiryTurn = runUntilAppliedByHost(host);
                    if (duplicateExpiryTurn.status() != SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED
                            || !(duplicateExpiryTurn.entry() instanceof com.nereusstream.delay.ownership
                                    .SourceReplayMutation duplicateExpiryEntry)
                            || !expiryFixture.mutation().equals(duplicateExpiryEntry.mutation())
                            || !(duplicateExpiryEntry.position() instanceof KafkaSourcePosition duplicatePosition)
                            || duplicatePosition.offset() != commandOffset + 2
                            || duplicateExpiryTurn.appliedOutcome() == null
                            || duplicateExpiryTurn.appliedOutcome().systemMutationResult().stableCode()
                                    != StableCode.NOT_FOUND
                            || expiryAuthorityResolutions.get() != 1) {
                        throw new IllegalStateException(
                                "real Kafka Target EXPIRE_GENERATION duplicate did not replay its first result");
                    }
                    final var appliedPosition = store.appliedShardLogPosition();
                    if (!(appliedPosition instanceof KafkaSourcePosition appliedKafka)
                            || appliedKafka.offset() != commandOffset + 2
                            || !source.sameSourceIdentity(appliedKafka)) {
                        throw new IllegalStateException(
                                "Target Store did not persist the duplicate expiry source frontier");
                    }
                    final var expiryCommittedOffsets = admin.listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get(10, TimeUnit.SECONDS);
                    final var expiryCommitted = expiryCommittedOffsets.get(
                            new TopicPartition(topic, scope.shard().partition()));
                    if (expiryCommitted == null || expiryCommitted.offset() != commandOffset + 3) {
                        throw new IllegalStateException(
                                "Kafka Broker did not commit first and duplicate Target expiry source records");
                    }
                    System.out.println(
                            "Kafka Target EXPIRE_GENERATION first application and immutable duplicate replay passed: "
                                    + "Broker offsets " + (commandOffset + 1) + "/" + (commandOffset + 2)
                                    + " ACKed; expiry authority resolved once.");
                    if (ackInjection.expiryAckResponseLoss()) {
                        System.out.println(
                                "Kafka Target EXPIRE_GENERATION ACK response-loss recovery passed: the Broker "
                                        + "committed the first expiry offset before the client lost the response; "
                                        + "retry ACKed the retained entry without another Store write.");
                    }
                }
            } finally {
                if (host != null) {
                    final var drained = host.drainAll(
                            new TargetOwnerDrainCoordinator.Request(
                                    Math.addExact(System.currentTimeMillis(), 60_000),
                                    new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10))),
                            new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                            System::currentTimeMillis);
                    if (!drained.complete()
                            || drained.shards().size() != 1
                            || drained.shards().getFirst().status() != TargetWorkerHostRuntime.Status.RELEASED) {
                        throw new IllegalStateException("real Kafka Target Worker Host did not drain completely: "
                                + drained.shards());
                    }
                    if (host.firstMaintenanceFailure() != null) {
                        throw new IllegalStateException("real Kafka Target Worker Host maintenance failed",
                                host.firstMaintenanceFailure());
                    }
                    activeHolder[0] = null;
                    System.out.println("Kafka Target Worker Host lifecycle passed: real maintenance tick, source "
                            + "apply/ACK, and exact Owner/Store drain.");
                } else if (worker.pendingSourceEntry().isPresent()) {
                    consumer.close();
                    if (!activeLeaseReleased.get() && !leases.release(active)) {
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
        if (ackInjection.mode() != AckMode.NO_INJECTION && !ackInjection.expiryAckResponseLoss()) {
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
                    ownerSessionIdentity,
                    command,
                    commandOffset,
                    config,
                    ackInjection.mode());
        }
        return expiryFixtureAfterTurn[0];
    }

    private static long runTargetSourceReplayAfterUnknown(
            final Admin admin,
            final String bootstrap,
            final String topic,
            final String groupId,
            final String clusterId,
            final UUID topicId,
            final TargetQuotaScope scope,
            final com.nereusstream.delay.ownership.SourceAssignment assignment,
            final OxiaOwnerLeaseStore leases,
            final byte[] ownerSessionIdentity,
            final PreparedCommand command,
            final long commandOffset,
            final ShardStoreConfig config,
            final AckMode ackMode)
            throws Exception {
        final OwnerLease replayAcquiring = leases.acquire(
                        assignment,
                        "target-source-replay-owner-" + UUID.randomUUID(),
                        ownerSessionIdentity,
                        System.currentTimeMillis(),
                        60_000)
                .orElseThrow(() -> new IllegalStateException("replacement Target Owner lease acquisition failed"));
        final OwnerLease[] ownerHolder = {replayAcquiring};
        try (var resources = KafkaSmokeWorkerResources.open(config);
                var store = ShardStore.openTarget(config, scope.shard(), resources)) {
            if (Long.compareUnsigned(
                            replayAcquiring.ownerEpoch(), store.runtimeMetadata().lastOpenedOwnerEpoch())
                    <= 0) {
                throw new IllegalStateException("replacement Target Owner epoch did not advance past the Store");
            }
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
            final var consumer =
                    newSourceConsumer(bootstrap, groupId, clusterId, topic, topicId, scope.shard(), ackMode);
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
        return replayAcquiring.ownerEpoch();
    }

    private static void runTargetSourceExpiryReplayAfterUnknown(
            final Admin admin,
            final String bootstrap,
            final String topic,
            final String groupId,
            final String clusterId,
            final UUID topicId,
            final TargetQuotaScope scope,
            final com.nereusstream.delay.ownership.SourceAssignment assignment,
            final OxiaOwnerLeaseStore leases,
            final byte[] ownerSessionIdentity,
            final com.nereusstream.delay.protocol.SystemMutation expiryMutation,
            final long expiryOffset,
            final long duplicateExpiryOffset,
            final Path storeRoot)
            throws Exception {
        final var config = ShardStoreConfig.defaults(storeRoot);
        final var recoveryAssignment = new com.nereusstream.delay.ownership.SourceAssignment(
                scope.shard(),
                Bytes.sha256(Bytes.utf8("target-expiry-replay-assignment-" + UUID.randomUUID())),
                Math.addExact(assignment.assignmentEpoch(), 1),
                new KafkaActivationBarrier(scope.shard(), clusterId, topicId, duplicateExpiryOffset));
        final OwnerLease acquiring = leases.acquire(
                        recoveryAssignment,
                        "target-expiry-replay-owner-" + UUID.randomUUID(),
                        ownerSessionIdentity,
                        System.currentTimeMillis(),
                        60_000)
                .orElseThrow(() -> new IllegalStateException("replacement expiry Owner lease acquisition failed"));
        final OwnerLease[] ownerHolder = {acquiring};
        try (var resources = KafkaSmokeWorkerResources.open(config);
                var store = ShardStore.openTarget(config, scope.shard(), resources)) {
            if (Long.compareUnsigned(acquiring.ownerEpoch(), store.runtimeMetadata().lastOpenedOwnerEpoch()) <= 0) {
                throw new IllegalStateException("replacement expiry Owner epoch did not advance past the Store");
            }
            final var persistedSource = store.appliedShardLogPosition();
            if (!(persistedSource instanceof KafkaSourcePosition persistedKafka)
                    || persistedKafka.offset() != expiryOffset
                    || !clusterId.equals(persistedKafka.authenticatedClusterId())
                    || !topicId.equals(persistedKafka.nativeTopicUuid())) {
                throw new IllegalStateException("replacement Owner did not reopen the Store at the ACK-unknown "
                        + "expiry position: " + persistedSource);
            }
            requireCommittedOffset(admin, groupId, topic, scope.shard(), expiryOffset + 1);
            final var reopened = TargetStoreBootstrap.reopen(
                    store,
                    scope,
                    new TargetStoreBackend.WriteLimits(128, 4 << 20),
                    budget(),
                    ownerReadAuthority(leases, acquiring));
            final var active = TargetWorkerOwnerActivation.activate(
                    reopened, store, recoveryAssignment, acquiring, leases, System::currentTimeMillis);
            ownerHolder[0] = active;
            final var workClasses = workClasses();
            final var closeControls = new TargetCloseStore(
                            reopened.backend(), scope, reopened.root().recoveryLineage(), 16, 1)
                    .reservationControls((reader, binding) -> Optional.empty());
            final var runtime = new TargetSourceApplyRuntime(
                    reopened,
                    store,
                    recoveryAssignment,
                    active,
                    new TargetSourceApplyRuntime.Authorities(
                            leases,
                            SourceReplaySuccessor.strictKafka(),
                            entry -> { throw new AssertionError("expiry replay resolved a quota grant"); },
                            entry -> { throw new AssertionError("expiry replay resolved a time fence"); },
                            entry -> { throw new AssertionError("expiry replay resolved expiry authority"); },
                            entry -> { throw new AssertionError("expiry replay resolved a Close control"); },
                            entry -> { throw new AssertionError("expiry replay resolved membership control"); },
                            ownerCommitAuthority(leases, active),
                            ownerReadAuthority(leases, active),
                            entry -> { throw new AssertionError("expiry replay resolved command authority"); }),
                    new TargetSourceApplyRuntime.Limits(2048, 16L << 20, 60_000_000_000L, 16, 1),
                    System::nanoTime);
            final var consumer = newSourceConsumer(
                    bootstrap, groupId, clusterId, topic, topicId, scope.shard(), AckMode.NO_INJECTION);
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
                    recoveryAssignment,
                    workClasses,
                    store,
                    resources,
                    runtime,
                    maintenance);
            try {
                final long sequenceBeforeDuplicate = store.latestSequenceNumber();
                final long mutationBeforeDuplicate = store.shardMutationSequence();
                final SourceApplyCoordinator.TurnResult duplicateExpiry = runUntilApplied(worker);
                final boolean exactDuplicateExpiry = duplicateExpiry.entry() instanceof com.nereusstream.delay.ownership
                                .SourceReplayMutation replay
                        && replay.mutation().equals(expiryMutation)
                        && replay.position() instanceof KafkaSourcePosition position
                        && position.offset() == duplicateExpiryOffset
                        && clusterId.equals(position.authenticatedClusterId())
                        && topicId.equals(position.nativeTopicUuid());
                final StableCode duplicateResult = duplicateExpiry.appliedOutcome() == null
                        || duplicateExpiry.appliedOutcome().systemMutationResult() == null
                        ? null
                        : duplicateExpiry.appliedOutcome().systemMutationResult().stableCode();
                final var appliedPosition = store.appliedShardLogPosition();
                if (duplicateExpiry.status() != SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED
                        || !exactDuplicateExpiry
                        || duplicateResult != StableCode.NOT_FOUND
                        || !(appliedPosition instanceof KafkaSourcePosition appliedKafka)
                        || appliedKafka.offset() != duplicateExpiryOffset
                        || !clusterId.equals(appliedKafka.authenticatedClusterId())
                        || !topicId.equals(appliedKafka.nativeTopicUuid())
                        || store.latestSequenceNumber() <= sequenceBeforeDuplicate
                        || store.shardMutationSequence() <= mutationBeforeDuplicate
                        || !active.sameIdentity(leases.current(scope.shard()).orElseThrow())) {
                    throw new IllegalStateException(
                            "replacement Owner did not durably advance and ACK the physical duplicate expiry: "
                                    + "status=" + duplicateExpiry.status() + ", exactDuplicate="
                                    + exactDuplicateExpiry + ", result=" + duplicateResult + ", position="
                                    + appliedPosition + ", entry=" + duplicateExpiry.entry() + ", failure="
                                    + duplicateExpiry.failure());
                }
                requireCommittedOffset(admin, groupId, topic, scope.shard(), duplicateExpiryOffset + 1);
                System.out.println("Kafka Target EXPIRE_GENERATION ACK UNKNOWN replacement-Owner recovery passed: "
                        + "same Store reopened at offset " + expiryOffset + "; Broker had committed through "
                        + (expiryOffset + 1) + "; replacement assignment resumed at physical duplicate offset "
                        + duplicateExpiryOffset + " and advanced/ACKed it without re-resolving expiry authority.");
            } finally {
                if (worker.pendingSourceEntry().isPresent()) {
                    consumer.close();
                    if (!leases.release(active)) {
                        throw new IllegalStateException("replacement expiry Owner release was not observed");
                    }
                    ownerHolder[0] = null;
                } else {
                    worker.pauseNewTurns();
                    final OwnerLease draining = leases.transition(active, ShardLifecycleState.DRAINING)
                            .orElseThrow(() -> new IllegalStateException("replacement expiry Owner could not drain"));
                    try {
                        worker.closeSource();
                    } finally {
                        if (!leases.release(draining)) {
                            throw new IllegalStateException("replacement expiry Owner release was not observed");
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

    private static void requireCommittedOffset(
            final Admin admin,
            final String groupId,
            final String topic,
            final ShardId shard,
            final long expectedOffset)
            throws Exception {
        final var committedOffsets = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS);
        final var committed = committedOffsets.get(new TopicPartition(topic, shard.partition()));
        if (committed == null || committed.offset() != expectedOffset) {
            throw new IllegalStateException("Kafka source group offset mismatch: expected " + expectedOffset
                    + " but was " + (committed == null ? null : committed.offset()));
        }
    }

    static GuardedConsumer<byte[], byte[]> newSourceConsumer(
            final String bootstrap,
            final String groupId,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard,
            final AckMode ackMode) {
        final Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if (ackMode == AckMode.NETWORK_RESPONSE_LOSS || ackMode == AckMode.EXPIRY_NETWORK_RESPONSE_LOSS) {
            config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000);
            config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2_000);
        }
        return KafkaClientArtifactSourceConsumerFactory.create(
                config,
                clusterId,
                topic,
                topicId,
                shard.partition());
    }

    @SuppressWarnings("unchecked")
    private static GuardedConsumer<byte[], byte[]> loseCommitSyncResponse(
            final GuardedConsumer<byte[], byte[]> delegate,
            final AtomicBoolean observed,
            final int commitInvocationToLose) {
        if (commitInvocationToLose <= 0) {
            throw new IllegalArgumentException("commit invocation to lose must be positive");
        }
        final var commitInvocations = new AtomicInteger();
        return (GuardedConsumer<byte[], byte[]>) Proxy.newProxyInstance(
                KafkaClientArtifactTargetWorkerSourceSmoke.class.getClassLoader(),
                new Class<?>[] {GuardedConsumer.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("commitSync")
                            && method.getParameterCount() == 1
                            && commitInvocations.incrementAndGet() == commitInvocationToLose) {
                        try {
                            method.invoke(delegate, arguments);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                        observed.set(true);
                        throw new IllegalStateException("simulated lost response after Kafka commitSync returned");
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    static TargetStoreBackend.CommitAuthority ownerCommitAuthority(
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

    static TargetStoreBackend.ReadAuthority ownerReadAuthority(
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

    static SourceApplyCoordinator.TurnResult runUntilAppliedByHost(final TargetWorkerHostRuntime host) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        SourceApplyCoordinator.TurnResult result;
        do {
            result = host.runNextSourceTurn(
                            new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                            System::currentTimeMillis)
                    .result();
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

    static SourceApplyCoordinator.TurnResult runUntilAckUnknownByHost(final TargetWorkerHostRuntime host) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        SourceApplyCoordinator.TurnResult result;
        do {
            result = host.runNextSourceTurn(
                            new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                            System::currentTimeMillis)
                    .result();
            if (result.status() == SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN) {
                return result;
            }
            if (result.status() != SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status() != SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException(
                        "real Kafka Target Worker Host did not reach the ACK uncertainty cut: " + result.status(),
                        result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("real Kafka Target source did not reach ACK UNKNOWN before the deadline");
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

    private static SourceApplyCoordinator.TurnResult runUntilApplyFailure(final TargetWorkerShardRuntime worker) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        SourceApplyCoordinator.TurnResult result;
        do {
            result = worker.runSourceTurn(
                    new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)), System::currentTimeMillis);
            if (result.status() == SourceApplyCoordinator.TurnStatus.APPLY_FAILURE) {
                return result;
            }
            if (result.status() != SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status() != SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException(
                        "real Kafka Target Worker did not reach the uncertain native-write cut: " + result.status(),
                        result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("real Kafka Target source did not reach APPLY_FAILURE before the deadline");
    }

    private static void writeProcessCrashState(
            final AckInjection injection,
            final String topic,
            final String groupId,
            final String clusterId,
            final UUID topicId,
            final TargetQuotaScope scope,
            final com.nereusstream.delay.ownership.SourceAssignment assignment,
            final PreparedCommand command,
            final long commandOffset,
            final long ownerEpoch,
            final String ownerLeasePrefix)
            throws Exception {
        final Properties state = new Properties();
        state.setProperty("topic", topic);
        state.setProperty("groupId", groupId);
        state.setProperty("clusterId", clusterId);
        state.setProperty("topicId", topicId.toString());
        state.setProperty("route", HexFormat.of().formatHex(scope.shard().routeIncarnation().bytes()));
        state.setProperty("partition", Integer.toString(scope.shard().partition()));
        state.setProperty("tenantScope", HexFormat.of().formatHex(scope.tenantScope()));
        state.setProperty("assignmentId", HexFormat.of().formatHex(assignment.assignmentId()));
        state.setProperty("assignmentEpoch", Long.toUnsignedString(assignment.assignmentEpoch()));
        state.setProperty("barrierOffset", "1");
        state.setProperty("commandOffset", Long.toString(commandOffset));
        state.setProperty("commandFrame", Base64.getEncoder().encodeToString(
                com.nereusstream.delay.protocol.CommandCodec.encodeFrame(command)));
        state.setProperty("ownerEpoch", Long.toUnsignedString(ownerEpoch));
        if (ownerLeasePrefix != null) {
            state.setProperty("ownerLeasePrefix", ownerLeasePrefix);
        }
        final Path stateParent = injection.stateFile().getParent();
        if (stateParent != null) {
            Files.createDirectories(stateParent);
        }
        try (var output = Files.newOutputStream(injection.stateFile())) {
            state.store(output, "test-only Target Worker process-crash recovery state");
        }
        final Path readyParent = injection.readyFile().getParent();
        if (readyParent != null) {
            Files.createDirectories(readyParent);
        }
        Files.writeString(injection.readyFile(), ProcessHandle.current().pid() + "\n");
        System.out.println("Kafka Target source process-crash cut reached: status=ACK_UNKNOWN, brokerOffset=2, "
                + "storeOffset=" + commandOffset + ", ownerEpoch=" + Long.toUnsignedString(ownerEpoch));
        System.out.flush();
        new CountDownLatch(1).await();
        throw new IllegalStateException("Target source process-crash cut resumed without a new JVM");
    }

    private static void replayAfterProcessCrash(final String bootstrap, final AckInjection injection)
            throws Exception {
        if (!Files.isRegularFile(injection.stateFile())
                || !Files.exists(injection.releaseFile())
                || !Files.exists(injection.droppedResponseFile())) {
            throw new IllegalStateException("process-crash resume requires saved state and a released ACK proxy");
        }
        final Properties saved = new Properties();
        try (var input = Files.newInputStream(injection.stateFile())) {
            saved.load(input);
        }
        final String topic = requireProperty(saved, "topic");
        final String groupId = requireProperty(saved, "groupId");
        final String clusterId = requireProperty(saved, "clusterId");
        final UUID topicId = UUID.fromString(requireProperty(saved, "topicId"));
        final ShardId shard = new ShardId(
                new RouteIncarnation(HexFormat.of().parseHex(requireProperty(saved, "route"))),
                Integer.parseInt(requireProperty(saved, "partition")));
        final TargetQuotaScope scope = new TargetQuotaScope(
                shard, HexFormat.of().parseHex(requireProperty(saved, "tenantScope")), null);
        final long assignmentEpoch = Long.parseUnsignedLong(requireProperty(saved, "assignmentEpoch"));
        final long barrierOffset = Long.parseLong(requireProperty(saved, "barrierOffset"));
        final long commandOffset = Long.parseLong(requireProperty(saved, "commandOffset"));
        final long previousOwnerEpoch = Long.parseUnsignedLong(requireProperty(saved, "ownerEpoch"));
        final var assignment = new com.nereusstream.delay.ownership.SourceAssignment(
                shard,
                HexFormat.of().parseHex(requireProperty(saved, "assignmentId")),
                assignmentEpoch,
                new KafkaActivationBarrier(shard, clusterId, topicId, barrierOffset));
        final PreparedCommand command = com.nereusstream.delay.protocol.CommandCodec.decodeFrame(
                Base64.getDecoder().decode(requireProperty(saved, "commandFrame")));
        final Map<String, Object> adminConfig = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        try (Admin admin = Admin.create(adminConfig)) {
            final String actualClusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid actualTopicId = admin.describeTopics(List.of(topic))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS)
                    .get(topic)
                    .topicId();
            if (!clusterId.equals(actualClusterId) || !topicId.equals(toUuid(actualTopicId))) {
                throw new IllegalStateException("process-crash recovery reached a different Kafka cluster/topic");
            }
            final var committedOffsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS);
            final var committed = committedOffsets.get(new TopicPartition(topic, shard.partition()));
            if (committed == null || committed.offset() != commandOffset + 1) {
                throw new IllegalStateException("Broker group offset changed before process-crash recovery");
            }
            final String ownerLeasePrefix = saved.getProperty("ownerLeasePrefix");
            final String oxiaEndpoint = configuredEnvironment("NEREUS_DELAY_OXIA_ENDPOINT");
            if ((ownerLeasePrefix != null) != (oxiaEndpoint != null)) {
                throw new IllegalStateException("process-crash recovery Owner authority mode changed between JVMs");
            }
            final byte[] ownerSessionIdentity;
            final OxiaOwnerLeaseStore leases;
            final OxiaSyncOwnerLeaseBackend.ClientHandle ownerClient;
            if (oxiaEndpoint == null) {
                final var leaseBackend = new InMemoryOwnerLeaseStore();
                leases = new OxiaOwnerLeaseStore(leaseBackend);
                final long leaseNow = System.currentTimeMillis();
                final OwnerLease priorEpoch = leaseBackend
                        .acquire(shard, "target-source-pre-crash-test-epoch", leaseNow, 60_000)
                        .orElseThrow(() -> new IllegalStateException("test prior Owner epoch seed failed"));
                if (Long.compareUnsigned(priorEpoch.ownerEpoch(), previousOwnerEpoch) != 0
                        || !leaseBackend.release(priorEpoch)) {
                    throw new IllegalStateException("test Owner epoch seed disagrees with the persisted Store epoch");
                }
                ownerSessionIdentity = Bytes.sha256(Bytes.utf8("target-source-process-crash-session"));
                ownerClient = null;
            } else {
                ownerClient = connectOwnerLeaseClient(
                        oxiaEndpoint,
                        configuredEnvironment("NEREUS_DELAY_OXIA_NAMESPACE", "default"),
                        "target-source-process-crash-resume-" + UUID.randomUUID(),
                        ownerLeasePrefix);
                leases = new OxiaOwnerLeaseStore(ownerClient.backend());
                waitForProcessCrashOwnerLeaseRelease(leases, shard, previousOwnerEpoch);
                ownerSessionIdentity = ownerClient.sessionIdentity();
            }
            try (ownerClient) {
                final long replacementOwnerEpoch = runTargetSourceReplayAfterUnknown(
                        admin,
                        bootstrap,
                        topic,
                        groupId,
                        clusterId,
                        topicId,
                        scope,
                        assignment,
                        leases,
                        ownerSessionIdentity,
                        command,
                        commandOffset,
                        ShardStoreConfig.defaults(injection.storeRoot()),
                        AckMode.NETWORK_RESPONSE_LOSS);
                System.out.println("Kafka Target source process-crash recovery passed: previousOwnerEpoch="
                        + Long.toUnsignedString(previousOwnerEpoch)
                        + ", replacementOwnerEpoch=" + Long.toUnsignedString(replacementOwnerEpoch)
                        + ", brokerOffset=" + (commandOffset + 1) + ", exact replay ACKed by a fresh JVM.");
            }
        }
    }

    private static String requireProperty(final Properties properties, final String name) {
        final String value = properties.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("process-crash state is missing " + name);
        }
        return value;
    }

    private static OxiaSyncOwnerLeaseBackend.ClientHandle openProcessCrashOwnerClient(
            final AckInjection injection) throws Exception {
        final String endpoint = configuredEnvironment("NEREUS_DELAY_OXIA_ENDPOINT");
        if (injection.crashPhase() == CrashPhase.NONE) {
            if (!injection.networkResponseLoss()
                    || !"1".equals(configuredEnvironment("NEREUS_DELAY_TARGET_SOURCE_OWNER_OXIA"))) {
                return null;
            }
            if (endpoint == null) {
                throw new IllegalStateException("live Target source Owner mode requires an Oxia endpoint");
            }
            return connectOwnerLeaseClient(
                    endpoint,
                    configuredEnvironment("NEREUS_DELAY_OXIA_NAMESPACE", "default"),
                    "target-source-network-owner-" + UUID.randomUUID(),
                    "nereus-delay/kafka-target-source/network/" + UUID.randomUUID());
        }
        return endpoint == null
                ? null
                : connectOwnerLeaseClient(
                        endpoint,
                        configuredEnvironment("NEREUS_DELAY_OXIA_NAMESPACE", "default"),
                        "target-source-process-crash-prepare-" + UUID.randomUUID(),
                        processCrashOwnerLeasePrefix(injection));
    }

    private static OxiaSyncOwnerLeaseBackend.ClientHandle connectOwnerLeaseClient(
            final String endpoint,
            final String namespace,
            final String clientIdentifier,
            final String ownerLeasePrefix) throws Exception {
        return OxiaSyncOwnerLeaseBackend.connect(
                endpoint,
                namespace,
                clientIdentifier,
                Duration.ofSeconds(15),
                ownerLeasePrefix);
    }

    private static String processCrashOwnerLeasePrefix(final AckInjection injection) {
        final String statePath = injection.stateFile().toAbsolutePath().normalize().toString();
        return "nereus-delay/kafka-target-source-process-crash/"
                + Bytes.hex(Bytes.sha256(Bytes.utf8(statePath)));
    }

    private static void waitForProcessCrashOwnerLeaseRelease(
            final OxiaOwnerLeaseStore leases, final ShardId shard, final long previousOwnerEpoch)
            throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        do {
            final Optional<OwnerLease> current = leases.current(shard);
            if (current.isEmpty()) {
                return;
            }
            if (Long.compareUnsigned(current.orElseThrow().ownerEpoch(), previousOwnerEpoch) != 0) {
                throw new IllegalStateException("another Owner acquired the process-crash Shard before recovery");
            }
            Thread.sleep(100);
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Oxia did not remove the killed process Owner lease before recovery");
    }

    private static String configuredEnvironment(final String name) {
        return configuredEnvironment(name, null);
    }

    private static String configuredEnvironment(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    enum AckMode {
        NO_INJECTION,
        CLIENT_DELEGATE_RESPONSE_LOSS,
        EXPIRY_ACK_RESPONSE_LOSS,
        EXPIRY_ACK_RESPONSE_LOSS_REOPEN,
        STORE_WRITE_RESPONSE_UNKNOWN,
        NETWORK_RESPONSE_LOSS,
        EXPIRY_NETWORK_RESPONSE_LOSS
    }

    private enum CrashPhase {
        NONE,
        PREPARE,
        RESUME
    }

    private record AckInjection(
            AckMode mode,
            Path holdFile,
            Path releaseFile,
            Path droppedResponseFile,
            CrashPhase crashPhase,
            Path stateFile,
            Path storeRoot,
            Path readyFile) {
        private static AckInjection acked() {
            return new AckInjection(
                    AckMode.NO_INJECTION, null, null, null, CrashPhase.NONE, null, null, null);
        }

        private static AckInjection withExpiryAckResponseLoss() {
            return new AckInjection(
                    AckMode.EXPIRY_ACK_RESPONSE_LOSS, null, null, null, CrashPhase.NONE, null, null, null);
        }

        private static AckInjection withExpiryAckResponseLossReopen() {
            return new AckInjection(
                    AckMode.EXPIRY_ACK_RESPONSE_LOSS_REOPEN, null, null, null, CrashPhase.NONE, null, null, null);
        }

        private static AckInjection from(final String[] arguments) {
            if (arguments.length == 2) {
                return new AckInjection(
                        AckMode.CLIENT_DELEGATE_RESPONSE_LOSS, null, null, null, CrashPhase.NONE, null, null, null);
            }
            if (arguments.length == 3 && "no-injection".equals(arguments[2])) {
                return acked();
            }
            if (arguments.length == 3 && "store-write-response-unknown".equals(arguments[2])) {
                return new AckInjection(
                        AckMode.STORE_WRITE_RESPONSE_UNKNOWN, null, null, null, CrashPhase.NONE, null, null, null);
            }
            if (arguments.length == 6 && "network-response-loss".equals(arguments[2])) {
                return new AckInjection(
                        AckMode.NETWORK_RESPONSE_LOSS,
                        Path.of(arguments[3]),
                        Path.of(arguments[4]),
                        Path.of(arguments[5]),
                        CrashPhase.NONE,
                        null,
                        null,
                        null);
            }
            if (arguments.length == 7
                    && "network-expiry-response-loss".equals(arguments[2])
                    && ("target-expire-not-found-network-ack-loss".equals(arguments[6])
                            || "target-expire-scheduled-message-network-ack-loss".equals(arguments[6]))) {
                return new AckInjection(
                        AckMode.EXPIRY_NETWORK_RESPONSE_LOSS,
                        Path.of(arguments[3]),
                        Path.of(arguments[4]),
                        Path.of(arguments[5]),
                        CrashPhase.NONE,
                        null,
                        null,
                        null);
            }
            if (arguments.length != 10 || !"network-response-loss-process-crash".equals(arguments[2])) {
                throw new IllegalArgumentException("unsupported Target source ACK injection mode");
            }
            final CrashPhase phase = switch (arguments[3]) {
                case "prepare" -> CrashPhase.PREPARE;
                case "resume" -> CrashPhase.RESUME;
                default -> throw new IllegalArgumentException("unsupported Target source process-crash phase");
            };
            return new AckInjection(
                    AckMode.NETWORK_RESPONSE_LOSS,
                    Path.of(arguments[4]),
                    Path.of(arguments[5]),
                    Path.of(arguments[6]),
                    phase,
                    Path.of(arguments[7]),
                    Path.of(arguments[8]),
                    Path.of(arguments[9]));
        }

        private boolean networkResponseLoss() {
            return mode == AckMode.NETWORK_RESPONSE_LOSS || expiryNetworkResponseLoss();
        }

        private boolean expiryNetworkResponseLoss() {
            return mode == AckMode.EXPIRY_NETWORK_RESPONSE_LOSS;
        }

        private boolean storeWriteResponseUnknown() {
            return mode == AckMode.STORE_WRITE_RESPONSE_UNKNOWN;
        }

        private boolean expiryAckResponseLoss() {
            return mode == AckMode.EXPIRY_ACK_RESPONSE_LOSS
                    || mode == AckMode.EXPIRY_ACK_RESPONSE_LOSS_REOPEN
                    || expiryNetworkResponseLoss();
        }

        private boolean expiryAckResponseLossReopen() {
            return mode == AckMode.EXPIRY_ACK_RESPONSE_LOSS_REOPEN;
        }

        private boolean acknowledgementResponseUnknown() {
            return mode == AckMode.CLIENT_DELEGATE_RESPONSE_LOSS || mode == AckMode.NETWORK_RESPONSE_LOSS;
        }
    }

    @FunctionalInterface
    private interface CheckedAction {
        void run() throws Exception;
    }

    record RootControl(PreparedControlOperation prepared, SystemMutation mutation) {}

    private record TargetExpiryFixture(
            com.nereusstream.delay.protocol.SystemMutation mutation,
            AuthorIdentity author,
            TrustedUtcIntervalEvidence evidence,
            long timestamp) {}

    private static TargetExpiryFixture targetExpiryFixture(
            final ShardId shard,
            final DelayMessageId messageId,
            final OwnerLease active,
            final KeyPair signingKeys) {
        final long timestamp = Math.addExact(System.currentTimeMillis(), 5);
        final long retryUntil = Math.addExact(timestamp, 600_000);
        final var evidence = new TrustedUtcIntervalEvidence(
                timestamp - 1,
                timestamp,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("kafka-target-expiry-source-smoke"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("kafka-target-expiry-source-smoke-evidence")),
                0,
                null);
        final var author = AuthorIdentity.owner(
                Bytes.utf8("kafka-target-worker-deployment"),
                Bytes.utf8("kafka-target-worker-host-run"),
                active.ownerEpoch(),
                Bytes.sha256(active.leaseToken()));
        final var body = new com.nereusstream.delay.protocol.TargetExpireGenerationBody(
                shard, retryUntil, messageId, 0, timestamp - 1, evidence);
        final var mutation = com.nereusstream.delay.protocol.SystemMutation.signed(
                shard,
                com.nereusstream.delay.protocol.SystemMutationType.EXPIRE_GENERATION,
                retryUntil,
                body.logicalOperationIdentity(),
                body.canonicalBytes(),
                author.canonicalBytes(),
                1,
                signingKeys.getPrivate());
        return new TargetExpiryFixture(mutation, author, evidence, timestamp);
    }

    static org.apache.kafka.clients.producer.RecordMetadata produce(
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

    @SuppressWarnings("unchecked")
    private static KafkaSourcePosition appendExpiryMutation(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard,
            final com.nereusstream.delay.protocol.SystemMutation mutation) {
        final Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName(),
                ProducerConfig.ACKS_CONFIG,
                "all");
        try (var producer = new KafkaProducer<byte[], byte[]>(config);
                KafkaClientArtifactShardLogMutationAppender appender =
                        new KafkaClientArtifactShardLogMutationAppender(
                                (GuardedProducer<byte[], byte[]>) producer,
                                shard,
                                clusterId,
                                topic,
                                topicId,
                                Duration.ofSeconds(15))) {
            final var outcome = appender.append(mutation);
            if (outcome.disposition()
                            != com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendDisposition.PERSISTED
                    || !(outcome.sourcePosition() instanceof KafkaSourcePosition position)
                    || !clusterId.equals(position.authenticatedClusterId())
                    || !topicId.equals(position.nativeTopicUuid())
                    || !shard.equals(position.shardId())) {
                throw new IllegalStateException(
                        "guarded Kafka Target expiry append did not persist at the exact Shard source: disposition="
                                + outcome.disposition() + ", sourcePosition=" + outcome.sourcePosition());
            }
            return position;
        }
    }

    static WorkClassExecutionRegistry workClasses() {
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

    static BoundedReadBudget budget() {
        return new BoundedReadBudget(2048, 32L << 20, 60_000_000_000L, System::nanoTime);
    }

    private static UUID toUuid(final Uuid uuid) {
        return new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    static byte[] bytes(final int size, final int value) {
        final byte[] result = new byte[size];
        Arrays.fill(result, (byte) value);
        return result;
    }

    static void deleteTree(final Path root) throws Exception {
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
