package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.PulsarSendRequest;
import io.nereusstream.delay.adapter.PulsarSendResult;
import io.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import io.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.nereusstream.delay.ownership.OwnerLease;
import io.nereusstream.delay.ownership.OwnerRecoveryCoordinator;
import io.nereusstream.delay.ownership.OwnerRecoveryTurn;
import io.nereusstream.delay.ownership.OwnedDelayShard;
import io.nereusstream.delay.ownership.ReplayTurnBudget;
import io.nereusstream.delay.ownership.ShardLifecycleState;
import io.nereusstream.delay.ownership.SourceAssignment;
import io.nereusstream.delay.ownership.SourceReplayCursor;
import io.nereusstream.delay.ownership.SourceReplayEntry;
import io.nereusstream.delay.ownership.SourceReplayRecord;
import io.nereusstream.delay.ownership.SourceReplaySuccessor;
import io.nereusstream.delay.ownership.WorkerShardRuntime;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.PulsarMetadataV1;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.V1ScheduleResolver;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
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
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Real Pulsar recovery, active Worker apply and synchronous ACK smoke. */
public final class PulsarClientArtifactWorkerSmoke {
    private static final String CLUSTER = "standalone";
    private static final byte[] INCARNATION = digest(43);
    private static final long CREATION_TIMESTAMP = 2001L;
    private static final long LEASE_DURATION_MS = 60_000;

    private PulsarClientArtifactWorkerSmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <topic>");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String topic = arguments[2] + "-worker-" + UUID.randomUUID();
        final String physicalTopic = "persistent://public/default/" + topic;
        final HttpClient admin = HttpClient.newHttpClient();
        createTopic(admin, adminUrl, topic);
        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
            runWorker(client, physicalTopic);
        } finally {
            deleteTopicIfPresent(admin, adminUrl, topic);
        }
    }

    private static void runWorker(final PulsarClient client, final String physicalTopic) throws Exception {
        final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand recoveryCommand = command(shard, "worker-recovery");
        send(client, guard, physicalTopic, recoveryCommand, "worker-recovery-producer");

        final OxiaSyncOwnerLeaseBackend.ClientHandle oxia = connectOxiaIfConfigured();
        try {
            final String subscription = "nereus-delay-worker-" + UUID.randomUUID();
            final GuardedConsumer<byte[]> nativeConsumer = PulsarClientArtifactSourceConsumerFactory.create(
                    client, guard, physicalTopic, subscription);
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof =
                    PulsarClientArtifactRecoverySourcePositioner.seekAfter(nativeConsumer, guard, physicalTopic, shard,
                            Optional.empty(), Duration.ofSeconds(5));
            final SourceAssignment assignment = new SourceAssignment(shard,
                    Bytes.sha256(Bytes.utf8("pulsar-worker-assignment")), 1,
                    PulsarActivationBarrier.empty(shard, INCARNATION, physicalTopic,
                            proof.connectionGeneration(), proof.attestationDigest()));
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
                            scheduleResolver());
                    final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease,
                            new io.nereusstream.delay.protocol.OwnerIdentityV1(bytes(16, 70), bytes(16, 71),
                                    lease.ownerEpoch(), Bytes.sha256(Bytes.utf8("pulsar-worker-fencing"))));

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
                        WorkerShardRuntime runtime = PulsarClientArtifactWorkerSourceFactory.create(nativeConsumer,
                                guard, physicalTopic, Duration.ofMillis(250), assignment, workClasses, ownedShard,
                                store, resources, authority, verificationKey.getPublic());
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
                            final var drain = runtime.drain(
                                    new io.nereusstream.delay.ownership.OwnerDrainCoordinator.DrainRequest(
                                            System.currentTimeMillis() + 5_000, 0, null),
                                    System::currentTimeMillis, () -> { });
                            if (drain.pendingCheckpointTask() != null || !authority.current(shard).isEmpty()) {
                                throw new IllegalStateException("Pulsar Worker drain did not release the owner lease");
                            }
                            runtimeDrained = true;
                            System.out.println("Pulsar Worker vertical smoke passed: assignment recovery ledger/entry="
                                    + recovered.ledgerId() + "/" + recovered.entryId()
                                    + ", active apply ledger/entry=" + activePosition.ledgerId() + "/"
                                    + activePosition.entryId() + ", guarded SUBSCRIBE, RocksDB WriteBatch and ACK");
                            if (oxia != null) {
                                System.out.println("Pulsar Worker authority smoke passed: real Oxia session-bound lease");
                            }
                        } finally {
                            if (!runtimeDrained) {
                                closeNative(nativeConsumer);
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
                    != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException("Pulsar Worker source turn failed: " + result.status(),
                        result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Pulsar Worker source record did not become visible before deadline");
    }

    private static void send(final PulsarClient client, final TopicResourceGuard guard, final String physicalTopic,
                             final PreparedCommand command, final String producerName) throws Exception {
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
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination, retryPolicy, deliverAt,
                deliverAt + 10_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0],
                Bytes.utf8("source-" + identity), null,
                AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of())), null, null);
        return PreparedCommand.scheduleV1(shard, intent, deliverAt + 20_000);
    }

    private static V1ScheduleResolver scheduleResolver() {
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

    private static void createTopic(final HttpClient client, final String adminUrl, final String topic)
            throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic;
        final String body = "{\"nereus.resource.guard.version\":\"1\","
                + "\"nereus.resource.incarnation\":\""
                + Base64.getUrlEncoder().withoutPadding().encodeToString(INCARNATION) + "\","
                + "\"nereus.resource.created-at\":\""
                + Long.toUnsignedString(CREATION_TIMESTAMP) + "\"}";
        for (int attempt = 0; attempt < 40; attempt++) {
            final HttpResponse<String> response = request(client, path, "PUT", body);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
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
