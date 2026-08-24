package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import com.nereusstream.delay.ownership.InMemoryWorkerAssignmentAuthority;
import com.nereusstream.delay.ownership.OwnedDelayShard;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OwnerRecoveryCoordinator;
import com.nereusstream.delay.ownership.OwnerRecoveryTurn;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.ownership.OxiaSyncWorkerAssignmentBackend;
import com.nereusstream.delay.ownership.ReplayTurnBudget;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.ownership.ShardLogMutationAppender;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceReplayCursor;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.WorkerAssignment;
import com.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import com.nereusstream.delay.ownership.WorkerAssignmentCoordinator;
import com.nereusstream.delay.ownership.WorkerShardRuntime;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimensionV1;
import com.nereusstream.delay.protocol.CapacityVectorV1;
import com.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import com.nereusstream.delay.protocol.OwnerIdentityV1;
import com.nereusstream.delay.protocol.ProfileKindV1;
import com.nereusstream.delay.protocol.ProfileRefV1;
import com.nereusstream.delay.protocol.ProtocolTupleV1;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.QuotaGrantRefV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubjectV1;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.V1ScheduleResolver;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
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
import java.util.Arrays;
import java.util.Base64;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;

/**
 * Real Pulsar vertical smoke for mixed System Mutation recovery and active
 * Worker apply.
 *
 * <p>The first signed mutation is consumed only by strict recovery. The
 * second mutation remains after the empty activation barrier and is applied
 * by the active Worker source loop before the guarded ACK. Both records stay
 * on one P1 topic and use the same guarded SUBSCRIBE connection proof.</p>
 */
public final class PulsarClientArtifactMutationWorkerSmoke {
    private static final String CLUSTER = "standalone";
    private static final byte[] INCARNATION = digest(53);
    private static final long CREATION_TIMESTAMP = 3001L;
    private static final long LEASE_DURATION_MS = 60_000;

    private PulsarClientArtifactMutationWorkerSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <mutation-worker-topic>");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String topic = arguments[2] + "-" + UUID.randomUUID();
        final String physicalTopic = "persistent://public/default/" + topic;
        final HttpClient admin = HttpClient.newHttpClient();
        createTopic(admin, adminUrl, topic);
        try {
            final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
            final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
            final KeyPair verificationKey =
                    KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final MutationFixture recoveryMutation = timeFence(shard, "recovery", verificationKey);
            final MutationFixture activeMutation = timeFence(shard, "active", verificationKey);
            try (PulsarClient client =
                    PulsarClient.builder().serviceUrl(serviceUrl).build()) {
                final PulsarSourcePositionPair positions = appendMutations(
                        client, guard, physicalTopic, shard, recoveryMutation.mutation(), activeMutation.mutation());
                if (positions.active().compareTo(positions.recovery()) <= 0) {
                    throw new IllegalStateException("Pulsar mutation Worker fixture is not physically ordered");
                }

                final GuardedConsumer<byte[]> recoveryNative = PulsarClientArtifactSourceConsumerFactory.create(
                        client, guard, physicalTopic, "nereus-delay-mutation-worker-recovery-" + UUID.randomUUID());
                final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof =
                        PulsarClientArtifactRecoverySourcePositioner.seekAfter(
                                recoveryNative, guard, physicalTopic, shard, Optional.empty(), Duration.ofSeconds(5));
                final SourceAssignment sourceAssignment = new SourceAssignment(
                        shard,
                        Bytes.sha256(Bytes.utf8("pulsar-mutation-worker-assignment")),
                        1,
                        PulsarActivationBarrier.empty(
                                shard,
                                INCARNATION,
                                physicalTopic,
                                proof.connectionGeneration(),
                                proof.attestationDigest()));
                final OxiaSyncOwnerLeaseBackend.ClientHandle oxia = connectOxiaIfConfigured();
                try {
                    final WorkerAssignmentAuthority assignmentAuthority = oxia == null
                            ? new InMemoryWorkerAssignmentAuthority()
                            : new OxiaSyncWorkerAssignmentBackend(
                                    oxia, "nereus-delay/pulsar-mutation-worker-placement/" + UUID.randomUUID());
                    final WorkerAssignment accepted =
                            publishAssignment(assignmentAuthority, sourceAssignment, oxia != null);
                    final SourceAssignment assignment = accepted.sourceAssignment();
                    final OxiaOwnerLeaseStore authority = oxia == null
                            ? new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore())
                            : new OxiaOwnerLeaseStore(oxia.backend());
                    final OwnerLease lease = authority
                            .acquire(
                                    assignment,
                                    "pulsar-mutation-worker",
                                    oxia == null
                                            ? Bytes.sha256(Bytes.utf8("pulsar-mutation-worker-session"))
                                            : oxia.sessionIdentity(),
                                    System.currentTimeMillis(),
                                    LEASE_DURATION_MS)
                            .orElseThrow();
                    final WorkClassExecutionRegistry workClasses = workClasses();
                    final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshot(shard);
                    final Path root = Files.createTempDirectory("nereus-delay-pulsar-mutation-worker-");
                    boolean runtimeDrained = false;
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
                                    new OwnerIdentityV1(
                                            bytes(16, 70),
                                            bytes(16, 71),
                                            lease.ownerEpoch(),
                                            Bytes.sha256(Bytes.utf8("pulsar-mutation-worker-fencing"))));

                            try (PulsarClientArtifactRecoverySourceCursor recovery =
                                    new PulsarClientArtifactRecoverySourceCursor(
                                            recoveryNative, guard, assignment, physicalTopic, Duration.ofMillis(250))) {
                                final SourceReplayCursor<SourceReplayEntry> cursor =
                                        SourceReplayCursor.of(firstOnly(recovery));
                                final OwnerRecoveryCoordinator coordinator = new OwnerRecoveryCoordinator(
                                        ownedShard,
                                        authority,
                                        assignment,
                                        SourceReplaySuccessor.strictPulsarBatchMember(),
                                        cursor,
                                        verificationKey.getPublic(),
                                        controlSnapshot,
                                        System::currentTimeMillis,
                                        new ReplayTurnBudget(1, 1_000_000, TimeUnit.SECONDS.toNanos(10)),
                                        workClasses);
                                OwnerRecoveryTurn turn;
                                do {
                                    turn = coordinator.runTurn();
                                } while (!turn.complete());
                                requireRecoveryOutcome(turn, recoveryMutation.mutation(), positions.recovery());
                                requireSystemMutation(
                                        delayShard, recoveryMutation.mutation(), positions.recovery(), "recovery");
                                if (!coordinator.complete()
                                        || ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                        || !samePosition(ownedShard.lastCatchupPosition(), positions.recovery())) {
                                    throw new IllegalStateException(
                                            "Pulsar mutation recovery did not activate at the first position");
                                }

                                final WorkerShardRuntime runtime = PulsarClientArtifactWorkerSourceFactory.create(
                                        recoveryNative,
                                        guard,
                                        physicalTopic,
                                        Duration.ofMillis(250),
                                        assignment,
                                        workClasses,
                                        ownedShard,
                                        store,
                                        resources,
                                        authority,
                                        verificationKey.getPublic());
                                try {
                                    final var result = runUntilApplied(runtime);
                                    if (!(result.entry() instanceof SourceReplayMutation replayed)
                                            || !activeMutation.mutation().equals(replayed.mutation())
                                            || !samePosition(replayed.position(), positions.active())) {
                                        throw new IllegalStateException(
                                                "Pulsar active Worker exposed a different mutation");
                                    }
                                    requireSystemMutation(
                                            delayShard, activeMutation.mutation(), positions.active(), "active");
                                    if (!samePosition(store.appliedShardLogPosition(), positions.active())) {
                                        throw new IllegalStateException(
                                                "Pulsar mutation Worker Store position did not reach the active entry");
                                    }

                                    final Path checkpointPath = root.resolve("mutation-worker-final-checkpoint");
                                    final byte[] checkpointId = Arrays.copyOf(
                                            Bytes.sha256(Bytes.utf8("pulsar-mutation-worker-final-checkpoint")), 16);
                                    final var drain = runtime.drain(
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
                                            || !authority.current(shard).isEmpty()) {
                                        throw new IllegalStateException(
                                                "Pulsar mutation Worker drain did not publish the final checkpoint "
                                                        + "and release the lease");
                                    }
                                    runtimeDrained = true;
                                    System.out.println(
                                            "Pulsar mutation Worker vertical smoke passed: recovery TIME_FENCE "
                                                    + "ledger/entry="
                                                    + positions.recovery().ledgerId() + "/"
                                                    + positions.recovery().entryId()
                                                    + ", active Store apply TIME_FENCE "
                                                    + "ledger/entry="
                                                    + positions.active().ledgerId() + "/"
                                                    + positions.active().entryId() + ", guarded SUBSCRIBE, RocksDB "
                                                    + "WriteBatch, ACK, and final checkpoint");
                                    if (oxia != null) {
                                        System.out.println("Pulsar mutation Worker authority smoke passed: real Oxia "
                                                + "session-bound lease");
                                    }
                                } finally {
                                    if (!runtimeDrained) {
                                        closeNative(recoveryNative);
                                    }
                                }
                            }
                        }
                    } finally {
                        deleteTree(root);
                        if (!runtimeDrained) {
                            closeNative(recoveryNative);
                        }
                    }
                } finally {
                    if (oxia != null) {
                        oxia.close();
                    }
                }
            }
        } finally {
            deleteTopicIfPresent(admin, adminUrl, topic);
        }
    }

    private static PulsarSourcePositionPair appendMutations(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final ShardId shard,
            final SystemMutation recovery,
            final SystemMutation active)
            throws Exception {
        final GuardedConsumer<byte[]> proofConsumer = PulsarClientArtifactSourceConsumerFactory.create(
                client, guard, physicalTopic, "nereus-delay-mutation-worker-append-" + UUID.randomUUID());
        try (PulsarClientArtifactShardLogMutationAppender appender = new PulsarClientArtifactShardLogMutationAppender(
                PulsarClientArtifactProducerFactory.create(
                        client,
                        CLUSTER,
                        INCARNATION,
                        physicalTopic,
                        CREATION_TIMESTAMP,
                        "nereus-delay-mutation-worker-producer"),
                proofConsumer,
                shard,
                CLUSTER,
                INCARNATION,
                physicalTopic,
                CREATION_TIMESTAMP,
                Duration.ofSeconds(15))) {
            final ShardLogMutationAppender.AppendOutcome first = appender.append(recovery);
            final ShardLogMutationAppender.AppendOutcome second = appender.append(active);
            return new PulsarSourcePositionPair(
                    requirePersisted(first, "recovery"), requirePersisted(second, "active"));
        } finally {
            closeNative(proofConsumer);
        }
    }

    private static PulsarSourcePosition requirePersisted(
            final ShardLogMutationAppender.AppendOutcome outcome, final String phase) {
        if (outcome.disposition() != ShardLogMutationAppender.AppendDisposition.PERSISTED
                || !(outcome.sourcePosition() instanceof PulsarSourcePosition position)) {
            throw new IllegalStateException(
                    "Pulsar " + phase + " mutation append was not persisted: " + outcome.disposition());
        }
        return position;
    }

    private static void requireRecoveryOutcome(
            final OwnerRecoveryTurn turn, final SystemMutation mutation, final PulsarSourcePosition expectedPosition) {
        if (turn.outcomes().size() != 1 || turn.outcomes().get(0).systemMutationResult() == null) {
            throw new IllegalStateException("Pulsar mutation recovery did not apply exactly one mutation");
        }
        final var outcome = turn.outcomes().get(0);
        final SystemMutationResult result = outcome.systemMutationResult();
        if (!Arrays.equals(result.mutationId(), mutation.systemMutationId())
                || !Arrays.equals(result.mutationHash(), mutation.mutationHash())
                || result.mutationType() != mutation.type()
                || !samePosition(outcome.position(), expectedPosition)) {
            throw new IllegalStateException("Pulsar recovery returned a different System Mutation");
        }
    }

    private static Iterator<SourceReplayEntry> firstOnly(final PulsarClientArtifactRecoverySourceCursor cursor) {
        return new Iterator<>() {
            private boolean yielded;

            @Override
            public boolean hasNext() {
                return !yielded && cursor.hasNext();
            }

            @Override
            public SourceReplayEntry next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException("Pulsar recovery first-entry view is exhausted");
                }
                yielded = true;
                return cursor.next();
            }
        };
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
                        "Pulsar mutation Worker source turn failed: " + result.status(), result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Pulsar active mutation did not become visible before deadline");
    }

    private static void requireSystemMutation(
            final DelayShard shard,
            final SystemMutation mutation,
            final PulsarSourcePosition expectedPosition,
            final String phase) {
        final SystemMutationResult result = shard.getSystemMutationResult(mutation.systemMutationId());
        if (result == null
                || !Arrays.equals(result.mutationHash(), mutation.mutationHash())
                || result.mutationType() != mutation.type()
                || result.applyStatus() != ApplyStatus.APPLIED
                || result.stableCode() != StableCode.OK
                || !samePosition(result.appliedSourcePosition(), expectedPosition)) {
            throw new IllegalStateException("Pulsar " + phase + " mutation Store result was not APPLIED/OK: " + result);
        }
    }

    private static boolean samePosition(final byte[] encoded, final PulsarSourcePosition expected) {
        return samePosition(SourcePositionCodec.decode(encoded), expected);
    }

    private static boolean samePosition(final SourcePosition actual, final PulsarSourcePosition expected) {
        return actual instanceof PulsarSourcePosition observed && observed.equals(expected);
    }

    private static boolean samePosition(
            final SourcePosition actual, final PulsarSourcePosition expected, final boolean allowLater) {
        return samePosition(actual, expected)
                || allowLater && actual instanceof PulsarSourcePosition observed && observed.compareTo(expected) > 0;
    }

    private static MutationFixture timeFence(final ShardId shard, final String identity, final KeyPair keyPair) {
        final long now = System.currentTimeMillis();
        final long closeThrough = Math.max(0, now - 1_000);
        final long retryUntil = Math.addExact(now, 60_000);
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(
                now,
                now + 1_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("pulsar-mutation-worker-clock-" + identity),
                1,
                2,
                3,
                Bytes.sha256(Bytes.utf8("pulsar-mutation-worker-evidence-" + identity)),
                0,
                null);
        final int keyVersion = 1;
        final byte[] proofId = Bytes.sha256(
                Bytes.utf8("nereus-delay-time-fence-proof-v1\0"),
                shard.routeIncarnation().bytes(),
                Bytes.u32beBits(shard.partition()),
                Bytes.i64be(closeThrough),
                Bytes.u32beBits(keyVersion),
                Bytes.lp32(evidence.canonicalBytes()));
        final byte[] body = com.nereusstream.delay.protocol.CanonicalProtobuf.message(output -> {
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(
                    output, 1, new ShardSubjectV1(shard).canonicalBytes());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(
                    output, 2, SystemMutationType.TIME_FENCE.wireValue());
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(output, 3, retryUntil);
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(output, 10, closeThrough);
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32Bits(output, 11, keyVersion);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 12, proofId);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 13, evidence.canonicalBytes());
        });
        return new MutationFixture(SystemMutation.signed(
                shard,
                SystemMutationType.TIME_FENCE,
                retryUntil,
                proofId,
                body,
                com.nereusstream.delay.protocol.AuthorIdentity.fence(
                                Bytes.utf8("pulsar-mutation-worker-fence"), keyVersion)
                        .canonicalBytes(),
                keyVersion,
                keyPair.getPrivate()));
    }

    private static OxiaSyncOwnerLeaseBackend.ClientHandle connectOxiaIfConfigured() {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        if (endpoint == null || endpoint.isBlank()) {
            return null;
        }
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        return OxiaSyncOwnerLeaseBackend.connectUnchecked(
                endpoint,
                namespace,
                "nereus-delay-pulsar-mutation-worker-" + UUID.randomUUID(),
                Duration.ofSeconds(15),
                "nereus-delay-pulsar-mutation-worker/" + UUID.randomUUID());
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static WorkerAssignment publishAssignment(
            final WorkerAssignmentAuthority authority,
            final SourceAssignment sourceAssignment,
            final boolean realOxia) {
        final WorkerAssignmentCoordinator coordinator = new WorkerAssignmentCoordinator(
                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
        final long now = System.currentTimeMillis();
        final WorkerPlacementPolicy.WorkerCandidate candidate = new WorkerPlacementPolicy.WorkerCandidate(
                "pulsar-mutation-worker",
                capacity(1),
                CapacityVectorV1.empty(),
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
                Bytes.sha256(Bytes.utf8("pulsar-mutation-worker-capacity-envelope")),
                1,
                List.of(candidate),
                capacity(1),
                CapacityVectorV1.empty(),
                CapacityVectorV1.empty(),
                null,
                now,
                0,
                0);
        final WorkerAssignmentAuthority.Publication publication =
                result.publication().orElseThrow();
        final WorkerAssignment accepted = coordinator.requireAccepted(
                sourceAssignment.shardId(), publication.revision(), publication.assignment());
        System.out.println("Pulsar mutation Worker assignment publication/acceptance passed: revision="
                + publication.revision() + ", worker=" + accepted.workerId() + ", authority="
                + (realOxia ? "real Oxia session-bound" : "in-memory"));
        return accepted;
    }

    private static CapacityVectorV1 capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimensionV1.COUNT];
        values[CapacityDimensionV1.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVectorV1(values);
    }

    private static V1ScheduleResolver scheduleResolver() {
        final byte[] tuple = Bytes.utf8("pulsar-mutation-worker-canonical-lane-tuple-v1");
        final com.nereusstream.delay.protocol.DestinationLaneId lane =
                com.nereusstream.delay.protocol.DestinationLaneId.derive(tuple);
        return new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(
                    final ShardId shard,
                    final com.nereusstream.delay.protocol.DelayMessageId message,
                    final com.nereusstream.delay.protocol.ScheduleIntentV1 intent,
                    final SourcePosition source) {
                return new ResolvedSchedule(lane, tuple, intent.inlinePayload(), null);
            }

            @Override
            public ResolvedPrepare resolvePrepare(
                    final ShardId shard,
                    final com.nereusstream.delay.protocol.DelayMessageId message,
                    final com.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1 body,
                    final SourcePosition source) {
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
            if (response.statusCode() != 409 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw new IllegalStateException(
                        "create topic failed with HTTP " + response.statusCode() + ": " + response.body());
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("topic create did not converge: " + topic);
    }

    private static void deleteTopicIfPresent(final HttpClient client, final String adminUrl, final String topic) {
        try {
            final HttpResponse<String> response = request(
                    client, adminUrl + "/admin/v2/persistent/public/default/" + topic + "?force=true", "DELETE", "");
            if (response.statusCode() >= 300 && response.statusCode() != 404) {
                System.err.println("Pulsar mutation Worker cleanup could not delete topic: " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("Pulsar mutation Worker cleanup failed: " + failure.getMessage());
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

    private static void closeNative(final GuardedConsumer<byte[]> consumer) {
        try {
            consumer.close();
        } catch (PulsarClientException failure) {
            throw new IllegalStateException("Pulsar mutation Worker native consumer close failed", failure);
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
        final byte[] value = new byte[32];
        Arrays.fill(value, (byte) seed);
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

    private record MutationFixture(SystemMutation mutation) {
        private MutationFixture {
            Objects.requireNonNull(mutation, "mutation");
        }
    }

    private record PulsarSourcePositionPair(PulsarSourcePosition recovery, PulsarSourcePosition active) {
        private PulsarSourcePositionPair {
            Objects.requireNonNull(recovery, "recovery");
            Objects.requireNonNull(active, "active");
        }
    }
}
