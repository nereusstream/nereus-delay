package com.nereusstream.delay.transport;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.PinnedPulsarDestinationAdapter;
import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.adapter.PulsarDestinationRequest;
import com.nereusstream.delay.adapter.PulsarJournalResource;
import com.nereusstream.delay.adapter.PulsarSendAckEvidence;
import com.nereusstream.delay.adapter.PulsarSendRequest;
import com.nereusstream.delay.adapter.PulsarSendResult;
import com.nereusstream.delay.adapter.PulsarTargetResource;
import com.nereusstream.delay.assessment.PersistentStagingActivation;
import com.nereusstream.delay.assessment.PersistentStagingEvidence;
import com.nereusstream.delay.assessment.PersistentStagingNativeCanaryIdentity;
import com.nereusstream.delay.assessment.PhysicalSendActivationGate;
import com.nereusstream.delay.ownership.ClaimHandoffWorkClassExecutor;
import com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import com.nereusstream.delay.ownership.InMemoryWorkerAssignmentAuthority;
import com.nereusstream.delay.ownership.OutcomeWorkClassExecutor;
import com.nereusstream.delay.ownership.OwnedDelayShard;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OwnerRecoveryCoordinator;
import com.nereusstream.delay.ownership.OwnerRecoveryTurn;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.ownership.OxiaSyncWorkerAssignmentBackend;
import com.nereusstream.delay.ownership.PublishAdmissionWorkClassExecutor;
import com.nereusstream.delay.ownership.ReplayTurnBudget;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.ownership.ShardLogMutationAppender;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceReplayCursor;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.WorkerAssignment;
import com.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import com.nereusstream.delay.ownership.WorkerAssignmentCoordinator;
import com.nereusstream.delay.ownership.WorkerCommandRuntime;
import com.nereusstream.delay.ownership.WorkerPhysicalPublishExecutor;
import com.nereusstream.delay.ownership.WorkerPublishOutcomeMutationFactory;
import com.nereusstream.delay.ownership.WorkerPublishPreparationCoordinator;
import com.nereusstream.delay.ownership.WorkerSchedulingRuntime;
import com.nereusstream.delay.ownership.WorkerShardRuntime;
import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.ActiveLaneState;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.ChannelResourceIdentity;
import com.nereusstream.delay.protocol.CompatibleControlSnapshot;
import com.nereusstream.delay.protocol.CredentialUseKind;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.EvidenceVerificationStatus;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedPublishDescriptor;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishEvidenceKind;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.ReadyCertificate;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleBinding;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.LaneRecord;
import com.nereusstream.delay.runtime.MessageRecord;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.runtime.ProfileCatalog;
import com.nereusstream.delay.runtime.PublishAttemptLedger;
import com.nereusstream.delay.runtime.ScheduleResolver;
import com.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import com.nereusstream.delay.scheduler.ProfileCatalogManagedNativeEligibilityAuthority;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.semantic.OxiaSyncHandoffPolicyTrustStore;
import com.nereusstream.delay.store.CheckpointFileInventory;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.ValueEnvelope;
import com.nereusstream.delay.store.WorkerLoadVector;
import com.nereusstream.delay.store.WorkerPlacementPolicy;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

/** Real Pulsar recovery, active Worker apply and synchronous ACK smoke. */
public final class PulsarClientArtifactWorkerSmoke {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final String CLUSTER = PulsarClientArtifactClientBuilder.clusterId();
    private static final byte[] INCARNATION = digest(43);
    private static final long CREATION_TIMESTAMP = 2001L;
    private static final byte[] DESTINATION_INCARNATION = digest(17);
    private static final long DESTINATION_CREATION_TIMESTAMP = 1001L;
    private static final byte[] JOURNAL_INCARNATION = digest(19);
    private static final long JOURNAL_CREATION_TIMESTAMP = 3001L;
    private static final ArtifactGenerationSet ARTIFACTS = ArtifactGenerationSet.current(
            1, PulsarSourceLock.digest(), Bytes.sha256(Bytes.utf8("pulsar-worker-current-schema-bundle")));
    private static final long LEASE_DURATION_MS = 60_000;
    private static final long SYSTEM_MUTATION_RETRY_WINDOW_MS = 60_000;
    private static final long DUE_DISCOVERY_MAX_BYTES = 900_000;
    private static final long MANAGED_HANDOFF_CANARY_DELAY_MS = 30_000;
    private static final String PREPARED_RECORD_RETENTION_SUBSCRIPTION = "nereus-delay-prepared-record-retention";
    // Test-only source authority fixture. A real Worker receives this pinned
    // verification/signing authority from the source-control plane; it must
    // not generate a new verification key during process recovery.
    private static final String SOURCE_AUTHORITY_PRIVATE_KEY_BASE64 =
            "MC4CAQAwBQYDK2VwBCIEINegcXn3Ts1nGJ/JeACDj7NYvL67V6wsJd7YSQxutEmH";
    private static final String SOURCE_AUTHORITY_PUBLIC_KEY_BASE64 =
            "MCowBQYDK2VwAyEAhGIVt9xTnQodXvVrnojU1erbwRf/f5XoBxqOS0MmAAk=";

    private PulsarClientArtifactWorkerSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3 && arguments.length != 4 && arguments.length != 5) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <topic> "
                    + "[run|prepare|resume|crash-wait|source-ack-crash-wait|source-ack-resume] [destination-topic]");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String mode = arguments.length >= 4 ? arguments[3] : "run";
        final String destinationTopic = arguments.length == 5 && !arguments[4].isBlank() ? arguments[4] : null;
        if (!mode.equals("run")
                && !mode.equals("prepare")
                && !mode.equals("resume")
                && !mode.equals("crash-wait")
                && !mode.equals("source-ack-crash-wait")
                && !mode.equals("source-ack-resume")) {
            throw new IllegalArgumentException("unknown Worker smoke mode: " + mode);
        }
        final String topic = mode.equals("run") ? arguments[2] + "-worker-" + UUID.randomUUID() : arguments[2];
        final String physicalTopic = "persistent://public/default/" + topic;
        final String destinationPhysicalTopic =
                destinationTopic == null ? null : "persistent://public/default/" + destinationTopic;
        final String journalTopic = destinationTopic == null ? null : destinationTopic + "-attempt-journal";
        final HttpClient admin = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        final boolean reuseExistingTopic = mode.equals("resume")
                || mode.equals("crash-wait")
                || mode.equals("source-ack-crash-wait")
                || mode.equals("source-ack-resume");
        createTopic(admin, adminUrl, topic, reuseExistingTopic);
        if (destinationTopic != null && !mode.equals("prepare")) {
            requirePhysicalTestingEnvironment();
            createTopic(
                    admin,
                    adminUrl,
                    destinationTopic,
                    reuseExistingTopic,
                    DESTINATION_INCARNATION,
                    DESTINATION_CREATION_TIMESTAMP);
            createTopic(
                    admin, adminUrl, journalTopic, reuseExistingTopic, JOURNAL_INCARNATION, JOURNAL_CREATION_TIMESTAMP);
        }
        try {
            final var clientBuilder = PulsarClientArtifactClientBuilder.builder(serviceUrl);
            try (PulsarClient client = clientBuilder.build()) {
                if (mode.equals("prepare")) {
                    prepareWorkerRecord(client, physicalTopic);
                } else {
                    runWorker(
                            client,
                            physicalTopic,
                            mode.equals("run"),
                            mode.equals("crash-wait"),
                            destinationPhysicalTopic,
                            mode.equals("source-ack-crash-wait"),
                            mode.equals("source-ack-resume"));
                }
            }
        } finally {
            if (!mode.equals("prepare") && !isPersistentStaging()) {
                deleteTopicIfPresent(admin, adminUrl, topic);
                if (destinationTopic != null) {
                    deleteTopicIfPresent(admin, adminUrl, destinationTopic);
                    deleteTopicIfPresent(admin, adminUrl, journalTopic);
                }
            }
        }
    }

    private static void prepareWorkerRecord(final PulsarClient client, final String physicalTopic) throws Exception {
        final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
        final ShardId shard = restartShard(physicalTopic);
        // Keep a durable, unacknowledged cursor on the prepared record. The
        // staging Broker may rotate the active ledger while the no-cache
        // compile step starts the post-failover probe; without a cursor, the
        // closed one-entry ledger can be trimmed before recovery evidence is
        // read. The cursor is closed, not deleted, so this does not leave an
        // active consumer or change the Worker ownership test.
        final GuardedConsumer<byte[]> retentionConsumer = PulsarClientArtifactSourceConsumerFactory.create(
                client, guard, physicalTopic, PREPARED_RECORD_RETENTION_SUBSCRIPTION);
        try {
            send(
                    client,
                    guard,
                    physicalTopic,
                    command(shard, "worker-restart-prepared"),
                    "worker-restart-preparation-producer");
        } finally {
            retentionConsumer.close();
        }
        System.out.println(
                "Pulsar Worker restart preparation passed: one guarded record persisted before broker restart");
    }

    private static KeyPair sourceAuthorityKeyPair() throws GeneralSecurityException {
        final KeyFactory factory = KeyFactory.getInstance("Ed25519");
        final byte[] privateKey = Base64.getDecoder().decode(SOURCE_AUTHORITY_PRIVATE_KEY_BASE64);
        final byte[] publicKey = Base64.getDecoder().decode(SOURCE_AUTHORITY_PUBLIC_KEY_BASE64);
        return new KeyPair(
                factory.generatePublic(new X509EncodedKeySpec(publicKey)),
                factory.generatePrivate(new PKCS8EncodedKeySpec(privateKey)));
    }

    private static void runWorker(
            final PulsarClient client,
            final String physicalTopic,
            final boolean seedRecovery,
            final boolean waitForProcessCrash,
            final String destinationPhysicalTopic,
            final boolean sourceAckResponseLossProcessCrashWait,
            final boolean sourceAckResponseLossProcessCrashResume)
            throws Exception {
        final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
        final ShardId shard = seedRecovery ? new ShardId(RouteIncarnation.random(), 0) : restartShard(physicalTopic);
        if (seedRecovery) {
            final PreparedCommand recoveryCommand = command(shard, "worker-recovery");
            send(client, guard, physicalTopic, recoveryCommand, "worker-recovery-producer");
        }

        final WorkerAuthorityResources workerAuthorities = openWorkerAuthorityResources();
        final OxiaSyncOwnerLeaseBackend.ClientHandle oxia = workerAuthorities.oxia();
        final PersistentStagingActivation.Loaded persistentActivation = workerAuthorities.persistentActivation();
        final ManagedHandoffConfiguration managedHandoff =
                managedHandoffConfiguration(persistentActivation, destinationPhysicalTopic);
        final Path root = workerStoreRoot();
        final boolean admissionRecoveryResume =
                hasAdmissionResponseLossProcessCrash() && !waitForProcessCrash && !seedRecovery;
        final boolean destinationRecoveryResume =
                hasDestinationResponseLossProcessCrash() && !waitForProcessCrash && !seedRecovery;
        final boolean preserveWorkerRoot = isPersistentStaging()
                || hasAdmissionResponseLossProcessCrash()
                || hasDestinationResponseLossProcessCrash()
                || sourceAckResponseLossProcessCrashWait
                || sourceAckResponseLossProcessCrashResume;
        try {
            final String subscription = "nereus-delay-worker-" + UUID.randomUUID();
            final GuardedConsumer<byte[]> rawNativeConsumer =
                    PulsarClientArtifactSourceConsumerFactory.create(client, guard, physicalTopic, subscription);
            final AtomicBoolean sourceAckResponseLossObserved = new AtomicBoolean();
            final GuardedConsumer<byte[]> nativeConsumer =
                    hasSourceAckResponseLoss() && !sourceAckResponseLossProcessCrashResume
                            ? responseLossConsumer(rawNativeConsumer, sourceAckResponseLossObserved)
                            : rawNativeConsumer;
            final Optional<PulsarSourcePosition> persistedRecoveryPosition = persistedPulsarPosition(root, shard);
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof =
                    PulsarClientArtifactRecoverySourcePositioner.seekAfter(
                            nativeConsumer,
                            guard,
                            physicalTopic,
                            shard,
                            persistedRecoveryPosition,
                            Duration.ofSeconds(5));
            final SourceAssignment sourceAssignment = new SourceAssignment(
                    shard,
                    Bytes.sha256(Bytes.utf8("pulsar-worker-assignment")),
                    1,
                    PulsarActivationBarrier.empty(
                            shard,
                            INCARNATION,
                            physicalTopic,
                            proof.connectionGeneration(),
                            proof.attestationDigest()));
            final WorkerAssignmentAuthority assignmentAuthority = oxia == null
                    ? new InMemoryWorkerAssignmentAuthority()
                    : new OxiaSyncWorkerAssignmentBackend(oxia, workerAssignmentPrefix());
            final AssignmentAcceptance assignmentAcceptance =
                    publishAssignment(assignmentAuthority, sourceAssignment, oxia != null);
            final WorkerAssignment assignmentProjection = assignmentAcceptance.assignment();
            final SourceAssignment assignment = assignmentProjection.sourceAssignment();
            final OxiaOwnerLeaseStore authority = oxia == null
                    ? new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore())
                    : new OxiaOwnerLeaseStore(oxia.backend());
            final long ownerNow = System.currentTimeMillis();
            final byte[] sessionIdentity =
                    oxia == null ? Bytes.sha256(Bytes.utf8("pulsar-worker-session")) : oxia.sessionIdentity();
            final OwnerLease lease = authority
                    .acquire(assignment, workerId(), sessionIdentity, ownerNow, LEASE_DURATION_MS)
                    .orElseThrow();
            System.out.println("Pulsar Worker owner lease acquired: worker=" + workerId()
                    + ", assignmentRevision=" + assignmentAcceptance.revision()
                    + ", placementEpoch=" + assignmentProjection.placementEpoch()
                    + ", ownerEpoch=" + Long.toUnsignedString(lease.ownerEpoch()));
            final WorkClassExecutionRegistry workClasses = workClasses();
            final KeyPair verificationKey = sourceAuthorityKeyPair();
            final CompatibleControlSnapshot controlSnapshot = controlSnapshot(shard);
            boolean runtimeDrained = false;
            try {
                final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
                try (SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                        ShardStore store = ShardStore.open(storeConfig, shard, resources)) {
                    final SourceAckResponseLossProcessCrashContext sourceAckCrashContext =
                            sourceAckResponseLossProcessCrashWait
                                    ? new SourceAckResponseLossProcessCrashContext(
                                            root, store, physicalTopic, shard, sourceAckResponseLossObserved)
                                    : null;
                    resources.bindWorkClassExecutionRegistry(workClasses);
                    store.recordControlSnapshot(controlSnapshot);
                    final DelayShard delayShard = new DelayShard(
                            store,
                            DelayShardConfig.defaults(),
                            null,
                            null,
                            scheduleResolver(destinationPhysicalTopic, managedHandoff));
                    final OwnerIdentity ownerIdentity = new OwnerIdentity(
                            bytes(16, 70),
                            bytes(16, 71),
                            lease.ownerEpoch(),
                            Bytes.sha256(Bytes.utf8("pulsar-worker-fencing")));
                    final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease, ownerIdentity);

                    if (waitForProcessCrash
                            && !hasAdmissionResponseLossProcessCrash()
                            && !hasDestinationResponseLossProcessCrash()) {
                        awaitWorkerProcessCrashGate(root, store, physicalTopic, shard);
                    }

                    try (PulsarClientArtifactRecoverySourceCursor recovery =
                            new PulsarClientArtifactRecoverySourceCursor(
                                    nativeConsumer, guard, assignment, physicalTopic, Duration.ofMillis(250))) {
                        final List<SourceReplayEntry> recoveryEntries = new ArrayList<>();
                        final SourceReplayCursor<SourceReplayEntry> cursor = SourceReplayCursor.of(new Iterator<>() {
                            @Override
                            public boolean hasNext() {
                                return recovery.hasNext();
                            }

                            @Override
                            public SourceReplayEntry next() {
                                final SourceReplayEntry entry = recovery.next();
                                recoveryEntries.add(entry);
                                return entry;
                            }
                        });
                        final SourceReplaySuccessor recoverySuccessor = destinationRecoveryResume
                                ? destinationResponseLossRecoverySuccessor(persistedRecoveryPosition.orElseThrow())
                                : SourceReplaySuccessor.strictPulsarBatchMember();
                        final OwnerRecoveryCoordinator coordinator = new OwnerRecoveryCoordinator(
                                ownedShard,
                                authority,
                                assignment,
                                recoverySuccessor,
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
                        if (!coordinator.complete() || ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
                            throw new IllegalStateException(
                                    "Pulsar Worker recovery did not activate at one exact record");
                        }
                        System.out.println("Pulsar Worker recovery observation: turnOutcomes="
                                + turn.outcomes().size() + ", replayEntries=" + recoveryEntries.size()
                                + ", replayOutcomes="
                                + recoveryEntries.stream()
                                        .filter(entry -> entry instanceof SourceReplayMutation mutation
                                                && mutation.mutation().type() == SystemMutationType.PUBLISH_OUTCOME)
                                        .count()
                                + ", lastCatchup=" + ownedShard.lastCatchupPosition()
                                + ", applied=" + store.appliedShardLogPosition()
                                + ", destinationRecoveryResume=" + destinationRecoveryResume
                                + ", outcomeApply=" + recoveryOutcomeSummary(turn));
                        final PulsarSourcePosition recovered;
                        if (turn.outcomes().size() == 1
                                && ownedShard.lastCatchupPosition() instanceof PulsarSourcePosition replayed) {
                            recovered = replayed;
                        } else if ((admissionRecoveryResume || sourceAckResponseLossProcessCrashResume)
                                && turn.outcomes().isEmpty()
                                && store.appliedShardLogPosition() instanceof PulsarSourcePosition persisted) {
                            recovered = persisted;
                        } else {
                            throw new IllegalStateException(
                                    "Pulsar Worker recovery did not activate at one exact record");
                        }
                        if (!recovered.shardId().equals(shard)
                                || !Arrays.equals(recovered.brokerResourceIncarnation(), INCARNATION)
                                || !recovered.physicalTopic().equals(physicalTopic)) {
                            throw new IllegalStateException("Pulsar Worker recovery position identity changed");
                        }

                        final PreparedCommand activeCommand;
                        final PreparedCommand physicalCommand;
                        final PulsarSourcePosition physicalSchedulePosition;
                        final DelayMessageId physicalMessageId;
                        final PhysicalPublishBridge physicalBridge;
                        final PublishAttemptLedger recoveredPublishAttempt;
                        final RecoveredDestinationOutcome recoveredDestinationOutcome;
                        if (destinationRecoveryResume) {
                            if (destinationPhysicalTopic == null) {
                                throw new IllegalStateException(
                                        "Pulsar destination response-loss recovery requires a destination topic");
                            }
                            activeCommand = null;
                            physicalCommand = null;
                            recoveredPublishAttempt = null;
                            recoveredDestinationOutcome =
                                    requireRecoveredDestinationOutcome(recoveryEntries, store, shard);
                            physicalSchedulePosition = recoveredDestinationOutcome.physicalSchedulePosition();
                            physicalMessageId = recoveredDestinationOutcome.messageId();
                            physicalBridge = null;
                        } else if (admissionRecoveryResume) {
                            if (destinationPhysicalTopic == null) {
                                throw new IllegalStateException(
                                        "Pulsar admission response-loss recovery requires a destination topic");
                            }
                            activeCommand = null;
                            physicalCommand = null;
                            recoveredPublishAttempt = requireAdmissionRecoveryAttempt(delayShard);
                            final var recoveredMessage =
                                    delayShard.getMessage(recoveredPublishAttempt.delayMessageId());
                            if (recoveredMessage == null || recoveredMessage.status() != MessageStatus.PUBLISHING) {
                                throw new IllegalStateException(
                                        "Pulsar admission response-loss recovery did not find the durable "
                                                + "PUBLISHING message");
                            }
                            final SourcePosition schedulePosition =
                                    SourcePositionCodec.decode(recoveredMessage.scheduleSourcePosition());
                            if (!(schedulePosition instanceof PulsarSourcePosition persistedSchedule)) {
                                throw new IllegalStateException(
                                        "Pulsar admission response-loss recovery has a non-Pulsar schedule position");
                            }
                            physicalSchedulePosition = persistedSchedule;
                            final LaneRecord recoveredLane = delayShard.getLane(recoveredPublishAttempt.laneId());
                            if (recoveredLane == null
                                    || !Arrays.equals(
                                            recoveredLane.laneIncarnation(),
                                            recoveredPublishAttempt.laneIncarnation())) {
                                throw new IllegalStateException(
                                        "Pulsar admission response-loss recovery changed the durable Lane identity");
                            }
                            physicalMessageId = recoveredPublishAttempt.delayMessageId();
                            physicalBridge = createPhysicalPublishBridge(
                                    client,
                                    nativeConsumer,
                                    physicalTopic,
                                    shard,
                                    physicalSchedulePosition,
                                    destinationPhysicalTopic,
                                    store,
                                    ownedShard,
                                    ownerIdentity,
                                    authority,
                                    workClasses,
                                    verificationKey,
                                    destinationProfile("worker-physical-publish"),
                                    capabilityProfile(),
                                    recoveredPublishAttempt.laneId(),
                                    recoveredLane.laneIncarnation(),
                                    1_000_000,
                                    null,
                                    0);
                            recoveredDestinationOutcome = null;
                        } else {
                            recoveredPublishAttempt = null;
                            activeCommand = command(shard, "worker-active");
                            send(client, guard, physicalTopic, activeCommand, "worker-active-producer");
                            physicalCommand = destinationPhysicalTopic == null
                                    ? null
                                    : managedHandoff == null
                                            ? command(
                                                    shard,
                                                    "worker-physical-publish",
                                                    Bytes.utf8("pulsar-worker-source-applied-payload"),
                                                    2_000)
                                            : managedHandoffCommand(
                                                    shard,
                                                    managedHandoff.identity(),
                                                    Bytes.utf8("pulsar-worker-managed-handoff-payload"));
                            physicalSchedulePosition = physicalCommand == null
                                    ? null
                                    : sendAndPosition(
                                            client,
                                            guard,
                                            physicalTopic,
                                            physicalCommand,
                                            "worker-physical-schedule-producer");
                            physicalMessageId = physicalCommand == null ? null : physicalCommand.delayMessageId();
                            physicalBridge = physicalCommand == null
                                    ? null
                                    : createPhysicalPublishBridge(
                                            client,
                                            nativeConsumer,
                                            physicalTopic,
                                            shard,
                                            physicalSchedulePosition,
                                            destinationPhysicalTopic,
                                            store,
                                            ownedShard,
                                            ownerIdentity,
                                            authority,
                                            workClasses,
                                            verificationKey,
                                            managedHandoff == null
                                                    ? destinationProfile("worker-physical-publish")
                                                    : managedHandoff
                                                            .identity()
                                                            .destination()
                                                            .ref(),
                                            managedHandoff == null
                                                    ? capabilityProfile()
                                                    : managedHandoff
                                                            .identity()
                                                            .capability()
                                                            .ref(),
                                            null,
                                            null,
                                            1_000_000,
                                            null,
                                            0,
                                            null,
                                            persistentActivation,
                                            managedHandoff);
                            recoveredDestinationOutcome = null;
                        }
                        try (physicalBridge) {
                            final WorkerShardRuntime runtime = PulsarClientArtifactWorkerSourceFactory.create(
                                    nativeConsumer,
                                    guard,
                                    physicalTopic,
                                    Duration.ofMillis(250),
                                    assignment,
                                    workClasses,
                                    ownedShard,
                                    store,
                                    resources,
                                    authority,
                                    verificationKey.getPublic(),
                                    null,
                                    null,
                                    null,
                                    null,
                                    physicalBridge == null ? null : physicalBridge.executor());
                            try {
                                final PulsarSourcePosition activePosition;
                                if (destinationRecoveryResume) {
                                    activePosition = recovered;
                                    requireAppliedRecoveryOutcome(turn);
                                    if (recoveredDestinationOutcome == null
                                            || recoveredDestinationOutcome
                                                            .outcome()
                                                            .sideEffect()
                                                    != 1
                                            || recoveredDestinationOutcome
                                                            .outcome()
                                                            .stableCode()
                                                    != StableCode.OK) {
                                        throw new IllegalStateException(
                                                "Pulsar destination response-loss recovery did not apply a definitive "
                                                        + "PUBLISHED Outcome");
                                    }
                                    requirePayload(
                                            client, destinationPhysicalTopic, recoveredDestinationOutcome.payload());
                                    if (chaosStateDumpDirectory() != null) {
                                        writeChaosStateDump(
                                                "pulsar-worker-destination-response-loss-process-crash",
                                                "RECOVERED_AFTER_FRESH_PROCESS",
                                                store.dbPath(),
                                                store,
                                                null,
                                                physicalSchedulePosition,
                                                "PUBLISHED",
                                                true,
                                                recoveredDestinationOutcome
                                                        .outcome()
                                                        .publishAttemptId(),
                                                null,
                                                recoveredDestinationOutcome.messageId());
                                    }
                                    System.out.println("Pulsar Worker destination response-loss fresh-process recovery "
                                            + "passed: the durable PUBLISH_OUTCOME was replayed after SIGKILL, the "
                                            + "exact destination payload was read back, and no second SEND was issued");
                                } else if (admissionRecoveryResume) {
                                    if (!(store.appliedShardLogPosition()
                                                    instanceof PulsarSourcePosition appliedPosition)
                                            || !appliedPosition.equals(recovered)) {
                                        throw new IllegalStateException(
                                                "Pulsar admission response-loss recovery changed the durable "
                                                        + "admission position");
                                    }
                                    activePosition = recovered;
                                    final WorkerShardRuntime.SourceBoundPhysicalPublishTurn physicalTurn =
                                            runSourceAppliedPhysicalPublish(
                                                    runtime,
                                                    delayShard,
                                                    ownedShard,
                                                    ownerIdentity,
                                                    authority,
                                                    store,
                                                    workClasses,
                                                    verificationKey,
                                                    physicalBridge,
                                                    physicalMessageId,
                                                    physicalSchedulePosition,
                                                    client,
                                                    null,
                                                    1_000_000);
                                    if (chaosStateDumpDirectory() != null) {
                                        writeChaosStateDump(
                                                "pulsar-worker-admission-response-loss-process-crash",
                                                "RECOVERED_AFTER_FRESH_PROCESS",
                                                store.dbPath(),
                                                store,
                                                physicalTurn.attempt().orElse(recoveredPublishAttempt),
                                                physicalSchedulePosition,
                                                "UNCERTAIN",
                                                true,
                                                null,
                                                null,
                                                null);
                                    }
                                } else {
                                    final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result =
                                            runUntilApplied(runtime, sourceAckCrashContext);
                                    if (!(result.entry() instanceof SourceReplayRecord record)
                                            || !record.command().equals(activeCommand)
                                            || !(record.position()
                                                    instanceof PulsarSourcePosition appliedActivePosition)) {
                                        throw new IllegalStateException(
                                                "Pulsar Worker active turn did not apply the second record");
                                    }
                                    activePosition = appliedActivePosition;
                                    final var applied = store.appliedShardLogPosition();
                                    if (!(applied instanceof PulsarSourcePosition appliedPosition)
                                            || !appliedPosition.equals(activePosition)
                                            || activePosition.compareTo(recovered) <= 0) {
                                        throw new IllegalStateException(
                                                "Pulsar Worker Store did not persist the active position");
                                    }
                                    if (physicalBridge != null) {
                                        final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult
                                                physicalSchedule = runUntilApplied(runtime, sourceAckCrashContext);
                                        if (!(physicalSchedule.entry() instanceof SourceReplayRecord physicalRecord)
                                                || !physicalRecord.command().equals(physicalCommand)
                                                || !(physicalSchedule.entry().position()
                                                        instanceof PulsarSourcePosition appliedPhysical)) {
                                            throw new IllegalStateException(
                                                    "Pulsar Worker physical Schedule was not source-applied");
                                        }
                                        if (!appliedPhysical.equals(physicalSchedulePosition)) {
                                            throw new IllegalStateException(
                                                    "Pulsar Worker physical Schedule Source Position changed "
                                                            + "across apply");
                                        }
                                        final WorkerShardRuntime.SourceBoundPhysicalPublishTurn physicalTurn =
                                                runSourceAppliedPhysicalPublish(
                                                        runtime,
                                                        delayShard,
                                                        ownedShard,
                                                        ownerIdentity,
                                                        authority,
                                                        store,
                                                        workClasses,
                                                        verificationKey,
                                                        physicalBridge,
                                                        physicalCommand,
                                                        physicalSchedulePosition,
                                                        client);
                                        if (chaosStateDumpDirectory() != null) {
                                            writeChaosStateDump(
                                                    "pulsar-worker-admission-response-loss-process-crash",
                                                    "RECOVERED_AFTER_FRESH_PROCESS",
                                                    store.dbPath(),
                                                    store,
                                                    physicalTurn.attempt().orElse(null),
                                                    physicalSchedulePosition,
                                                    "PUBLISHED",
                                                    true,
                                                    null,
                                                    null,
                                                    null);
                                        }
                                    }
                                }
                                if (sourceAckResponseLossProcessCrashResume) {
                                    final PulsarSourcePosition ackSourcePosition =
                                            persistedRecoveryPosition.orElseThrow(() -> new IllegalStateException(
                                                    "Pulsar source ACK response-loss fresh process lost "
                                                            + "its durable source position"));
                                    final boolean replayedAckSource = recoveryEntries.stream()
                                            .anyMatch(entry -> Arrays.equals(
                                                    entry.position().canonicalBytes(),
                                                    ackSourcePosition.canonicalBytes()));
                                    writeSourceAckResponseLossStateDump(
                                            "RECOVERED_AFTER_FRESH_PROCESS",
                                            root,
                                            store,
                                            physicalTopic,
                                            shard,
                                            ackSourcePosition,
                                            store.appliedShardLogPosition(),
                                            recoveryEntries.size(),
                                            replayedAckSource);
                                }
                                if (hasWorkerProcessCrash()
                                        && !waitForProcessCrash
                                        && !hasAdmissionResponseLossProcessCrash()
                                        && !hasDestinationResponseLossProcessCrash()) {
                                    writeWorkerProcessStateDump(
                                            "RECOVERED_AFTER_FRESH_PROCESS", root, store, physicalTopic, shard, true);
                                }
                                if (admissionRecoveryResume) {
                                    requireUncertainDrainBlocked(runtime, ownedShard, authority, store, shard);
                                } else {
                                    final long finalOwnerEpoch =
                                            ownedShard.lease().ownerEpoch();
                                    final Path checkpointPath = finalCheckpointPath(root, shard, finalOwnerEpoch);
                                    final byte[] checkpointId = finalCheckpointId(shard, finalOwnerEpoch);
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
                                                "Pulsar Worker drain did not publish the final checkpoint and release "
                                                        + "the exact owner lease");
                                    }
                                    runtimeDrained = true;
                                    System.out.println(
                                            "Pulsar Worker vertical smoke passed: assignment recovery ledger/entry="
                                                    + recovered.ledgerId() + "/" + recovered.entryId()
                                                    + ", active apply ledger/entry=" + activePosition.ledgerId() + "/"
                                                    + activePosition.entryId()
                                                    + ", guarded SUBSCRIBE, RocksDB WriteBatch, ACK, "
                                                    + "and final checkpoint");
                                    if (oxia != null) {
                                        System.out.println(
                                                "Pulsar Worker authority smoke passed: real Oxia session-bound lease");
                                    }
                                }
                                if (hasSourceAckResponseLoss()) {
                                    if (!sourceAckResponseLossObserved.get()) {
                                        throw new IllegalStateException(
                                                "Pulsar Worker source ACK response-loss wrapper did not lose "
                                                        + "a response");
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
                if (!preserveWorkerRoot) {
                    deleteTree(root);
                }
                if (!runtimeDrained) {
                    closeNative(nativeConsumer);
                }
            }
        } finally {
            workerAuthorities.close();
        }
    }

    /**
     * Exercises the source-ordered Admission to physical destination bridge.
     * The Claim and certificate are deliberately supplied by this bounded
     * smoke authority; the source log remains authoritative for Admission and
     * Outcome application.
     */
    static WorkerShardRuntime.SourceBoundPhysicalPublishTurn runSourceAppliedPhysicalPublish(
            final WorkerShardRuntime runtime,
            final DelayShard delayShard,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final PreparedCommand physicalCommand,
            final PulsarSourcePosition physicalSchedulePosition,
            final PulsarClient client)
            throws Exception {
        return runSourceAppliedPhysicalPublish(
                runtime,
                delayShard,
                ownedShard,
                ownerIdentity,
                authority,
                store,
                workClasses,
                verificationKey,
                bridge,
                physicalCommand.delayMessageId(),
                physicalSchedulePosition,
                client,
                null,
                1_000_000);
    }

    static WorkerShardRuntime.SourceBoundPhysicalPublishTurn runSourceAppliedPhysicalPublish(
            final WorkerShardRuntime runtime,
            final DelayShard delayShard,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final DelayMessageId physicalMessageId,
            final PulsarSourcePosition physicalSchedulePosition,
            final PulsarClient client,
            final byte[] payloadOverride,
            final long workClassBytes)
            throws Exception {
        return runSourceAppliedPhysicalPublish(
                runtime,
                delayShard,
                ownedShard,
                ownerIdentity,
                authority,
                store,
                workClasses,
                verificationKey,
                bridge,
                physicalMessageId,
                physicalSchedulePosition,
                client,
                payloadOverride,
                workClassBytes,
                null);
    }

    static WorkerShardRuntime.SourceBoundPhysicalPublishTurn runSourceAppliedPhysicalPublish(
            final WorkerShardRuntime runtime,
            final DelayShard delayShard,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final DelayMessageId physicalMessageId,
            final PulsarSourcePosition physicalSchedulePosition,
            final PulsarClient client,
            final byte[] payloadOverride,
            final long workClassBytes,
            final ClaimExecutionAdmission sharedClaimAdmission)
            throws Exception {
        if (workClassBytes <= 0 || (payloadOverride != null && payloadOverride.length > workClassBytes)) {
            throw new IllegalArgumentException("Pulsar physical publish payload exceeds the admitted work-class bytes");
        }
        final var message = delayShard.getMessage(physicalMessageId);
        final boolean admissionRecoveryResume =
                hasAdmissionResponseLossProcessCrash() && !hasWorkerAdmissionResponseLoss();
        if (message == null
                || (message.status() != MessageStatus.SCHEDULED
                        && !(admissionRecoveryResume && message.status() == MessageStatus.PUBLISHING))
                || !message.laneId().equals(bridge.laneId())) {
            throw new IllegalStateException(
                    "source-applied physical Schedule did not create the expected scheduled message");
        }
        delayShard.activateLaneReadiness(
                bridge.laneId(),
                bridge.laneIncarnation(),
                bridge.channel(),
                bridge.readyCertificate(),
                bridge.evidenceCursors());
        final var lane = delayShard.getLane(bridge.laneId());
        if (lane == null || !lane.schedulable()) {
            throw new IllegalStateException("source-applied physical Lane did not become schedulable");
        }
        bindActiveOwnerPublishGraph(
                runtime,
                ownedShard,
                ownerIdentity,
                authority,
                store,
                workClasses,
                verificationKey,
                bridge,
                workClassBytes,
                sharedClaimAdmission);
        final long eligibleAt = bridge.managedHandoffSnapshot() == null
                ? message.deliverAtEpochMs()
                : Math.subtractExact(
                        message.deliverAtEpochMs(),
                        bridge.managedHandoffSnapshot().effectiveLeadMs());
        waitUntil(eligibleAt);

        final byte[] payload = payloadOverride == null ? message.payload() : Bytes.copy(payloadOverride);
        if (admissionRecoveryResume) {
            final PublishAttemptLedger recoveredAttempt = requireAdmissionRecoveryAttempt(delayShard);
            final SourcePosition recoveredAdmission = SourcePositionCodec.decode(recoveredAttempt.sourcePosition());
            if (!(recoveredAdmission instanceof PulsarSourcePosition admissionPosition)
                    || admissionPosition.compareTo(physicalSchedulePosition) <= 0) {
                throw new IllegalStateException(
                        "Pulsar admission response-loss fresh process recovered a non-source-bound Admission");
            }
            final WorkerPhysicalPublishExecutor.Submission submission = runtime.submitPhysicalPublish(
                    recoveredAttempt.publishAttemptId(), payload, System::currentTimeMillis);
            if (submission.state() != WorkerPhysicalPublishExecutor.SubmissionState.OUTCOME_HANDOFF_QUEUED) {
                throw new IllegalStateException(
                        "Pulsar admission response-loss fresh process did not hold the foreign-owner attempt: "
                                + submission.state());
            }
            final WorkerShardRuntime.SourceBoundPhysicalPublishTurn physicalTurn =
                    new WorkerShardRuntime.SourceBoundPhysicalPublishTurn(
                            WorkerShardRuntime.SourceBoundPhysicalPublishStatus.PHYSICAL_SUBMITTED,
                            0,
                            Optional.empty(),
                            Optional.of(recoveredAttempt),
                            Optional.of(submission),
                            null);
            return finishAdmissionRecoveryHold(runtime, delayShard, workClasses, bridge, client, physicalTurn);
        }
        WorkerShardRuntime.DueClaimPublishPhysicalTurn dueClaimPublish = null;
        // A large payload can exceed the Lane's initial DRR quantum. Spend a
        // bounded number of ordinary due turns to accumulate the exact head
        // credit; keep every submitted due task inside its WorkClass cap.
        for (int schedulerTurn = 0; schedulerTurn < 32; schedulerTurn++) {
            final long dueEarliest = Math.max(System.currentTimeMillis(), eligibleAt);
            final long dueLatest = bridge.managedHandoffSnapshot() == null
                    ? Math.addExact(dueEarliest, 500)
                    : Math.min(Math.addExact(dueEarliest, 500), Math.subtractExact(message.deliverAtEpochMs(), 1));
            if (dueLatest < dueEarliest) {
                throw new IllegalStateException("Managed Handoff missed its pre-delivery Admission interval");
            }
            final TrustedUtcIntervalEvidence dueEvidence = evidence(dueEarliest, dueLatest, "pulsar-worker-due-clock");
            final long dueTaskRequestBytes = Math.addExact(44L, dueEvidence.canonicalBytes().length);
            final long dueDiscoveryBytes = Math.min(
                    Math.max(DUE_DISCOVERY_MAX_BYTES, (long) payload.length),
                    Math.subtractExact(workClassBytes, dueTaskRequestBytes));
            if (dueDiscoveryBytes <= 0) {
                throw new IllegalArgumentException(
                        "Pulsar physical publish work-class bytes cannot contain due discovery request");
            }
            dueClaimPublish = runtime.runDueClaimPublishPhysicalTurn(
                    dueEvidence,
                    new SchedulerBudget(1, dueDiscoveryBytes, TimeUnit.SECONDS.toNanos(2)),
                    message.expireAtEpochMs() - 1,
                    claimCharge(payload.length),
                    System::currentTimeMillis,
                    new SchedulerBudget(1, workClassBytes, TimeUnit.SECONDS.toNanos(2)),
                    16,
                    new SchedulerBudget(1, workClassBytes, TimeUnit.SECONDS.toNanos(2)),
                    16,
                    ignored -> Optional.of(payload));
            if (dueClaimPublish.dueClaimPublishTurn().claimResult().isPresent()) {
                break;
            }
        }
        final var dueClaim = dueClaimPublish.dueClaimPublishTurn();
        final var claimResult = dueClaim.claimResult()
                .orElseThrow(
                        () -> new IllegalStateException("provider-driven Worker turn did not return a Claim result"));
        if (claimResult.kind() != ClaimHandoffWorkClassExecutor.ResultKind.CLAIMED) {
            throw new IllegalStateException("provider-driven Worker Claim was not admitted: " + claimResult.kind());
        }
        final var admissionSubmission = dueClaim.publishSubmission()
                .orElseThrow(
                        () -> new IllegalStateException("provider-driven Worker turn did not queue Publish Admission"));
        final var admissionResult = admissionSubmission
                .result()
                .orElseThrow(() -> new IllegalStateException("provider-driven Publish Admission has no result"));
        final WorkerShardRuntime.SourceBoundPhysicalPublishTurn physicalTurn = dueClaimPublish
                .physicalTurn()
                .orElseThrow(
                        () -> new IllegalStateException("provider-driven Worker turn did not start physical publish"));
        final PulsarSourcePosition admissionPosition;
        if (admissionResult.kind()
                == com.nereusstream.delay.ownership.PublishAdmissionWorkClassExecutor.ResultKind.ENQUEUED) {
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
                == com.nereusstream.delay.ownership.PublishAdmissionWorkClassExecutor.ResultKind.UNKNOWN) {
            final var recoveredAttempt = physicalTurn
                    .attempt()
                    .orElseThrow(() ->
                            new IllegalStateException("UNKNOWN Publish Admission did not recover a PUBLISHING attempt: "
                                    + physicalTurn.status() + "/" + physicalTurn.failure()));
            final SourcePosition recoveredPosition = SourcePositionCodec.decode(recoveredAttempt.sourcePosition());
            if (!(recoveredPosition instanceof PulsarSourcePosition recoveredAdmissionPosition)
                    || recoveredAdmissionPosition.compareTo(physicalSchedulePosition) <= 0) {
                throw new IllegalStateException(
                        "Pulsar Worker recovered UNKNOWN Publish Admission was not source-bound");
            }
            admissionPosition = recoveredAdmissionPosition;
            System.out.println("Pulsar Worker recovered UNKNOWN Publish Admission from exact source mutation: "
                    + admissionPosition);
            if (bridge.admissionResponseLoss()) {
                if (!bridge.admissionResponseLossObserved()) {
                    throw new IllegalStateException(
                            "Pulsar Worker admission response-loss wrapper did not discard a persisted response");
                }
                if (chaosStateDumpDirectory() != null) {
                    writeChaosStateDump(
                            "pulsar-worker-admission-response-loss-process-crash",
                            "ADMISSION_RESPONSE_LOSS_PERSISTED",
                            store.dbPath(),
                            store,
                            recoveredAttempt,
                            physicalSchedulePosition,
                            "PUBLISHING",
                            false,
                            null,
                            null,
                            null);
                }
                System.out.println("Pulsar Worker Publish Admission response-loss smoke passed: the real Shard Log "
                        + "mutation was persisted, its local append response was discarded, and exact source replay "
                        + "recovered the PUBLISHING admission");
                if (hasAdmissionResponseLossProcessCrash()) {
                    final WorkerPhysicalPublishExecutor.Submission cutSubmission = physicalTurn
                            .physicalSubmission()
                            .orElseThrow(
                                    () -> new IllegalStateException("admission crash cut has no physical submission"));
                    final PublishAttemptLedger cutAttempt = requireAdmissionRecoveryAttempt(delayShard);
                    if (cutSubmission.state() != WorkerPhysicalPublishExecutor.SubmissionState.DEFERRED
                            || cutSubmission.physicalCall().isPresent()
                            || !cutAttempt.mappingDurable()
                            || !cutAttempt.hasAllocatedJournalSequence()
                            || !cutAttempt.hasJournalPosition()
                            || cutAttempt.retirementPending()) {
                        throw new IllegalStateException(
                                "admission crash cut is not exactly after durable MAPPED and before ownership/SEND");
                    }
                    awaitAdmissionResponseLossProcessCrashGate(store.dbPath());
                }
            }
        } else {
            throw new IllegalStateException(
                    "Pulsar Worker provider-driven Publish Admission was not source-bound: " + admissionResult.kind());
        }
        return finishPhysicalPublish(
                runtime,
                delayShard,
                store,
                workClasses,
                bridge,
                client,
                physicalSchedulePosition,
                payload,
                admissionPosition,
                physicalTurn);
    }

    private static WorkerShardRuntime.SourceBoundPhysicalPublishTurn finishAdmissionRecoveryHold(
            final WorkerShardRuntime runtime,
            final DelayShard delayShard,
            final WorkClassExecutionRegistry workClasses,
            final PhysicalPublishBridge bridge,
            final PulsarClient client,
            final WorkerShardRuntime.SourceBoundPhysicalPublishTurn physicalTurn)
            throws Exception {
        final PublishAttemptLedger attempt = physicalTurn
                .attempt()
                .orElseThrow(() -> new IllegalStateException("recovery hold did not retain its durable attempt"));
        final WorkerPhysicalPublishExecutor.Submission submission =
                physicalTurn.physicalSubmission().orElseThrow();
        waitForPhysicalCompletion(submission);
        final DestinationPublishResult result = submission.physicalResult().orElseThrow();
        if (result.disposition() != DestinationPublishResult.Disposition.UNKNOWN
                || result.stableCode() != StableCode.RECOVERY_FIRST_SEND_UNCERTAIN
                || submission.physicalCall().isPresent()) {
            throw new IllegalStateException("foreign-owner recovery reached a target SEND or the wrong hold result");
        }

        final SourceReplayMutation outcomeRecord = awaitPublishOutcome(runtime, workClasses);
        final PublishOutcomeBody outcome =
                PublishOutcomeBody.decode(outcomeRecord.mutation().canonicalBody());
        final var application =
                delayShard.getSystemMutationResult(outcomeRecord.mutation().systemMutationId());
        if (application == null
                || application.applyStatus() != com.nereusstream.delay.runtime.ApplyStatus.APPLIED
                || application.stableCode() != StableCode.RECOVERY_FIRST_SEND_UNCERTAIN) {
            throw new IllegalStateException("foreign-owner recovery Outcome was not source-applied: "
                    + (application == null ? "missing" : application.applyStatus() + "/" + application.stableCode()));
        }
        if (outcome.sideEffect() != 3
                || outcome.disposition() != 4
                || outcome.stableCode() != StableCode.RECOVERY_FIRST_SEND_UNCERTAIN
                || outcome.retryDecision().kind() != 5
                || outcome.retryDecision().hasNextRetryAt()
                || !Arrays.equals(outcome.publishAttemptId(), attempt.publishAttemptId())) {
            throw new IllegalStateException("foreign-owner recovery did not source-apply the closed UNKNOWN hold");
        }
        final var message = delayShard.getMessage(attempt.delayMessageId());
        final PublishAttemptLedger uncertain = delayShard.findOpenPublishAttempt(attempt.publishAttemptId());
        if (message == null
                || message.status() != MessageStatus.UNCERTAIN
                || uncertain == null
                || uncertain.state() != com.nereusstream.delay.runtime.AttemptLedgerState.UNCERTAIN
                || !uncertain.mappingDurable()
                || !uncertain.hasAllocatedJournalSequence()
                || !uncertain.hasJournalPosition()
                || uncertain.retirementPending()) {
            throw new IllegalStateException("foreign-owner recovery did not retain the retired Journal proof: "
                    + "message=" + (message == null ? "missing" : message.status())
                    + ", attempt=" + (uncertain == null ? "missing" : uncertain.state())
                    + ", mapping=" + (uncertain != null && uncertain.mappingDurable())
                    + ", sequence=" + (uncertain != null && uncertain.hasAllocatedJournalSequence())
                    + ", position=" + (uncertain != null && uncertain.hasJournalPosition())
                    + ", retirementPending=" + (uncertain != null && uncertain.retirementPending()));
        }
        requireNoPayload(client, bridge.destinationPhysicalTopic());
        System.out.println("Pulsar Worker admission fresh-process recovery hold passed: old Owner attempt="
                + Bytes.hex(attempt.publishAttemptId())
                + ", Journal RETIRED_NOT_PUBLISHED is durable, Outcome=RECOVERY_FIRST_SEND_UNCERTAIN, target SEND=0");
        return physicalTurn;
    }

    private static void requireUncertainDrainBlocked(
            final WorkerShardRuntime runtime,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final ShardId shard) {
        try {
            runtime.drain(
                    new com.nereusstream.delay.ownership.OwnerDrainCoordinator.DrainRequest(
                            System.currentTimeMillis() + 30_000, 0, null),
                    System::currentTimeMillis,
                    () -> {});
            throw new IllegalStateException("UNCERTAIN recovery obligation incorrectly completed a clean drain");
        } catch (IllegalStateException expected) {
            if (!"owner drain callback quiescence budget exhausted".equals(expected.getMessage())) {
                throw expected;
            }
        }
        final Optional<OwnerLease> current = authority.current(shard);
        if (ownedShard.state() != ShardLifecycleState.DRAINING
                || !runtime.sourcePaused()
                || store.isClosed()
                || current.isEmpty()
                || !ownedShard.lease().sameIdentity(current.orElseThrow())) {
            throw new IllegalStateException("UNCERTAIN recovery drain did not remain fail-closed and retryable");
        }
        System.out.println("Pulsar Worker admission recovery drain guard passed: unresolved UNCERTAIN obligation "
                + "blocked clean checkpoint/lease release; disposable session teardown remains non-authoritative");
    }

    private static SourceReplayMutation awaitPublishOutcome(
            final WorkerShardRuntime runtime, final WorkClassExecutionRegistry workClasses) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            workClasses.runTurn(new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)));
            final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult turn = runtime.runSourceTurn(
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)), System::currentTimeMillis);
            if (turn.status() == com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                if (turn.entry() instanceof SourceReplayMutation mutation
                        && mutation.mutation().type() == SystemMutationType.PUBLISH_OUTCOME) {
                    return mutation;
                }
                continue;
            }
            if (turn.status() != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && turn.status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                    .WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException(
                        "Pulsar Worker Publish Outcome source turn failed: " + turn.status(), turn.failure());
            }
            TimeUnit.MILLISECONDS.sleep(25);
        }
        throw new IllegalStateException("source-applied PUBLISH_OUTCOME did not become visible before deadline");
    }

    private static WorkerShardRuntime.SourceBoundPhysicalPublishTurn finishPhysicalPublish(
            final WorkerShardRuntime runtime,
            final DelayShard delayShard,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final PhysicalPublishBridge bridge,
            final PulsarClient client,
            final PulsarSourcePosition physicalSchedulePosition,
            final byte[] payload,
            final PulsarSourcePosition admissionPosition,
            final WorkerShardRuntime.SourceBoundPhysicalPublishTurn physicalTurn)
            throws Exception {
        final PublishAttemptLedger attempt = physicalTurn
                .attempt()
                .orElseThrow(
                        () -> new IllegalStateException("physical publish result did not retain its durable attempt"));
        final byte[] publishAttemptId = attempt.publishAttemptId();
        if (physicalTurn.status() != WorkerShardRuntime.SourceBoundPhysicalPublishStatus.PHYSICAL_SUBMITTED) {
            throw new IllegalStateException("source-applied PUBLISHING did not submit physical publish: "
                    + physicalTurn.status() + "/" + physicalTurn.failure());
        }
        final WorkerPhysicalPublishExecutor.Submission submission =
                physicalTurn.physicalSubmission().orElseThrow();
        waitForPhysicalCompletion(submission);
        final DestinationPublishResult physicalResult =
                submission.physicalResult().orElseThrow();
        if (physicalResult.disposition() != DestinationPublishResult.Disposition.PUBLISHED
                || physicalResult.evidence() == null) {
            throw new IllegalStateException("source-applied physical publish did not return typed PUBLISHED evidence: "
                    + physicalResult.disposition() + "/" + physicalResult.stableCode());
        }
        final PublishAdmissionBody physicalAdmission = PublishAdmissionBody.decode(attempt.admissionBytes());
        final var physicalDescriptor = physicalAdmission.descriptor().value();
        final boolean managedHandoff = bridge.managedHandoffSnapshot() != null;
        if (managedHandoff
                && (physicalDescriptor.deliveryContract()
                                != com.nereusstream.delay.protocol.DeliveryContract.PULSAR_NATIVE_DELIVERY
                        || physicalDescriptor.actionAtEpochMs() >= physicalDescriptor.deliverAtEpochMs()
                        || !bridge.managedHandoffSnapshot().equals(physicalDescriptor.handoffPolicySnapshot())
                        || physicalResult.brokerPersistenceTimeEpochMs() < physicalDescriptor.actionAtEpochMs()
                        || physicalResult.brokerPersistenceTimeEpochMs() >= physicalDescriptor.deliverAtEpochMs())) {
            throw new IllegalStateException(
                    "real Managed Handoff did not persist inside its frozen native-delivery interval");
        }

        if (hasDestinationResponseLossProcessCrash()) {
            if (!bridge.destinationResponseLoss() || !bridge.destinationResponseEvidenceResolved()) {
                throw new IllegalStateException(
                        "Pulsar destination response-loss process crash reached without resolved SEND evidence");
            }
            workClasses.runTurn(new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)));
            if (workClasses.pending(WorkClass.OUTCOME_AND_CONTROL) != 0) {
                throw new IllegalStateException(
                        "Pulsar destination response-loss process crash did not persist PUBLISH_OUTCOME first");
            }
            final SystemMutation outcomeMutation = submission
                    .outcomeMutation()
                    .orElseThrow(() -> new IllegalStateException(
                            "Pulsar destination response-loss process crash has no queued PUBLISH_OUTCOME"));
            final RecordedMutationAppend recordedAppend = bridge.lastMutationAppend();
            if (recordedAppend == null
                    || !Arrays.equals(recordedAppend.mutation().encodeFrame(), outcomeMutation.encodeFrame())
                    || recordedAppend.outcome().disposition() != ShardLogMutationAppender.AppendDisposition.PERSISTED) {
                throw new IllegalStateException("Pulsar destination response-loss process crash did not persist the "
                        + "exact PUBLISH_OUTCOME: "
                        + (recordedAppend == null
                                ? "no append observed"
                                : recordedAppend.outcome().disposition()));
            }
            System.out.println("Pulsar Worker destination response-loss outcome append observed: disposition="
                    + recordedAppend.outcome().disposition() + ", sourcePosition="
                    + recordedAppend.outcome().sourcePosition());
            if (chaosStateDumpDirectory() != null) {
                writeChaosStateDump(
                        "pulsar-worker-destination-response-loss-process-crash",
                        "DESTINATION_RESPONSE_LOSS_PERSISTED",
                        store.dbPath(),
                        store,
                        attempt,
                        physicalSchedulePosition,
                        "PUBLISHING",
                        false,
                        null,
                        null,
                        attempt.delayMessageId());
            }
            awaitDestinationResponseLossProcessCrashGate(store.dbPath());
        }

        SourceReplayMutation outcomeRecord = null;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            workClasses.runTurn(new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)));
            final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult turn = runtime.runSourceTurn(
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)), System::currentTimeMillis);
            if (turn.status() == com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                if (turn.entry() instanceof SourceReplayMutation mutation
                        && mutation.mutation().type() == SystemMutationType.PUBLISH_OUTCOME) {
                    outcomeRecord = mutation;
                    break;
                }
                continue;
            }
            if (turn.status() != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && turn.status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                    .WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException(
                        "Pulsar Worker Publish Outcome source turn failed: " + turn.status(), turn.failure());
            }
            TimeUnit.MILLISECONDS.sleep(25);
        }
        if (outcomeRecord == null) {
            throw new IllegalStateException("source-applied PUBLISH_OUTCOME did not become visible before deadline");
        }
        final PublishOutcomeBody outcome =
                PublishOutcomeBody.decode(outcomeRecord.mutation().canonicalBody());
        final var application =
                delayShard.getSystemMutationResult(outcomeRecord.mutation().systemMutationId());
        if (application == null
                || application.applyStatus() != com.nereusstream.delay.runtime.ApplyStatus.APPLIED
                || application.stableCode() != StableCode.OK) {
            throw new IllegalStateException("Pulsar Worker Publish Outcome was not source-applied: "
                    + (application == null ? "missing" : application.applyStatus() + "/" + application.stableCode()));
        }
        if (outcome.sideEffect() != 1
                || outcome.stableCode() != StableCode.OK
                || !Arrays.equals(outcome.publishAttemptId(), publishAttemptId)) {
            throw new IllegalStateException("Pulsar Worker Publish Outcome was not a definitive PUBLISHED result");
        }
        final PublishEvidence evidence = PublishEvidence.decode(outcome.evidence());
        if (evidence.evidenceKind() != PublishEvidenceKind.PULSAR_SEND_ACK
                || evidence.verificationStatus() != EvidenceVerificationStatus.VERIFIED_PUBLISHED) {
            throw new IllegalStateException("Pulsar Worker Publish Outcome carried the wrong evidence branch");
        }
        evidence.requireBusinessMutation(publishAttemptId, true);
        if (!(outcomeRecord.position() instanceof PulsarSourcePosition outcomePosition)) {
            throw new IllegalStateException("source-applied typed Publish Outcome has a non-Pulsar source position");
        }
        final var finalMessage = delayShard.getMessage(attempt.delayMessageId());
        if (finalMessage == null
                || finalMessage.status() != MessageStatus.PUBLISHED
                || delayShard.findOpenPublishAttempt(publishAttemptId) != null) {
            throw new IllegalStateException("source-applied typed Publish Outcome did not close the PUBLISHED attempt");
        }
        requirePayload(client, bridge.destinationPhysicalTopic(), payload, managedHandoff ? 4 : 1);
        if (bridge.destinationResponseLoss()) {
            if (!bridge.destinationResponseEvidenceResolved()) {
                throw new IllegalStateException(
                        "Pulsar Worker destination response-loss provider did not resolve evidence");
            }
            System.out.println("Pulsar Worker destination response-loss smoke passed: real SEND persisted the "
                    + "exact payload, the local response was discarded, and typed PULSAR_SEND_ACK evidence "
                    + "resolved the source-applied PUBLISHED Outcome");
        }
        if (bridge.attemptJournalResponseLoss()) {
            if (bridge.attemptJournalResponseLossRecoveries() != 3) {
                throw new IllegalStateException("Pulsar Worker Attempt Journal response-loss smoke expected three "
                        + "exact readback recoveries but observed "
                        + bridge.attemptJournalResponseLossRecoveries());
            }
            System.out.println("Pulsar Worker Attempt Journal response-loss smoke passed: MAPPED, "
                    + "OWNERSHIP_STARTED, and PUBLISHED were each persisted before the client response was "
                    + "discarded and recovered by exact guarded contiguous readback");
        }
        if (managedHandoff) {
            writeManagedHandoffEvidence(
                    bridge,
                    attempt,
                    physicalDescriptor,
                    physicalResult,
                    evidence,
                    physicalSchedulePosition,
                    admissionPosition,
                    outcomePosition);
        }
        System.out.println("Pulsar Worker source-applied physical publish passed: Admission source ledger="
                + admissionPosition.ledgerId() + "/" + admissionPosition.entryId()
                + ", typed PULSAR_SEND_ACK target ledger/entry=" + branchNumber(evidence, 3) + "/"
                + branchNumber(evidence, 4) + ", Outcome source ledger=" + outcomePosition.ledgerId()
                + "/" + outcomePosition.entryId() + ", exact payload readback");
        return physicalTurn;
    }

    static void bindActiveOwnerPublishGraph(
            final WorkerShardRuntime runtime,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge) {
        bindActiveOwnerPublishGraph(
                runtime, ownedShard, ownerIdentity, authority, store, workClasses, verificationKey, bridge, 1_000_000);
    }

    static void bindActiveOwnerPublishGraph(
            final WorkerShardRuntime runtime,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final long workClassBytes) {
        bindActiveOwnerPublishGraph(
                runtime,
                ownedShard,
                ownerIdentity,
                authority,
                store,
                workClasses,
                verificationKey,
                bridge,
                workClassBytes,
                null);
    }

    static void bindActiveOwnerPublishGraph(
            final WorkerShardRuntime runtime,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final PhysicalPublishBridge bridge,
            final long workClassBytes,
            final ClaimExecutionAdmission sharedClaimAdmission) {
        final WorkerSchedulingRuntime scheduling = WorkerSchedulingRuntime.openForActiveOwnerFromTypedLanes(
                workClasses,
                ownedShard,
                authority,
                store,
                ownerIdentity,
                List.of(bridge.laneId()),
                8,
                bridge.managedHandoffAuthority());
        final ClaimExecutionAdmission permits =
                sharedClaimAdmission == null ? new ClaimExecutionAdmission(1, workClassBytes) : sharedClaimAdmission;
        permits.registerShard(new ClaimExecutionAdmission.ShardSpec(runtime.shardId(), 1, workClassBytes));
        permits.registerLane(new ClaimExecutionAdmission.LaneSpec(
                runtime.shardId(), bridge.laneId(), bridge.laneIncarnation(), 0, 0, 1, workClassBytes));
        permits.openReady(runtime.shardId(), bridge.laneId(), bridge.laneIncarnation());
        final ClaimHandoffWorkClassExecutor claimExecutor = new ClaimHandoffWorkClassExecutor(
                workClasses,
                ownedShard,
                authority,
                scheduling.scheduler(),
                permits,
                ignored -> ClaimHandoffWorkClassExecutor.PrerequisiteDecision.available());
        final PublishAdmissionWorkClassExecutor publishExecutor = new PublishAdmissionWorkClassExecutor(
                workClasses,
                ownedShard,
                authority,
                permits,
                bridge.appender(),
                ignored -> PublishAdmissionWorkClassExecutor.PrerequisiteDecision.available(),
                bridge.artifacts(),
                bridge.managedHandoffAuthority());
        final WorkerCommandRuntime commandRuntime =
                new WorkerCommandRuntime(workClasses, store.sharedResources(), claimExecutor, publishExecutor);
        final WorkerPublishPreparationCoordinator preparation =
                new WorkerPublishPreparationCoordinator(ownedShard, authority, System::currentTimeMillis, request -> {
                    final long expiry = Math.min(
                            request.claim().materialization().expireAtEpochMs(),
                            request.readyCertificate().validUntilEpochMs());
                    final long retryUntil = expiry - 1;
                    final long earliest = Math.max(
                            Math.max(
                                    System.currentTimeMillis(),
                                    request.claim().materialization().actionAtEpochMs()),
                            request.readyCertificate().issuedAt().latestEpochMs());
                    if (retryUntil <= earliest) {
                        return Optional.empty();
                    }
                    long latest = Math.min(retryUntil - 1, Math.addExact(earliest, 500));
                    if (request.claim().materialization().actionAtEpochMs()
                            < request.claim().materialization().deliverAtEpochMs()) {
                        latest = Math.min(
                                latest,
                                Math.subtractExact(
                                        request.claim().materialization().deliverAtEpochMs(), 1));
                    }
                    if (latest < earliest) {
                        return Optional.empty();
                    }
                    return Optional.of(new WorkerCommandRuntime.PublishPreparation(
                            request.channel(),
                            request.readyCertificate(),
                            evidence(earliest, latest, "pulsar-worker-provider-preparation"),
                            retryUntil,
                            1,
                            verificationKey.getPrivate(),
                            System::currentTimeMillis));
                });
        runtime.bindActiveOwnerPublishGraph(scheduling, commandRuntime, preparation);
    }

    static void waitForPhysicalCompletion(final WorkerPhysicalPublishExecutor.Submission submission) throws Exception {
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
            final PulsarDestinationRequest request,
            final byte[] preparedPublishHash,
            final byte[] producerNameHash,
            final AtomicReference<GuardedMessageId> responseLostMessage,
            final AtomicBoolean responseEvidenceResolved) {
        final GuardedMessageId messageId = responseLostMessage.get();
        if (messageId == null) {
            return Optional.empty();
        }
        final TopicResourceGuard expectedGuard = new TopicResourceGuard(
                request.authenticatedClusterId(),
                request.resourceIncarnation(),
                request.physicalTopicCreationTimestamp());
        if (!expectedGuard.equals(messageId.resourceGuard())
                || !request.physicalTopic().equals(messageId.physicalTopic())
                || request.partition() != messageId.partition()
                || !(messageId instanceof MessageIdAdv advanced)
                || advanced.getLedgerId() < 0
                || advanced.getEntryId() < 0
                || advanced.getPartitionIndex() != request.partition()) {
            return Optional.empty();
        }
        final GuardedSendSuccessEvidence evidence = messageId.responseEvidence();
        final TopicResourceGuardAttestation expectedAttestation =
                new TopicResourceGuardAttestation(expectedGuard, request.physicalTopic(), request.partition());
        if (evidence == null
                || !expectedAttestation.equals(evidence.attestation())
                || evidence.ledgerId() != advanced.getLedgerId()
                || evidence.entryId() != advanced.getEntryId()
                || evidence.brokerEntryTimestamp() != messageId.brokerEntryTimestamp()) {
            return Optional.empty();
        }
        final int rawBatchIndex = advanced.getBatchIndex();
        final int rawBatchSize = advanced.getBatchSize();
        final int normalizedBatchIndex = rawBatchIndex < 0 ? 0 : rawBatchIndex;
        if (rawBatchIndex >= 0 && (rawBatchSize <= 0 || Integer.compareUnsigned(rawBatchIndex, rawBatchSize) >= 0)) {
            return Optional.empty();
        }
        final PublishEvidence typed = PulsarSendAckEvidence.published(
                request,
                preparedPublishHash,
                producerNameHash,
                advanced.getLedgerId(),
                advanced.getEntryId(),
                normalizedBatchIndex,
                evidence.brokerEntryTimestamp(),
                evidence.sequenceId(),
                evidence.authenticatedResponseCommandSha256());
        typed.requireBusinessMutation(request.publishAttemptId(), true);
        responseEvidenceResolved.set(true);
        return Optional.of(
                new PulsarClientArtifactDestinationTransport.ResolvedPublish(typed, evidence.brokerEntryTimestamp()));
    }

    private static Optional<PulsarClientArtifactDestinationTransport.ResolvedRecordPublish>
            resolvePreparedDestinationResponseLoss(
                    final PulsarPreparedRecord record,
                    final ArtifactGenerationSet artifacts,
                    final byte[] producerNameHash,
                    final AtomicReference<GuardedMessageId> responseLostMessage,
                    final AtomicBoolean responseEvidenceResolved) {
        final GuardedMessageId messageId = responseLostMessage.get();
        if (messageId == null
                || record.template().targetResource().kind()
                        != com.nereusstream.delay.protocol.BrokerResourceIdentity.Kind.PULSAR) {
            return Optional.empty();
        }
        final PulsarBrokerResourceIdentity target =
                record.template().targetResource().pulsar();
        final int partition = Math.toIntExact(record.template().physicalPartition());
        final TopicResourceGuard expectedGuard = new TopicResourceGuard(
                target.authenticatedClusterId(), target.resourceIncarnation(), target.physicalTopicCreationTimestamp());
        if (!expectedGuard.equals(messageId.resourceGuard())
                || !target.physicalTopic().equals(messageId.physicalTopic())
                || partition != messageId.partition()
                || !(messageId instanceof MessageIdAdv advanced)
                || advanced.getLedgerId() < 0
                || advanced.getEntryId() < 0
                || advanced.getPartitionIndex() != partition) {
            return Optional.empty();
        }
        final GuardedSendSuccessEvidence evidence = messageId.responseEvidence();
        final TopicResourceGuardAttestation expectedAttestation =
                new TopicResourceGuardAttestation(expectedGuard, target.physicalTopic(), partition);
        if (evidence == null
                || !expectedAttestation.equals(evidence.attestation())
                || evidence.ledgerId() != advanced.getLedgerId()
                || evidence.entryId() != advanced.getEntryId()
                || evidence.brokerEntryTimestamp() != messageId.brokerEntryTimestamp()) {
            return Optional.empty();
        }
        final int rawBatchIndex = advanced.getBatchIndex();
        final int rawBatchSize = advanced.getBatchSize();
        final int normalizedBatchIndex = rawBatchIndex < 0 ? 0 : rawBatchIndex;
        final int batchSize = rawBatchIndex < 0 ? 1 : rawBatchSize;
        if (rawBatchIndex >= 0 && (rawBatchSize <= 0 || Integer.compareUnsigned(rawBatchIndex, rawBatchSize) >= 0)) {
            return Optional.empty();
        }
        final PublishEvidence typed = PulsarSendAckEvidence.publishedRecord(
                record,
                artifacts,
                producerNameHash,
                advanced.getLedgerId(),
                advanced.getEntryId(),
                normalizedBatchIndex,
                batchSize,
                evidence.brokerEntryTimestamp(),
                evidence.protocolVersion(),
                evidence.connectionGeneration(),
                evidence.producerId(),
                evidence.sequenceId(),
                evidence.sendCommandSha256(),
                evidence.authenticatedResponseCommandSha256());
        responseEvidenceResolved.set(true);
        return Optional.of(new PulsarClientArtifactDestinationTransport.ResolvedRecordPublish(
                typed,
                advanced.getLedgerId(),
                advanced.getEntryId(),
                normalizedBatchIndex,
                batchSize,
                evidence.brokerEntryTimestamp(),
                evidence.protocolVersion(),
                evidence.connectionGeneration(),
                evidence.producerId(),
                evidence.sequenceId(),
                evidence.sendCommandSha256(),
                evidence.authenticatedResponseCommandSha256()));
    }

    @SuppressWarnings("unchecked")
    private static Producer<byte[]> responseLossProducer(
            final Producer<byte[]> delegate, final AtomicReference<GuardedMessageId> responseLostMessage) {
        return (Producer<byte[]>) Proxy.newProxyInstance(
                PulsarClientArtifactWorkerSmoke.class.getClassLoader(),
                new Class<?>[] {Producer.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("newMessage") && method.getParameterCount() == 0) {
                        final TypedMessageBuilder<byte[]> builder =
                                (TypedMessageBuilder<byte[]>) invoke(delegate, method, arguments);
                        return responseLossBuilder(builder, responseLostMessage);
                    }
                    return invoke(delegate, method, arguments);
                });
    }

    @SuppressWarnings("unchecked")
    private static TypedMessageBuilder<byte[]> responseLossBuilder(
            final TypedMessageBuilder<byte[]> delegate, final AtomicReference<GuardedMessageId> responseLostMessage) {
        return (TypedMessageBuilder<byte[]>) Proxy.newProxyInstance(
                PulsarClientArtifactWorkerSmoke.class.getClassLoader(),
                new Class<?>[] {TypedMessageBuilder.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("value") && method.getParameterCount() == 1) {
                        invoke(delegate, method, arguments);
                        return proxy;
                    }
                    if (method.getName().equals("sendAsync") && method.getParameterCount() == 0) {
                        final CompletableFuture<MessageId> sent =
                                (CompletableFuture<MessageId>) invoke(delegate, method, arguments);
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

    private static boolean hasWorkerAttemptJournalResponseLoss() {
        return "1".equals(System.getenv("NEREUS_DELAY_PULSAR_WORKER_ATTEMPT_JOURNAL_RESPONSE_LOSS"));
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
        private final ShardLogMutationAppender delegate;
        private final AtomicBoolean responseLossObserved;

        private AdmissionResponseLossMutationAppender(
                final ShardLogMutationAppender delegate, final AtomicBoolean responseLossObserved) {
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
            if (delegate instanceof AutoCloseable resource) {
                try {
                    resource.close();
                } catch (RuntimeException failure) {
                    throw failure;
                } catch (Exception failure) {
                    throw new IllegalStateException("Pulsar Worker mutation appender close failed", failure);
                }
            }
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
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey)
            throws Exception {
        return createPhysicalPublishBridge(
                client,
                nativeConsumer,
                sourcePhysicalTopic,
                shard,
                physicalSchedulePosition,
                destinationPhysicalTopic,
                store,
                ownedShard,
                ownerIdentity,
                authority,
                workClasses,
                verificationKey,
                destinationProfile("worker-physical-publish"),
                capabilityProfile(),
                null,
                null,
                1_000_000,
                null,
                0);
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
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final long workClassBytes)
            throws Exception {
        return createPhysicalPublishBridge(
                client,
                nativeConsumer,
                sourcePhysicalTopic,
                shard,
                physicalSchedulePosition,
                destinationPhysicalTopic,
                store,
                ownedShard,
                ownerIdentity,
                authority,
                workClasses,
                verificationKey,
                destinationProfile,
                capabilityProfile,
                null,
                null,
                workClassBytes,
                null,
                0);
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
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final DestinationLaneId requestedLaneId,
            final byte[] requestedLaneIncarnation,
            final long workClassBytes)
            throws Exception {
        return createPhysicalPublishBridge(
                client,
                nativeConsumer,
                sourcePhysicalTopic,
                shard,
                physicalSchedulePosition,
                destinationPhysicalTopic,
                store,
                ownedShard,
                ownerIdentity,
                authority,
                workClasses,
                verificationKey,
                destinationProfile,
                capabilityProfile,
                requestedLaneId,
                requestedLaneIncarnation,
                workClassBytes,
                null,
                0);
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
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final DestinationLaneId requestedLaneId,
            final byte[] requestedLaneIncarnation,
            final long workClassBytes,
            final DestinationPhysicalAdmission sharedPhysicalAdmission)
            throws Exception {
        return createPhysicalPublishBridge(
                client,
                nativeConsumer,
                sourcePhysicalTopic,
                shard,
                physicalSchedulePosition,
                destinationPhysicalTopic,
                store,
                ownedShard,
                ownerIdentity,
                authority,
                workClasses,
                verificationKey,
                destinationProfile,
                capabilityProfile,
                requestedLaneId,
                requestedLaneIncarnation,
                workClassBytes,
                sharedPhysicalAdmission,
                0);
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
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final DestinationLaneId requestedLaneId,
            final byte[] requestedLaneIncarnation,
            final long workClassBytes,
            final DestinationPhysicalAdmission sharedPhysicalAdmission,
            final int destinationPartition)
            throws Exception {
        return createPhysicalPublishBridge(
                client,
                nativeConsumer,
                sourcePhysicalTopic,
                shard,
                physicalSchedulePosition,
                destinationPhysicalTopic,
                store,
                ownedShard,
                ownerIdentity,
                authority,
                workClasses,
                verificationKey,
                destinationProfile,
                capabilityProfile,
                requestedLaneId,
                requestedLaneIncarnation,
                workClassBytes,
                sharedPhysicalAdmission,
                destinationPartition,
                null);
    }

    /**
     * Creates a Pulsar destination bridge while allowing the source Shard Log
     * append authority to be supplied by another guarded adapter. The target
     * SEND remains Pulsar-native; only the common Delay Shard mutation append
     * is externalized for cross-adapter Worker graphs.
     */
    static PhysicalPublishBridge createPhysicalPublishBridge(
            final PulsarClient client,
            final GuardedConsumer<?> nativeConsumer,
            final String sourcePhysicalTopic,
            final ShardId shard,
            final com.nereusstream.delay.protocol.SourcePosition physicalSchedulePosition,
            final String destinationPhysicalTopic,
            final ShardStore store,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final DestinationLaneId requestedLaneId,
            final byte[] requestedLaneIncarnation,
            final long workClassBytes,
            final DestinationPhysicalAdmission sharedPhysicalAdmission,
            final int destinationPartition,
            final ShardLogMutationAppender suppliedAppender)
            throws Exception {
        return createPhysicalPublishBridge(
                client,
                nativeConsumer,
                sourcePhysicalTopic,
                shard,
                physicalSchedulePosition,
                destinationPhysicalTopic,
                store,
                ownedShard,
                ownerIdentity,
                authority,
                workClasses,
                verificationKey,
                destinationProfile,
                capabilityProfile,
                requestedLaneId,
                requestedLaneIncarnation,
                workClassBytes,
                sharedPhysicalAdmission,
                destinationPartition,
                suppliedAppender,
                null,
                null);
    }

    private static PhysicalPublishBridge createPhysicalPublishBridge(
            final PulsarClient client,
            final GuardedConsumer<?> nativeConsumer,
            final String sourcePhysicalTopic,
            final ShardId shard,
            final com.nereusstream.delay.protocol.SourcePosition physicalSchedulePosition,
            final String destinationPhysicalTopic,
            final ShardStore store,
            final OwnedDelayShard ownedShard,
            final OwnerIdentity ownerIdentity,
            final OxiaOwnerLeaseStore authority,
            final WorkClassExecutionRegistry workClasses,
            final KeyPair verificationKey,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final DestinationLaneId requestedLaneId,
            final byte[] requestedLaneIncarnation,
            final long workClassBytes,
            final DestinationPhysicalAdmission sharedPhysicalAdmission,
            final int destinationPartition,
            final ShardLogMutationAppender suppliedAppender,
            final PersistentStagingActivation.Loaded persistentActivation,
            final ManagedHandoffConfiguration managedHandoff)
            throws Exception {
        if (workClassBytes <= 0) {
            throw new IllegalArgumentException("Pulsar physical publish work-class bytes must be positive");
        }
        if (destinationPartition < 0) {
            throw new IllegalArgumentException("Pulsar physical publish destination partition must be non-negative");
        }
        if ((requestedLaneId == null) != (requestedLaneIncarnation == null)) {
            throw new IllegalArgumentException("Pulsar physical publish lane identity must be supplied as a pair");
        }
        if ((persistentActivation == null) != (managedHandoff == null)) {
            throw new IllegalArgumentException("Managed Handoff activation and composition must be supplied together");
        }
        final ArtifactGenerationSet artifacts =
                persistentActivation == null ? ARTIFACTS : persistentActivation.artifacts();
        final byte[] tenantScope = managedHandoff == null
                ? bytes(32, 61)
                : managedHandoff.identity().tenantRouteScopeDigest();
        final byte[] laneTuple = canonicalLaneTuple(
                tenantScope, destinationPhysicalTopic, destinationProfile, capabilityProfile, destinationPartition);
        final DestinationLaneId laneId =
                requestedLaneId == null ? DestinationLaneId.derive(laneTuple) : requestedLaneId;
        final byte[] laneIncarnation = requestedLaneIncarnation == null
                ? LaneRecord.initial(laneId, physicalSchedulePosition).laneIncarnation()
                : Bytes.copy(requestedLaneIncarnation);
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        final boolean reusePersistedLaneReadiness =
                requestedLaneId != null && hasAdmissionResponseLossProcessCrash() && !hasWorkerAdmissionResponseLoss();
        final ActiveLaneState persistedLane =
                reusePersistedLaneReadiness ? ownedShard.getActiveLaneState(laneId) : null;
        if (reusePersistedLaneReadiness
                && (persistedLane == null
                        || !Arrays.equals(persistedLane.laneIncarnation(), laneIncarnation)
                        || persistedLane.runtimeReadiness() != com.nereusstream.delay.runtime.RuntimeReadiness.READY
                        || !persistedLane.destinationProfile().equals(destinationProfile)
                        || !persistedLane.capabilityProfile().equals(capabilityProfile)
                        || persistedLane.readyCertificate() == null)) {
            throw new IllegalStateException(
                    "Pulsar admission response-loss recovery did not find the exact durable READY Lane proof");
        }
        final com.nereusstream.delay.protocol.BrokerResourceIdentity target =
                destinationResource(destinationPhysicalTopic);
        if (managedHandoff != null) {
            if (!managedHandoff.identity().destination().ref().equals(destinationProfile)
                    || !managedHandoff.identity().capability().ref().equals(capabilityProfile)
                    || !com.nereusstream.delay.protocol.BrokerResourceIdentity.pulsar(
                                    managedHandoff.identity().target())
                            .equals(target)
                    || !Arrays.equals(
                            managedHandoff.identity().policyScopeDigest(), persistentActivation.policyScopeDigest())) {
                throw new IllegalArgumentException("Managed Handoff bridge differs from its activated exact scope");
            }
            managedHandoff.activateAt(physicalSchedulePosition);
        }
        final String producerName = destinationProducerName(laneId, laneIncarnation, target, destinationPartition);
        final byte[] producerIdentity = Bytes.utf8(producerName);
        final ChannelResourceIdentity channel;
        final ReadyCertificate readyCertificate;
        final List<EvidenceCursor> evidenceCursors;
        if (persistedLane == null) {
            final TopicResourceGuard destinationGuard =
                    new TopicResourceGuard(CLUSTER, DESTINATION_INCARNATION, DESTINATION_CREATION_TIMESTAMP);
            final byte[] attestationDigest =
                    PulsarClientArtifactSourceRecordConsumer.attestationDigest(new TopicResourceGuardAttestation(
                            destinationGuard, destinationPhysicalTopic, destinationPartition));
            final long now = Math.max(1, System.currentTimeMillis());
            final TrustedUtcIntervalEvidence issuedAt =
                    evidence(Math.max(0, now - 1), now, "pulsar-worker-channel-issued");
            channel = channel(
                    laneId,
                    laneIncarnation,
                    target,
                    destinationPhysicalTopic,
                    attestationDigest,
                    issuedAt,
                    destinationProfile,
                    destinationPartition,
                    producerIdentity);
            final long validUntil = Math.addExact(now, 60_000);
            readyCertificate = readyCertificate(
                    ownerIdentity,
                    store.metadata().storeIncarnation(),
                    laneId,
                    laneIncarnation,
                    channel,
                    target,
                    destinationPhysicalTopic,
                    attestationDigest,
                    issuedAt,
                    validUntil,
                    destinationProfile,
                    destinationPartition);
            evidenceCursors = List.of(EvidenceCursor.pulsar(
                    laneId.bytes(),
                    laneIncarnation,
                    DESTINATION_INCARNATION,
                    destinationPartition,
                    1,
                    0,
                    destinationPhysicalTopic,
                    DESTINATION_CREATION_TIMESTAMP,
                    0,
                    0,
                    0,
                    1));
        } else {
            readyCertificate = ReadyCertificate.decode(persistedLane.readyCertificate());
            channel = ChannelResourceIdentity.decode(readyCertificate.channel());
            if (!Arrays.equals(channel.destinationLaneId(), laneId.bytes())
                    || !Arrays.equals(channel.laneIncarnation(), laneIncarnation)
                    || !channel.targetResource().equals(target)
                    || channel.physicalPartition() != destinationPartition
                    || !Arrays.equals(channel.producerOrTransactionalIdentity(), producerIdentity)
                    || !Arrays.equals(
                            readyCertificate.storeIncarnation(),
                            store.metadata().storeIncarnation())) {
                throw new IllegalStateException(
                        "Pulsar admission response-loss recovery changed the durable Lane proof identity");
            }
            evidenceCursors = readyCertificate.evidenceCursors();
        }
        final DestinationPhysicalAdmission physicalAdmission = sharedPhysicalAdmission == null
                ? new DestinationPhysicalAdmission(1, workClassBytes)
                : sharedPhysicalAdmission;
        if (sharedPhysicalAdmission == null) {
            physicalAdmission.registerTargetCluster(CLUSTER, 1, workClassBytes);
        }
        physicalAdmission.registerLane(new DestinationPhysicalAdmission.LaneSpec(
                laneId, laneIncarnation, CLUSTER, 1, 1, 1, workClassBytes, 1, workClassBytes));
        physicalAdmission.openReady(laneId);
        final byte[] producerNameHash = Bytes.sha256(Bytes.utf8(producerName));
        final boolean destinationResponseLoss = hasWorkerDestinationResponseLoss();
        final AtomicReference<GuardedMessageId> responseLostMessage = new AtomicReference<>();
        final AtomicBoolean responseEvidenceResolved = new AtomicBoolean();
        final Producer<byte[]> rawProducer = PulsarClientArtifactProducerFactory.create(
                client,
                CLUSTER,
                DESTINATION_INCARNATION,
                destinationPhysicalTopic,
                DESTINATION_CREATION_TIMESTAMP,
                producerName);
        final Producer<byte[]> producer =
                destinationResponseLoss ? responseLossProducer(rawProducer, responseLostMessage) : rawProducer;
        final PulsarClientArtifactDestinationTransport.PublishEvidenceProvider evidenceProvider;
        if (destinationResponseLoss) {
            evidenceProvider = new PulsarClientArtifactDestinationTransport.PublishEvidenceProvider() {
                @Override
                public Optional<PulsarClientArtifactDestinationTransport.ResolvedPublish> resolve(
                        final PulsarDestinationRequest request, final byte[] preparedHash, final Throwable failure) {
                    return resolveDestinationResponseLoss(
                            request, preparedHash, producerNameHash, responseLostMessage, responseEvidenceResolved);
                }

                @Override
                public Optional<PulsarClientArtifactDestinationTransport.ResolvedRecordPublish> resolve(
                        final PulsarPreparedRecord record,
                        final ArtifactGenerationSet artifacts,
                        final Throwable failure) {
                    return resolvePreparedDestinationResponseLoss(
                            record, artifacts, producerNameHash, responseLostMessage, responseEvidenceResolved);
                }
            };
        } else {
            evidenceProvider = null;
        }
        final com.nereusstream.delay.transport.PulsarClientArtifactDestinationTransport transport =
                new PulsarClientArtifactDestinationTransport(
                        producer,
                        CLUSTER,
                        DESTINATION_INCARNATION,
                        destinationPhysicalTopic,
                        DESTINATION_CREATION_TIMESTAMP,
                        destinationPartition,
                        producerNameHash,
                        evidenceProvider);
        final PulsarTargetResource destinationTarget = new PulsarTargetResource(
                CLUSTER,
                DESTINATION_INCARNATION,
                destinationPhysicalTopic,
                DESTINATION_CREATION_TIMESTAMP,
                destinationPartition);
        final PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(destinationTarget, transport);
        final ShardLogMutationAppender realAppender;
        if (suppliedAppender == null) {
            final String mutationProducerName = "pulsar-worker-mutation-" + UUID.randomUUID();
            realAppender = new PulsarClientArtifactShardLogMutationAppender(
                    PulsarClientArtifactProducerFactory.create(
                            client,
                            CLUSTER,
                            INCARNATION,
                            sourcePhysicalTopic,
                            CREATION_TIMESTAMP,
                            mutationProducerName),
                    nativeConsumer,
                    shard,
                    CLUSTER,
                    INCARNATION,
                    sourcePhysicalTopic,
                    CREATION_TIMESTAMP,
                    Duration.ofSeconds(20));
        } else {
            realAppender = suppliedAppender;
        }
        final boolean admissionResponseLoss = hasWorkerAdmissionResponseLoss();
        final AtomicBoolean admissionResponseLossObserved = new AtomicBoolean();
        final ShardLogMutationAppender selectedAppender;
        if (admissionResponseLoss) {
            final AdmissionResponseLossMutationAppender wrapper =
                    new AdmissionResponseLossMutationAppender(realAppender, admissionResponseLossObserved);
            selectedAppender = wrapper;
        } else {
            selectedAppender = realAppender;
        }
        final RecordingMutationAppender appender = new RecordingMutationAppender(selectedAppender);
        final AuthorIdentity author = AuthorIdentity.owner(
                ownerIdentity.deploymentId(),
                ownerIdentity.workerRunId(),
                ownerIdentity.ownerEpoch(),
                ownerIdentity.leaseFencingDigest());
        final WorkerPublishOutcomeMutationFactory outcomeFactory = new WorkerPublishOutcomeMutationFactory(
                (attempt, request, result) -> {
                    final PublishAdmissionBody admission = PublishAdmissionBody.decode(attempt.admissionBytes());
                    final long retryDeadline =
                            attempt.hasRetryWindow() ? attempt.retryDeadlineEpochMs() : request.deliverAtEpochMs();
                    final boolean unknownHold = result.disposition() == DestinationPublishResult.Disposition.UNKNOWN;
                    final long firstAttemptAt = attempt.hasRetryWindow()
                            ? attempt.firstAttemptAtEpochMs()
                            : admission.decisionTime().latestEpochMs();
                    final long observedAt = result.brokerPersistenceTimeEpochMs() >= 0
                            ? result.brokerPersistenceTimeEpochMs()
                            : System.currentTimeMillis();
                    final TrustedUtcIntervalEvidence outcomeObservation =
                            evidence(observedAt, observedAt, "pulsar-worker-publish-observed");
                    final long mutationRetryUntil =
                            Math.addExact(outcomeObservation.latestEpochMs(), SYSTEM_MUTATION_RETRY_WINDOW_MS);
                    return new WorkerPublishOutcomeMutationFactory.OutcomeContext(
                            mutationRetryUntil,
                            unknownHold ? 4 : 0,
                            admission.chargeVector().canonicalBytes(),
                            outcomeObservation,
                            unknownHold
                                    ? recoveryHoldRetryDecision(
                                            firstAttemptAt, retryDeadline, attempt.attemptNo(), result.stableCode())
                                    : retryDecision(firstAttemptAt, retryDeadline, attempt.attemptNo()));
                },
                author.canonicalBytes(),
                1,
                verificationKey.getPrivate());
        final OutcomeWorkClassExecutor outcomes =
                new OutcomeWorkClassExecutor(workClasses, ownedShard, authority, appender);
        final boolean admissionCrashCut = hasAdmissionResponseLossProcessCrash() && admissionResponseLoss;
        final AtomicInteger physicalGateChecks = new AtomicInteger();
        final ExecutorService physicalExecutor =
                Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        final PhysicalSendActivationGate persistentPhysicalGate =
                persistentActivation == null ? null : persistentActivation.physicalGate();
        final WorkerPhysicalPublishExecutor executor = new WorkerPhysicalPublishExecutor(
                adapter,
                physicalAdmission,
                workClasses,
                physicalExecutor,
                outcomes,
                (attempt, request, ownerClock) -> {
                    ownedShard.requirePhysicalPublishAuthoritativelyStrict(authority, attempt, ownerClock);
                    if (admissionCrashCut && physicalGateChecks.incrementAndGet() >= 2) {
                        return WorkerPhysicalPublishExecutor.Decision.deferred(StableCode.CAPABILITY_UNAVAILABLE, null);
                    }
                    return WorkerPhysicalPublishExecutor.Decision.allowed();
                },
                outcomeFactory,
                ownedShard::fence,
                persistentPhysicalGate);
        final String journalPhysicalTopic = destinationPhysicalTopic + "-attempt-journal";
        final PulsarJournalResource journalResource = new PulsarJournalResource(
                CLUSTER, JOURNAL_INCARNATION, journalPhysicalTopic, JOURNAL_CREATION_TIMESTAMP, shard.partition());
        final String journalProducerName = attemptJournalProducerName(shard, journalResource);
        final String replaySubscriptionName = attemptJournalReplaySubscription(ownerIdentity, journalResource);
        final boolean attemptJournalResponseLoss = hasWorkerAttemptJournalResponseLoss();
        final Producer<byte[]> rawJournalProducer = PulsarClientArtifactProducerFactory.create(
                client,
                journalResource.authenticatedClusterId(),
                journalResource.resourceIncarnation(),
                journalResource.physicalTopic(),
                journalResource.physicalTopicCreationTimestamp(),
                journalProducerName);
        final Producer<byte[]> journalProducerTransport = attemptJournalResponseLoss
                ? responseLossProducer(rawJournalProducer, new AtomicReference<>())
                : rawJournalProducer;
        final PulsarClientArtifactAttemptJournal journalTransport =
                PulsarClientArtifactAttemptJournal.openWithProducerForTesting(
                        client,
                        shard,
                        journalResource,
                        journalProducerTransport,
                        replaySubscriptionName,
                        Duration.ofSeconds(20));
        final PulsarAttemptJournal.ProducerKey journalProducer =
                new PulsarAttemptJournal.ProducerKey(laneId, laneIncarnation, producerNameHash, destinationTarget);
        executor.bindManagedPulsarContext(new WorkerPhysicalPublishExecutor.ManagedPulsarContext(
                journalTransport.journal(),
                journalProducer,
                artifacts,
                candidate -> {
                    if (!artifacts.equals(candidate)) {
                        throw new IllegalStateException("physical artifact generation differs from the admission");
                    }
                },
                new WorkerPhysicalPublishExecutor.JournalProjectionSink() {
                    @Override
                    public void recordMapped(
                            final PublishAttemptLedger attempt, final long sequenceId, final byte[] journalPosition) {
                        ownedShard.recordAttemptJournalMappingAuthoritativelyStrict(
                                authority, attempt, sequenceId, journalPosition, System::currentTimeMillis);
                    }

                    @Override
                    public void markRetirementPending(final PublishAttemptLedger attempt) {
                        ownedShard.markAttemptJournalRetirementPendingAuthoritativelyStrict(
                                authority, attempt, System::currentTimeMillis);
                    }

                    @Override
                    public void recordRetired(final PublishAttemptLedger attempt, final byte[] journalPosition) {
                        ownedShard.recordAttemptJournalRetirementAuthoritativelyStrict(
                                authority, attempt, journalPosition, System::currentTimeMillis);
                    }
                },
                managedHandoff == null
                        ? null
                        : (admission, candidateArtifacts, trustedTime, admissionPosition) -> {
                            final var descriptor = admission.descriptor().value();
                            final ScheduleBinding binding = ownedShard.getScheduleBinding(descriptor.messageId());
                            if (binding == null) {
                                throw new IllegalStateException(
                                        "physical Managed Handoff has no source-bound Schedule binding");
                            }
                            final var publication = persistentActivation.policyPublication();
                            managedHandoff
                                    .authority()
                                    .requireFrozen(
                                            descriptor,
                                            descriptor.materialization(
                                                    publication.head().ref(publication.oxiaVersion())),
                                            binding,
                                            trustedTime,
                                            admissionPosition);
                        },
                managedHandoff == null
                        ? null
                        : () -> {
                            final long now = System.currentTimeMillis();
                            return evidence(now, now, "pulsar-worker-managed-handoff-physical-time");
                        },
                ownerIdentity.ownerEpoch()));
        return new PhysicalPublishBridge(
                executor,
                appender,
                appender,
                journalTransport,
                physicalExecutor,
                artifacts,
                managedHandoff == null ? null : managedHandoff.authority(),
                managedHandoff == null
                        ? null
                        : persistentActivation.policyPublication().head().snapshot(),
                laneId,
                laneIncarnation,
                destinationProfile,
                capabilityProfile,
                target,
                channel,
                readyCertificate,
                evidenceCursors,
                destinationPhysicalTopic,
                destinationResponseLoss,
                responseEvidenceResolved,
                attemptJournalResponseLoss,
                admissionResponseLoss,
                admissionResponseLossObserved);
    }

    private static String attemptJournalProducerName(final ShardId shard, final PulsarJournalResource journalResource) {
        final byte[] identity = Bytes.sha256(
                Bytes.utf8("nereus-delay-attempt-journal-producer\0"),
                Bytes.utf8(shard.routeIncarnation().uuid().toString()),
                Bytes.u32be(shard.partition()),
                journalResource.exactResourceCanonicalBytes(1));
        return "nereus-delay-journal-" + Bytes.hex(Arrays.copyOf(identity, 16));
    }

    private static String attemptJournalReplaySubscription(
            final OwnerIdentity ownerIdentity, final PulsarJournalResource journalResource) {
        final byte[] identity = Bytes.sha256(
                Bytes.utf8("nereus-delay-attempt-journal-replay\0"),
                ownerIdentity.canonicalBytes(),
                journalResource.exactResourceCanonicalBytes(1));
        return "nereus-delay-journal-replay-" + Bytes.hex(Arrays.copyOf(identity, 16));
    }

    private static ChannelResourceIdentity channel(
            final DestinationLaneId laneId,
            final byte[] laneIncarnation,
            final com.nereusstream.delay.protocol.BrokerResourceIdentity target,
            final String physicalTopic,
            final byte[] attestationDigest,
            final TrustedUtcIntervalEvidence issuedAt,
            final ProfileRef destinationProfile,
            final int destinationPartition,
            final byte[] producer) {
        final byte[] binding = Bytes.sha256(Bytes.utf8("pulsar-worker-channel-binding"), target.canonicalBytes());
        final byte[] fingerprint =
                Bytes.sha256(Bytes.utf8("pulsar-worker-channel-fingerprint"), Bytes.utf8(physicalTopic));
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, AdapterKind.PULSAR.wireValue());
            CanonicalProtobuf.uint32(output, 2, ChannelKind.PULSAR_DEDUP_PRODUCER.wireValue());
            CanonicalProtobuf.bytes(output, 3, laneId.bytes());
            CanonicalProtobuf.bytes(output, 4, laneIncarnation);
            CanonicalProtobuf.bytes(output, 5, target.canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, destinationPartition);
            CanonicalProtobuf.uint64(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, 0);
            CanonicalProtobuf.bytes(output, 9, producer);
            CanonicalProtobuf.bytes(output, 10, Bytes.sha256(producer));
            CanonicalProtobuf.bytes(output, 11, target.canonicalBytes());
            CanonicalProtobuf.uint64(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, attestationDigest);
        });
        final CredentialUseLease lease = new CredentialUseLease(
                destinationProfile,
                CredentialUseKind.DESTINATION_CHANNEL,
                CredentialUseLease.destinationChannelHolderScope(prefix),
                1,
                binding,
                fingerprint,
                issuedAt,
                Math.addExact(issuedAt.latestEpochMs(), 60_000),
                1);
        return new ChannelResourceIdentity(
                AdapterKind.PULSAR,
                ChannelKind.PULSAR_DEDUP_PRODUCER,
                laneId.bytes(),
                laneIncarnation,
                target,
                destinationPartition,
                1,
                0,
                producer,
                Bytes.sha256(producer),
                target,
                1L,
                attestationDigest,
                1,
                binding,
                fingerprint,
                lease);
    }

    private static String destinationProducerName(
            final DestinationLaneId laneId,
            final byte[] laneIncarnation,
            final com.nereusstream.delay.protocol.BrokerResourceIdentity target,
            final int destinationPartition) {
        final byte[] identity = Bytes.sha256(
                Bytes.utf8("nereus-delay-managed-pulsar-producer\0"),
                laneId.bytes(),
                laneIncarnation,
                target.canonicalBytes(),
                Bytes.u32be(destinationPartition));
        return "nereus-delay-managed-" + Bytes.hex(Arrays.copyOf(identity, 16));
    }

    private static ReadyCertificate readyCertificate(
            final com.nereusstream.delay.protocol.OwnerIdentity owner,
            final byte[] storeIncarnation,
            final DestinationLaneId laneId,
            final byte[] laneIncarnation,
            final ChannelResourceIdentity channel,
            final com.nereusstream.delay.protocol.BrokerResourceIdentity target,
            final String physicalTopic,
            final byte[] attestationDigest,
            final TrustedUtcIntervalEvidence issuedAt,
            final long validUntil,
            final ProfileRef destinationProfile,
            final int destinationPartition) {
        final ActivationBarrier barrier =
                ActivationBarrier.pulsar(target, destinationPartition, 0, 0, 0, 1, 1, attestationDigest);
        final EvidenceCursor cursor = EvidenceCursor.pulsar(
                laneId.bytes(),
                laneIncarnation,
                DESTINATION_INCARNATION,
                destinationPartition,
                1,
                0,
                physicalTopic,
                DESTINATION_CREATION_TIMESTAMP,
                0,
                0,
                0,
                1);
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
            CanonicalProtobuf.bytes(output, 16, Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate\0"), prefix));
        });
        return ReadyCertificate.decode(encoded);
    }

    static byte[] canonicalLaneTuple(
            final String physicalTopic, final ProfileRef destination, final ProfileRef capability) {
        return canonicalLaneTuple(physicalTopic, destination, capability, 0);
    }

    static byte[] canonicalLaneTuple(
            final String physicalTopic,
            final ProfileRef destination,
            final ProfileRef capability,
            final int destinationPartition) {
        return canonicalLaneTuple(bytes(32, 61), physicalTopic, destination, capability, destinationPartition);
    }

    private static byte[] canonicalLaneTuple(
            final byte[] tenantRouteScopeDigest,
            final String physicalTopic,
            final ProfileRef destination,
            final ProfileRef capability,
            final int destinationPartition) {
        if (destinationPartition < 0) {
            throw new IllegalArgumentException("Pulsar destination partition must be non-negative");
        }
        Bytes.requireLength(tenantRouteScopeDigest, 32, "tenantRouteScopeDigest");
        return Bytes.concat(
                tenantRouteScopeDigest,
                Bytes.u8(AdapterKind.PULSAR.wireValue()),
                Bytes.lp32(Bytes.utf8(CLUSTER)),
                Bytes.u8(2),
                DESTINATION_INCARNATION,
                Bytes.u64be(DESTINATION_CREATION_TIMESTAMP),
                Bytes.lp32(Bytes.utf8(physicalTopic)),
                Bytes.u32be(destinationPartition),
                Bytes.lp32(destination.profileId()),
                Bytes.u64be(destination.version()),
                destination.semanticHash(),
                Bytes.lp32(capability.profileId()),
                Bytes.u64be(capability.version()),
                capability.semanticHash(),
                Bytes.u8(2),
                Bytes.u32be(0));
    }

    private static com.nereusstream.delay.protocol.BrokerResourceIdentity destinationResource(
            final String physicalTopic) {
        return com.nereusstream.delay.protocol.BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                CLUSTER, DESTINATION_INCARNATION, physicalTopic, DESTINATION_CREATION_TIMESTAMP));
    }

    private static ProfileRef destinationProfile(final String identity) {
        return new ProfileRef(
                Bytes.utf8("destination-" + identity),
                1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)),
                ProfileKind.DESTINATION);
    }

    static ProfileRef capabilityProfile() {
        return new ProfileRef(
                Bytes.utf8("pulsar-worker-capability"),
                1,
                Bytes.sha256(Bytes.utf8("pulsar-worker-capability-semantic")),
                ProfileKind.DELIVERY_CAPABILITY);
    }

    private static byte[] claimCharge(final long payloadBytes) {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 1, payloadBytes, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                .canonicalBytes();
    }

    private static byte[] retryDecision(
            final long firstAttemptAt, final long retryDeadline, final int completedAttemptNo) {
        final RetryPolicyRef policy = new RetryPolicyRef(
                Bytes.utf8("pulsar-worker-retry"), 1, Bytes.sha256(Bytes.utf8("pulsar-worker-retry-semantic")));
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

    private static byte[] recoveryHoldRetryDecision(
            final long firstAttemptAt, final long retryDeadline, final int completedAttemptNo, final StableCode cause) {
        final RetryPolicyRef policy = new RetryPolicyRef(
                Bytes.utf8("pulsar-worker-retry"), 1, Bytes.sha256(Bytes.utf8("pulsar-worker-retry-semantic")));
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 5);
            CanonicalProtobuf.bytes(output, 2, policy.canonicalBytes());
            CanonicalProtobuf.uint32(output, 3, completedAttemptNo);
            CanonicalProtobuf.int64(output, 4, firstAttemptAt);
            CanonicalProtobuf.int64(output, 5, retryDeadline);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, cause.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest, final long latest, final String identity) {
        return new TrustedUtcIntervalEvidence(
                earliest,
                latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8(identity),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8(identity + "-proof")),
                0,
                null);
    }

    private static void waitUntil(final long epochMs) throws Exception {
        while (System.currentTimeMillis() < epochMs) {
            TimeUnit.MILLISECONDS.sleep(Math.min(50, Math.max(1, epochMs - System.currentTimeMillis())));
        }
    }

    private static DelayMessageId messageId(final PreparedCommand command) {
        return command.delayMessageId();
    }

    private static long branchNumber(final PublishEvidence evidence, final int number) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(evidence.branch());
        while (reader.hasRemaining()) {
            final CanonicalProtobuf.Reader.Field field = reader.next();
            if (field.number() == number) {
                return field.unsignedValue();
            }
        }
        throw new IllegalStateException("Pulsar SEND ACK branch is missing field " + number);
    }

    private static byte[] branchBytes(final PublishEvidence evidence, final int number) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(evidence.branch());
        while (reader.hasRemaining()) {
            final CanonicalProtobuf.Reader.Field field = reader.next();
            if (field.number() == number && field.wireType() == 2) {
                return field.rawValue();
            }
        }
        throw new IllegalStateException("Pulsar SEND ACK branch is missing bytes field " + number);
    }

    private static void writeManagedHandoffEvidence(
            final PhysicalPublishBridge bridge,
            final PublishAttemptLedger attempt,
            final PreparedPublishDescriptor descriptor,
            final DestinationPublishResult result,
            final PublishEvidence evidence,
            final PulsarSourcePosition schedulePosition,
            final PulsarSourcePosition admissionPosition,
            final PulsarSourcePosition outcomePosition)
            throws Exception {
        final String configured = System.getenv("NEREUS_DELAY_PERSISTENT_STAGING_MANAGED_EVIDENCE");
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException("persistent Managed Handoff evidence path is not configured");
        }
        final List<PulsarAttemptJournal.JournalRecord> records = bridge.journalRecords();
        final List<PulsarAttemptJournal.RecordKind> expectedKinds = List.of(
                PulsarAttemptJournal.RecordKind.MAPPED,
                PulsarAttemptJournal.RecordKind.OWNERSHIP_STARTED,
                PulsarAttemptJournal.RecordKind.PUBLISHED);
        if (records.size() != expectedKinds.size()
                || !records.stream()
                        .map(PulsarAttemptJournal.JournalRecord::kind)
                        .toList()
                        .equals(expectedKinds)) {
            throw new IllegalStateException("Managed Handoff Attempt Journal is not mapping-before-send complete");
        }
        final long sequenceId = records.getFirst().mapping().sequenceId();
        final byte[] mappingId = records.getFirst().mapping().mappingId();
        if (sequenceId != branchNumber(evidence, 13)
                || records.stream()
                        .anyMatch(record ->
                                !Arrays.equals(mappingId, record.mapping().mappingId()))) {
            throw new IllegalStateException("Managed Handoff SEND sequence differs from its durable Journal mapping");
        }
        final JsonArray journal = new JsonArray();
        for (PulsarAttemptJournal.JournalRecord record : records) {
            final JsonObject row = new JsonObject();
            row.addProperty("kind", record.kind().name());
            row.addProperty("sequenceId", record.mapping().sequenceId());
            row.addProperty(
                    "positionSha256", Bytes.hex(Bytes.sha256(record.position().canonicalBytes())));
            journal.add(row);
        }
        final JsonObject value = new JsonObject();
        value.addProperty("schema", "nereus-delay.managed-handoff-canary-evidence");
        value.addProperty("schemaGeneration", 2);
        value.addProperty("verdict", "PASS");
        value.addProperty("productionPath", true);
        value.addProperty("productionAuthority", false);
        value.addProperty("nativeAdmission", 1);
        value.addProperty("nativeSend", 1);
        value.addProperty("handedOff", 1);
        value.addProperty("deliveryContract", descriptor.deliveryContract().name());
        value.addProperty("actionAtEpochMs", descriptor.actionAtEpochMs());
        value.addProperty("deliverAtEpochMs", descriptor.deliverAtEpochMs());
        value.addProperty("brokerPersistenceTimeEpochMs", result.brokerPersistenceTimeEpochMs());
        value.addProperty(
                "policyGeneration",
                Long.toUnsignedString(descriptor.handoffPolicySnapshot().generation()));
        value.addProperty(
                "policyScopeDigest",
                Bytes.hex(descriptor.handoffPolicySnapshot().policyScopeDigest()));
        value.addProperty(
                "policySnapshotDigest",
                Bytes.hex(descriptor.handoffPolicySnapshot().snapshotDigest()));
        value.addProperty("publishAttemptId", Bytes.hex(attempt.publishAttemptId()));
        value.addProperty("preparedPublishHash", Bytes.hex(descriptor.preparedPublishHash()));
        value.addProperty("recordTemplateHash", Bytes.hex(descriptor.recordTemplateHash()));
        value.addProperty("preparedRecordHash", Bytes.hex(branchBytes(evidence, 17)));
        value.addProperty("sequenceId", sequenceId);
        value.addProperty("sendCommandSha256", Bytes.hex(branchBytes(evidence, 19)));
        value.addProperty("authenticatedResponseCommandSha256", Bytes.hex(branchBytes(evidence, 20)));
        value.addProperty("p1SourceLock", Bytes.hex(branchBytes(evidence, 21)));
        value.addProperty("artifactSetDigest", Bytes.hex(branchBytes(evidence, 22)));
        value.addProperty("schedulePositionSha256", Bytes.hex(Bytes.sha256(schedulePosition.canonicalBytes())));
        value.addProperty("admissionPositionSha256", Bytes.hex(Bytes.sha256(admissionPosition.canonicalBytes())));
        value.addProperty("outcomePositionSha256", Bytes.hex(Bytes.sha256(outcomePosition.canonicalBytes())));
        value.addProperty("destinationResponseLossResolved", bridge.destinationResponseEvidenceResolved());
        final int responseLossRecoveries = bridge.attemptJournalResponseLossRecoveries();
        value.addProperty("attemptJournalResponseLossRecoveries", responseLossRecoveries);
        final int startupReplayRecords = bridge.restartAttemptJournalAndReplay();
        final List<PulsarAttemptJournal.JournalRecord> replayedRecords = bridge.journalRecords();
        if (startupReplayRecords != expectedKinds.size()
                || !replayedRecords.stream()
                        .map(PulsarAttemptJournal.JournalRecord::kind)
                        .toList()
                        .equals(expectedKinds)
                || replayedRecords.stream()
                        .anyMatch(record ->
                                !Arrays.equals(mappingId, record.mapping().mappingId()))) {
            throw new IllegalStateException(
                    "Managed Handoff Attempt Journal did not reconstruct the exact durable state after restart");
        }
        value.addProperty("attemptJournalStartupReplayRecords", startupReplayRecords);
        value.addProperty("attemptJournalStartupReplayVerified", true);
        value.add("journal", journal);
        PersistentStagingEvidence.writeNew(
                Path.of(configured), (GSON.toJson(value) + "\n").getBytes(StandardCharsets.UTF_8));
    }

    private static void requirePayload(
            final PulsarClient client, final String physicalTopic, final byte[] expectedPayload) throws Exception {
        requirePayload(client, physicalTopic, expectedPayload, 1);
    }

    private static void requirePayload(
            final PulsarClient client,
            final String physicalTopic,
            final byte[] expectedPayload,
            final int maximumMessages)
            throws Exception {
        if (maximumMessages <= 0) {
            throw new IllegalArgumentException("maximumMessages must be positive");
        }
        final TopicResourceGuard guard =
                new TopicResourceGuard(CLUSTER, DESTINATION_INCARNATION, DESTINATION_CREATION_TIMESTAMP);
        final GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(
                client, guard, physicalTopic, "nereus-delay-p1-worker-destination-" + physicalTopic.hashCode());
        try {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
            for (int received = 0; received < maximumMessages; received++) {
                final long remaining = deadline - System.nanoTime();
                final int remainingMs = remaining <= 0
                        ? 0
                        : Math.toIntExact(
                                Math.min(Integer.MAX_VALUE, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining))));
                final Message<byte[]> message =
                        remainingMs == 0 ? null : consumer.receive(remainingMs, TimeUnit.MILLISECONDS);
                if (message == null) {
                    break;
                }
                consumer.acknowledge(message);
                if (Arrays.equals(expectedPayload, message.getValue())) {
                    return;
                }
            }
            throw new IllegalStateException("source-applied typed destination payload was not read back exactly");
        } finally {
            consumer.close();
        }
    }

    private static void requireNoPayload(final PulsarClient client, final String physicalTopic) throws Exception {
        final TopicResourceGuard guard =
                new TopicResourceGuard(CLUSTER, DESTINATION_INCARNATION, DESTINATION_CREATION_TIMESTAMP);
        final GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(
                client, guard, physicalTopic, "nereus-delay-p1-worker-no-send-" + UUID.randomUUID());
        try {
            final Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            if (message != null) {
                throw new IllegalStateException("foreign-owner recovery emitted an unexpected destination message");
            }
        } finally {
            consumer.close();
        }
    }

    private static void writeField(
            final java.io.ByteArrayOutputStream output, final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
        } else {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        }
    }

    private record ManagedHandoffConfiguration(
            PersistentStagingActivation.Loaded activation,
            PersistentStagingNativeCanaryIdentity.Identity identity,
            OxiaSyncHandoffPolicyTrustStore trustStore,
            AtomicReference<SourcePosition> trustPosition,
            ProfileCatalogManagedNativeEligibilityAuthority authority) {
        private ManagedHandoffConfiguration {
            Objects.requireNonNull(activation, "activation");
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(trustStore, "trustStore");
            Objects.requireNonNull(trustPosition, "trustPosition");
            Objects.requireNonNull(authority, "authority");
        }

        private void activateAt(final SourcePosition sourcePosition) {
            final SourcePosition exact = Objects.requireNonNull(sourcePosition, "sourcePosition");
            final HandoffPolicySnapshot snapshot =
                    activation.policyPublication().head().snapshot();
            trustStore.installIssuerKey(activation.trustedKeyGeneration(), activation.trustedPublicKey(), exact);
            trustStore.activatePolicy(identity.policyScopeDigest(), snapshot.generation(), exact);
            if (!trustPosition.compareAndSet(null, exact)
                    && !Arrays.equals(trustPosition.get().canonicalBytes(), exact.canonicalBytes())) {
                throw new IllegalStateException("Managed Handoff trust position changed after activation");
            }
        }

        private long effectiveLeadMs() {
            final long effectiveLeadMs =
                    activation.policyPublication().head().snapshot().effectiveLeadMs();
            if (effectiveLeadMs <= 0 || effectiveLeadMs > PersistentStagingNativeCanaryIdentity.MAX_HANDOFF_LEAD_MS) {
                throw new IllegalStateException(
                        "Managed Handoff policy lead is outside the exact destination profile bound: "
                                + effectiveLeadMs);
            }
            return effectiveLeadMs;
        }
    }

    static final class PhysicalPublishBridge implements AutoCloseable {
        private final WorkerPhysicalPublishExecutor executor;
        private final RecordingMutationAppender appender;
        private final AutoCloseable appenderResource;
        private PulsarClientArtifactAttemptJournal journalResource;
        private final AutoCloseable physicalExecutorResource;
        private final ArtifactGenerationSet artifacts;
        private final ProfileCatalogManagedNativeEligibilityAuthority managedHandoffAuthority;
        private final HandoffPolicySnapshot managedHandoffSnapshot;
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final ProfileRef destinationProfile;
        private final ProfileRef capabilityProfile;
        private final com.nereusstream.delay.protocol.BrokerResourceIdentity targetResource;
        private final ChannelResourceIdentity channel;
        private final ReadyCertificate readyCertificate;
        private final List<EvidenceCursor> evidenceCursors;
        private final String destinationPhysicalTopic;
        private final boolean destinationResponseLoss;
        private final AtomicBoolean destinationResponseEvidenceResolved;
        private final boolean attemptJournalResponseLoss;
        private final boolean admissionResponseLoss;
        private final AtomicBoolean admissionResponseLossObserved;

        private PhysicalPublishBridge(
                final WorkerPhysicalPublishExecutor executor,
                final RecordingMutationAppender appender,
                final AutoCloseable appenderResource,
                final PulsarClientArtifactAttemptJournal journalResource,
                final AutoCloseable physicalExecutorResource,
                final ArtifactGenerationSet artifacts,
                final ProfileCatalogManagedNativeEligibilityAuthority managedHandoffAuthority,
                final HandoffPolicySnapshot managedHandoffSnapshot,
                final DestinationLaneId laneId,
                final byte[] laneIncarnation,
                final ProfileRef destinationProfile,
                final ProfileRef capabilityProfile,
                final com.nereusstream.delay.protocol.BrokerResourceIdentity targetResource,
                final ChannelResourceIdentity channel,
                final ReadyCertificate readyCertificate,
                final List<EvidenceCursor> evidenceCursors,
                final String destinationPhysicalTopic,
                final boolean destinationResponseLoss,
                final AtomicBoolean destinationResponseEvidenceResolved,
                final boolean attemptJournalResponseLoss,
                final boolean admissionResponseLoss,
                final AtomicBoolean admissionResponseLossObserved) {
            this.executor = executor;
            this.appender = appender;
            this.appenderResource = appenderResource;
            this.journalResource = journalResource;
            this.physicalExecutorResource = physicalExecutorResource;
            this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
            if ((managedHandoffAuthority == null) != (managedHandoffSnapshot == null)) {
                throw new IllegalArgumentException(
                        "Managed Handoff authority and frozen activation snapshot must be paired");
            }
            this.managedHandoffAuthority = managedHandoffAuthority;
            this.managedHandoffSnapshot = managedHandoffSnapshot;
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
            this.attemptJournalResponseLoss = attemptJournalResponseLoss;
            this.admissionResponseLoss = admissionResponseLoss;
            this.admissionResponseLossObserved = admissionResponseLossObserved;
        }

        WorkerPhysicalPublishExecutor executor() {
            return executor;
        }

        ArtifactGenerationSet artifacts() {
            return artifacts;
        }

        ProfileCatalogManagedNativeEligibilityAuthority managedHandoffAuthority() {
            return managedHandoffAuthority;
        }

        HandoffPolicySnapshot managedHandoffSnapshot() {
            return managedHandoffSnapshot;
        }

        ShardLogMutationAppender appender() {
            return appender;
        }

        RecordedMutationAppend lastMutationAppend() {
            return appender.last();
        }

        DestinationLaneId laneId() {
            return laneId;
        }

        byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }

        ProfileRef destinationProfile() {
            return destinationProfile;
        }

        ProfileRef capabilityProfile() {
            return capabilityProfile;
        }

        com.nereusstream.delay.protocol.BrokerResourceIdentity targetResource() {
            return targetResource;
        }

        ChannelResourceIdentity channel() {
            return channel;
        }

        ReadyCertificate readyCertificate() {
            return readyCertificate;
        }

        List<EvidenceCursor> evidenceCursors() {
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

        boolean attemptJournalResponseLoss() {
            return attemptJournalResponseLoss;
        }

        int attemptJournalResponseLossRecoveries() {
            return journalResource.responseLossRecoveries();
        }

        int restartAttemptJournalAndReplay() throws PulsarClientException {
            journalResource = journalResource.reopenAfterCloseForTesting();
            return journalResource.replayedRecords();
        }

        List<PulsarAttemptJournal.JournalRecord> journalRecords() {
            return journalResource.journal().records();
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
                physicalExecutorResource.close();
            } catch (Exception closeFailure) {
                final RuntimeException runtimeFailure = closeFailure instanceof RuntimeException
                        ? (RuntimeException) closeFailure
                        : new IllegalStateException("Pulsar Worker physical executor close failed", closeFailure);
                if (failure == null) {
                    failure = runtimeFailure;
                } else {
                    failure.addSuppressed(runtimeFailure);
                }
            }
            try {
                journalResource.close();
            } catch (Exception closeFailure) {
                final RuntimeException runtimeFailure = closeFailure instanceof RuntimeException
                        ? (RuntimeException) closeFailure
                        : new IllegalStateException("Pulsar Worker Attempt Journal close failed", closeFailure);
                if (failure == null) {
                    failure = runtimeFailure;
                } else {
                    failure.addSuppressed(runtimeFailure);
                }
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

    private static final class RecordingMutationAppender implements ShardLogMutationAppender, AutoCloseable {
        private final ShardLogMutationAppender delegate;
        private volatile RecordedMutationAppend last;

        private RecordingMutationAppender(final ShardLogMutationAppender delegate) {
            this.delegate = java.util.Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public AppendOutcome append(final SystemMutation mutation) {
            final AppendOutcome outcome =
                    java.util.Objects.requireNonNull(delegate.append(mutation), "Shard Log append outcome");
            last = new RecordedMutationAppend(mutation, outcome);
            return outcome;
        }

        private RecordedMutationAppend last() {
            return last;
        }

        @Override
        public void close() {
            if (delegate instanceof AutoCloseable resource) {
                try {
                    resource.close();
                } catch (RuntimeException failure) {
                    throw failure;
                } catch (Exception failure) {
                    throw new IllegalStateException("Pulsar Worker mutation appender close failed", failure);
                }
            }
        }
    }

    private record RecordedMutationAppend(SystemMutation mutation, ShardLogMutationAppender.AppendOutcome outcome) {}

    private static ShardId restartShard(final String physicalTopic) {
        return new ShardId(
                new RouteIncarnation(Arrays.copyOf(
                        Bytes.sha256(Bytes.utf8("nereus-delay-pulsar-worker-restart/" + physicalTopic)),
                        RouteIncarnation.LENGTH)),
                0);
    }

    private static Path finalCheckpointPath(final Path root, final ShardId shard, final long ownerEpoch) {
        return root.resolve("worker-final-checkpoints")
                .resolve(Bytes.hex(shard.routeIncarnation().bytes()))
                .resolve(Integer.toUnsignedString(shard.partition()))
                .resolve(Long.toUnsignedString(ownerEpoch));
    }

    private static byte[] finalCheckpointId(final ShardId shard, final long ownerEpoch) {
        return Arrays.copyOf(
                Bytes.sha256(
                        Bytes.utf8("nereus-delay/pulsar-worker-final-checkpoint/generation-1"),
                        shard.routeIncarnation().bytes(),
                        Bytes.u32beBits(shard.partition()),
                        Bytes.u64beBits(ownerEpoch)),
                16);
    }

    private static AssignmentAcceptance publishAssignment(
            final WorkerAssignmentAuthority authority,
            final SourceAssignment sourceAssignment,
            final boolean realOxia) {
        final WorkerAssignmentCoordinator coordinator = new WorkerAssignmentCoordinator(
                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
        final long now = System.currentTimeMillis();
        final String workerId = workerId();
        final Optional<WorkerAssignmentAuthority.Publication> current = authority.current(sourceAssignment.shardId());
        final long expectedRevision =
                current.map(WorkerAssignmentAuthority.Publication::revision).orElse(0L);
        final long placementEpoch;
        try {
            placementEpoch = current.isEmpty()
                    ? 1L
                    : Math.addExact(current.orElseThrow().assignment().placementEpoch(), 1L);
        } catch (ArithmeticException exhausted) {
            throw new IllegalStateException("Pulsar Worker placement epoch exhausted", exhausted);
        }
        final String currentWorkerId =
                current.map(publication -> publication.assignment().workerId()).orElse(null);
        final WorkerPlacementPolicy.WorkerCandidate candidate = new WorkerPlacementPolicy.WorkerCandidate(
                workerId,
                capacity(1),
                CapacityVector.empty(),
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
                Bytes.sha256(Bytes.utf8("pulsar-worker-capacity-envelope")),
                placementEpoch,
                List.of(candidate),
                capacity(1),
                CapacityVector.empty(),
                CapacityVector.empty(),
                currentWorkerId,
                now,
                0,
                expectedRevision);
        final WorkerAssignmentAuthority.Publication publication =
                result.publication().orElseThrow();
        final WorkerAssignment accepted = coordinator.requireAccepted(
                sourceAssignment.shardId(), publication.revision(), publication.assignment());
        System.out.println("Pulsar Worker assignment publication/acceptance passed: revision="
                + publication.revision() + ", worker=" + accepted.workerId() + ", authority="
                + (realOxia ? "real Oxia session-bound" : "in-memory") + ", previousWorker="
                + (currentWorkerId == null ? "none" : currentWorkerId) + ", placementEpoch="
                + Long.toUnsignedString(accepted.placementEpoch()));
        return new AssignmentAcceptance(accepted, publication.revision());
    }

    private record AssignmentAcceptance(WorkerAssignment assignment, long revision) {}

    private static CapacityVector capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[CapacityDimension.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVector(values);
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
                "nereus-delay-pulsar-worker-" + workerId() + "-" + UUID.randomUUID(),
                Duration.ofSeconds(15),
                workerAuthorityPrefix());
    }

    private static String workerId() {
        return configured("NEREUS_DELAY_PULSAR_WORKER_ID", "pulsar-worker");
    }

    private static String workerAuthorityPrefix() {
        return configured(
                "NEREUS_DELAY_PULSAR_WORKER_AUTHORITY_PREFIX",
                "nereus-delay/pulsar-worker-authority/" + UUID.randomUUID());
    }

    private static String workerAssignmentPrefix() {
        return configured(
                "NEREUS_DELAY_PULSAR_WORKER_ASSIGNMENT_PREFIX",
                "nereus-delay/pulsar-worker-placement/" + UUID.randomUUID());
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static Path workerStoreRoot() throws Exception {
        final String configuredRoot = System.getenv("NEREUS_DELAY_PULSAR_WORKER_ROOT");
        if (configuredRoot == null || configuredRoot.isBlank()) {
            return Files.createTempDirectory("nereus-delay-pulsar-worker-");
        }
        final Path root = Path.of(configuredRoot).toAbsolutePath().normalize();
        Files.createDirectories(root);
        return root;
    }

    private static Optional<PulsarSourcePosition> persistedPulsarPosition(final Path root, final ShardId shard)
            throws Exception {
        final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                ShardStore store = ShardStore.open(storeConfig, shard, resources)) {
            final SourcePosition position = store.appliedShardLogPosition();
            if (position == null) {
                return Optional.empty();
            }
            if (!(position instanceof PulsarSourcePosition pulsarPosition)) {
                throw new IllegalStateException("Pulsar Worker recovery Store has a non-Pulsar source position");
            }
            return Optional.of(pulsarPosition);
        }
    }

    private static PublishAttemptLedger requireAdmissionRecoveryAttempt(final DelayShard delayShard) {
        final List<PublishAttemptLedger> attempts = delayShard.listOpenPublishAttempts();
        if (attempts.size() != 1
                || attempts.get(0).state() != com.nereusstream.delay.runtime.AttemptLedgerState.PUBLISHING) {
            throw new IllegalStateException(
                    "Pulsar admission response-loss recovery did not find exactly one durable PUBLISHING attempt");
        }
        return attempts.get(0);
    }

    /**
     * The real Pulsar recovery cursor is positioned by a broker seek after the
     * exact persisted source position. Pulsar entry IDs are not required to be
     * numerically contiguous, so that broker-positioned boundary is the
     * adapter's successor proof for the first post-crash record; same-entry
     * batch members retain the strict proof thereafter.
     */
    private static SourceReplaySuccessor destinationResponseLossRecoverySuccessor(
            final PulsarSourcePosition persistedPosition) {
        final PulsarSourcePosition floor = java.util.Objects.requireNonNull(persistedPosition, "persistedPosition");
        final SourceReplaySuccessor batchSuccessor = SourceReplaySuccessor.strictPulsarBatchMember();
        final AtomicBoolean seekBoundaryConsumed = new AtomicBoolean();
        return (previous, current) -> {
            if (!seekBoundaryConsumed.get()
                    && Arrays.equals(previous.canonicalBytes(), floor.canonicalBytes())
                    && current.compareTo(previous) > 0) {
                seekBoundaryConsumed.set(true);
                return true;
            }
            return batchSuccessor.isSuccessor(previous, current);
        };
    }

    private static String recoveryOutcomeSummary(final OwnerRecoveryTurn turn) {
        if (turn.outcomes().isEmpty()) {
            return "none";
        }
        final var result = turn.outcomes().get(turn.outcomes().size() - 1).systemMutationResult();
        if (result == null) {
            return "command";
        }
        return result.applyStatus() + "/" + result.stableCode();
    }

    private static void requireAppliedRecoveryOutcome(final OwnerRecoveryTurn turn) {
        if (turn.outcomes().size() != 1 || turn.outcomes().get(0).systemMutationResult() == null) {
            throw new IllegalStateException(
                    "Pulsar destination response-loss recovery did not apply exactly one System Mutation Outcome");
        }
        final var result = turn.outcomes().get(0).systemMutationResult();
        if (result.applyStatus() != com.nereusstream.delay.runtime.ApplyStatus.APPLIED
                || result.stableCode() != StableCode.OK) {
            throw new IllegalStateException("Pulsar destination response-loss recovery Outcome was not applied: "
                    + result.applyStatus() + "/" + result.stableCode());
        }
    }

    private static RecoveredDestinationOutcome requireRecoveredDestinationOutcome(
            final List<SourceReplayEntry> recoveryEntries, final ShardStore store, final ShardId shard) {
        SourceReplayMutation outcomeMutation = null;
        for (SourceReplayEntry entry : recoveryEntries) {
            if (entry instanceof SourceReplayMutation mutation
                    && mutation.mutation().type() == SystemMutationType.PUBLISH_OUTCOME) {
                outcomeMutation = mutation;
            }
        }
        if (outcomeMutation == null) {
            throw new IllegalStateException(
                    "Pulsar destination response-loss recovery did not replay a PUBLISH_OUTCOME");
        }
        final PublishOutcomeBody outcome =
                PublishOutcomeBody.decode(outcomeMutation.mutation().canonicalBody());
        if (outcome.sideEffect() != 1
                || outcome.stableCode() != StableCode.OK
                || !Arrays.equals(
                        outcome.publishAttemptId(), outcomeMutation.mutation().logicalOperationIdentity())) {
            throw new IllegalStateException(
                    "Pulsar destination response-loss recovery replayed a non-definitive or divergent Outcome");
        }
        final PublishEvidence evidence = PublishEvidence.decode(outcome.evidence());
        if (evidence.evidenceKind() != PublishEvidenceKind.PULSAR_SEND_ACK
                || evidence.verificationStatus() != EvidenceVerificationStatus.VERIFIED_PUBLISHED) {
            throw new IllegalStateException(
                    "Pulsar destination response-loss recovery replayed the wrong destination evidence branch");
        }
        evidence.requireBusinessMutation(outcome.publishAttemptId(), true);

        MessageRecord recoveredMessage = null;
        DelayMessageId recoveredMessageId = null;
        final List<ShardStore.KeyValue> messages =
                store.scan(ColumnFamily.ID, new byte[] {1, 1}, new byte[] {1, 2}, 128);
        System.out.println("Pulsar destination response-loss recovery message scan: entries=" + messages.size());
        for (ShardStore.KeyValue entry : messages) {
            final byte[] key = entry.key();
            if (key.length != 2 + DelayMessageId.LENGTH || key[0] != 1 || key[1] != 1) {
                continue;
            }
            final DelayMessageId candidateId = new DelayMessageId(Arrays.copyOfRange(key, 2, key.length));
            if (!candidateId.routingId().shardId().equals(shard)) {
                continue;
            }
            final MessageRecord candidate =
                    MessageRecord.decode(ValueEnvelope.decode(entry.value(), 1).payload());
            System.out.println("Pulsar destination response-loss recovery message observation: id="
                    + Bytes.hex(candidateId.bytes()) + ", shardMatches="
                    + candidateId.routingId().shardId().equals(shard)
                    + ", status=" + candidate.status());
            if (candidate.status() != MessageStatus.PUBLISHED) {
                continue;
            }
            if (recoveredMessage != null) {
                throw new IllegalStateException(
                        "Pulsar destination response-loss recovery found multiple PUBLISHED messages");
            }
            recoveredMessage = candidate;
            recoveredMessageId = candidateId;
        }
        if (recoveredMessage == null || recoveredMessageId == null) {
            throw new IllegalStateException(
                    "Pulsar destination response-loss recovery did not find the durable PUBLISHED message");
        }
        final SourcePosition schedulePosition = SourcePositionCodec.decode(recoveredMessage.scheduleSourcePosition());
        if (!(schedulePosition instanceof PulsarSourcePosition pulsarSchedulePosition)) {
            throw new IllegalStateException(
                    "Pulsar destination response-loss recovery found a non-Pulsar schedule position");
        }
        return new RecoveredDestinationOutcome(
                recoveredMessageId,
                pulsarSchedulePosition,
                recoveredMessage.payload(),
                outcome,
                outcomeMutation.position());
    }

    private record RecoveredDestinationOutcome(
            DelayMessageId messageId,
            PulsarSourcePosition physicalSchedulePosition,
            byte[] payload,
            PublishOutcomeBody outcome,
            SourcePosition outcomePosition) {
        private RecoveredDestinationOutcome {
            payload = Bytes.copy(payload);
        }

        @Override
        public byte[] payload() {
            return Bytes.copy(payload);
        }
    }

    private static void awaitWorkerProcessCrashGate(
            final Path root, final ShardStore store, final String physicalTopic, final ShardId shard) throws Exception {
        final String gate = System.getenv("NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_GATE");
        final String pidFile = System.getenv("NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_PID_FILE");
        if (gate == null || gate.isBlank() || pidFile == null || pidFile.isBlank()) {
            throw new IllegalStateException("Pulsar Worker crash-wait requires process-crash gate and PID file paths");
        }
        final Path gatePath = Path.of(gate).toAbsolutePath().normalize();
        final Path pidPath = Path.of(pidFile).toAbsolutePath().normalize();
        Files.deleteIfExists(gatePath);
        Files.deleteIfExists(pidPath);
        Files.createFile(gatePath);
        Files.writeString(pidPath, Long.toString(ProcessHandle.current().pid()));
        writeWorkerProcessStateDump("PULSAR_WORKER_PROCESS_CRASH_READY", root, store, physicalTopic, shard, false);
        System.out.println("Pulsar Worker process-crash cut reached: sourceRuntimeReady=true, "
                + "nextSourceRecordUnacked=true, storeRoot=" + root);
        while (Files.exists(gatePath)) {
            Thread.sleep(50L);
        }
    }

    private static boolean hasAdmissionResponseLossProcessCrash() {
        return "1".equals(System.getenv("NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_ONLY"));
    }

    private static boolean hasWorkerProcessCrash() {
        return "1".equals(System.getenv("NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY"));
    }

    private static boolean hasDestinationResponseLossProcessCrash() {
        return "1".equals(System.getenv("NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_ONLY"));
    }

    private static Path chaosStateDumpDirectory() {
        final String configured = System.getenv("NEREUS_DELAY_PULSAR_CHAOS_STATE_DUMP_DIR");
        return configured == null || configured.isBlank()
                ? null
                : Path.of(configured).toAbsolutePath().normalize();
    }

    private static void awaitAdmissionResponseLossProcessCrashGate(final Path dbPath) throws Exception {
        final String gate = System.getenv("NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_GATE");
        final String pidFile =
                System.getenv("NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_PID_FILE");
        if (gate == null || gate.isBlank() || pidFile == null || pidFile.isBlank()) {
            throw new IllegalStateException(
                    "Pulsar Worker admission response-loss crash requires gate and PID file paths");
        }
        final Path gatePath = Path.of(gate).toAbsolutePath().normalize();
        final Path pidPath = Path.of(pidFile).toAbsolutePath().normalize();
        Files.deleteIfExists(gatePath);
        Files.deleteIfExists(pidPath);
        Files.createFile(gatePath);
        Files.writeString(pidPath, Long.toString(ProcessHandle.current().pid()));
        System.out.println("Pulsar Worker admission response-loss process-crash cut reached: "
                + "durableAdmissionState=PUBLISHING, storeRoot=" + dbPath);
        while (Files.exists(gatePath)) {
            Thread.sleep(50L);
        }
    }

    private static void awaitDestinationResponseLossProcessCrashGate(final Path dbPath) throws Exception {
        final String gate = System.getenv("NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_GATE");
        final String pidFile =
                System.getenv("NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_PID_FILE");
        if (gate == null || gate.isBlank() || pidFile == null || pidFile.isBlank()) {
            throw new IllegalStateException(
                    "Pulsar Worker destination response-loss crash requires gate and PID file paths");
        }
        final Path gatePath = Path.of(gate).toAbsolutePath().normalize();
        final Path pidPath = Path.of(pidFile).toAbsolutePath().normalize();
        Files.deleteIfExists(gatePath);
        Files.deleteIfExists(pidPath);
        Files.createFile(gatePath);
        Files.writeString(pidPath, Long.toString(ProcessHandle.current().pid()));
        System.out.println("Pulsar Worker destination response-loss process-crash cut reached: "
                + "physicalSendEvidenceResolved=true, outcomePersisted=true, sourceApplyStarted=false, storeRoot="
                + dbPath);
        while (Files.exists(gatePath)) {
            Thread.sleep(50L);
        }
    }

    private static void writeChaosStateDump(
            final String cell,
            final String phase,
            final Path root,
            final ShardStore store,
            final PublishAttemptLedger attempt,
            final PulsarSourcePosition physicalSchedulePosition,
            final String observedAttemptState,
            final boolean outcomeApplied,
            final byte[] publishAttemptIdOverride,
            final byte[] attemptSourcePositionOverride,
            final DelayMessageId messageIdOverride)
            throws Exception {
        final Path directory = chaosStateDumpDirectory();
        if (directory == null) {
            return;
        }
        Files.createDirectories(directory);
        final Path target = directory.resolve(
                "ADMISSION_RESPONSE_LOSS_PERSISTED".equals(phase) || "DESTINATION_RESPONSE_LOSS_PERSISTED".equals(phase)
                        ? "before-process-crash.json"
                        : "after-fresh-process.json");
        final var metadata = store.metadata();
        final var appliedPosition = store.appliedShardLogPosition();
        final String attemptSourcePosition = attemptSourcePositionOverride == null
                ? (attempt == null ? null : Bytes.hex(attempt.sourcePosition()))
                : Bytes.hex(attemptSourcePositionOverride);
        final String publishAttemptId = publishAttemptIdOverride == null
                ? (attempt == null ? null : Bytes.hex(attempt.publishAttemptId()))
                : Bytes.hex(publishAttemptIdOverride);
        final String messageId = messageIdOverride == null
                ? (attempt == null ? null : Bytes.hex(attempt.delayMessageId().bytes()))
                : Bytes.hex(messageIdOverride.bytes());
        final String appliedSourcePosition =
                appliedPosition == null ? null : Bytes.hex(appliedPosition.canonicalBytes());
        final String schedulePosition =
                physicalSchedulePosition == null ? null : Bytes.hex(physicalSchedulePosition.canonicalBytes());
        final String shard = metadata.shardId().routeIncarnation().uuid() + "/"
                + metadata.shardId().partition();
        final String json = "{\n"
                + " \"schema\": \"nereus-delay-chaos-durable-state-dump\",\n"
                + " \"cell\": " + jsonString(cell) + ",\n"
                + " \"phase\": " + jsonString(phase) + ",\n"
                + " \"process_pid\": " + ProcessHandle.current().pid() + ",\n"
                + " \"store_root\": " + jsonString(root.toString()) + ",\n"
                + " \"shard\": " + jsonString(shard) + ",\n"
                + " \"store_incarnation\": " + jsonString(Bytes.hex(metadata.storeIncarnation())) + ",\n"
                + " \"db_identity\": " + jsonString(Bytes.hex(metadata.dbIdentity())) + ",\n"
                + " \"physical_schedule_position\": " + jsonNullable(schedulePosition) + ",\n"
                + " \"applied_source_position\": " + jsonNullable(appliedSourcePosition) + ",\n"
                + " \"shard_mutation_sequence\": " + store.shardMutationSequence() + ",\n"
                + " \"attempt_state\": " + jsonString(observedAttemptState) + ",\n"
                + " \"publish_attempt_id\": " + jsonNullable(publishAttemptId) + ",\n"
                + " \"attempt_source_position\": " + jsonNullable(attemptSourcePosition) + ",\n"
                + " \"message_id\": " + jsonNullable(messageId) + ",\n"
                + " \"outcome_applied\": " + outcomeApplied + ",\n"
                + " \"durable_store_read\": true,\n"
                + " \"dump_forced\": true\n"
                + "}\n";
        try (FileChannel channel = FileChannel.open(
                target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(java.nio.ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
            channel.force(true);
        }
    }

    private static void writeSourceAckResponseLossStateDump(
            final String phase,
            final Path root,
            final ShardStore store,
            final String physicalTopic,
            final ShardId shard,
            final SourcePosition ackSourcePosition,
            final SourcePosition appliedSourcePosition,
            final int recoveryReplayedEntries,
            final boolean recoveryReplayedAckSource)
            throws Exception {
        final Path directory = chaosStateDumpDirectory();
        if (directory == null) {
            return;
        }
        Files.createDirectories(directory);
        final Path target = directory.resolve(
                "SOURCE_ACK_RESPONSE_LOSS_PERSISTED".equals(phase)
                        ? "before-process-crash.json"
                        : "after-fresh-process.json");
        final var metadata = store.metadata();
        final String shardValue = metadata.shardId().routeIncarnation().uuid() + "/"
                + metadata.shardId().partition();
        final String json = "{\n"
                + " \"schema\": \"nereus-delay-chaos-durable-state-dump\",\n"
                + " \"cell\": \"pulsar-source-ack-response-loss\",\n"
                + " \"phase\": " + jsonString(phase) + ",\n"
                + " \"process_pid\": " + ProcessHandle.current().pid() + ",\n"
                + " \"store_root\": " + jsonString(root.toString()) + ",\n"
                + " \"physical_topic\": " + jsonString(physicalTopic) + ",\n"
                + " \"cluster_id\": " + jsonString(CLUSTER) + ",\n"
                + " \"route_uuid\": "
                + jsonString(metadata.shardId().routeIncarnation().uuid().toString()) + ",\n"
                + " \"shard\": " + jsonString(shardValue) + ",\n"
                + " \"partition\": " + shard.partition() + ",\n"
                + " \"store_incarnation\": " + jsonString(Bytes.hex(metadata.storeIncarnation())) + ",\n"
                + " \"db_identity\": " + jsonString(Bytes.hex(metadata.dbIdentity())) + ",\n"
                + " \"source_ack_source_position\": "
                + jsonNullable(ackSourcePosition == null ? null : Bytes.hex(ackSourcePosition.canonicalBytes())) + ",\n"
                + " \"applied_source_position\": "
                + jsonNullable(appliedSourcePosition == null ? null : Bytes.hex(appliedSourcePosition.canonicalBytes()))
                + ",\n"
                + " \"recovery_replayed_entries\": " + recoveryReplayedEntries + ",\n"
                + " \"recovery_replayed_ack_source\": " + recoveryReplayedAckSource + ",\n"
                + " \"source_apply_durable\": true,\n"
                + " \"source_ack_committed\": true,\n"
                + " \"ack_response_lost\": true,\n"
                + " \"duplicate_source_apply_observed\": " + recoveryReplayedAckSource + ",\n"
                + " \"durable_store_read\": true,\n"
                + " \"dump_forced\": true\n"
                + "}\n";
        try (FileChannel channel = FileChannel.open(
                target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(java.nio.ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
            channel.force(true);
        }
    }

    private static void writeWorkerProcessStateDump(
            final String phase,
            final Path root,
            final ShardStore store,
            final String physicalTopic,
            final ShardId shard,
            final boolean sourceAckCommitted)
            throws Exception {
        final Path directory = chaosStateDumpDirectory();
        if (directory == null) {
            return;
        }
        Files.createDirectories(directory);
        final Path target = directory.resolve(
                "PULSAR_WORKER_PROCESS_CRASH_READY".equals(phase)
                        ? "before-process-crash.json"
                        : "after-fresh-process.json");
        final var metadata = store.metadata();
        final var appliedPosition = store.appliedShardLogPosition();
        final String appliedSourcePosition =
                appliedPosition == null ? null : Bytes.hex(appliedPosition.canonicalBytes());
        final String shardValue = metadata.shardId().routeIncarnation().uuid() + "/"
                + metadata.shardId().partition();
        final String json = "{\n"
                + " \"schema\": \"nereus-delay-chaos-durable-state-dump\",\n"
                + " \"cell\": \"pulsar-worker-process-crash\",\n"
                + " \"phase\": " + jsonString(phase) + ",\n"
                + " \"process_pid\": " + ProcessHandle.current().pid() + ",\n"
                + " \"store_root\": " + jsonString(root.toString()) + ",\n"
                + " \"physical_topic\": " + jsonString(physicalTopic) + ",\n"
                + " \"cluster_id\": " + jsonString(CLUSTER) + ",\n"
                + " \"route_uuid\": "
                + jsonString(metadata.shardId().routeIncarnation().uuid().toString()) + ",\n"
                + " \"shard\": " + jsonString(shardValue) + ",\n"
                + " \"partition\": " + shard.partition() + ",\n"
                + " \"store_incarnation\": " + jsonString(Bytes.hex(metadata.storeIncarnation())) + ",\n"
                + " \"db_identity\": " + jsonString(Bytes.hex(metadata.dbIdentity())) + ",\n"
                + " \"applied_source_position\": " + jsonNullable(appliedSourcePosition) + ",\n"
                + " \"source_record_prepared\": true,\n"
                + " \"source_record_applied\": " + sourceAckCommitted + ",\n"
                + " \"source_ack_committed\": " + sourceAckCommitted + ",\n"
                + " \"durable_store_read\": true,\n"
                + " \"dump_forced\": true\n"
                + "}\n";
        try (FileChannel channel = FileChannel.open(
                target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(java.nio.ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
            channel.force(true);
        }
    }

    private static String jsonString(final String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String jsonNullable(final String value) {
        return value == null ? "null" : jsonString(value);
    }

    private static com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult runUntilApplied(
            final WorkerShardRuntime runtime) {
        return runUntilApplied(runtime, null);
    }

    private static com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult runUntilApplied(
            final WorkerShardRuntime runtime, final SourceAckResponseLossProcessCrashContext sourceAckCrashContext) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result;
        do {
            result = runtime.runSourceTurn(
                    new SchedulerBudget(1, 1_000_000, TimeUnit.SECONDS.toNanos(2)), System::currentTimeMillis);
            if (result.status()
                    == com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                return result;
            }
            if (result.status() == com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN
                    && sourceAckCrashContext != null
                    && sourceAckCrashContext.responseLossObserved.get()
                    && sourceAckCrashContext.dumpWritten.compareAndSet(false, true)) {
                if (result.entry() == null) {
                    throw new IllegalStateException(
                            "Pulsar source ACK response-loss turn did not retain its exact source entry");
                }
                try {
                    sourceAckCrashContext.store.flushAndSync();
                    writeSourceAckResponseLossStateDump(
                            "SOURCE_ACK_RESPONSE_LOSS_PERSISTED",
                            sourceAckCrashContext.root,
                            sourceAckCrashContext.store,
                            sourceAckCrashContext.physicalTopic,
                            sourceAckCrashContext.shard,
                            result.entry().position(),
                            sourceAckCrashContext.store.appliedShardLogPosition(),
                            0,
                            false);
                    awaitSourceAckResponseLossProcessCrashGate(sourceAckCrashContext.root);
                } catch (Exception failure) {
                    throw new IllegalStateException(
                            "Pulsar source ACK response-loss process-crash cut could not persist its gate", failure);
                }
            }
            if (result.status() != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS
                    && result.status() != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN
                    && result.status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                    .ACK_DEFINITIVELY_NOT_ACKED) {
                throw new IllegalStateException(
                        "Pulsar Worker source turn failed: " + result.status(), result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Pulsar Worker source record did not become visible before deadline");
    }

    private static boolean hasSourceAckResponseLoss() {
        return "1".equals(System.getenv("NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS"));
    }

    private static void awaitSourceAckResponseLossProcessCrashGate(final Path root) throws Exception {
        final String gate = System.getenv("NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS_PROCESS_CRASH_GATE");
        final String pidFile = System.getenv("NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS_PROCESS_CRASH_PID_FILE");
        if (gate == null || gate.isBlank() || pidFile == null || pidFile.isBlank()) {
            throw new IllegalStateException("Pulsar source ACK response-loss crash requires gate and PID file paths");
        }
        final Path gatePath = Path.of(gate).toAbsolutePath().normalize();
        final Path pidPath = Path.of(pidFile).toAbsolutePath().normalize();
        Files.deleteIfExists(gatePath);
        Files.deleteIfExists(pidPath);
        Files.createFile(gatePath);
        Files.writeString(pidPath, Long.toString(ProcessHandle.current().pid()));
        System.out.println("Pulsar Worker source ACK response-loss process-crash cut reached: "
                + "durableSourceApply=true, brokerAckAccepted=true, ackResponseLost=true, storeRoot=" + root);
        while (Files.exists(gatePath)) {
            Thread.sleep(50L);
        }
    }

    private static final class SourceAckResponseLossProcessCrashContext {
        private final Path root;
        private final ShardStore store;
        private final String physicalTopic;
        private final ShardId shard;
        private final AtomicBoolean responseLossObserved;
        private final AtomicBoolean dumpWritten = new AtomicBoolean();

        private SourceAckResponseLossProcessCrashContext(
                final Path root,
                final ShardStore store,
                final String physicalTopic,
                final ShardId shard,
                final AtomicBoolean responseLossObserved) {
            this.root = root;
            this.store = store;
            this.physicalTopic = physicalTopic;
            this.shard = shard;
            this.responseLossObserved = responseLossObserved;
        }
    }

    @SuppressWarnings("unchecked")
    private static GuardedConsumer<byte[]> responseLossConsumer(
            final GuardedConsumer<byte[]> delegate, final AtomicBoolean responseLossObserved) {
        return (GuardedConsumer<byte[]>) Proxy.newProxyInstance(
                PulsarClientArtifactWorkerSmoke.class.getClassLoader(),
                new Class<?>[] {GuardedConsumer.class},
                (proxy, method, arguments) -> {
                    final Object result = invoke(delegate, method, arguments);
                    if (method.getName().equals("acknowledge")
                            && method.getParameterCount() == 1
                            && responseLossObserved.compareAndSet(false, true)) {
                        throw new IllegalStateException("simulated committed Pulsar source ACK response loss");
                    }
                    return result;
                });
    }

    private static Object invoke(final Object target, final java.lang.reflect.Method method, final Object[] arguments)
            throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    private static void send(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final PreparedCommand command,
            final String producerName)
            throws Exception {
        sendAndPosition(client, guard, physicalTopic, command, producerName);
    }

    static PulsarSourcePosition sendAndPosition(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final PreparedCommand command,
            final String producerName)
            throws Exception {
        final PulsarClientArtifactSendTransport transport = new PulsarClientArtifactSendTransport(
                PulsarClientArtifactProducerFactory.create(
                        client,
                        guard.authenticatedClusterId(),
                        guard.resourceIncarnation(),
                        physicalTopic,
                        guard.topicCreationTimestamp(),
                        producerName),
                CLUSTER,
                INCARNATION,
                physicalTopic,
                CREATION_TIMESTAMP,
                0);
        try {
            final PulsarSendResult result = transport
                    .send(new PulsarSendRequest(
                            CLUSTER,
                            INCARNATION,
                            physicalTopic,
                            CREATION_TIMESTAMP,
                            0,
                            command.commandId(),
                            com.nereusstream.delay.protocol.CommandCodec.encodeManagedFrame(command)))
                    .toCompletableFuture()
                    .get(15, TimeUnit.SECONDS);
            if (result.disposition() != PulsarSendResult.Disposition.PERSISTED) {
                throw new IllegalStateException(
                        "guarded Pulsar Worker producer did not persist: " + result.disposition());
            }
            return new PulsarSourcePosition(
                    command.shardId(),
                    INCARNATION,
                    physicalTopic,
                    result.ledgerId(),
                    result.entryId(),
                    result.batchIndex(),
                    result.batchSize(),
                    result.batched() ? PulsarSourcePosition.EntryKind.BATCH : PulsarSourcePosition.EntryKind.NON_BATCH,
                    result.brokerEntryTimestampEpochMs());
        } finally {
            transport.close();
        }
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        return command(shard, identity, new byte[0], 1_000);
    }

    private static PreparedCommand command(
            final ShardId shard, final String identity, final byte[] payload, final long delayMs) {
        final ProfileRef destination = destinationProfile(identity);
        final RetryPolicyRef retryPolicy = new RetryPolicyRef(
                Bytes.utf8("retry-" + identity), 1, Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + delayMs;
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination,
                retryPolicy,
                deliverAt,
                deliverAt + 10_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                NativeDeliveryPolicy.FORBID,
                payload,
                Bytes.utf8("source-" + identity),
                null,
                AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())),
                null,
                null);
        return PreparedCommand.schedule(shard, intent, deliverAt + 20_000);
    }

    private static PreparedCommand managedHandoffCommand(
            final ShardId shard, final PersistentStagingNativeCanaryIdentity.Identity identity, final byte[] payload) {
        final long deliverAt = Math.addExact(System.currentTimeMillis(), MANAGED_HANDOFF_CANARY_DELAY_MS);
        final RetryPolicyRef retryPolicy = new RetryPolicyRef(
                Bytes.utf8("pulsar-worker-managed-handoff-retry"),
                1,
                Bytes.sha256(Bytes.utf8("pulsar-worker-managed-handoff-retry-semantic")));
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                identity.destination().ref(),
                retryPolicy,
                deliverAt,
                Math.addExact(deliverAt, 30_000),
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                NativeDeliveryPolicy.ALLOW_AUTO_FAST_AND_MANAGED_HANDOFF,
                Bytes.utf8("pulsar-worker-managed-handoff-ordering"),
                payload,
                null,
                AdapterMetadata.pulsar(new PulsarMetadata(
                        Bytes.utf8("managed-handoff-key"),
                        PulsarMetadata.KeyEncoding.UTF8,
                        Bytes.utf8("managed-handoff-ordering"),
                        List.of(new PulsarMetadata.Property("managed-handoff", "persistent-staging")))),
                null,
                System.currentTimeMillis());
        return PreparedCommand.schedule(shard, intent, Math.addExact(deliverAt, 40_000));
    }

    private static ManagedHandoffConfiguration managedHandoffConfiguration(
            final PersistentStagingActivation.Loaded activation, final String destinationPhysicalTopic) {
        if (!hasManagedHandoffCanary()) {
            if (activation != null) {
                throw new IllegalArgumentException("persistent activation was loaded outside a Managed Handoff run");
            }
            return null;
        }
        if (activation == null || destinationPhysicalTopic == null) {
            throw new IllegalStateException(
                    "Managed Handoff canary requires complete persistent activation and an exact destination");
        }
        final PersistentStagingNativeCanaryIdentity.Identity identity = PersistentStagingNativeCanaryIdentity.create(
                CLUSTER,
                DESTINATION_INCARNATION,
                destinationPhysicalTopic,
                DESTINATION_CREATION_TIMESTAMP,
                activation.artifacts());
        if (!Arrays.equals(identity.policyScopeDigest(), activation.policyScopeDigest())
                || !Arrays.equals(
                        identity.policyScopeDigest(),
                        activation.policyPublication().head().scopeDigest())
                || !activation
                        .policyPublication()
                        .head()
                        .snapshot()
                        .allows(com.nereusstream.delay.protocol.HandoffPath.MANAGED_HANDOFF)) {
            throw new IllegalStateException("persistent activation does not authorize the exact Managed Handoff");
        }
        final ProfileCatalog catalog = profileCatalog(identity);
        final var trustStore = activation.handoffPolicyTrustStore();
        final AtomicReference<SourcePosition> trustPosition = new AtomicReference<>();
        final ProfileCatalogManagedNativeEligibilityAuthority authority =
                new ProfileCatalogManagedNativeEligibilityAuthority(
                        catalog,
                        activation.handoffPolicyAuthority(),
                        trustStore,
                        activation.artifacts(),
                        () -> Objects.requireNonNull(trustPosition.get(), "Managed Handoff source trust position"));
        return new ManagedHandoffConfiguration(activation, identity, trustStore, trustPosition, authority);
    }

    private static ProfileCatalog profileCatalog(final PersistentStagingNativeCanaryIdentity.Identity identity) {
        return new ProfileCatalog() {
            @Override
            public ProfileSemanticEnvelope resolve(final ProfileRef reference) {
                if (identity.destination().ref().equals(reference)) {
                    return identity.destination();
                }
                return identity.capability().ref().equals(reference) ? identity.capability() : null;
            }

            @Override
            public com.nereusstream.delay.protocol.CredentialBinding resolveBinding(
                    final ProfileRef profile, final long secretGeneration) {
                return null;
            }

            @Override
            public com.nereusstream.delay.protocol.CredentialBindingHead resolveHead(final ProfileRef profile) {
                return null;
            }

            @Override
            public com.nereusstream.delay.protocol.CredentialBindingProtection resolveProtection(
                    final ProfileRef profile, final long secretGeneration) {
                return null;
            }
        };
    }

    private static void requirePhysicalTestingEnvironment() {
        final String classification = System.getenv("NEREUS_DELAY_ENVIRONMENT_CLASSIFICATION");
        if (!"DISPOSABLE_LOCAL".equals(classification) && !"STAGING".equals(classification)) {
            throw new IllegalStateException(
                    "real Worker physical testing requires environment classification DISPOSABLE_LOCAL or STAGING");
        }
    }

    private static boolean isPersistentStaging() {
        return "STAGING".equals(System.getenv("NEREUS_DELAY_ENVIRONMENT_CLASSIFICATION"));
    }

    private static boolean hasManagedHandoffCanary() {
        return "1".equals(System.getenv("NEREUS_DELAY_PULSAR_WORKER_MANAGED_HANDOFF"));
    }

    private static WorkerAuthorityResources openWorkerAuthorityResources() throws Exception {
        final OxiaSyncOwnerLeaseBackend.ClientHandle oxia = connectOxiaIfConfigured();
        try {
            final PersistentStagingActivation.Loaded activation =
                    hasManagedHandoffCanary() ? PersistentStagingActivation.loadFromEnvironment() : null;
            return new WorkerAuthorityResources(oxia, activation);
        } catch (Exception | Error failure) {
            if (oxia != null) {
                oxia.close();
            }
            throw failure;
        }
    }

    private record WorkerAuthorityResources(
            OxiaSyncOwnerLeaseBackend.ClientHandle oxia, PersistentStagingActivation.Loaded persistentActivation)
            implements Closeable {
        @Override
        public void close() throws IOException {
            IOException failure = null;
            if (persistentActivation != null) {
                try {
                    persistentActivation.close();
                } catch (IOException closeFailure) {
                    failure = closeFailure;
                }
            }
            if (oxia != null) {
                try {
                    oxia.close();
                } catch (IOException closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    static byte[] attemptJournalIncarnation() {
        return Bytes.copy(JOURNAL_INCARNATION);
    }

    static long attemptJournalCreationTimestamp() {
        return JOURNAL_CREATION_TIMESTAMP;
    }

    private static ScheduleResolver scheduleResolver(
            final String destinationPhysicalTopic, final ManagedHandoffConfiguration managedHandoff) {
        if (destinationPhysicalTopic != null) {
            return new ScheduleResolver() {
                @Override
                public ResolvedSchedule resolveSchedule(
                        final ShardId shard,
                        final DelayMessageId message,
                        final CanonicalScheduleIntent intent,
                        final com.nereusstream.delay.protocol.SourcePosition source) {
                    final ProfileRef capability = managedHandoff == null
                            ? capabilityProfile()
                            : managedHandoff.identity().capability().ref();
                    final byte[] tenantScope = managedHandoff == null
                            ? bytes(32, 61)
                            : managedHandoff.identity().tenantRouteScopeDigest();
                    final byte[] tuple =
                            canonicalLaneTuple(tenantScope, destinationPhysicalTopic, intent.profile(), capability, 0);
                    final Long actionAt = managedHandoff == null
                            ? null
                            : Math.subtractExact(intent.deliverAtEpochMs(), managedHandoff.effectiveLeadMs());
                    return new ResolvedSchedule(
                            DestinationLaneId.derive(tuple), tuple, intent.inlinePayload(), null, actionAt);
                }

                @Override
                public ResolvedPrepare resolvePrepare(
                        final ShardId shard,
                        final DelayMessageId message,
                        final com.nereusstream.delay.protocol.PrepareLargeScheduleBody body,
                        final com.nereusstream.delay.protocol.SourcePosition source) {
                    final ProfileRef capability = managedHandoff == null
                            ? capabilityProfile()
                            : managedHandoff.identity().capability().ref();
                    final byte[] tenantScope = managedHandoff == null
                            ? bytes(32, 61)
                            : managedHandoff.identity().tenantRouteScopeDigest();
                    final byte[] tuple = canonicalLaneTuple(
                            tenantScope,
                            destinationPhysicalTopic,
                            body.intentWithoutPayload().profile(),
                            capability,
                            0);
                    return new ResolvedPrepare(DestinationLaneId.derive(tuple), tuple);
                }
            };
        }
        final byte[] tuple = Bytes.utf8("pulsar-worker-canonical-lane-tuple");
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
        return workClasses(1_000_000);
    }

    static WorkClassExecutionRegistry workClasses(final long workClassBytes) {
        if (workClassBytes <= 0) {
            throw new IllegalArgumentException("Pulsar Worker work-class bytes must be positive");
        }
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
                            workClassBytes,
                            1,
                            workClassBytes,
                            workClassBytes,
                            protectedClass ? 1 : 0,
                            protectedClass ? 1 : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(
                new WorkClassRuntimeConfig(
                        policies, TimeUnit.SECONDS.toNanos(5), TimeUnit.SECONDS.toNanos(30), 16, 8_000_000),
                System::nanoTime);
    }

    private static void createTopic(
            final HttpClient client, final String adminUrl, final String topic, final boolean allowExisting)
            throws Exception {
        createTopic(client, adminUrl, topic, allowExisting, INCARNATION, CREATION_TIMESTAMP);
    }

    private static void createTopic(
            final HttpClient client,
            final String adminUrl,
            final String topic,
            final boolean allowExisting,
            final byte[] incarnation,
            final long creationTimestamp)
            throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic;
        final String body = "{\"nereus.resource.guard.version\":\"1\","
                + "\"nereus.resource.incarnation\":\""
                + Base64.getUrlEncoder().withoutPadding().encodeToString(incarnation) + "\","
                + "\"nereus.resource.created-at\":\""
                + Long.toUnsignedString(creationTimestamp) + "\"}";
        for (int attempt = 0; attempt < 120; attempt++) {
            final HttpResponse<String> response;
            try {
                response = request(client, path, "PUT", body);
            } catch (java.io.IOException failure) {
                if (attempt == 119) {
                    throw failure;
                }
                TimeUnit.MILLISECONDS.sleep(500L);
                continue;
            }
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return;
            }
            if (allowExisting && response.statusCode() == 409) {
                return;
            }
            if (response.statusCode() != 409 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw failure("create topic", response);
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
                System.err.println("Pulsar Worker smoke cleanup could not delete topic: " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("Pulsar Worker smoke cleanup failed: " + failure.getMessage());
        }
    }

    private static HttpResponse<String> request(
            final HttpClient client, final String path, final String method, final String body) throws Exception {
        return PulsarClientArtifactAdminHttp.request(client, path, method, body);
    }

    private static IllegalStateException failure(final String operation, final HttpResponse<String> response) {
        return new IllegalStateException(
                operation + " failed with HTTP " + response.statusCode() + ": " + response.body());
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
