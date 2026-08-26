package com.nereusstream.delay.transport;

import com.google.protobuf.ByteString;
import com.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import com.nereusstream.delay.adapter.S3CompatiblePayloadObjectStore;
import com.nereusstream.delay.gateway.GatewayGrpcContext;
import com.nereusstream.delay.gateway.GatewayGrpcServer;
import com.nereusstream.delay.gateway.GatewayIngressService;
import com.nereusstream.delay.gateway.GatewayPayloadIngressService;
import com.nereusstream.delay.gateway.GatewayPayloadStoreAuthority;
import com.nereusstream.delay.gateway.GatewayScheduleService;
import com.nereusstream.delay.gateway.MutualTlsJwtGatewayTenantAuthority;
import com.nereusstream.delay.gateway.OxiaGatewayAdmissionController;
import com.nereusstream.delay.gateway.OxiaGatewayAuditSink;
import com.nereusstream.delay.gateway.OxiaGatewayIdempotencyStore;
import com.nereusstream.delay.gateway.RsaSha256GatewayJwtVerifier;
import com.nereusstream.delay.gateway.wire.DelayGatewayGrpc;
import com.nereusstream.delay.gateway.wire.GatewayAttestPayloadUploadRequest;
import com.nereusstream.delay.gateway.wire.GatewayCommitLargeScheduleRequest;
import com.nereusstream.delay.gateway.wire.GatewayIssuePayloadUploadHandleRequest;
import com.nereusstream.delay.gateway.wire.GatewayPayloadAttestationResponse;
import com.nereusstream.delay.gateway.wire.GatewayPayloadUploadHandleResponse;
import com.nereusstream.delay.gateway.wire.GatewayPrepareLargeScheduleRequest;
import com.nereusstream.delay.gateway.wire.GatewayRouteSelector;
import com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome;
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
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceReplayCursor;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.WorkerAssignment;
import com.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import com.nereusstream.delay.ownership.WorkerAssignmentCoordinator;
import com.nereusstream.delay.ownership.WorkerShardFleetRuntime;
import com.nereusstream.delay.ownership.WorkerShardRuntime;
import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CompatibleControlSnapshot;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.EnqueueOutcomeKind;
import com.nereusstream.delay.protocol.IngressCredentialBindingRef;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.KafkaIngressRouteResource;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.ObjectStoreProviderKind;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadAttestationOutcome;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadProofTrustSetActivatePayload;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.PayloadProofVerifierKey;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleOutcome;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteLifecycle;
import com.nereusstream.delay.protocol.RoutePartitionPolicy;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.RoutingHashVersion;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.StableError;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.route.OxiaRouteAuthoritySession;
import com.nereusstream.delay.route.OxiaSignedRouteSnapshotProvider;
import com.nereusstream.delay.route.OxiaSignedRouteSnapshotPublisher;
import com.nereusstream.delay.route.RouteHash;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.InMemoryPayloadProofTrustSetCatalog;
import com.nereusstream.delay.runtime.LaneRecord;
import com.nereusstream.delay.runtime.MessageRecord;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.runtime.PayloadReservation;
import com.nereusstream.delay.runtime.PayloadReservationStatus;
import com.nereusstream.delay.runtime.ScheduleResolver;
import com.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.DefaultDelaySemanticCore;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.semantic.SecureLogicalUuidV7Generator;
import com.nereusstream.delay.store.CheckpointFileInventory;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerLoadVector;
import com.nereusstream.delay.store.WorkerPlacementPolicy;
import com.nereusstream.delay.submission.DefaultSubmissionCoordinator;
import com.nereusstream.delay.submission.KafkaManagedSubmissionOutcomeProjector;
import com.nereusstream.delay.submission.RouteBoundSubmissionTransportPlanResolver;
import com.nereusstream.delay.submission.SubmissionOutcomeProjectorRegistry;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.GuardedConsumerRecords;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Opt-in production-authority vertical proof for the large-payload path.
 *
 * <p>The process requires real Kafka, Oxia, Gateway certificates and a
 * versioned S3-compatible Object Store. It publishes the source-ordered trust
 * marker before the Route barrier, then drives Prepare, upload/attest and
 * Commit through the authenticated Gateway while a real Worker consumes the
 * same Kafka Shard Log.</p>
 */
public final class KafkaClientArtifactLargePayloadGatewaySmoke {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(250);
    private static final long LEASE_DURATION_MS = 60_000;
    private static final long PAYLOAD_BYTES = (1L << 20) + 4_096;
    private static final long LARGE_PAYLOAD_WORK_CLASS_BYTES = 2_000_000;

    private KafkaClientArtifactLargePayloadGatewaySmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <large-payload-topic>");
        }
        final String bootstrap = arguments[0];
        final String topic = arguments[1] + "-" + UUID.randomUUID();
        final String destinationPhysicalTopic =
                configuredNullable("NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC");
        final String receiptPhysicalTopic =
                destinationPhysicalTopic == null ? null : destinationPhysicalTopic + "-receipt";
        final String oxiaEndpoint = requiredEnv("NEREUS_DELAY_OXIA_ENDPOINT");
        final String minioEndpoint = requiredEnv("NEREUS_DELAY_MINIO_ENDPOINT");
        final String minioAccessKey = requiredEnv("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String minioSecretKey = requiredEnv("NEREUS_DELAY_MINIO_SECRET_KEY");
        final String minioBucket = requiredEnv("NEREUS_DELAY_MINIO_BUCKET");
        final String minioRegion = configured("NEREUS_DELAY_MINIO_REGION", "us-east-1");
        final Duration minioRequestTimeout = configuredDuration("NEREUS_DELAY_MINIO_REQUEST_TIMEOUT_MS", 60_000);
        final String minioFaultMode = configured("NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_MODE", "NONE");
        final boolean expectPrecommitFailure =
                switch (minioFaultMode) {
                    case "NONE", "PUT_503_AFTER_COMMIT", "PUT_TIMEOUT_AFTER_COMMIT" -> false;
                    case "PUT_503_BEFORE_COMMIT", "PUT_TIMEOUT_BEFORE_COMMIT" -> true;
                    default ->
                        throw new IllegalArgumentException(
                                "unsupported large-payload MinIO fault mode: " + minioFaultMode);
                };
        final String minioSessionToken = configuredNullable("NEREUS_DELAY_MINIO_SESSION_TOKEN");
        final Path serverCertificate = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_CERT");
        final Path serverPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_KEY");
        final Path trustedClientCertificates = requiredPath("NEREUS_DELAY_GATEWAY_CA_CERT");
        final Path clientCertificate = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_CERT");
        final Path clientPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_KEY");
        final int gatewayPort = Integer.parseInt(requiredEnv("NEREUS_DELAY_GATEWAY_PORT"));
        if (gatewayPort <= 0 || gatewayPort > 65_535) {
            throw new IllegalArgumentException("NEREUS_DELAY_GATEWAY_PORT must be 1..65535");
        }

        final URI minioUri = URI.create(minioEndpoint);
        final AuthenticatedTenantContext tenant =
                new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
        final KeyPair proofKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final long proofNow = System.currentTimeMillis();
        final PayloadProofVerifierKey proofVerifierKey = PayloadProofVerifierKey.fromPublicKey(
                7, proofKeys.getPublic(), Math.max(0, proofNow - 60_000), proofNow + 3_600_000);
        final PayloadProofTrustSetSemantic trustSet = new PayloadProofTrustSetSemantic(1, List.of(proofVerifierKey));
        final ProfileSemanticEnvelope objectStoreProfile =
                objectStoreProfile(minioUri, minioRegion, minioBucket, minioAccessKey);
        final byte[] payload = payload();
        final byte[] payloadHash = Bytes.sha256(payload);
        final KeyPair controlKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        if ("1".equals(configured("NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD", "0"))) {
            runMultiShard(
                    bootstrap,
                    arguments[1],
                    destinationPhysicalTopic,
                    receiptPhysicalTopic,
                    oxiaEndpoint,
                    minioUri,
                    minioRegion,
                    minioBucket,
                    minioAccessKey,
                    minioSecretKey,
                    minioSessionToken,
                    minioRequestTimeout,
                    minioFaultMode,
                    serverCertificate,
                    serverPrivateKey,
                    trustedClientCertificates,
                    clientCertificate,
                    clientPrivateKey,
                    gatewayPort,
                    tenant,
                    proofKeys,
                    trustSet,
                    objectStoreProfile,
                    payload,
                    payloadHash,
                    controlKeys);
            return;
        }
        final RouteIncarnation routeIncarnation = RouteIncarnation.random();
        final ShardId shard = new ShardId(routeIncarnation, 0);
        final RouteSelectionHint routeHint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
        final SystemMutation trustActivation = trustActivation(shard, trustSet.ref(), tenant, controlKeys);
        final PreparedCommand beforeRoute = command(shard, "large-payload-before-route");
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                10_000);

        try (Admin admin = Admin.create(adminConfiguration)) {
            ensureTopic(admin, topic);
            if (destinationPhysicalTopic != null) {
                ensureTopic(admin, destinationPhysicalTopic);
                ensureTopic(admin, receiptPhysicalTopic);
            }
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final UUID nativeTopicId = toUuid(topicId);
            final UUID destinationTopicId = destinationPhysicalTopic == null
                    ? null
                    : toUuid(describe(admin, destinationPhysicalTopic).topicId());
            final UUID receiptTopicId = receiptPhysicalTopic == null
                    ? null
                    : toUuid(describe(admin, receiptPhysicalTopic).topicId());
            appendFrame(bootstrap, clusterId, topic, topicId, trustActivation.encodeFrame(), 0);
            appendFrame(bootstrap, clusterId, topic, topicId, CommandCodec.encodeManagedFrame(beforeRoute), 1);

            final org.apache.kafka.clients.consumer.GuardedFetchEvidence fetchEvidence =
                    fetchEvidence(bootstrap, clusterId, topic, nativeTopicId, shard);
            if (fetchEvidence.firstRecordOffset() != 0
                    || fetchEvidence.lastRecordOffset() != 1
                    || fetchEvidence.lastStableOffset() < 2) {
                throw new IllegalStateException(
                        "Kafka large-payload barrier proof did not cover activation and pre-route records: "
                                + fetchEvidence);
            }
            final long barrierOffset = 2;
            final RouteSnapshot snapshot = routeSnapshot(
                    clusterId, topic, nativeTopicId, routeIncarnation, fetchEvidence, tenant, controlKeys);
            final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
            final String routePrefix = "nereus-delay/kafka-large-payload-route/" + UUID.randomUUID();
            final String assignmentPrefix = "nereus-delay/kafka-large-payload-assignment/" + UUID.randomUUID();
            final String gatewayPrefix = "nereus-delay/kafka-large-payload-gateway/" + UUID.randomUUID();

            try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                            oxiaEndpoint,
                            namespace,
                            "nereus-delay-large-route-publisher-" + UUID.randomUUID(),
                            Duration.ofSeconds(15),
                            routePrefix);
                    OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                            oxiaEndpoint,
                            namespace,
                            "nereus-delay-large-route-provider-" + UUID.randomUUID(),
                            Duration.ofSeconds(15),
                            routePrefix);
                    OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle =
                            OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                    oxiaEndpoint,
                                    namespace,
                                    "nereus-delay-large-assignment-" + UUID.randomUUID(),
                                    Duration.ofSeconds(15),
                                    assignmentPrefix);
                    OxiaSyncOwnerLeaseBackend.ClientHandle admissionHandle = OxiaSyncOwnerLeaseBackend.connectUnchecked(
                            oxiaEndpoint,
                            namespace,
                            "nereus-delay-large-admission-" + UUID.randomUUID(),
                            Duration.ofSeconds(15),
                            gatewayPrefix + "/admission-client");
                    OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyHandle =
                            OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                    oxiaEndpoint,
                                    namespace,
                                    "nereus-delay-large-idempotency-" + UUID.randomUUID(),
                                    Duration.ofSeconds(15),
                                    gatewayPrefix + "/idempotency-client");
                    OxiaSyncOwnerLeaseBackend.ClientHandle auditHandle = OxiaSyncOwnerLeaseBackend.connectUnchecked(
                            oxiaEndpoint,
                            namespace,
                            "nereus-delay-large-audit-" + UUID.randomUUID(),
                            Duration.ofSeconds(15),
                            gatewayPrefix + "/audit-client")) {
                final OxiaSignedRouteSnapshotPublisher publisher =
                        new OxiaSignedRouteSnapshotPublisher(publisherSession, routePrefix, controlKeys.getPublic());
                final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                        providerSession, routePrefix, controlKeys.getPublic(), System::currentTimeMillis);
                publisher.publish(routeHint, snapshot, 0);
                provider.refresh().toCompletableFuture().join();

                final WorkerAssignmentAuthority assignmentAuthority =
                        new OxiaSyncWorkerAssignmentBackend(assignmentHandle, assignmentPrefix);
                final RouteWorkerAssignmentCoordinator routeCoordinator = new RouteWorkerAssignmentCoordinator(
                        provider,
                        new WorkerAssignmentCoordinator(
                                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)),
                                assignmentAuthority));
                final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement =
                        routeCoordinator.placeActive(tenant, routeHint, placementRequest(System.currentTimeMillis()));
                final WorkerAssignment accepted = routeCoordinator.requireAccepted(
                        tenant,
                        placement.publication().revision(),
                        placement.publication().assignment());
                requireRouteAssignment(accepted, snapshot, clusterId, nativeTopicId, barrierOffset);

                final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(assignmentHandle.backend());
                final OwnerLease lease = ownerAuthority
                        .acquire(
                                accepted.sourceAssignment(),
                                "kafka-large-payload-worker",
                                assignmentHandle.sessionIdentity(),
                                System.currentTimeMillis(),
                                LEASE_DURATION_MS)
                        .orElseThrow();
                final WorkClassExecutionRegistry workClasses = workClasses();
                final CompatibleControlSnapshot controlSnapshot = controlSnapshot(shard, destinationProfile());
                final Path root = Files.createTempDirectory("nereus-delay-kafka-large-payload-");
                boolean assignmentWithdrawn = false;
                try {
                    final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
                    try (SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                            ShardStore store = ShardStore.open(storeConfig, shard, resources);
                            InMemoryCommandTransportRegistry transports = new InMemoryCommandTransportRegistry()) {
                        resources.bindWorkClassExecutionRegistry(workClasses);
                        store.recordControlSnapshot(controlSnapshot);
                        final InMemoryPayloadProofTrustSetCatalog trustCatalog =
                                new InMemoryPayloadProofTrustSetCatalog();
                        trustCatalog.publish(trustSet);
                        final ScheduleResolver resolver = destinationPhysicalTopic == null
                                ? scheduleResolver()
                                : scheduleResolver(clusterId, destinationTopicId, destinationPhysicalTopic);
                        final DelayShard delayShard =
                                new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver, trustCatalog);
                        final com.nereusstream.delay.protocol.OwnerIdentity ownerIdentity =
                                new com.nereusstream.delay.protocol.OwnerIdentity(
                                        bytes(16, 70),
                                        bytes(16, 71),
                                        lease.ownerEpoch(),
                                        Bytes.sha256(Bytes.utf8("large-payload-worker-fence")));
                        final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease, ownerIdentity);

                        final List<SourceReplayEntry> recoveryEntries = recoveryEntries(
                                bootstrap,
                                clusterId,
                                topic,
                                nativeTopicId,
                                accepted.sourceAssignment(),
                                trustActivation,
                                beforeRoute);
                        recover(
                                accepted,
                                ownerAuthority,
                                ownedShard,
                                recoveryEntries,
                                controlKeys,
                                controlSnapshot,
                                workClasses);
                        if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                || !(ownedShard.lastCatchupPosition() instanceof KafkaSourcePosition position)
                                || position.offset() != 1) {
                            throw new IllegalStateException(
                                    "large-payload Worker recovery did not apply the source-ordered pre-route records");
                        }

                        final KafkaCommandTransportKey transportKey = new KafkaCommandTransportKey(
                                clusterId,
                                topic,
                                nativeTopicId,
                                0,
                                new CredentialBindingKey(1, new Digest32(bytes(32, 41)), new Digest32(bytes(32, 42))));
                        final KafkaProducer<byte[], byte[]> gatewayProducer = kafkaProducer(bootstrap);
                        transports.register(new ProductionKafkaProduceTransport(
                                transportKey,
                                new ProductionKafkaProduceTransport.Configuration(
                                        -1, true, true, "large-payload-gateway-kafka-client"),
                                new KafkaClientArtifactProduceTransport(
                                        (GuardedProducer<byte[], byte[]>) gatewayProducer)));
                        final DefaultSubmissionCoordinator submissions = new DefaultSubmissionCoordinator(
                                new RouteBoundSubmissionTransportPlanResolver(provider, System::currentTimeMillis),
                                transports,
                                SubmissionOutcomeProjectorRegistry.of(
                                        new KafkaManagedSubmissionOutcomeProjector(transportKey)));
                        final DefaultDelaySemanticCore core = new DefaultDelaySemanticCore(
                                provider, new SecureLogicalUuidV7Generator(), System::currentTimeMillis);
                        final S3CompatiblePayloadObjectStore payloadStore = new S3CompatiblePayloadObjectStore(
                                objectStoreProfile,
                                minioUri,
                                minioRegion,
                                minioBucket,
                                minioAccessKey,
                                minioSecretKey,
                                minioSessionToken,
                                tenant.tenantRoutingScope(),
                                trustSet,
                                7,
                                Long.MAX_VALUE,
                                proofKeys.getPrivate(),
                                null,
                                HttpClient.newBuilder()
                                        .connectTimeout(Duration.ofSeconds(10))
                                        .followRedirects(HttpClient.Redirect.NEVER)
                                        .build(),
                                Clock.systemUTC(),
                                minioRequestTimeout);

                        final OxiaGatewayAdmissionController admission = new OxiaGatewayAdmissionController(
                                admissionHandle,
                                gatewayPrefix + "/admission",
                                System::currentTimeMillis,
                                new OxiaGatewayAdmissionController.Limits(4, 8_000_000, 2, 2, 30_000, 8));
                        final OxiaGatewayIdempotencyStore idempotency = new OxiaGatewayIdempotencyStore(
                                idempotencyHandle,
                                gatewayPrefix + "/idempotency",
                                System::currentTimeMillis,
                                10_000,
                                30_000);
                        final OxiaGatewayAuditSink audit =
                                new OxiaGatewayAuditSink(auditHandle, gatewayPrefix + "/audit");
                        final GatewayScheduleService schedule =
                                new GatewayScheduleService(core, idempotency, submissions, System::currentTimeMillis);
                        final KeyPair jwtKeys = gatewayJwtKeys();
                        final com.nereusstream.delay.gateway.MutualTlsJwtGatewayTenantAuthority tenantAuthority =
                                new MutualTlsJwtGatewayTenantAuthority(new RsaSha256GatewayJwtVerifier(
                                        jwtKeys.getPublic(),
                                        "nereus-delay-gateway-e2e-issuer",
                                        "nereus-delay-gateway-e2e",
                                        "gateway-e2e-key",
                                        Clock.systemUTC(),
                                        30,
                                        600));
                        final GatewayIngressService ingress = new GatewayIngressService(
                                schedule, tenantAuthority, admission, audit, System::currentTimeMillis);
                        final GatewayPayloadStoreAuthority payloadAuthority = new GatewayPayloadStoreAuthority(
                                tenant.tenantRoutingScope(),
                                (receipt, kind, now) -> payloadStore.issueUploadHandle(receipt, kind, now),
                                (receipt, handle, now) -> payloadStore.attest(receipt, handle, now));
                        final GatewayPayloadIngressService payloadIngress = new GatewayPayloadIngressService(
                                payloadAuthority, tenantAuthority, admission, audit, System::currentTimeMillis);
                        final GatewayGrpcServer server = GatewayGrpcServer.mutualTls(
                                gatewayPort,
                                serverCertificate,
                                serverPrivateKey,
                                trustedClientCertificates,
                                new com.nereusstream.delay.gateway.GatewayGrpcService(
                                        ingress, GatewayGrpcContext.provider(), payloadIngress));
                        final String token = token(jwtKeys, tenant, certificateFingerprint(clientCertificate));
                        WorkerShardRuntime runtime = null;
                        KafkaClientArtifactWorkerSmoke.PhysicalPublishBridge physicalBridge = null;
                        boolean runtimeClosed = false;
                        try {
                            server.start();
                            final ManagedChannel channel = channel(
                                    gatewayPort, trustedClientCertificates, clientCertificate, clientPrivateKey);
                            try {
                                final DelayGatewayGrpc.DelayGatewayBlockingStub gateway = stub(channel, token);
                                final CanonicalScheduleIntent intent = largeScheduleIntent(System.currentTimeMillis());
                                final GatewayPrepareLargeScheduleRequest prepareRequest = prepareRequest(
                                        intent, payload.length, payloadHash, trustSet.ref(), objectStoreProfile.ref());
                                final GatewaySubmissionOutcome prepareResponse =
                                        gateway.prepareLargeSchedule(prepareRequest);
                                final CanonicalCommandQueuedReceipt prepareReceipt =
                                        requireQueued(prepareResponse, "PrepareLargeSchedule", barrierOffset);
                                final KafkaSourcePosition preparePosition =
                                        (KafkaSourcePosition) prepareReceipt.sourcePosition();
                                final byte[] reservationId = reservationId(prepareReceipt);
                                final DestinationLaneId physicalLaneId = destinationPhysicalTopic == null
                                        ? null
                                        : laneId(clusterId, destinationTopicId, destinationPhysicalTopic);
                                runtime = KafkaClientArtifactWorkerSourceFactory.create(
                                        workerConsumer(
                                                bootstrap,
                                                "nereus-delay-large-worker-" + UUID.randomUUID(),
                                                clusterId,
                                                topic,
                                                nativeTopicId,
                                                shard),
                                        topic,
                                        POLL_TIMEOUT,
                                        accepted.sourceAssignment(),
                                        workClasses,
                                        ownedShard,
                                        store,
                                        resources,
                                        ownerAuthority,
                                        controlKeys.getPublic(),
                                        null,
                                        null,
                                        null,
                                        null,
                                        null);
                                requireApplied(runUntilApplied(runtime), "PrepareLargeSchedule");
                                final PayloadReservation reservation = Optional.ofNullable(
                                                delayShard.getReservation(reservationId))
                                        .orElseThrow(() -> new IllegalStateException(
                                                "Worker did not persist the payload reservation"));
                                if (reservation.status() != PayloadReservationStatus.RESERVED) {
                                    throw new IllegalStateException(
                                            "Prepare did not leave a RESERVED payload reservation: "
                                                    + reservation.status());
                                }
                                payloadStore.register(reservation, trustSet.ref(), objectStoreProfile.ref());
                                final PayloadReservationReceipt receipt = payloadStore.reservationReceipt(reservation);

                                final GatewayPayloadUploadHandleResponse handleResponse =
                                        gateway.issuePayloadUploadHandle(
                                                GatewayIssuePayloadUploadHandleRequest.newBuilder()
                                                        .setPayloadReservationReceipt(
                                                                ByteString.copyFrom(receipt.payload()))
                                                        .setUploadHandleKind(
                                                                UploadHandleKind.OPAQUE_SINGLE_PUT.wireValue())
                                                        .build());
                                final PayloadUploadHandleResponse handleDomain =
                                        PayloadUploadHandleResponse.decode(handleResponse
                                                .getPayloadUploadHandleResponse()
                                                .toByteArray());
                                if (handleDomain.outcome() != PayloadUploadHandleOutcome.ISSUED) {
                                    throw new IllegalStateException("Gateway did not issue the payload upload handle: "
                                            + handleDomain.outcome());
                                }
                                final OpaquePayloadUploadHandle handle = handleDomain.issued();
                                try {
                                    payloadStore.upload(receipt, handle, payload, System.currentTimeMillis());
                                } catch (RuntimeException failure) {
                                    if (!expectPrecommitFailure) {
                                        throw failure;
                                    }
                                    requirePrecommitFailure(failure, minioFaultMode);
                                    final PayloadReservation retained = Optional.ofNullable(
                                                    delayShard.getReservation(reservationId))
                                            .orElseThrow();
                                    if (retained.status() != PayloadReservationStatus.RESERVED
                                            || delayShard.getMessage(prepareReceipt
                                                            .command()
                                                            .delayMessageId())
                                                    != null
                                            || latestOffset(admin, topic, 0) != barrierOffset + 1) {
                                        throw new IllegalStateException(
                                                "Kafka pre-commit payload failure crossed the Commit boundary");
                                    }
                                    final PayloadAttestationResponse absent =
                                            payloadStore.attest(receipt, handle, System.currentTimeMillis());
                                    if (absent.outcome() != PayloadAttestationOutcome.OBJECT_NOT_READY_RETRYABLE) {
                                        throw new IllegalStateException(
                                                "Kafka pre-commit payload failure did not leave the object absent: "
                                                        + absent.outcome());
                                    }
                                    final Path checkpointPath = root.resolve("large-payload-precommit-checkpoint");
                                    final byte[] checkpointId = Arrays.copyOf(
                                            Bytes.sha256(Bytes.utf8("large-payload-precommit-checkpoint")), 16);
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
                                            || !ownerAuthority.current(shard).isEmpty()) {
                                        throw new IllegalStateException(
                                                "Kafka pre-commit payload failure did not drain and release the Owner");
                                    }
                                    runtime.close();
                                    runtimeClosed = true;
                                    if (!assignmentAuthority.withdraw(placement.publication())) {
                                        throw new IllegalStateException(
                                                "Kafka pre-commit Worker assignment was not withdrawn exactly");
                                    }
                                    assignmentWithdrawn = true;
                                    System.out.println(
                                            "Kafka + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker "
                                                    + "+ MinIO large-payload pre-commit fail-closed E2E passed: fault="
                                                    + minioFaultMode + ", Prepare retained RESERVED, Commit absent, "
                                                    + "payload object absent, owner released");
                                    return;
                                }
                                final GatewayPayloadAttestationResponse attestationResponse =
                                        gateway.attestPayloadUpload(GatewayAttestPayloadUploadRequest.newBuilder()
                                                .setPayloadReservationReceipt(ByteString.copyFrom(receipt.payload()))
                                                .setOpaquePayloadUploadHandle(
                                                        ByteString.copyFrom(handle.canonicalBytes()))
                                                .build());
                                final PayloadAttestationResponse attestation =
                                        PayloadAttestationResponse.decode(attestationResponse
                                                .getPayloadAttestationResponse()
                                                .toByteArray());
                                if (attestation.outcome() != PayloadAttestationOutcome.ATTESTED
                                        || attestation.proof() == null
                                        || new String(
                                                        attestation.proof().immutableObjectVersion(),
                                                        StandardCharsets.UTF_8)
                                                .startsWith("sha256-")) {
                                    throw new IllegalStateException(
                                            "Gateway/MinIO did not return a provider-issued immutable payload proof");
                                }
                                final CanonicalPayloadCommitProof proof = attestation.proof();
                                final GatewayCommitLargeScheduleRequest commitRequest = commitRequest(receipt, proof);
                                final GatewaySubmissionOutcome commitResponse =
                                        gateway.commitLargeSchedule(commitRequest);
                                final CanonicalCommandQueuedReceipt commitReceipt =
                                        requireQueued(commitResponse, "CommitLargeSchedule", barrierOffset + 1);
                                final KafkaSourcePosition commitPosition =
                                        (KafkaSourcePosition) commitReceipt.sourcePosition();
                                requireApplied(runUntilApplied(runtime), "CommitLargeSchedule");
                                final PayloadReservation committed = Optional.ofNullable(
                                                delayShard.getReservation(reservationId))
                                        .orElseThrow();
                                final MessageRecord message = Optional.ofNullable(delayShard.getMessage(
                                                prepareReceipt.command().delayMessageId()))
                                        .orElseThrow();
                                if (committed.status() != PayloadReservationStatus.COMMITTED
                                        || message.status() != MessageStatus.SCHEDULED
                                        || message.payloadReference() == null
                                        || !Arrays.equals(
                                                message.payloadReference().immutableObjectVersion(),
                                                proof.immutableObjectVersion())
                                        || !Arrays.equals(
                                                message.payloadReference().proofId(), proof.proofId())) {
                                    throw new IllegalStateException(
                                            "Worker did not persist the exact committed Object Store reference");
                                }
                                final byte[] objectPayload = payloadStore.readPayload(message.payloadReference());
                                if (!Arrays.equals(objectPayload, payload)) {
                                    throw new IllegalStateException(
                                            "Worker Object Store readback did not match the committed payload");
                                }
                                if (!commitReceipt
                                        .command()
                                        .commandType()
                                        .name()
                                        .equals("COMMIT_LARGE_SCHEDULE")) {
                                    throw new IllegalStateException(
                                            "Commit receipt does not identify COMMIT_LARGE_SCHEDULE");
                                }
                                awaitConfiguredBrokerCut();
                                if (physicalLaneId != null) {
                                    final LaneRecord physicalLane = Optional.ofNullable(
                                                    delayShard.getLane(physicalLaneId))
                                            .orElseThrow(() -> new IllegalStateException(
                                                    "Worker did not persist the physical destination Lane"));
                                    physicalBridge = KafkaClientArtifactWorkerSmoke.createPhysicalPublishBridge(
                                            bootstrap,
                                            clusterId,
                                            topic,
                                            nativeTopicId,
                                            shard,
                                            preparePosition,
                                            destinationPhysicalTopic,
                                            destinationTopicId,
                                            receiptPhysicalTopic,
                                            receiptTopicId,
                                            store,
                                            ownedShard,
                                            ownerIdentity,
                                            ownerAuthority,
                                            workClasses,
                                            controlKeys,
                                            destinationProfile(),
                                            capabilityProfile(),
                                            physicalLaneId,
                                            physicalLane.laneIncarnation(),
                                            LARGE_PAYLOAD_WORK_CLASS_BYTES);
                                    runtime.bindPhysicalPublishExecutor(physicalBridge.executor());
                                }
                                if (physicalBridge != null) {
                                    KafkaClientArtifactWorkerSmoke.runSourceAppliedPhysicalPublish(
                                            runtime,
                                            delayShard,
                                            ownedShard,
                                            ownerIdentity,
                                            ownerAuthority,
                                            store,
                                            workClasses,
                                            controlKeys,
                                            physicalBridge,
                                            prepareReceipt.command().delayMessageId(),
                                            commitPosition,
                                            objectPayload,
                                            bootstrap,
                                            clusterId,
                                            LARGE_PAYLOAD_WORK_CLASS_BYTES);
                                }
                                final byte[] duplicate = gateway.prepareLargeSchedule(prepareRequest)
                                        .toByteArray();
                                if (!Arrays.equals(prepareResponse.toByteArray(), duplicate)) {
                                    throw new IllegalStateException(
                                            "real Oxia Gateway idempotency did not return exact Prepare bytes");
                                }
                                final long latest = latestOffset(admin, topic, 0);
                                final long expectedLatest = barrierOffset + (physicalBridge == null ? 2 : 4);
                                if (latest != expectedLatest) {
                                    throw new IllegalStateException(
                                            "duplicate Prepare appended an unexpected Kafka record: latest=" + latest
                                                    + ", expected=" + expectedLatest);
                                }
                                final Path checkpointPath = root.resolve("large-payload-final-checkpoint");
                                final byte[] checkpointId = java.util.Arrays.copyOf(
                                        Bytes.sha256(Bytes.utf8("large-payload-final-checkpoint")), 16);
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
                                            "large-payload Worker drain did not publish the final checkpoint "
                                                    + "or release the owner lease");
                                }
                                runtime.close();
                                runtimeClosed = true;
                                if (!assignmentAuthority.withdraw(placement.publication())) {
                                    throw new IllegalStateException(
                                            "large-payload Worker assignment was not withdrawn exactly");
                                }
                                assignmentWithdrawn = true;
                                System.out.println("Kafka + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker "
                                        + "+ MinIO large-payload authority E2E passed: activationOffset=0, "
                                        + "barrierOffset=" + barrierOffset + ", prepareOffset="
                                        + prepareReceipt.sourcePosition() + ", commitOffset="
                                        + commitReceipt.sourcePosition()
                                        + ", providerVersion="
                                        + new String(proof.immutableObjectVersion(), StandardCharsets.UTF_8)
                                        + ", exactGatewayIdempotency=true");
                            } finally {
                                channel.shutdownNow();
                                channel.awaitTermination(10, TimeUnit.SECONDS);
                            }
                        } finally {
                            try {
                                if (!runtimeClosed && runtime != null) {
                                    try {
                                        runtime.close();
                                    } catch (RuntimeException cleanupFailure) {
                                        System.err.println("Kafka large-payload runtime cleanup deferred: "
                                                + cleanupFailure.getMessage());
                                    }
                                }
                            } finally {
                                if (physicalBridge != null) {
                                    physicalBridge.close();
                                }
                                server.close();
                            }
                        }
                    }
                } finally {
                    if (!assignmentWithdrawn) {
                        assignmentAuthority.withdraw(placement.publication());
                    }
                    deleteTree(root);
                    provider.close();
                }
            }
        }
    }

    /**
     * Runs the same authenticated Gateway/Object Store path over two source
     * shards admitted to one fair Worker fleet. The normal smoke remains the
     * single-shard production authority receipt; this opt-in path specifically
     * proves that Large Payload reservations do not collapse the Route/Owner
     * boundary back to partition zero.
     */
    private static void runMultiShard(
            final String bootstrap,
            final String topicPrefix,
            final String destinationPhysicalTopic,
            final String receiptPhysicalTopic,
            final String oxiaEndpoint,
            final URI minioUri,
            final String minioRegion,
            final String minioBucket,
            final String minioAccessKey,
            final String minioSecretKey,
            final String minioSessionToken,
            final Duration minioRequestTimeout,
            final String minioFaultMode,
            final Path serverCertificate,
            final Path serverPrivateKey,
            final Path trustedClientCertificates,
            final Path clientCertificate,
            final Path clientPrivateKey,
            final int gatewayPort,
            final AuthenticatedTenantContext tenant,
            final KeyPair proofKeys,
            final PayloadProofTrustSetSemantic trustSet,
            final ProfileSemanticEnvelope objectStoreProfile,
            final byte[] payload,
            final byte[] payloadHash,
            final KeyPair controlKeys)
            throws Exception {
        if (!"NONE".equals(minioFaultMode)) {
            throw new IllegalArgumentException(
                    "multi-shard large-payload mode currently requires MinIO fault mode NONE");
        }
        if (destinationPhysicalTopic == null || receiptPhysicalTopic == null) {
            throw new IllegalArgumentException("multi-shard large-payload mode requires a Kafka destination topic");
        }
        final int shardCount = 2;
        final String topic = topicPrefix + "-" + UUID.randomUUID();
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                10_000);

        try (Admin admin = Admin.create(adminConfiguration)) {
            ensureTopic(admin, topic, shardCount);
            ensureTopic(admin, destinationPhysicalTopic, shardCount);
            ensureTopic(admin, receiptPhysicalTopic, shardCount);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final UUID nativeTopicId = toUuid(topicId);
            final UUID destinationTopicId =
                    toUuid(describe(admin, destinationPhysicalTopic).topicId());
            final UUID receiptTopicId =
                    toUuid(describe(admin, receiptPhysicalTopic).topicId());
            final RouteIncarnation routeIncarnation = RouteIncarnation.random();
            final List<LargeShardProbe> probes = new ArrayList<>(shardCount);
            for (int partition = 0; partition < shardCount; partition++) {
                final ShardId shard = new ShardId(routeIncarnation, partition);
                final SystemMutation activation = trustActivation(shard, trustSet.ref(), tenant, controlKeys);
                final PreparedCommand beforeRoute = command(shard, "large-payload-multi-before-" + partition);
                appendFrame(bootstrap, clusterId, topic, topicId, activation.encodeFrame(), partition, 0);
                appendFrame(
                        bootstrap,
                        clusterId,
                        topic,
                        topicId,
                        CommandCodec.encodeManagedFrame(beforeRoute),
                        partition,
                        1);
                final org.apache.kafka.clients.consumer.GuardedFetchEvidence evidence =
                        fetchEvidence(bootstrap, clusterId, topic, nativeTopicId, shard);
                if (evidence.firstRecordOffset() != 0
                        || evidence.lastRecordOffset() != 1
                        || evidence.lastStableOffset() < 2) {
                    throw new IllegalStateException(
                            "Kafka multi-shard Large Payload barrier proof did not cover partition " + partition + ": "
                                    + evidence);
                }
                probes.add(new LargeShardProbe(shard, activation, beforeRoute, evidence, 2));
            }

            final RouteSelectionHint routeHint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
            final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
            final String routePrefix = "nereus-delay/kafka-large-payload-multi-route/" + UUID.randomUUID();
            final String assignmentPrefix = "nereus-delay/kafka-large-payload-multi-assignment/" + UUID.randomUUID();
            final String gatewayPrefix = "nereus-delay/kafka-large-payload-multi-gateway/" + UUID.randomUUID();
            try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                            oxiaEndpoint,
                            namespace,
                            "nereus-delay-large-multi-route-publisher-" + UUID.randomUUID(),
                            Duration.ofSeconds(15),
                            routePrefix);
                    OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                            oxiaEndpoint,
                            namespace,
                            "nereus-delay-large-multi-route-provider-" + UUID.randomUUID(),
                            Duration.ofSeconds(15),
                            routePrefix);
                    OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle =
                            OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                    oxiaEndpoint,
                                    namespace,
                                    "nereus-delay-large-multi-assignment-" + UUID.randomUUID(),
                                    Duration.ofSeconds(15),
                                    assignmentPrefix)) {
                final RouteSnapshot snapshot = multiRouteSnapshot(
                        clusterId, topic, nativeTopicId, routeIncarnation, probes, tenant, controlKeys);
                final OxiaSignedRouteSnapshotPublisher publisher =
                        new OxiaSignedRouteSnapshotPublisher(publisherSession, routePrefix, controlKeys.getPublic());
                final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                        providerSession, routePrefix, controlKeys.getPublic(), System::currentTimeMillis);
                final long routeRevision =
                        publisher.publish(routeHint, snapshot, 0).revision();
                provider.refresh().toCompletableFuture().join();

                final WorkerAssignmentAuthority assignmentAuthority =
                        new OxiaSyncWorkerAssignmentBackend(assignmentHandle, assignmentPrefix);
                final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(
                        provider,
                        new WorkerAssignmentCoordinator(
                                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)),
                                assignmentAuthority));
                final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(assignmentHandle.backend());
                final List<LargeShardAdmission> admissions = new ArrayList<>(shardCount);
                final Set<String> assignedWorkers = new HashSet<>();
                for (LargeShardProbe probe : probes) {
                    final int partition = probe.shard().partition();
                    final String workerId = "kafka-large-payload-worker-" + (char) ('a' + partition);
                    final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement = coordinator.placeActive(
                            tenant, routeHint, placementRequest(System.currentTimeMillis(), partition, workerId));
                    final WorkerAssignment accepted = coordinator.requireAccepted(
                            tenant,
                            placement.publication().revision(),
                            placement.publication().assignment());
                    requireRouteAssignment(accepted, snapshot, clusterId, nativeTopicId, probe.barrierOffset());
                    if (!workerId.equals(accepted.workerId()) || !assignedWorkers.add(accepted.workerId())) {
                        throw new IllegalStateException(
                                "Kafka multi-shard Large Payload placement did not retain a unique Worker: "
                                        + "partition=" + partition + ", expected=" + workerId + ", actual="
                                        + accepted.workerId());
                    }
                    final OwnerLease lease = ownerAuthority
                            .acquire(
                                    accepted.sourceAssignment(),
                                    workerId,
                                    assignmentHandle.sessionIdentity(),
                                    System.currentTimeMillis(),
                                    LEASE_DURATION_MS)
                            .orElseThrow();
                    admissions.add(new LargeShardAdmission(probe, placement.publication(), accepted, lease));
                }
                if (assignedWorkers.size() != shardCount) {
                    throw new IllegalStateException(
                            "Kafka multi-shard Large Payload placement did not span two Worker identities");
                }

                final WorkClassExecutionRegistry workClasses = workClasses();
                final long fleetWorkClassBytes = Math.multiplyExact((long) shardCount, LARGE_PAYLOAD_WORK_CLASS_BYTES);
                final ClaimExecutionAdmission claimAdmission =
                        new ClaimExecutionAdmission(shardCount, fleetWorkClassBytes);
                final DestinationPhysicalAdmission physicalAdmission =
                        new DestinationPhysicalAdmission(shardCount, fleetWorkClassBytes);
                physicalAdmission.registerTargetCluster(clusterId, shardCount, fleetWorkClassBytes);
                final Path root = Files.createTempDirectory("nereus-delay-kafka-large-payload-multi-");
                final List<ShardStore> stores = new ArrayList<>(shardCount);
                final List<DelayShard> delayShards = new ArrayList<>(shardCount);
                final List<WorkerShardRuntime> runtimes = new ArrayList<>(shardCount);
                final List<OwnedDelayShard> ownedShards = new ArrayList<>(shardCount);
                final List<com.nereusstream.delay.protocol.OwnerIdentity> ownerIdentities = new ArrayList<>(shardCount);
                final List<KafkaClientArtifactWorkerSmoke.PhysicalPublishBridge> physicalBridges =
                        new ArrayList<>(shardCount);
                WorkerShardFleetRuntime fleet = null;
                boolean assignmentsWithdrawn = false;
                try (SharedRocksDbResources resources = new SharedRocksDbResources(ShardStoreConfig.defaults(root));
                        InMemoryCommandTransportRegistry transports = new InMemoryCommandTransportRegistry()) {
                    resources.bindWorkClassExecutionRegistry(workClasses);
                    final InMemoryPayloadProofTrustSetCatalog trustCatalog = new InMemoryPayloadProofTrustSetCatalog();
                    trustCatalog.publish(trustSet);
                    for (LargeShardAdmission admission : admissions) {
                        final LargeShardProbe probe = admission.probe();
                        final ShardStore store =
                                ShardStore.open(ShardStoreConfig.defaults(root), probe.shard(), resources);
                        stores.add(store);
                        store.recordControlSnapshot(controlSnapshot(probe.shard(), destinationProfile()));
                        final DelayShard delayShard = new DelayShard(
                                store,
                                DelayShardConfig.defaults(),
                                null,
                                null,
                                scheduleResolver(
                                        clusterId,
                                        destinationTopicId,
                                        destinationPhysicalTopic,
                                        probe.shard().partition()),
                                trustCatalog);
                        delayShards.add(delayShard);
                        final OwnerLease lease = admission.lease();
                        final com.nereusstream.delay.protocol.OwnerIdentity ownerIdentity =
                                new com.nereusstream.delay.protocol.OwnerIdentity(
                                        bytes(16, 70 + probe.shard().partition()),
                                        bytes(16, 90 + probe.shard().partition()),
                                        lease.ownerEpoch(),
                                        Bytes.sha256(Bytes.utf8("large-payload-multi-worker-fence-"
                                                + probe.shard().partition())));
                        final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease, ownerIdentity);
                        ownedShards.add(ownedShard);
                        ownerIdentities.add(ownerIdentity);
                        final List<SourceReplayEntry> recovery = recoveryEntries(
                                bootstrap,
                                clusterId,
                                topic,
                                nativeTopicId,
                                admission.assignment().sourceAssignment(),
                                probe.activation(),
                                probe.beforeRoute());
                        recover(
                                admission.assignment(),
                                ownerAuthority,
                                ownedShard,
                                recovery,
                                controlKeys,
                                controlSnapshot(probe.shard(), destinationProfile()),
                                workClasses);
                        if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                || !(ownedShard.lastCatchupPosition() instanceof KafkaSourcePosition position)
                                || position.offset() != 1
                                || !position.shardId().equals(probe.shard())) {
                            throw new IllegalStateException(
                                    "Kafka multi-shard Large Payload recovery did not apply partition "
                                            + probe.shard().partition() + " activation and pre-route records");
                        }
                        final String workerGroup = "nereus-delay-large-payload-multi-worker-"
                                + probe.shard().partition() + "-" + UUID.randomUUID();
                        runtimes.add(KafkaClientArtifactWorkerSourceFactory.create(
                                workerConsumer(bootstrap, workerGroup, clusterId, topic, nativeTopicId, probe.shard()),
                                topic,
                                POLL_TIMEOUT,
                                admission.assignment().sourceAssignment(),
                                workClasses,
                                ownedShard,
                                store,
                                resources,
                                ownerAuthority,
                                controlKeys.getPublic(),
                                null,
                                null,
                                null,
                                null,
                                null));
                        final KafkaCommandTransportKey key = new KafkaCommandTransportKey(
                                clusterId,
                                topic,
                                nativeTopicId,
                                probe.shard().partition(),
                                new CredentialBindingKey(1, new Digest32(bytes(32, 41)), new Digest32(bytes(32, 42))));
                        final KafkaProducer<byte[], byte[]> producer = kafkaProducer(bootstrap);
                        transports.register(new ProductionKafkaProduceTransport(
                                key,
                                new ProductionKafkaProduceTransport.Configuration(
                                        -1, true, true, "large-payload-multi-gateway-kafka-client"),
                                new KafkaClientArtifactProduceTransport((GuardedProducer<byte[], byte[]>) producer)));
                    }
                    fleet = new WorkerShardFleetRuntime(workClasses, resources, runtimes);
                    final DefaultSubmissionCoordinator submissions = new DefaultSubmissionCoordinator(
                            new RouteBoundSubmissionTransportPlanResolver(provider, System::currentTimeMillis),
                            transports,
                            SubmissionOutcomeProjectorRegistry.of(new KafkaManagedSubmissionOutcomeProjector()));
                    final DefaultDelaySemanticCore core = new DefaultDelaySemanticCore(
                            provider, new SecureLogicalUuidV7Generator(), System::currentTimeMillis);
                    final S3CompatiblePayloadObjectStore payloadStore = new S3CompatiblePayloadObjectStore(
                            objectStoreProfile,
                            minioUri,
                            minioRegion,
                            minioBucket,
                            minioAccessKey,
                            minioSecretKey,
                            minioSessionToken,
                            tenant.tenantRoutingScope(),
                            trustSet,
                            7,
                            Long.MAX_VALUE,
                            proofKeys.getPrivate(),
                            null,
                            HttpClient.newBuilder()
                                    .connectTimeout(Duration.ofSeconds(10))
                                    .followRedirects(HttpClient.Redirect.NEVER)
                                    .build(),
                            Clock.systemUTC(),
                            minioRequestTimeout);

                    try (OxiaSyncOwnerLeaseBackend.ClientHandle admissionHandle =
                                    OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                            oxiaEndpoint,
                                            namespace,
                                            "nereus-delay-large-multi-admission-" + UUID.randomUUID(),
                                            Duration.ofSeconds(15),
                                            gatewayPrefix + "/admission-client");
                            OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyHandle =
                                    OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                            oxiaEndpoint,
                                            namespace,
                                            "nereus-delay-large-multi-idempotency-" + UUID.randomUUID(),
                                            Duration.ofSeconds(15),
                                            gatewayPrefix + "/idempotency-client");
                            OxiaSyncOwnerLeaseBackend.ClientHandle auditHandle =
                                    OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                            oxiaEndpoint,
                                            namespace,
                                            "nereus-delay-large-multi-audit-" + UUID.randomUUID(),
                                            Duration.ofSeconds(15),
                                            gatewayPrefix + "/audit-client")) {
                        final OxiaGatewayAdmissionController admission = new OxiaGatewayAdmissionController(
                                admissionHandle,
                                gatewayPrefix + "/admission",
                                System::currentTimeMillis,
                                new OxiaGatewayAdmissionController.Limits(8, 16_000_000, 4, 4, 30_000, 16));
                        final OxiaGatewayIdempotencyStore idempotency = new OxiaGatewayIdempotencyStore(
                                idempotencyHandle,
                                gatewayPrefix + "/idempotency",
                                System::currentTimeMillis,
                                10_000,
                                30_000);
                        final OxiaGatewayAuditSink audit =
                                new OxiaGatewayAuditSink(auditHandle, gatewayPrefix + "/audit");
                        final GatewayScheduleService schedule =
                                new GatewayScheduleService(core, idempotency, submissions, System::currentTimeMillis);
                        final KeyPair jwtKeys = gatewayJwtKeys();
                        final MutualTlsJwtGatewayTenantAuthority tenantAuthority =
                                new MutualTlsJwtGatewayTenantAuthority(new RsaSha256GatewayJwtVerifier(
                                        jwtKeys.getPublic(),
                                        "nereus-delay-gateway-e2e-issuer",
                                        "nereus-delay-gateway-e2e",
                                        "gateway-e2e-key",
                                        Clock.systemUTC(),
                                        30,
                                        600));
                        final GatewayIngressService ingress = new GatewayIngressService(
                                schedule, tenantAuthority, admission, audit, System::currentTimeMillis);
                        final GatewayPayloadStoreAuthority payloadAuthority = new GatewayPayloadStoreAuthority(
                                tenant.tenantRoutingScope(),
                                (receipt, kind, now) -> payloadStore.issueUploadHandle(receipt, kind, now),
                                (receipt, handle, now) -> payloadStore.attest(receipt, handle, now));
                        final GatewayPayloadIngressService payloadIngress = new GatewayPayloadIngressService(
                                payloadAuthority, tenantAuthority, admission, audit, System::currentTimeMillis);
                        final GatewayGrpcServer server = GatewayGrpcServer.mutualTls(
                                gatewayPort,
                                serverCertificate,
                                serverPrivateKey,
                                trustedClientCertificates,
                                new com.nereusstream.delay.gateway.GatewayGrpcService(
                                        ingress, GatewayGrpcContext.provider(), payloadIngress));
                        server.start();
                        final ManagedChannel channel =
                                channel(gatewayPort, trustedClientCertificates, clientCertificate, clientPrivateKey);
                        try {
                            final DelayGatewayGrpc.DelayGatewayBlockingStub gateway =
                                    stub(channel, token(jwtKeys, tenant, certificateFingerprint(clientCertificate)));
                            for (LargeShardAdmission admissionRecord : admissions) {
                                final int partition =
                                        admissionRecord.probe().shard().partition();
                                final byte[] orderingKey = orderingKeyForPartition(snapshot, tenant, partition);
                                final CanonicalScheduleIntent intent =
                                        largeScheduleIntent(System.currentTimeMillis(), orderingKey);
                                final GatewayPrepareLargeScheduleRequest prepareRequest = prepareRequest(
                                        intent,
                                        payload.length,
                                        payloadHash,
                                        trustSet.ref(),
                                        objectStoreProfile.ref(),
                                        partition);
                                final GatewaySubmissionOutcome prepareResponse =
                                        gateway.prepareLargeSchedule(prepareRequest);
                                final CanonicalCommandQueuedReceipt prepareReceipt = requireQueued(
                                        prepareResponse,
                                        "PrepareLargeSchedule",
                                        partition,
                                        admissionRecord.probe().barrierOffset());
                                final int runtimeIndex = partition;
                                requireApplied(
                                        runUntilApplied(
                                                fleet, admissionRecord.probe().shard()),
                                        "PrepareLargeSchedule partition=" + partition);
                                final byte[] reservationId = reservationId(prepareReceipt);
                                final PayloadReservation reservation = Optional.ofNullable(
                                                delayShards.get(runtimeIndex).getReservation(reservationId))
                                        .orElseThrow();
                                if (reservation.status() != PayloadReservationStatus.RESERVED) {
                                    throw new IllegalStateException(
                                            "Kafka multi-shard Prepare did not leave RESERVED partition=" + partition
                                                    + ": " + reservation.status());
                                }
                                payloadStore.register(reservation, trustSet.ref(), objectStoreProfile.ref());
                                final PayloadReservationReceipt receipt = payloadStore.reservationReceipt(reservation);
                                final GatewayPayloadUploadHandleResponse handleResponse =
                                        gateway.issuePayloadUploadHandle(
                                                GatewayIssuePayloadUploadHandleRequest.newBuilder()
                                                        .setPayloadReservationReceipt(
                                                                ByteString.copyFrom(receipt.payload()))
                                                        .setUploadHandleKind(
                                                                UploadHandleKind.OPAQUE_SINGLE_PUT.wireValue())
                                                        .build());
                                final PayloadUploadHandleResponse handleDomain =
                                        PayloadUploadHandleResponse.decode(handleResponse
                                                .getPayloadUploadHandleResponse()
                                                .toByteArray());
                                if (handleDomain.outcome() != PayloadUploadHandleOutcome.ISSUED) {
                                    throw new IllegalStateException(
                                            "Kafka multi-shard Gateway did not issue upload handle partition="
                                                    + partition + ": " + handleDomain.outcome());
                                }
                                final OpaquePayloadUploadHandle handle = handleDomain.issued();
                                payloadStore.upload(receipt, handle, payload, System.currentTimeMillis());
                                final PayloadAttestationResponse attestation = PayloadAttestationResponse.decode(
                                        gateway.attestPayloadUpload(GatewayAttestPayloadUploadRequest.newBuilder()
                                                        .setPayloadReservationReceipt(
                                                                ByteString.copyFrom(receipt.payload()))
                                                        .setOpaquePayloadUploadHandle(
                                                                ByteString.copyFrom(handle.canonicalBytes()))
                                                        .build())
                                                .getPayloadAttestationResponse()
                                                .toByteArray());
                                if (attestation.outcome() != PayloadAttestationOutcome.ATTESTED
                                        || attestation.proof() == null) {
                                    throw new IllegalStateException(
                                            "Kafka multi-shard Gateway/MinIO attestation failed partition=" + partition
                                                    + ": " + attestation.outcome());
                                }
                                final CanonicalPayloadCommitProof proof = attestation.proof();
                                final GatewaySubmissionOutcome commitResponse =
                                        gateway.commitLargeSchedule(commitRequest(receipt, proof, partition));
                                final CanonicalCommandQueuedReceipt commitReceipt = requireQueued(
                                        commitResponse,
                                        "CommitLargeSchedule",
                                        partition,
                                        admissionRecord.probe().barrierOffset() + 1);
                                requireApplied(
                                        runUntilApplied(
                                                fleet, admissionRecord.probe().shard()),
                                        "CommitLargeSchedule partition=" + partition);
                                final PayloadReservation committed = Optional.ofNullable(
                                                delayShards.get(runtimeIndex).getReservation(reservationId))
                                        .orElseThrow();
                                final MessageRecord message = Optional.ofNullable(delayShards
                                                .get(runtimeIndex)
                                                .getMessage(
                                                        prepareReceipt.command().delayMessageId()))
                                        .orElseThrow();
                                if (committed.status() != PayloadReservationStatus.COMMITTED
                                        || message.status() != MessageStatus.SCHEDULED
                                        || message.payloadReference() == null
                                        || !Arrays.equals(
                                                message.payloadReference().immutableObjectVersion(),
                                                proof.immutableObjectVersion())
                                        || !Arrays.equals(
                                                message.payloadReference().proofId(), proof.proofId())
                                        || !Arrays.equals(
                                                payloadStore.readPayload(message.payloadReference()), payload)) {
                                    throw new IllegalStateException(
                                            "Kafka multi-shard Worker did not persist exact Object Store "
                                                    + "reference/readback partition=" + partition);
                                }
                                if (!Arrays.equals(
                                        prepareResponse.toByteArray(),
                                        gateway.prepareLargeSchedule(prepareRequest)
                                                .toByteArray())) {
                                    throw new IllegalStateException(
                                            "Kafka multi-shard Oxia Gateway idempotency changed Prepare bytes "
                                                    + "partition=" + partition);
                                }
                                final LaneRecord physicalLane = Optional.ofNullable(
                                                delayShards.get(runtimeIndex).getLane(message.laneId()))
                                        .orElseThrow(() -> new IllegalStateException(
                                                "Kafka multi-shard Worker did not persist the physical "
                                                        + "destination Lane"));
                                final KafkaClientArtifactWorkerSmoke.PhysicalPublishBridge physicalBridge =
                                        KafkaClientArtifactWorkerSmoke.createPhysicalPublishBridge(
                                                bootstrap,
                                                clusterId,
                                                topic,
                                                nativeTopicId,
                                                admissionRecord.probe().shard(),
                                                (KafkaSourcePosition) commitReceipt.sourcePosition(),
                                                destinationPhysicalTopic,
                                                destinationTopicId,
                                                receiptPhysicalTopic,
                                                receiptTopicId,
                                                stores.get(runtimeIndex),
                                                ownedShards.get(runtimeIndex),
                                                ownerIdentities.get(runtimeIndex),
                                                ownerAuthority,
                                                workClasses,
                                                controlKeys,
                                                destinationProfile(),
                                                capabilityProfile(),
                                                message.laneId(),
                                                physicalLane.laneIncarnation(),
                                                LARGE_PAYLOAD_WORK_CLASS_BYTES,
                                                physicalAdmission,
                                                partition);
                                physicalBridges.add(physicalBridge);
                                runtimes.get(runtimeIndex).bindPhysicalPublishExecutor(physicalBridge.executor());
                                KafkaClientArtifactWorkerSmoke.runSourceAppliedPhysicalPublish(
                                        runtimes.get(runtimeIndex),
                                        delayShards.get(runtimeIndex),
                                        ownedShards.get(runtimeIndex),
                                        ownerIdentities.get(runtimeIndex),
                                        ownerAuthority,
                                        stores.get(runtimeIndex),
                                        workClasses,
                                        controlKeys,
                                        physicalBridge,
                                        prepareReceipt.command().delayMessageId(),
                                        (KafkaSourcePosition) commitReceipt.sourcePosition(),
                                        payload,
                                        bootstrap,
                                        clusterId,
                                        LARGE_PAYLOAD_WORK_CLASS_BYTES,
                                        claimAdmission);
                                final long sourceRecordCount = latestOffset(admin, topic, partition);
                                if (sourceRecordCount != admissionRecord.probe().barrierOffset() + 4) {
                                    throw new IllegalStateException(
                                            "Kafka multi-shard physical publish appended an unexpected "
                                                    + "source record count "
                                                    + "partition=" + partition + ", latest=" + sourceRecordCount);
                                }
                                System.out.println("Kafka multi-shard Large Payload partition=" + partition
                                        + " passed: prepare=" + prepareReceipt.sourcePosition() + ", commit="
                                        + commitReceipt.sourcePosition() + ", objectVersion="
                                        + new String(proof.immutableObjectVersion(), StandardCharsets.UTF_8)
                                        + ", destinationEgress=true, sourceRecords=" + sourceRecordCount);
                            }
                            for (int index = 0; index < runtimes.size(); index++) {
                                final int partition =
                                        admissions.get(index).probe().shard().partition();
                                final Path checkpointPath =
                                        root.resolve("large-payload-multi-final-checkpoint-" + partition);
                                final byte[] checkpointId = Arrays.copyOf(
                                        Bytes.sha256(Bytes.utf8("large-payload-multi-final-checkpoint-" + partition)),
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
                                        || !ownerAuthority
                                                .current(admissions
                                                        .get(index)
                                                        .probe()
                                                        .shard())
                                                .isEmpty()) {
                                    throw new IllegalStateException(
                                            "Kafka multi-shard Large Payload drain did not release partition "
                                                    + partition + " checkpoint/owner");
                                }
                            }
                            fleet.close();
                            fleet = null;
                            for (LargeShardAdmission admissionRecord : admissions) {
                                if (!assignmentAuthority.withdraw(admissionRecord.publication())) {
                                    throw new IllegalStateException(
                                            "Kafka multi-shard Large Payload assignment withdrawal failed: "
                                                    + admissionRecord.probe().shard());
                                }
                            }
                            assignmentsWithdrawn = true;
                            System.out.println("Kafka signed Route -> two guarded Fetch barriers -> Oxia multi-shard "
                                    + "Assignment/Owner -> one Worker fleet -> Gateway mTLS/JWT -> two Large Payload "
                                    + "reservations -> real MinIO upload/attest/commit/readback/checkpoint passed: "
                                    + "fetchPartitions=" + shardCount + ", routeRevision=" + routeRevision
                                    + ", workers=" + assignedWorkers + ", sourceBarriers="
                                    + probes.stream()
                                            .map(LargeShardProbe::barrierOffset)
                                            .toList()
                                    + ", exactGatewayIdempotency=true");
                        } finally {
                            channel.shutdownNow();
                            channel.awaitTermination(10, TimeUnit.SECONDS);
                            server.close();
                        }
                    }
                } finally {
                    if (fleet != null) {
                        try {
                            fleet.close();
                        } catch (RuntimeException cleanupFailure) {
                            System.err.println(
                                    "Kafka multi-shard Worker fleet cleanup deferred: " + cleanupFailure.getMessage());
                        }
                    }
                    if (!assignmentsWithdrawn) {
                        for (LargeShardAdmission admission : admissions) {
                            assignmentAuthority.withdraw(admission.publication());
                        }
                    }
                    for (KafkaClientArtifactWorkerSmoke.PhysicalPublishBridge physicalBridge : physicalBridges) {
                        try {
                            physicalBridge.close();
                        } catch (RuntimeException cleanupFailure) {
                            System.err.println("Kafka multi-shard physical destination cleanup deferred: "
                                    + cleanupFailure.getMessage());
                        }
                    }
                    for (ShardStore store : stores) {
                        try {
                            store.close();
                        } catch (RuntimeException cleanupFailure) {
                            System.err.println(
                                    "Kafka multi-shard Store cleanup deferred: " + cleanupFailure.getMessage());
                        }
                    }
                    deleteTree(root);
                }
            }
            final List<String> topics = new ArrayList<>(List.of(topic));
            topics.add(destinationPhysicalTopic);
            topics.add(receiptPhysicalTopic);
            admin.deleteTopics(topics).all().get(10, TimeUnit.SECONDS);
        }
    }

    static KafkaProducer<byte[], byte[]> kafkaProducer(final String bootstrap) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(configuration, new ByteArraySerializer(), new ByteArraySerializer());
    }

    static org.apache.kafka.clients.consumer.GuardedFetchEvidence fetchEvidence(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard) {
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, "nereus-delay-large-barrier-" + UUID.randomUUID()),
                clusterId,
                topic,
                topicId,
                shard.partition());
        final TopicPartition topicPartition = new TopicPartition(topic, shard.partition());
        final ConsumerResourceGuard guard = new ConsumerResourceGuard(
                clusterId,
                topic,
                new Uuid(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits()),
                shard.partition());
        try {
            consumer.assign(List.of(topicPartition));
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(POLL_TIMEOUT);
                final org.apache.kafka.clients.consumer.GuardedFetchEvidence evidence =
                        KafkaClientArtifactFetchEvidence.requireBatch(records, guard);
                if (evidence != null && evidence.firstRecordOffset() == 0 && evidence.lastRecordOffset() >= 1) {
                    return evidence;
                }
            }
            throw new IllegalStateException("Kafka guarded Fetch evidence did not cover both source records");
        } finally {
            consumer.close();
        }
    }

    static List<SourceReplayEntry> recoveryEntries(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final SourceAssignment assignment,
            final SystemMutation activation,
            final PreparedCommand beforeRoute) {
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, "nereus-delay-large-recovery-" + UUID.randomUUID()),
                clusterId,
                topic,
                topicId,
                assignment.shardId().partition());
        final List<SourceReplayEntry> entries = new ArrayList<>();
        try (KafkaClientArtifactRecoverySourceCursor cursor =
                new KafkaClientArtifactRecoverySourceCursor(consumer, assignment, topic, 0, POLL_TIMEOUT)) {
            for (int index = 0; index < 2; index++) {
                if (!cursor.hasNext()) {
                    throw new IllegalStateException("Kafka recovery source ended before the activation barrier");
                }
                entries.add(cursor.next());
            }
        }
        if (!(entries.get(0) instanceof SourceReplayMutation mutation)
                || mutation.mutation().type() != SystemMutationType.APPLY_SHARD_CONTROL
                || !(entries.get(1) instanceof SourceReplayRecord record)
                || !record.command().equals(beforeRoute)
                || !activation.shardId().equals(entries.get(0).position().shardId())
                || !(entries.get(0).position() instanceof KafkaSourcePosition first)
                || !(entries.get(1).position() instanceof KafkaSourcePosition second)
                || first.offset() != 0
                || second.offset() != 1) {
            throw new IllegalStateException(
                    "Kafka recovery did not return the exact trust activation and pre-route records");
        }
        return List.copyOf(entries);
    }

    static void recover(
            final WorkerAssignment accepted,
            final OxiaOwnerLeaseStore authority,
            final OwnedDelayShard ownedShard,
            final List<SourceReplayEntry> entries,
            final KeyPair verificationKeys,
            final CompatibleControlSnapshot controlSnapshot,
            final WorkClassExecutionRegistry workClasses) {
        final OwnerRecoveryCoordinator recovery = new OwnerRecoveryCoordinator(
                ownedShard,
                authority,
                accepted.sourceAssignment(),
                SourceReplaySuccessor.strictKafka(),
                SourceReplayCursor.of(entries.iterator()),
                verificationKeys.getPublic(),
                controlSnapshot,
                System::currentTimeMillis,
                new ReplayTurnBudget(2, 1_000_000, TimeUnit.SECONDS.toNanos(10)),
                workClasses);
        OwnerRecoveryTurn turn;
        do {
            turn = recovery.runTurn();
        } while (!turn.complete());
        if (!recovery.complete() || turn.outcomes().size() != 2) {
            throw new IllegalStateException(
                    "Kafka large-payload Worker recovery did not apply exactly two source records");
        }
    }

    static com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult runUntilApplied(
            final WorkerShardRuntime runtime) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result;
        do {
            result = runtime.runSourceTurn(
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)), System::currentTimeMillis);
            if (result.status()
                    == com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                return result;
            }
            if (result.status() != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                    .WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException(
                        "Kafka large-payload Worker source turn failed: " + result.status(), result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Kafka large-payload Worker source record did not become visible");
    }

    static void requireApplied(
            final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result, final String operation) {
        if (result.status() != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
            throw new IllegalStateException(
                    operation + " was not applied and ACKed: " + result.status(), result.failure());
        }
    }

    static com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult runUntilApplied(
            final WorkerShardFleetRuntime fleet, final ShardId expectedShard) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            final WorkerShardFleetRuntime.SourceTurn turn = fleet.runNextSourceTurn(
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)), System::currentTimeMillis);
            if (turn.shardId().equals(expectedShard)
                    && turn.result().status()
                            == com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                return turn.result();
            }
            if (turn.result().status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED
                    && turn.result().status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && turn.result().status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                    .WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException(
                        "Kafka multi-shard Large Payload source turn failed: shard=" + turn.shardId() + ", status="
                                + turn.result().status(),
                        turn.result().failure());
            }
        }
        throw new IllegalStateException(
                "Kafka multi-shard Large Payload source record did not become visible: " + expectedShard);
    }

    private static void requirePrecommitFailure(final RuntimeException failure, final String faultMode) {
        final String message = failure.getMessage() == null ? failure.toString() : failure.getMessage();
        if ("PUT_503_BEFORE_COMMIT".equals(faultMode) && !message.contains("HTTP 503")) {
            throw new IllegalStateException("Kafka pre-commit 503 did not retain the provider failure", failure);
        }
        if ("PUT_TIMEOUT_BEFORE_COMMIT".equals(faultMode) && !message.contains("S3 payload request failed")) {
            throw new IllegalStateException("Kafka pre-commit timeout did not retain the provider failure", failure);
        }
    }

    static CanonicalCommandQueuedReceipt requireQueued(
            final GatewaySubmissionOutcome response, final String operation, final long expectedOffset) {
        return requireQueued(response, operation, -1, expectedOffset);
    }

    static CanonicalCommandQueuedReceipt requireQueued(
            final GatewaySubmissionOutcome response,
            final String operation,
            final int expectedPartition,
            final long expectedOffset) {
        if (!response.hasSubmissionOutcomeNdr1()) {
            final StableError error =
                    StableError.decode(response.getPreparationError().toByteArray());
            throw new IllegalStateException(operation + " returned preparation error: stage=" + error.stage()
                    + ", code=" + error.code() + ", retryability=" + error.retryability()
                    + ", retryAtEpochMs=" + error.retryAtEpochMs()
                    + ", diagnosticCode=" + error.diagnosticCode());
        }
        final SubmissionOutcomeMessage outcome = SubmissionOutcomeMessage.decode(
                response.getSubmissionOutcomeNdr1().toByteArray());
        if (outcome.kind() != com.nereusstream.delay.protocol.SubmissionOutcomeKind.MANAGED
                || outcome.managed().kind() != EnqueueOutcomeKind.QUEUED) {
            final String detail;
            if (outcome.kind() != com.nereusstream.delay.protocol.SubmissionOutcomeKind.MANAGED) {
                detail = "kind=" + outcome.kind();
            } else if (outcome.managed().kind() == EnqueueOutcomeKind.DEFINITELY_NOT_QUEUED) {
                detail = "kind=" + outcome.managed().kind() + ", code="
                        + outcome.managed().definitelyNotQueued().error().code()
                        + ", stage="
                        + outcome.managed().definitelyNotQueued().error().stage();
            } else if (outcome.managed().kind() == EnqueueOutcomeKind.ENQUEUE_UNCERTAIN) {
                detail = "kind=" + outcome.managed().kind() + ", code="
                        + outcome.managed().uncertain().error().code()
                        + ", stage=" + outcome.managed().uncertain().error().stage();
            } else {
                detail = "kind=" + outcome.managed().kind();
            }
            throw new IllegalStateException(operation + " did not produce a managed QUEUED outcome: " + detail);
        }
        final CanonicalCommandQueuedReceipt receipt = outcome.managed().queued();
        if (!(receipt.sourcePosition() instanceof KafkaSourcePosition position)
                || position.offset() != expectedOffset
                || (expectedPartition >= 0 && position.shardId().partition() != expectedPartition)) {
            throw new IllegalStateException(operation + " Kafka source position mismatch: expectedPartition="
                    + expectedPartition + ", expectedOffset=" + expectedOffset + ", actual="
                    + receipt.sourcePosition());
        }
        return receipt;
    }

    static byte[] reservationId(final CanonicalCommandQueuedReceipt receipt) {
        return Bytes.sha256(
                Bytes.utf8("nereus-delay-reservation-id\0"),
                receipt.command().commandId().bytes(),
                receipt.command().delayMessageId().bytes(),
                receipt.command().commandHash());
    }

    static GatewayPrepareLargeScheduleRequest prepareRequest(
            final CanonicalScheduleIntent intent,
            final long payloadLength,
            final byte[] payloadHash,
            final PayloadProofTrustSetRef trustSet,
            final ProfileRef objectStoreProfile) {
        return prepareRequest(intent, payloadLength, payloadHash, trustSet, objectStoreProfile, 0);
    }

    static GatewayPrepareLargeScheduleRequest prepareRequest(
            final CanonicalScheduleIntent intent,
            final long payloadLength,
            final byte[] payloadHash,
            final PayloadProofTrustSetRef trustSet,
            final ProfileRef objectStoreProfile,
            final int partition) {
        return GatewayPrepareLargeScheduleRequest.newBuilder()
                .setIdempotencyKey(ByteString.copyFrom(bytes(16, 80 + (partition * 2))))
                .setRoute(GatewayRouteSelector.newBuilder()
                        .setIngressAdapterKind(AdapterKind.KAFKA.wireValue())
                        .setRouteAliasUtf8Nfc(ByteString.copyFromUtf8("primary")))
                .setCanonicalScheduleIntent(ByteString.copyFrom(intent.canonicalBytes()))
                .setExpectedPayloadLength(payloadLength)
                .setPayloadSha256(ByteString.copyFrom(payloadHash))
                .setReservationTtlMs(120_000)
                .setPayloadProofTrustSetRef(ByteString.copyFrom(trustSet.canonicalBytes()))
                .setObjectStoreProfileRef(ByteString.copyFrom(objectStoreProfile.canonicalBytes()))
                .setRetryUntilEpochMs(System.currentTimeMillis() + 120_000)
                .build();
    }

    static GatewayCommitLargeScheduleRequest commitRequest(
            final PayloadReservationReceipt receipt, final CanonicalPayloadCommitProof proof) {
        return commitRequest(receipt, proof, 0);
    }

    static GatewayCommitLargeScheduleRequest commitRequest(
            final PayloadReservationReceipt receipt, final CanonicalPayloadCommitProof proof, final int partition) {
        return GatewayCommitLargeScheduleRequest.newBuilder()
                .setIdempotencyKey(ByteString.copyFrom(bytes(16, 81 + (partition * 2))))
                .setPayloadReservationReceipt(ByteString.copyFrom(receipt.payload()))
                .setCanonicalPayloadCommitProof(ByteString.copyFrom(proof.canonicalBytes()))
                .setRetryUntilEpochMs(System.currentTimeMillis() + 120_000)
                .build();
    }

    static CanonicalScheduleIntent largeScheduleIntent(final long now) {
        return largeScheduleIntent(now, Bytes.utf8("large-payload-key"));
    }

    static CanonicalScheduleIntent largeScheduleIntent(final long now, final byte[] orderingKey) {
        final long deliverAt = now + 15_000;
        return CanonicalScheduleIntent.forPrepare(
                destinationProfile(),
                retryPolicy(),
                deliverAt,
                deliverAt + 120_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                orderingKey,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destinationProfile(),
                retryPolicy(),
                deliverAt,
                deliverAt + 10_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8(identity),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        return PreparedCommand.schedule(shard, intent, deliverAt + 20_000);
    }

    static ProfileRef destinationProfile() {
        return new ProfileRef(
                Bytes.utf8("destination-large-payload"),
                1,
                Bytes.sha256(Bytes.utf8("destination-large-payload-semantic")),
                ProfileKind.DESTINATION);
    }

    static RetryPolicyRef retryPolicy() {
        return new RetryPolicyRef(
                Bytes.utf8("retry-large-payload"), 1, Bytes.sha256(Bytes.utf8("retry-large-payload-semantic")));
    }

    static ProfileSemanticEnvelope objectStoreProfile(
            final URI endpoint, final String region, final String bucket, final String accessKey) {
        final ObjectStoreProfileSemantic semantic = new ObjectStoreProfileSemantic(
                ObjectStoreProviderKind.S3_COMPATIBLE,
                S3CompatiblePayloadObjectStore.endpointConfigDigest(endpoint, region, bucket),
                S3CompatiblePayloadObjectStore.credentialAuthorizationScopeDigest(accessKey, region, bucket),
                1,
                true,
                true,
                true,
                true,
                bytes(32, 20),
                8L << 20,
                ObjectStoreProfileSemantic.SINGLE_PUT,
                1,
                bytes(32, 21));
        return new ProfileSemanticEnvelope(ProfileKind.OBJECT_STORE, Bytes.utf8("large-payload-store"), 1, semantic);
    }

    static byte[] payload() {
        final byte[] value = new byte[(int) PAYLOAD_BYTES];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (index * 31 + 7);
        }
        return value;
    }

    static SystemMutation trustActivation(
            final ShardId shard,
            final PayloadProofTrustSetRef trustSet,
            final AuthenticatedTenantContext tenant,
            final KeyPair signingKeys) {
        final long retryUntil = System.currentTimeMillis() + 300_000;
        final ControlRef controlRef = new ControlRef(
                Bytes.sha256(Bytes.utf8("large-payload-trust-op")),
                Bytes.sha256(Bytes.utf8("large-payload-trust-request")),
                1);
        final byte[] body = trustSetControlBody(shard, controlRef, trustSet, retryUntil);
        return SystemMutation.signed(
                shard,
                SystemMutationType.APPLY_SHARD_CONTROL,
                retryUntil,
                controlRef.logicalOperationIdentity(12),
                body,
                AuthorIdentity.control(
                                Bytes.sha256(Bytes.utf8("large-payload-control-actor")),
                                Bytes.sha256(Bytes.utf8("large-payload-control-role")),
                                tenant.authenticatedTenantScopeHash())
                        .canonicalBytes(),
                1,
                signingKeys.getPrivate());
    }

    private static byte[] trustSetControlBody(
            final ShardId shard,
            final ControlRef controlRef,
            final PayloadProofTrustSetRef trustSet,
            final long retryUntil) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] payload = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(
                output, 12, new PayloadProofTrustSetActivatePayload(trustSet).canonicalBytes()));
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntil);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, 12);
            CanonicalProtobuf.uint32(output, 12, trustSet.version());
            CanonicalProtobuf.bytes(output, 13, trustSet.semanticHash());
            CanonicalProtobuf.bytes(output, 15, payload);
        });
    }

    static ScheduleResolver scheduleResolver() {
        final byte[] tuple = Bytes.utf8("large-payload-kafka-canonical-lane-tuple");
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

    static ScheduleResolver scheduleResolver(
            final String clusterId, final UUID destinationTopicId, final String destinationPhysicalTopic) {
        return scheduleResolver(clusterId, destinationTopicId, destinationPhysicalTopic, 0);
    }

    static ScheduleResolver scheduleResolver(
            final String clusterId,
            final UUID destinationTopicId,
            final String destinationPhysicalTopic,
            final int destinationPartition) {
        final ProfileRef destination = destinationProfile();
        final ProfileRef capability = capabilityProfile();
        final byte[] tuple = canonicalLaneTuple(
                clusterId, destinationTopicId, destinationPhysicalTopic, destination, capability, destinationPartition);
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        final ScheduleResolver compatibilityResolver = scheduleResolver();
        return new ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(
                    final ShardId shard,
                    final com.nereusstream.delay.protocol.DelayMessageId message,
                    final CanonicalScheduleIntent intent,
                    final com.nereusstream.delay.protocol.SourcePosition source) {
                // The pre-route Schedule is a recovery/barrier fixture. Keep
                // it on the legacy compatibility lane so a physical
                // Large-Payload Prepare lane cannot claim that older work.
                return compatibilityResolver.resolveSchedule(shard, message, intent, source);
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

    private static DestinationLaneId laneId(
            final String clusterId, final UUID destinationTopicId, final String destinationPhysicalTopic) {
        return DestinationLaneId.derive(canonicalLaneTuple(
                clusterId, destinationTopicId, destinationPhysicalTopic, destinationProfile(), capabilityProfile()));
    }

    private static ProfileRef capabilityProfile() {
        return new ProfileRef(
                Bytes.utf8("kafka-worker-capability"),
                1,
                Bytes.sha256(Bytes.utf8("kafka-worker-capability-semantic")),
                ProfileKind.DELIVERY_CAPABILITY);
    }

    private static byte[] canonicalLaneTuple(
            final String clusterId,
            final UUID topicId,
            final String physicalTopic,
            final ProfileRef destination,
            final ProfileRef capability) {
        return canonicalLaneTuple(clusterId, topicId, physicalTopic, destination, capability, 0);
    }

    private static byte[] canonicalLaneTuple(
            final String clusterId,
            final UUID topicId,
            final String physicalTopic,
            final ProfileRef destination,
            final ProfileRef capability,
            final int physicalPartition) {
        if (physicalTopic == null || physicalTopic.isBlank()) {
            throw new IllegalArgumentException("Kafka physical topic must be nonblank");
        }
        if (physicalPartition < 0) {
            throw new IllegalArgumentException("Kafka physical partition must be non-negative");
        }
        final byte[] topicUuid = uuidBytes(topicId);
        return Bytes.concat(
                Bytes.sha256(Bytes.utf8("kafka-worker-tenant-routing-scope")),
                Bytes.u8(AdapterKind.KAFKA.wireValue()),
                Bytes.lp32(Bytes.utf8(clusterId)),
                Bytes.u8(1),
                topicUuid,
                Bytes.lp32(topicUuid),
                Bytes.u32be(physicalPartition),
                Bytes.lp32(destination.profileId()),
                Bytes.u64beBits(destination.version()),
                destination.semanticHash(),
                Bytes.lp32(capability.profileId()),
                Bytes.u64beBits(capability.version()),
                capability.semanticHash(),
                Bytes.u8(1),
                Bytes.sha256(Bytes.utf8("kafka-worker-ordering-domain")));
    }

    static RouteSnapshot routeSnapshot(
            final String clusterId,
            final String topic,
            final UUID topicId,
            final RouteIncarnation incarnation,
            final org.apache.kafka.clients.consumer.GuardedFetchEvidence evidence,
            final AuthenticatedTenantContext tenant,
            final KeyPair signingKeys) {
        final long now = System.currentTimeMillis();
        final BrokerResourceIdentity broker =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(clusterId, topicId));
        final RoutePartitionPolicy policy = new RoutePartitionPolicy(
                0,
                ActivationBarrier.kafka(broker, 0, 2, evidence.lastStableOffset()),
                zeroQuota(),
                1,
                Bytes.sha256(Bytes.utf8("large-payload-fetch-proof\0"), evidence.fetchResponseBodySha256()));
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                now - 100,
                now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("large-payload-route-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("large-payload-route-issued-at")),
                0,
                null);
        return RouteSnapshot.create(
                incarnation,
                tenant.authenticatedTenantScopeHash(),
                tenant.tenantRoutingScope(),
                RouteLifecycle.ACTIVE_FOR_NEW,
                now + 30_000,
                new com.nereusstream.delay.protocol.KafkaIngressRouteResource(clusterId, topic, topicId, 1),
                RoutingHashVersion.ROUTING_HASH,
                new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1),
                1,
                List.of(policy),
                100,
                200,
                1 << 20,
                2 << 20,
                10,
                8 << 20,
                180_000,
                now - 1_000,
                now + 300_000,
                new IngressCredentialBindingRef(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                Bytes.sha256(Bytes.utf8("large-payload-route-prerequisite")),
                issuedAt,
                1,
                signingKeys.getPrivate());
    }

    private static RouteSnapshot multiRouteSnapshot(
            final String clusterId,
            final String topic,
            final UUID topicId,
            final RouteIncarnation incarnation,
            final List<LargeShardProbe> probes,
            final AuthenticatedTenantContext tenant,
            final KeyPair signingKeys) {
        final long now = System.currentTimeMillis();
        final BrokerResourceIdentity broker =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(clusterId, topicId));
        final List<RoutePartitionPolicy> policies = probes.stream()
                .map(probe -> {
                    final int partition = probe.shard().partition();
                    final org.apache.kafka.clients.consumer.GuardedFetchEvidence evidence = probe.evidence();
                    return new RoutePartitionPolicy(
                            partition,
                            ActivationBarrier.kafka(
                                    broker, partition, probe.barrierOffset(), evidence.lastStableOffset()),
                            zeroQuota(),
                            1,
                            Bytes.sha256(
                                    Bytes.utf8("large-payload-multi-fetch-proof\0"),
                                    evidence.fetchResponseBodySha256()));
                })
                .toList();
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                now - 100,
                now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("large-payload-multi-route-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("large-payload-multi-route-issued-at")),
                0,
                null);
        return RouteSnapshot.create(
                incarnation,
                tenant.authenticatedTenantScopeHash(),
                tenant.tenantRoutingScope(),
                RouteLifecycle.ACTIVE_FOR_NEW,
                now + 30_000,
                new KafkaIngressRouteResource(clusterId, topic, topicId, probes.size()),
                RoutingHashVersion.ROUTING_HASH,
                new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1),
                1,
                policies,
                100,
                200,
                1 << 20,
                2 << 20,
                10,
                8 << 20,
                180_000,
                now - 1_000,
                now + 300_000,
                new IngressCredentialBindingRef(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                Bytes.sha256(Bytes.utf8("large-payload-multi-route-prerequisite")),
                issuedAt,
                1,
                signingKeys.getPrivate());
    }

    static RouteWorkerAssignmentCoordinator.PlacementRequest placementRequest(final long now) {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(
                0,
                Bytes.sha256(Bytes.utf8("large-payload-worker-assignment")),
                1,
                Bytes.sha256(Bytes.utf8("large-payload-worker-capacity")),
                1,
                List.of(new WorkerPlacementPolicy.WorkerCandidate(
                        "kafka-large-payload-worker",
                        capacity(2),
                        CapacityVector.empty(),
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
                CapacityVector.empty(),
                CapacityVector.empty(),
                null,
                now,
                0,
                0);
    }

    static RouteWorkerAssignmentCoordinator.PlacementRequest placementRequest(
            final long now, final int partition, final String workerId) {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(
                partition,
                Bytes.sha256(Bytes.utf8("large-payload-multi-worker-assignment-" + partition + "-" + workerId)),
                1,
                Bytes.sha256(Bytes.utf8("large-payload-multi-worker-capacity-" + partition + "-" + workerId)),
                1,
                List.of(new WorkerPlacementPolicy.WorkerCandidate(
                        workerId,
                        capacity(2),
                        CapacityVector.empty(),
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
                CapacityVector.empty(),
                CapacityVector.empty(),
                null,
                now,
                0,
                0);
    }

    private static byte[] orderingKeyForPartition(
            final RouteSnapshot snapshot, final AuthenticatedTenantContext tenant, final int partition) {
        for (int attempt = 0; attempt < 100_000; attempt++) {
            final byte[] candidate = Bytes.utf8("large-payload-multi-ordering-key-" + attempt);
            if (RouteHash.partition(snapshot, tenant.tenantRoutingScope(), candidate) == partition) {
                return candidate;
            }
        }
        throw new IllegalStateException("could not find deterministic ordering key for Kafka partition " + partition);
    }

    static void requireRouteAssignment(
            final WorkerAssignment assignment,
            final RouteSnapshot snapshot,
            final String clusterId,
            final UUID topicId,
            final long barrierOffset) {
        if (!assignment.routeBound()
                || !Arrays.equals(snapshot.snapshotDigest(), assignment.routeSnapshotDigest())
                || !(assignment.sourceAssignment().activationBarrier() instanceof KafkaActivationBarrier barrier)
                || !clusterId.equals(barrier.authenticatedClusterId())
                || !topicId.equals(barrier.nativeTopicUuid())
                || barrier.exclusiveOffset() != barrierOffset) {
            throw new IllegalStateException("Oxia assignment did not retain the signed Kafka activation barrier");
        }
    }

    static CompatibleControlSnapshot controlSnapshot(final ShardId shard, final ProfileRef destinationProfile) {
        return new CompatibleControlSnapshot(
                new ShardSubject(shard),
                List.of(new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1)),
                List.of(destinationProfile),
                zeroQuota());
    }

    static WorkClassExecutionRegistry workClasses() {
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
                            LARGE_PAYLOAD_WORK_CLASS_BYTES,
                            1,
                            LARGE_PAYLOAD_WORK_CLASS_BYTES,
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

    private static CapacityVector capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[CapacityDimension.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVector(values);
    }

    private static QuotaGrantRef zeroQuota() {
        return new QuotaGrantRef(
                bytes(32, 50),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    }

    static void appendFrame(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final Uuid topicId,
            final byte[] frame,
            final long expectedOffset)
            throws Exception {
        appendFrame(bootstrap, clusterId, topic, topicId, frame, 0, expectedOffset);
    }

    static void appendFrame(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final Uuid topicId,
            final byte[] frame,
            final int partition,
            final long expectedOffset)
            throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = kafkaProducer(bootstrap)) {
            final GuardedProducer<byte[], byte[]> guarded = (GuardedProducer<byte[], byte[]>) producer;
            final var metadata = guarded.sendGuarded(
                            new ProducerRecord<>(topic, partition, null, frame),
                            new org.apache.kafka.clients.producer.ProducerResourceGuard(
                                    clusterId, topic, topicId, partition))
                    .get(10, TimeUnit.SECONDS);
            if (metadata.recordMetadata().offset() != expectedOffset) {
                throw new IllegalStateException("Kafka large-payload append offset mismatch: expected=" + expectedOffset
                        + ", actual=" + metadata.recordMetadata().offset());
            }
        }
    }

    static Map<String, Object> consumerConfiguration(final String bootstrap, final String groupId) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configuration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return configuration;
    }

    private static GuardedConsumer<byte[], byte[]> workerConsumer(
            final String bootstrap,
            final String groupId,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard) {
        return KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, groupId), clusterId, topic, topicId, shard.partition());
    }

    private static long latestOffset(final Admin admin, final String topic, final int partition) throws Exception {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        return admin.listOffsets(Map.of(topicPartition, OffsetSpec.latest()))
                .all()
                .get(10, TimeUnit.SECONDS)
                .get(topicPartition)
                .offset();
    }

    private static void ensureTopic(final Admin admin, final String topic) throws Exception {
        ensureTopic(admin, topic, 1);
    }

    private static void ensureTopic(final Admin admin, final String topic, final int partitions) throws Exception {
        if (partitions <= 0) {
            throw new IllegalArgumentException("Kafka large-payload topic must contain at least one partition");
        }
        try {
            final TopicDescription existing = describe(admin, topic);
            if (existing != null) {
                if (existing.partitions().size() != partitions) {
                    throw new IllegalStateException("Kafka large-payload topic has unexpected partition count: topic="
                            + topic + ", expected=" + partitions + ", actual="
                            + existing.partitions().size());
                }
                return;
            }
        } catch (Exception missing) {
            // Create below.
        }
        final NewTopic newTopic = new NewTopic(topic, partitions, (short) 3);
        newTopic.configs(Map.of("message.timestamp.type", "LogAppendTime"));
        admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                final TopicDescription created = describe(admin, topic);
                if (created != null && created.partitions().size() == partitions) {
                    return;
                }
            } catch (Exception ignored) {
                // Retry while metadata converges.
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("Kafka large-payload topic metadata did not converge");
    }

    private static TopicDescription describe(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(List.of(topic))
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .get(topic);
    }

    static KeyPair gatewayJwtKeys() throws GeneralSecurityException {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2_048);
        return generator.generateKeyPair();
    }

    static String token(
            final KeyPair keyPair, final AuthenticatedTenantContext tenant, final byte[] certificateFingerprint)
            throws GeneralSecurityException {
        final long now = Instant.now().getEpochSecond();
        final String header = "{\"alg\":\"RS256\",\"typ\":\"JWT\",\"kid\":\"gateway-e2e-key\"}";
        final String claims =
                "{" + "\"iss\":\"nereus-delay-gateway-e2e-issuer\"," + "\"aud\":\"nereus-delay-gateway-e2e\","
                        + "\"sub\":\"gateway-e2e-client\"," + "\"tenant\":\"tenant-e2e\",\"tenant_scope_hash\":\""
                        + encode(tenant.authenticatedTenantScopeHash()) + "\",\"tenant_routing_scope\":\""
                        + encode(tenant.tenantRoutingScope()) + "\",\"iat\":" + (now - 100) + ",\"nbf\":"
                        + (now - 100) + ",\"exp\":" + (now + 300) + ",\"jti\":\"gateway-e2e-jwt-"
                        + UUID.randomUUID() + "\",\"cnf\":{\"x5t#S256\":\"" + encode(certificateFingerprint)
                        + "\"}}";
        final String encodedHeader = encode(header.getBytes(StandardCharsets.UTF_8));
        final String encodedClaims = encode(claims.getBytes(StandardCharsets.UTF_8));
        final String input = encodedHeader + "." + encodedClaims;
        final Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(keyPair.getPrivate());
        signature.update(input.getBytes(StandardCharsets.US_ASCII));
        return input + "." + encode(signature.sign());
    }

    static DelayGatewayGrpc.DelayGatewayBlockingStub stub(final ManagedChannel channel, final String token) {
        final Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
        return DelayGatewayGrpc.newBlockingStub(
                ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(headers)));
    }

    static ManagedChannel channel(
            final int port, final Path ca, final Path clientCertificate, final Path clientPrivateKey)
            throws SSLException {
        final io.grpc.netty.shaded.io.netty.handler.ssl.SslContext sslContext = GrpcSslContexts.forClient()
                .trustManager(ca.toFile())
                .keyManager(clientCertificate.toFile(), clientPrivateKey.toFile())
                .build();
        return NettyChannelBuilder.forAddress("127.0.0.1", port)
                .sslContext(sslContext)
                .build();
    }

    static byte[] certificateFingerprint(final Path certificate) throws Exception {
        final CertificateFactory factory = CertificateFactory.getInstance("X.509");
        final X509Certificate parsed;
        try (InputStream input = Files.newInputStream(certificate)) {
            parsed = (X509Certificate) factory.generateCertificate(input);
        }
        return Bytes.sha256(parsed.getEncoded());
    }

    private static String requiredEnv(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required for the real large-payload authority smoke");
        }
        return value;
    }

    private static Path requiredPath(final String name) {
        final Path path = Path.of(requiredEnv(name));
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException(name + " is not a regular file: " + path);
        }
        return path;
    }

    private static void awaitConfiguredBrokerCut() throws Exception {
        final String readyValue = configuredNullable("NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER_READY");
        final String releaseValue = configuredNullable("NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER_RELEASE");
        if (readyValue == null && releaseValue == null) {
            return;
        }
        if (readyValue == null || releaseValue == null) {
            throw new IllegalArgumentException("large-payload Broker cut requires both ready and release paths");
        }
        final Path ready = Path.of(readyValue);
        final Path release = Path.of(releaseValue);
        Files.deleteIfExists(ready);
        Files.deleteIfExists(release);
        Files.createFile(ready);
        final long timeoutSeconds =
                Long.parseLong(configured("NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER_TIMEOUT_SECONDS", "180"));
        if (timeoutSeconds <= 0) {
            throw new IllegalArgumentException(
                    "NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER_TIMEOUT_SECONDS must be positive");
        }
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (!Files.exists(release)) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("large-payload Broker cut release was not observed: " + release);
            }
            Thread.sleep(100);
        }
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static Duration configuredDuration(final String name, final long fallbackMillis) {
        final String value = configured(name, Long.toString(fallbackMillis));
        try {
            final long millis = Long.parseLong(value);
            if (millis <= 0) {
                throw new IllegalArgumentException(name + " must be positive milliseconds");
            }
            return Duration.ofMillis(millis);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(name + " must be positive milliseconds", failure);
        }
    }

    private static String configuredNullable(final String name) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? null : value;
    }

    private static UUID toUuid(final Uuid value) {
        return new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static String encode(final byte[] value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
    }

    private static void deleteTree(final Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        }
    }

    private record LargeShardProbe(
            ShardId shard,
            SystemMutation activation,
            PreparedCommand beforeRoute,
            org.apache.kafka.clients.consumer.GuardedFetchEvidence evidence,
            long barrierOffset) {}

    private record LargeShardAdmission(
            LargeShardProbe probe,
            WorkerAssignmentAuthority.Publication publication,
            WorkerAssignment assignment,
            OwnerLease lease) {}
}
