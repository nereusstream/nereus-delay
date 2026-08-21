package io.nereusstream.delay.transport;

import com.google.protobuf.ByteString;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import io.nereusstream.delay.adapter.DestinationPublishResult;
import io.nereusstream.delay.adapter.S3CompatiblePayloadObjectStore;
import io.nereusstream.delay.gateway.GatewayGrpcContext;
import io.nereusstream.delay.gateway.GatewayGrpcServer;
import io.nereusstream.delay.gateway.GatewayIngressService;
import io.nereusstream.delay.gateway.GatewayPayloadIngressService;
import io.nereusstream.delay.gateway.GatewayPayloadStoreAuthority;
import io.nereusstream.delay.gateway.GatewayScheduleService;
import io.nereusstream.delay.gateway.MutualTlsJwtGatewayTenantAuthority;
import io.nereusstream.delay.gateway.OxiaGatewayAdmissionController;
import io.nereusstream.delay.gateway.OxiaGatewayAuditSink;
import io.nereusstream.delay.gateway.OxiaGatewayIdempotencyStore;
import io.nereusstream.delay.gateway.RsaSha256GatewayJwtVerifier;
import io.nereusstream.delay.gateway.v1.DelayGatewayV1Grpc;
import io.nereusstream.delay.gateway.v1.GatewayAttestPayloadUploadRequestV1;
import io.nereusstream.delay.gateway.v1.GatewayCommitLargeScheduleRequestV1;
import io.nereusstream.delay.gateway.v1.GatewayIssuePayloadUploadHandleRequestV1;
import io.nereusstream.delay.gateway.v1.GatewayPayloadAttestationResponseV1;
import io.nereusstream.delay.gateway.v1.GatewayPayloadUploadHandleResponseV1;
import io.nereusstream.delay.gateway.v1.GatewayPrepareLargeScheduleRequestV1;
import io.nereusstream.delay.gateway.v1.GatewayRouteSelectorV1;
import io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1;
import io.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.nereusstream.delay.ownership.OxiaSyncWorkerAssignmentBackend;
import io.nereusstream.delay.ownership.OwnerLease;
import io.nereusstream.delay.ownership.OwnerRecoveryCoordinator;
import io.nereusstream.delay.ownership.OwnedDelayShard;
import io.nereusstream.delay.ownership.RouteWorkerAssignmentCoordinator;
import io.nereusstream.delay.ownership.ShardLogMutationAppender;
import io.nereusstream.delay.ownership.ShardLifecycleState;
import io.nereusstream.delay.ownership.SourceAssignment;
import io.nereusstream.delay.ownership.SourceReplayCursor;
import io.nereusstream.delay.ownership.SourceReplayEntry;
import io.nereusstream.delay.ownership.SourceReplayMutation;
import io.nereusstream.delay.ownership.SourceReplayRecord;
import io.nereusstream.delay.ownership.SourceReplaySuccessor;
import io.nereusstream.delay.ownership.WorkerAssignment;
import io.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import io.nereusstream.delay.ownership.WorkerCommandRuntime;
import io.nereusstream.delay.ownership.WorkerPhysicalPublishExecutor;
import io.nereusstream.delay.ownership.WorkerShardRuntime;
import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CapacityDimensionV1;
import io.nereusstream.delay.protocol.CapacityVectorV1;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.ChannelResourceIdentityV1;
import io.nereusstream.delay.protocol.ControlRef;
import io.nereusstream.delay.protocol.CredentialUseKindV1;
import io.nereusstream.delay.protocol.CredentialUseLeaseV1;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import io.nereusstream.delay.protocol.IngressCredentialBindingRefV1;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.KafkaIngressRouteResourceV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.PayloadAttestationOutcomeV1;
import io.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetActivatePayloadV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetSemanticV1;
import io.nereusstream.delay.protocol.PayloadProofVerifierKeyV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleOutcomeV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PublishEvidenceKindV1;
import io.nereusstream.delay.protocol.PublishEvidenceV1;
import io.nereusstream.delay.protocol.PublishOutcomeBody;
import io.nereusstream.delay.protocol.ReadyCertificateV1;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PulsarIngressRouteResourceV1;
import io.nereusstream.delay.protocol.PulsarMetadataV1;
import io.nereusstream.delay.protocol.PulsarPhysicalPartitionIdentityV1;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteLifecycleV1;
import io.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.RoutingHashVersionV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.StableErrorV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.protocol.UploadHandleKindV1;
import io.nereusstream.delay.route.OxiaRouteAuthoritySession;
import io.nereusstream.delay.route.OxiaSignedRouteSnapshotProvider;
import io.nereusstream.delay.route.OxiaSignedRouteSnapshotPublisher;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.InMemoryPayloadProofTrustSetCatalog;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.MessageRecord;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.PayloadReservation;
import io.nereusstream.delay.runtime.PayloadReservationStatus;
import io.nereusstream.delay.runtime.V1ScheduleResolver;
import io.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.DefaultDelaySemanticCore;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.semantic.SecureLogicalUuidV7Generator;
import io.nereusstream.delay.store.CheckpointFileInventory;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import io.nereusstream.delay.store.WorkerLoadVector;
import io.nereusstream.delay.store.WorkerPlacementPolicy;
import io.nereusstream.delay.submission.DefaultSubmissionCoordinator;
import io.nereusstream.delay.submission.KafkaManagedSubmissionOutcomeProjector;
import io.nereusstream.delay.submission.PulsarManagedSubmissionOutcomeProjector;
import io.nereusstream.delay.submission.RouteBoundSubmissionTransportPlanResolver;
import io.nereusstream.delay.submission.SubmissionOutcomeProjectorRegistry;
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
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;

import javax.net.ssl.SSLException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Real cross-adapter production-authority proof.  The source command log and
 * the destination physical/evidence log are deliberately owned by different
 * upstream clients in each direction.
 */
public final class CrossAdapterLargePayloadGatewaySmoke {
    private static final String PULSAR_CLUSTER = "standalone";
    private static final byte[] PULSAR_SOURCE_INCARNATION = digest(43);
    private static final long PULSAR_SOURCE_CREATED_AT = 2_001L;
    private static final byte[] PULSAR_DESTINATION_INCARNATION = digest(17);
    private static final long PULSAR_DESTINATION_CREATED_AT = 1_001L;
    private static final long LEASE_DURATION_MS = 60_000L;
    private static final long WORK_CLASS_BYTES = 2_000_000L;
    private static final long PAYLOAD_BYTES = (1L << 20) + 4_096L;
    private static final Duration KAFKA_POLL_TIMEOUT = Duration.ofMillis(250);
    private static final Duration PULSAR_RECEIVE_TIMEOUT = Duration.ofMillis(250);

    private CrossAdapterLargePayloadGatewaySmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 4) {
            throw new IllegalArgumentException(
                    "usage: <kafka-bootstrap> <pulsar-service-url> <pulsar-admin-url> <K_TO_P|P_TO_K>");
        }
        final String direction = arguments[3];
        if (!"K_TO_P".equals(direction) && !"P_TO_K".equals(direction)) {
            throw new IllegalArgumentException("cross direction must be K_TO_P or P_TO_K: " + direction);
        }
        final CrossConfig configuration = CrossConfig.load(arguments);
        if ("K_TO_P".equals(direction)) {
            runKafkaToPulsar(configuration);
        } else {
            runPulsarToKafka(configuration);
        }
    }

    private record CrossConfig(String kafkaBootstrap, String pulsarServiceUrl, String pulsarAdminUrl,
                               String direction, String oxiaEndpoint, URI minioEndpoint, String minioRegion,
                               String minioBucket, String minioAccessKey, String minioSecretKey,
                               String minioSessionToken, Duration minioRequestTimeout, Path gatewayServerCertificate,
                               Path gatewayServerKey, Path gatewayCaCertificate, Path gatewayClientCertificate,
                               Path gatewayClientKey, int gatewayPort, String namespace, List<String> pulsarAdminUrls) {
        private static CrossConfig load(final String[] arguments) {
            final String minioEndpoint = requiredEnv("NEREUS_DELAY_MINIO_ENDPOINT");
            final String minioAccessKey = requiredEnv("NEREUS_DELAY_MINIO_ACCESS_KEY");
            final String minioSecretKey = requiredEnv("NEREUS_DELAY_MINIO_SECRET_KEY");
            final String minioBucket = requiredEnv("NEREUS_DELAY_MINIO_BUCKET");
            final String serverCertificate = requiredEnv("NEREUS_DELAY_GATEWAY_SERVER_CERT");
            final String serverKey = requiredEnv("NEREUS_DELAY_GATEWAY_SERVER_KEY");
            final String caCertificate = requiredEnv("NEREUS_DELAY_GATEWAY_CA_CERT");
            final String clientCertificate = requiredEnv("NEREUS_DELAY_GATEWAY_CLIENT_CERT");
            final String clientKey = requiredEnv("NEREUS_DELAY_GATEWAY_CLIENT_KEY");
            final int gatewayPort = Integer.parseInt(requiredEnv("NEREUS_DELAY_GATEWAY_PORT"));
            if (gatewayPort <= 0 || gatewayPort > 65_535) {
                throw new IllegalArgumentException("NEREUS_DELAY_GATEWAY_PORT must be 1..65535");
            }
            final Path serverCertificatePath = regularFile(serverCertificate, "NEREUS_DELAY_GATEWAY_SERVER_CERT");
            final Path serverKeyPath = regularFile(serverKey, "NEREUS_DELAY_GATEWAY_SERVER_KEY");
            final Path caCertificatePath = regularFile(caCertificate, "NEREUS_DELAY_GATEWAY_CA_CERT");
            final Path clientCertificatePath = regularFile(clientCertificate, "NEREUS_DELAY_GATEWAY_CLIENT_CERT");
            final Path clientKeyPath = regularFile(clientKey, "NEREUS_DELAY_GATEWAY_CLIENT_KEY");
            final String adminList = configured("NEREUS_DELAY_PULSAR_ADMIN_URLS", arguments[2]);
            final List<String> pulsarAdminUrls = Arrays.stream(adminList.split(","))
                    .map(String::trim).filter(value -> !value.isBlank()).toList();
            if (pulsarAdminUrls.isEmpty()) {
                throw new IllegalArgumentException("at least one Pulsar admin URL is required");
            }
            return new CrossConfig(arguments[0], arguments[1], arguments[2], arguments[3],
                    requiredEnv("NEREUS_DELAY_OXIA_ENDPOINT"), URI.create(minioEndpoint),
                    configured("NEREUS_DELAY_MINIO_REGION", "us-east-1"), minioBucket, minioAccessKey,
                    minioSecretKey, configuredNullable("NEREUS_DELAY_MINIO_SESSION_TOKEN"),
                    configuredDuration("NEREUS_DELAY_MINIO_REQUEST_TIMEOUT_MS", 60_000L), serverCertificatePath,
                    serverKeyPath, caCertificatePath, clientCertificatePath, clientKeyPath, gatewayPort,
                    configured("NEREUS_DELAY_OXIA_NAMESPACE", "default"), pulsarAdminUrls);
        }
    }

    private static void runKafkaToPulsar(final CrossConfig configuration) throws Exception {
        final String sourceTopic = "nereus-delay-cross-k-source-" + UUID.randomUUID();
        final String destinationName = "nereus-delay-cross-p-destination-" + UUID.randomUUID();
        final String destinationTopic = "persistent://public/default/" + destinationName;
        final HttpClient pulsarAdmin = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
        PulsarClientArtifactLargePayloadGatewaySmoke.createTopic(pulsarAdmin, configuration.pulsarAdminUrl(),
                destinationName, PULSAR_DESTINATION_INCARNATION, PULSAR_DESTINATION_CREATED_AT);
        final AuthenticatedTenantContext tenant = tenant();
        final KeyPair proofKeys = ed25519();
        final PayloadProofTrustSetSemanticV1 trustSet = trustSet(proofKeys);
        final ProfileSemanticEnvelopeV1 objectStoreProfile = objectStoreProfile(configuration);
        final byte[] payload = payload();
        final byte[] payloadHash = Bytes.sha256(payload);
        final KeyPair controlKeys = ed25519();
        final RouteIncarnation routeIncarnation = RouteIncarnation.random();
        final ShardId shard = new ShardId(routeIncarnation, 0);
        final SystemMutation activation = KafkaClientArtifactLargePayloadGatewaySmoke.trustActivation(
                shard, trustSet.ref(), tenant, controlKeys);
        final PreparedCommand beforeRoute = sourceCommand(shard, "cross-k-to-p-before-route", AdapterKindV1.PULSAR);
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.kafkaBootstrap(),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(configuration.pulsarServiceUrl()).build();
             Admin admin = Admin.create(adminConfiguration)) {
            ensureKafkaTopic(admin, sourceTopic, 1);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid sourceTopicUuid = describeKafkaTopic(admin, sourceTopic).topicId();
            final UUID sourceTopicId = toUuid(sourceTopicUuid);
            KafkaClientArtifactLargePayloadGatewaySmoke.appendFrame(configuration.kafkaBootstrap(), clusterId,
                    sourceTopic, sourceTopicUuid, activation.encodeFrame(), 0);
            KafkaClientArtifactLargePayloadGatewaySmoke.appendFrame(configuration.kafkaBootstrap(), clusterId,
                    sourceTopic, sourceTopicUuid, CommandCodec.encodeFrameV1(beforeRoute), 1);
            final org.apache.kafka.clients.consumer.GuardedFetchEvidence fetchEvidence =
                    KafkaClientArtifactLargePayloadGatewaySmoke.fetchEvidence(configuration.kafkaBootstrap(), clusterId,
                            sourceTopic, sourceTopicId, shard);
            if (fetchEvidence.firstRecordOffset() != 0 || fetchEvidence.lastRecordOffset() != 1
                    || fetchEvidence.lastStableOffset() < 2) {
                throw new IllegalStateException("K->P source barrier did not cover activation and pre-route records");
            }
            final RouteSnapshotV1 snapshot = KafkaClientArtifactLargePayloadGatewaySmoke.routeSnapshot(clusterId,
                    sourceTopic, sourceTopicId, routeIncarnation, fetchEvidence, tenant, controlKeys);
            final RouteSelectionHint routeHint = new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));
            final String routePrefix = "nereus-delay/cross-k-to-p-route/" + UUID.randomUUID();
            final String assignmentPrefix = "nereus-delay/cross-k-to-p-assignment/" + UUID.randomUUID();
            final String gatewayPrefix = "nereus-delay/cross-k-to-p-gateway/" + UUID.randomUUID();
            try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                         configuration.oxiaEndpoint(), configuration.namespace(), "cross-k-to-p-route-publisher-"
                                 + UUID.randomUUID(), Duration.ofSeconds(15), routePrefix);
                 OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                         configuration.oxiaEndpoint(), configuration.namespace(), "cross-k-to-p-route-provider-"
                                 + UUID.randomUUID(), Duration.ofSeconds(15), routePrefix);
                 OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle = OxiaSyncOwnerLeaseBackend.connectUnchecked(
                         configuration.oxiaEndpoint(), configuration.namespace(), "cross-k-to-p-assignment-"
                                 + UUID.randomUUID(), Duration.ofSeconds(15), assignmentPrefix)) {
                final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(
                        publisherSession, routePrefix, controlKeys.getPublic());
                final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                        providerSession, routePrefix, controlKeys.getPublic(), System::currentTimeMillis);
                publisher.publish(routeHint, snapshot, 0);
                provider.refresh().toCompletableFuture().join();
                final WorkerAssignmentAuthority assignmentAuthority = new OxiaSyncWorkerAssignmentBackend(
                        assignmentHandle, assignmentPrefix);
                final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(provider,
                        new io.nereusstream.delay.ownership.WorkerAssignmentCoordinator(new WorkerPlacementPolicy(
                                new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), assignmentAuthority));
                final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement = coordinator.placeActive(
                        tenant, routeHint, KafkaClientArtifactLargePayloadGatewaySmoke.placementRequest(
                                System.currentTimeMillis()));
                final WorkerAssignment accepted = coordinator.requireAccepted(tenant,
                        placement.publication().revision(), placement.publication().assignment());
                KafkaClientArtifactLargePayloadGatewaySmoke.requireRouteAssignment(accepted, snapshot, clusterId,
                        sourceTopicId, 2);
                final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(assignmentHandle.backend());
                final OwnerLease lease = ownerAuthority.acquire(accepted.sourceAssignment(),
                        "cross-k-to-p-worker", assignmentHandle.sessionIdentity(), System.currentTimeMillis(),
                        LEASE_DURATION_MS).orElseThrow();
                final WorkClassExecutionRegistry workClasses = workClasses();
                final ProfileRefV1 destinationProfile = KafkaClientArtifactLargePayloadGatewaySmoke.destinationProfile();
                final CompatibleControlSnapshotV1 controlSnapshot =
                        KafkaClientArtifactLargePayloadGatewaySmoke.controlSnapshot(shard, destinationProfile);
                final Path root = Files.createTempDirectory("nereus-delay-cross-k-to-p-");
                final OwnerIdentityV1 ownerIdentity = new OwnerIdentityV1(bytes(16, 70), bytes(16, 71),
                        lease.ownerEpoch(), Bytes.sha256(Bytes.utf8("cross-k-to-p-owner-fence")));
                boolean assignmentWithdrawn = false;
                WorkerShardRuntime runtime = null;
                CrossTargetBridge physicalBridge = null;
                try (SharedRocksDbResources resources = new SharedRocksDbResources(ShardStoreConfig.defaults(root));
                     ShardStore store = ShardStore.open(ShardStoreConfig.defaults(root), shard, resources);
                     InMemoryCommandTransportRegistry transports = new InMemoryCommandTransportRegistry()) {
                    resources.bindWorkClassExecutionRegistry(workClasses);
                    store.recordControlSnapshot(controlSnapshot);
                    final InMemoryPayloadProofTrustSetCatalog trustCatalog = new InMemoryPayloadProofTrustSetCatalog();
                    trustCatalog.publish(trustSet);
                    final DelayShard delayShard = new DelayShard(store, DelayShardConfig.defaults(), null, null,
                            PulsarClientArtifactLargePayloadGatewaySmoke.scheduleResolver(destinationTopic), trustCatalog);
                    final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease, ownerIdentity);
                    final CredentialBindingKey binding = credentialBinding(snapshot);
                    final KafkaCommandTransportKey transportKey = new KafkaCommandTransportKey(clusterId, sourceTopic,
                            sourceTopicId, 0, binding);
                    final KafkaProducer<byte[], byte[]> gatewayProducer = KafkaClientArtifactLargePayloadGatewaySmoke
                            .kafkaProducer(configuration.kafkaBootstrap());
                    transports.register(new ProductionKafkaProduceTransport(transportKey,
                            new ProductionKafkaProduceTransport.Configuration(-1, true, true,
                                    "cross-k-to-p-gateway-client"),
                            new KafkaClientArtifactProduceTransport((GuardedProducer<byte[], byte[]>) gatewayProducer)));
                    final DefaultSubmissionCoordinator submissions = new DefaultSubmissionCoordinator(
                            new RouteBoundSubmissionTransportPlanResolver(provider, System::currentTimeMillis),
                            transports, SubmissionOutcomeProjectorRegistry.of(
                                    new KafkaManagedSubmissionOutcomeProjector(transportKey)));
                    final DefaultDelaySemanticCore core = new DefaultDelaySemanticCore(provider,
                            new SecureLogicalUuidV7Generator(), System::currentTimeMillis);
                    final S3CompatiblePayloadObjectStore payloadStore = newPayloadStore(configuration,
                            tenant, trustSet, objectStoreProfile, proofKeys);
                    try (GatewayFixture gatewayFixture = GatewayFixture.open(configuration, gatewayPrefix, tenant,
                            provider, core, submissions, payloadStore)) {
                        gatewayFixture.start();
                        try (ManagedChannelHandle channelHandle = new ManagedChannelHandle(tlsChannel(
                                configuration.gatewayPort(), configuration.gatewayCaCertificate(),
                                configuration.gatewayClientCertificate(), configuration.gatewayClientKey()))) {
                            final ManagedChannel channel = channelHandle.channel();
                            final DelayGatewayV1Grpc.DelayGatewayV1BlockingStub gateway = gatewayStub(channel,
                                    gatewayToken(gatewayFixture.jwtKeys(), tenant,
                                            certificateFingerprint(configuration.gatewayClientCertificate())));
                            final GatewayPrepareLargeScheduleRequestV1 prepareRequest =
                                    KafkaClientArtifactLargePayloadGatewaySmoke.prepareRequest(
                                    PulsarClientArtifactLargePayloadGatewaySmoke.largeScheduleIntent(
                                            System.currentTimeMillis()), payload.length, payloadHash,
                                            trustSet.ref(), objectStoreProfile.ref());
                            final GatewaySubmissionOutcomeV1 prepareResponse = gateway.prepareLargeSchedule(prepareRequest);
                            final CommandQueuedReceiptV1 prepareReceipt =
                                    KafkaClientArtifactLargePayloadGatewaySmoke.requireQueued(prepareResponse,
                                            "K->P PrepareLargeSchedule", 2);
                            final KafkaSourcePosition preparePosition = (KafkaSourcePosition)
                                    prepareReceipt.sourcePosition();
                            final byte[] reservationId = KafkaClientArtifactLargePayloadGatewaySmoke
                                    .reservationId(prepareReceipt);
                            final List<SourceReplayEntry> entries = KafkaClientArtifactLargePayloadGatewaySmoke
                                    .recoveryEntries(configuration.kafkaBootstrap(), clusterId, sourceTopic, sourceTopicId,
                                            accepted.sourceAssignment(), activation, beforeRoute);
                            KafkaClientArtifactLargePayloadGatewaySmoke.recover(accepted, ownerAuthority, ownedShard,
                                    entries, controlKeys, controlSnapshot, workClasses);
                            runtime = KafkaClientArtifactWorkerSourceFactory.create(
                                    KafkaClientArtifactSourceConsumerFactory.create(
                                            KafkaClientArtifactLargePayloadGatewaySmoke.consumerConfiguration(
                                                    configuration.kafkaBootstrap(), "cross-k-to-p-worker-" + UUID.randomUUID()),
                                            clusterId, sourceTopic, sourceTopicId, 0), sourceTopic, KAFKA_POLL_TIMEOUT,
                                    accepted.sourceAssignment(), workClasses, ownedShard, store, resources,
                                    ownerAuthority, controlKeys.getPublic(), null, null, null, null, null);
                            KafkaClientArtifactLargePayloadGatewaySmoke.requireApplied(
                                    KafkaClientArtifactLargePayloadGatewaySmoke.runUntilApplied(runtime),
                                    "K->P PrepareLargeSchedule");
                            final PayloadReservation reservation = Optional.ofNullable(
                                    delayShard.getReservation(reservationId)).orElseThrow();
                            if (reservation.status() != PayloadReservationStatus.RESERVED) {
                                throw new IllegalStateException("K->P Prepare did not leave RESERVED reservation: "
                                        + reservation.status());
                            }
                            payloadStore.register(reservation, trustSet.ref(), objectStoreProfile.ref());
                            final PayloadReservationReceiptV1 receipt = payloadStore.reservationReceipt(reservation);
                            final GatewayPayloadUploadHandleResponseV1 handleResponse = gateway
                                    .issuePayloadUploadHandle(GatewayIssuePayloadUploadHandleRequestV1.newBuilder()
                                            .setPayloadReservationReceiptV1(ByteString.copyFrom(receipt.payload()))
                                            .setUploadHandleKind(UploadHandleKindV1.OPAQUE_SINGLE_PUT.wireValue()).build());
                            final PayloadUploadHandleResponseV1 handleDomain = PayloadUploadHandleResponseV1.decode(
                                    handleResponse.getPayloadUploadHandleResponseV1().toByteArray());
                            if (handleDomain.outcome() != PayloadUploadHandleOutcomeV1.ISSUED) {
                                throw new IllegalStateException("K->P Gateway did not issue upload handle: "
                                        + handleDomain.outcome());
                            }
                            final OpaquePayloadUploadHandleV1 handle = handleDomain.issued();
                            payloadStore.upload(receipt, handle, payload, System.currentTimeMillis());
                            final GatewayPayloadAttestationResponseV1 attestationResponse = gateway
                                    .attestPayloadUpload(GatewayAttestPayloadUploadRequestV1.newBuilder()
                                            .setPayloadReservationReceiptV1(ByteString.copyFrom(receipt.payload()))
                                            .setOpaquePayloadUploadHandleV1(ByteString.copyFrom(handle.canonicalBytes()))
                                            .build());
                            final PayloadAttestationResponseV1 attestation = PayloadAttestationResponseV1.decode(
                                    attestationResponse.getPayloadAttestationResponseV1().toByteArray());
                            if (attestation.outcome() != PayloadAttestationOutcomeV1.ATTESTED
                                    || attestation.proof() == null) {
                                throw new IllegalStateException("K->P Gateway/MinIO did not attest payload");
                            }
                            final PayloadCommitProofV1 proof = attestation.proof();
                            final GatewaySubmissionOutcomeV1 commitResponse = gateway.commitLargeSchedule(
                                    KafkaClientArtifactLargePayloadGatewaySmoke.commitRequest(receipt, proof));
                            final CommandQueuedReceiptV1 commitReceipt =
                                    KafkaClientArtifactLargePayloadGatewaySmoke.requireQueued(commitResponse,
                                            "K->P CommitLargeSchedule", 3);
                            final KafkaSourcePosition commitPosition = (KafkaSourcePosition)
                                    commitReceipt.sourcePosition();
                            KafkaClientArtifactLargePayloadGatewaySmoke.requireApplied(
                                    KafkaClientArtifactLargePayloadGatewaySmoke.runUntilApplied(runtime),
                                    "K->P CommitLargeSchedule");
                            final MessageRecord message = Optional.ofNullable(
                                    delayShard.getMessage(prepareReceipt.command().delayMessageId())).orElseThrow();
                            if (delayShard.getReservation(reservationId).status()
                                    != PayloadReservationStatus.COMMITTED || message.status() != MessageStatus.SCHEDULED
                                    || message.payloadReference() == null
                                    || !Arrays.equals(message.payloadReference().immutableObjectVersion(),
                                    proof.immutableObjectVersion())
                                    || !Arrays.equals(message.payloadReference().proofId(), proof.proofId())) {
                                throw new IllegalStateException("K->P Worker did not persist committed Object Store reference");
                            }
                            final byte[] objectPayload = payloadStore.readPayload(message.payloadReference());
                            if (!Arrays.equals(payload, objectPayload)) {
                                throw new IllegalStateException("K->P Object Store readback mismatch");
                            }
                            final LaneRecord lane = Optional.ofNullable(delayShard.getLane(message.laneId()))
                                    .orElseThrow(() -> new IllegalStateException("K->P target Lane missing"));
                            final ShardLogMutationAppender sourceAppender = new KafkaClientArtifactShardLogMutationAppender(
                                    (GuardedProducer<byte[], byte[]>) KafkaClientArtifactLargePayloadGatewaySmoke
                                            .kafkaProducer(configuration.kafkaBootstrap()), shard, clusterId, sourceTopic,
                                    sourceTopicId, Duration.ofSeconds(20));
                            physicalBridge = new PulsarTargetBridge(
                                    PulsarClientArtifactWorkerSmoke.createPhysicalPublishBridge(pulsarClient, null, null,
                                    shard, commitPosition, destinationTopic, store, ownedShard,
                                            ownerIdentity, ownerAuthority, workClasses, controlKeys, destinationProfile,
                                            PulsarClientArtifactWorkerSmoke.capabilityProfile(), message.laneId(),
                                            lane.laneIncarnation(), WORK_CLASS_BYTES, null, 0, sourceAppender),
                                    pulsarClient, destinationTopic);
                            runtime.bindPhysicalPublishExecutor(physicalBridge.executor());
                            runCrossPhysical(runtime, delayShard, ownedShard, ownerIdentity, ownerAuthority,
                                    store, workClasses, controlKeys, physicalBridge,
                                    prepareReceipt.command().delayMessageId(),
                                    commitPosition, objectPayload);
                            final byte[] duplicate = gateway.prepareLargeSchedule(prepareRequest).toByteArray();
                            if (!Arrays.equals(prepareResponse.toByteArray(), duplicate)) {
                                throw new IllegalStateException("K->P Oxia Gateway idempotency did not return exact bytes");
                            }
                            drainAndRelease(runtime, root, ownerAuthority, shard, "cross-k-to-p-final-checkpoint");
                            runtime.close();
                            assignmentWithdrawn = assignmentAuthority.withdraw(placement.publication());
                            if (!assignmentWithdrawn) {
                                throw new IllegalStateException("K->P assignment withdrawal was not exact");
                            }
                            System.out.println("K_TO_P Gateway + real Kafka source + real Pulsar target + real Oxia + MinIO passed: "
                                    + "prepare=" + preparePosition.offset() + ", commit=" + commitPosition.offset()
                                    + ", targetEvidence=PULSAR_SEND_ACK, exactPayload=true, idempotency=true");
                        }
                    }
                } finally {
                    if (physicalBridge != null) {
                        physicalBridge.close();
                    }
                    if (runtime != null) {
                        try {
                            runtime.close();
                        } catch (RuntimeException ignored) {
                            // The authoritative failure, if any, is retained by the main path.
                        }
                    }
                    if (!assignmentWithdrawn) {
                        assignmentAuthority.withdraw(placement.publication());
                    }
                    deleteTree(root);
                }
            } finally {
                deleteKafkaTopic(admin, sourceTopic);
            }
        } finally {
            PulsarClientArtifactLargePayloadGatewaySmoke.deleteTopic(pulsarAdmin,
                    configuration.pulsarAdminUrls(), destinationName);
        }
    }

    private static void runPulsarToKafka(final CrossConfig configuration) throws Exception {
        final String sourceBase = "nereus-delay-cross-p-source-" + UUID.randomUUID();
        final String sourcePhysicalName = sourceBase + "-partition-0";
        final String sourcePhysicalBase = "persistent://public/default/" + sourceBase;
        final String sourcePhysicalTopic = "persistent://public/default/" + sourcePhysicalName;
        final String destinationTopic = "nereus-delay-cross-k-destination-" + UUID.randomUUID();
        final String receiptTopic = "nereus-delay-cross-k-receipt-" + UUID.randomUUID();
        final HttpClient pulsarAdmin = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
        PulsarClientArtifactLargePayloadGatewaySmoke.createPartitionedTopic(pulsarAdmin,
                configuration.pulsarAdminUrls().get(0), sourceBase, PULSAR_SOURCE_INCARNATION,
                PULSAR_SOURCE_CREATED_AT, configuration.pulsarAdminUrls());
        final AuthenticatedTenantContext tenant = tenant();
        final KeyPair proofKeys = ed25519();
        final PayloadProofTrustSetSemanticV1 trustSet = trustSet(proofKeys);
        final ProfileSemanticEnvelopeV1 objectStoreProfile = objectStoreProfile(configuration);
        final byte[] payload = payload();
        final byte[] payloadHash = Bytes.sha256(payload);
        final KeyPair controlKeys = ed25519();
        final RouteIncarnation routeIncarnation = RouteIncarnation.random();
        final ShardId shard = new ShardId(routeIncarnation, 0);
        final SystemMutation activation = PulsarClientArtifactLargePayloadGatewaySmoke.trustActivation(
                shard, trustSet.ref(), tenant, controlKeys);
            final PreparedCommand beforeRoute = sourceCommand(shard, "cross-p-to-k-before-route", AdapterKindV1.KAFKA);
        final TopicResourceGuard sourceGuard = new TopicResourceGuard(PULSAR_CLUSTER,
                PULSAR_SOURCE_INCARNATION, PULSAR_SOURCE_CREATED_AT);
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.kafkaBootstrap(),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(configuration.pulsarServiceUrl()).build();
             Admin admin = Admin.create(adminConfiguration)) {
            ensureKafkaTopic(admin, destinationTopic, 1);
            ensureKafkaTopic(admin, receiptTopic, 1);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final UUID destinationTopicId = toUuid(describeKafkaTopic(admin, destinationTopic).topicId());
            final UUID receiptTopicId = toUuid(describeKafkaTopic(admin, receiptTopic).topicId());
            final PulsarSourcePosition activationPosition =
                    PulsarClientArtifactLargePayloadGatewaySmoke.sendFrameAndPosition(pulsarClient, sourceGuard,
                            sourcePhysicalTopic, shard, activation.encodeFrame(), "cross-p-to-k-activation");
            final PulsarSourcePosition beforeRoutePosition = PulsarClientArtifactWorkerSmoke.sendAndPosition(
                    pulsarClient, sourceGuard, sourcePhysicalTopic, beforeRoute, "cross-p-to-k-before-route");
            if (activationPosition.compareWithinShard(beforeRoutePosition) >= 0) {
                throw new IllegalStateException("P->K source fixture order is not increasing");
            }
            final org.apache.pulsar.client.api.GuardedConsumer<byte[]> nativeConsumer =
                    PulsarClientArtifactSourceConsumerFactory.create(pulsarClient, sourceGuard,
                            sourcePhysicalTopic, "cross-p-to-k-worker-" + UUID.randomUUID());
            try {
                final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof sourceProof =
                        PulsarClientArtifactRecoverySourcePositioner.seekAfter(nativeConsumer, sourceGuard,
                                sourcePhysicalTopic, shard, Optional.empty(), Duration.ofSeconds(5));
                final RouteSnapshotV1 snapshot = PulsarClientArtifactLargePayloadGatewaySmoke.routeSnapshot(
                        sourcePhysicalBase, sourcePhysicalTopic, routeIncarnation, beforeRoutePosition,
                        sourceProof, tenant, controlKeys);
                final RouteSelectionHint routeHint = new RouteSelectionHint(AdapterKindV1.PULSAR,
                        Bytes.utf8("primary"));
                final String routePrefix = "nereus-delay/cross-p-to-k-route/" + UUID.randomUUID();
                final String assignmentPrefix = "nereus-delay/cross-p-to-k-assignment/" + UUID.randomUUID();
                final String gatewayPrefix = "nereus-delay/cross-p-to-k-gateway/" + UUID.randomUUID();
                try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                             configuration.oxiaEndpoint(), configuration.namespace(), "cross-p-to-k-route-publisher-"
                                     + UUID.randomUUID(), Duration.ofSeconds(15), routePrefix);
                     OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                             configuration.oxiaEndpoint(), configuration.namespace(), "cross-p-to-k-route-provider-"
                                     + UUID.randomUUID(), Duration.ofSeconds(15), routePrefix);
                     OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle =
                             OxiaSyncOwnerLeaseBackend.connectUnchecked(configuration.oxiaEndpoint(),
                                     configuration.namespace(), "cross-p-to-k-assignment-" + UUID.randomUUID(),
                                     Duration.ofSeconds(15), assignmentPrefix)) {
                    final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(
                            publisherSession, routePrefix, controlKeys.getPublic());
                    final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                            providerSession, routePrefix, controlKeys.getPublic(), System::currentTimeMillis);
                    publisher.publish(routeHint, snapshot, 0);
                    provider.refresh().toCompletableFuture().join();
                    final WorkerAssignmentAuthority assignmentAuthority = new OxiaSyncWorkerAssignmentBackend(
                            assignmentHandle, assignmentPrefix);
                    final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(
                            provider, new io.nereusstream.delay.ownership.WorkerAssignmentCoordinator(
                                    new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(
                                            1_000, 0, 0, 0, 0)), assignmentAuthority));
                    final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement = coordinator.placeActive(
                            tenant, routeHint, PulsarClientArtifactLargePayloadGatewaySmoke.placementRequest(
                                    System.currentTimeMillis()));
                    final WorkerAssignment accepted = coordinator.requireAccepted(tenant,
                            placement.publication().revision(), placement.publication().assignment());
                    PulsarClientArtifactLargePayloadGatewaySmoke.requireRouteAssignment(accepted, snapshot,
                            beforeRoutePosition, sourceProof);
                    final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(assignmentHandle.backend());
                    final OwnerLease lease = ownerAuthority.acquire(accepted.sourceAssignment(),
                            "cross-p-to-k-worker", assignmentHandle.sessionIdentity(), System.currentTimeMillis(),
                            LEASE_DURATION_MS).orElseThrow();
                    final WorkClassExecutionRegistry workClasses = workClasses();
                    final ProfileRefV1 destinationProfile = KafkaClientArtifactLargePayloadGatewaySmoke
                            .destinationProfile();
                    final CompatibleControlSnapshotV1 controlSnapshot =
                            KafkaClientArtifactLargePayloadGatewaySmoke.controlSnapshot(shard, destinationProfile);
                    final Path root = Files.createTempDirectory("nereus-delay-cross-p-to-k-");
                    final OwnerIdentityV1 ownerIdentity = new OwnerIdentityV1(bytes(16, 72), bytes(16, 73),
                            lease.ownerEpoch(), Bytes.sha256(Bytes.utf8("cross-p-to-k-owner-fence")));
                    boolean assignmentWithdrawn = false;
                    WorkerShardRuntime runtime = null;
                    CrossTargetBridge physicalBridge = null;
                    try (SharedRocksDbResources resources = new SharedRocksDbResources(ShardStoreConfig.defaults(root));
                         ShardStore store = ShardStore.open(ShardStoreConfig.defaults(root), shard, resources);
                         InMemoryCommandTransportRegistry transports = new InMemoryCommandTransportRegistry()) {
                        resources.bindWorkClassExecutionRegistry(workClasses);
                        store.recordControlSnapshot(controlSnapshot);
                        final InMemoryPayloadProofTrustSetCatalog trustCatalog =
                                new InMemoryPayloadProofTrustSetCatalog();
                        trustCatalog.publish(trustSet);
                        final DelayShard delayShard = new DelayShard(store, DelayShardConfig.defaults(), null, null,
                                KafkaClientArtifactLargePayloadGatewaySmoke.scheduleResolver(clusterId,
                                        destinationTopicId, destinationTopic), trustCatalog);
                        final PulsarCommandTransportKey transportKey = new PulsarCommandTransportKey(
                                PULSAR_CLUSTER, sourcePhysicalTopic, new Bytes32(PULSAR_SOURCE_INCARNATION),
                                PULSAR_SOURCE_CREATED_AT, 0,
                                credentialBinding(snapshot));
                        final PulsarClientArtifactSendTransport managedTransport =
                                new PulsarClientArtifactSendTransport(PulsarClientArtifactProducerFactory.create(
                                        pulsarClient, PULSAR_CLUSTER, PULSAR_SOURCE_INCARNATION,
                                        sourcePhysicalTopic, PULSAR_SOURCE_CREATED_AT, "cross-p-to-k-managed"),
                                        PULSAR_CLUSTER, PULSAR_SOURCE_INCARNATION, sourcePhysicalTopic,
                                        PULSAR_SOURCE_CREATED_AT, 0);
                        final PulsarClientArtifactSendTransport nativeTransport =
                                new PulsarClientArtifactSendTransport(PulsarClientArtifactProducerFactory.create(
                                        pulsarClient, PULSAR_CLUSTER, PULSAR_SOURCE_INCARNATION,
                                        sourcePhysicalTopic, PULSAR_SOURCE_CREATED_AT, "cross-p-to-k-native"),
                                        PULSAR_CLUSTER, PULSAR_SOURCE_INCARNATION, sourcePhysicalTopic,
                                        PULSAR_SOURCE_CREATED_AT, 0);
                        transports.register(new ProductionPulsarSendTransport(transportKey,
                                new ProductionPulsarSendTransport.Configuration(true, true, true,
                                        "cross-p-to-k-gateway-client"), managedTransport, nativeTransport));
                        final DefaultSubmissionCoordinator submissions = new DefaultSubmissionCoordinator(
                                new RouteBoundSubmissionTransportPlanResolver(provider, System::currentTimeMillis),
                                transports, SubmissionOutcomeProjectorRegistry.of(
                                        new PulsarManagedSubmissionOutcomeProjector(transportKey)));
                        final DefaultDelaySemanticCore core = new DefaultDelaySemanticCore(provider,
                                new SecureLogicalUuidV7Generator(), System::currentTimeMillis);
                        final S3CompatiblePayloadObjectStore payloadStore = newPayloadStore(configuration, tenant,
                                trustSet, objectStoreProfile, proofKeys);
                        try (GatewayFixture gatewayFixture = GatewayFixture.open(configuration, gatewayPrefix, tenant,
                                provider, core, submissions, payloadStore)) {
                            gatewayFixture.start();
                            try (ManagedChannelHandle channelHandle = new ManagedChannelHandle(tlsChannel(
                                    configuration.gatewayPort(), configuration.gatewayCaCertificate(),
                                    configuration.gatewayClientCertificate(), configuration.gatewayClientKey()))) {
                                final DelayGatewayV1Grpc.DelayGatewayV1BlockingStub gateway = gatewayStub(
                                        channelHandle.channel(), gatewayToken(gatewayFixture.jwtKeys(), tenant,
                                                certificateFingerprint(configuration.gatewayClientCertificate())));
                                final GatewayPrepareLargeScheduleRequestV1 prepareRequest =
                                        PulsarClientArtifactLargePayloadGatewaySmoke.prepareRequest(
                                                KafkaClientArtifactLargePayloadGatewaySmoke.largeScheduleIntent(
                                                        System.currentTimeMillis()), payload.length, payloadHash,
                                                trustSet.ref(), objectStoreProfile.ref());
                                final GatewaySubmissionOutcomeV1 prepareResponse =
                                        gateway.prepareLargeSchedule(prepareRequest);
                                final CommandQueuedReceiptV1 prepareReceipt =
                                        PulsarClientArtifactLargePayloadGatewaySmoke.requireQueued(prepareResponse,
                                                "P->K PrepareLargeSchedule", beforeRoutePosition);
                                final PulsarSourcePosition preparePosition =
                                        (PulsarSourcePosition) prepareReceipt.sourcePosition();
                                final byte[] reservationId = PulsarClientArtifactLargePayloadGatewaySmoke
                                        .reservationId(prepareReceipt);
                                final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease,
                                        ownerIdentity);
                                PulsarClientArtifactLargePayloadGatewaySmoke.recover(nativeConsumer, sourceGuard,
                                        accepted, ownerAuthority, ownedShard, activation, beforeRoute, controlKeys,
                                        controlSnapshot, workClasses, sourcePhysicalTopic);
                                runtime = PulsarClientArtifactWorkerSourceFactory.create(nativeConsumer, sourceGuard,
                                        sourcePhysicalTopic, PULSAR_RECEIVE_TIMEOUT, accepted.sourceAssignment(),
                                        workClasses, ownedShard, store, resources, ownerAuthority,
                                        controlKeys.getPublic(), null, null, null, null, null);
                                PulsarClientArtifactLargePayloadGatewaySmoke.requireApplied(
                                        PulsarClientArtifactLargePayloadGatewaySmoke.runUntilApplied(runtime),
                                        "P->K PrepareLargeSchedule");
                                final PayloadReservation reservation = Optional.ofNullable(
                                        delayShard.getReservation(reservationId)).orElseThrow();
                                if (reservation.status() != PayloadReservationStatus.RESERVED) {
                                    throw new IllegalStateException("P->K Prepare did not leave RESERVED reservation: "
                                            + reservation.status());
                                }
                                payloadStore.register(reservation, trustSet.ref(), objectStoreProfile.ref());
                                final PayloadReservationReceiptV1 receipt = payloadStore.reservationReceipt(reservation);
                                final GatewayPayloadUploadHandleResponseV1 handleResponse =
                                        gateway.issuePayloadUploadHandle(GatewayIssuePayloadUploadHandleRequestV1
                                                .newBuilder().setPayloadReservationReceiptV1(
                                                        ByteString.copyFrom(receipt.payload()))
                                                .setUploadHandleKind(UploadHandleKindV1.OPAQUE_SINGLE_PUT.wireValue())
                                                .build());
                                final PayloadUploadHandleResponseV1 handleDomain =
                                        PayloadUploadHandleResponseV1.decode(handleResponse
                                                .getPayloadUploadHandleResponseV1().toByteArray());
                                if (handleDomain.outcome() != PayloadUploadHandleOutcomeV1.ISSUED) {
                                    throw new IllegalStateException("P->K Gateway did not issue upload handle: "
                                            + handleDomain.outcome());
                                }
                                final OpaquePayloadUploadHandleV1 handle = handleDomain.issued();
                                payloadStore.upload(receipt, handle, payload, System.currentTimeMillis());
                                final GatewayPayloadAttestationResponseV1 attestationResponse =
                                        gateway.attestPayloadUpload(GatewayAttestPayloadUploadRequestV1.newBuilder()
                                                .setPayloadReservationReceiptV1(ByteString.copyFrom(receipt.payload()))
                                                .setOpaquePayloadUploadHandleV1(ByteString.copyFrom(
                                                        handle.canonicalBytes())).build());
                                final PayloadAttestationResponseV1 attestation = PayloadAttestationResponseV1.decode(
                                        attestationResponse.getPayloadAttestationResponseV1().toByteArray());
                                if (attestation.outcome() != PayloadAttestationOutcomeV1.ATTESTED
                                        || attestation.proof() == null) {
                                    throw new IllegalStateException("P->K Gateway/MinIO did not attest payload");
                                }
                                final PayloadCommitProofV1 proof = attestation.proof();
                                final GatewaySubmissionOutcomeV1 commitResponse = gateway.commitLargeSchedule(
                                        PulsarClientArtifactLargePayloadGatewaySmoke.commitRequest(receipt, proof));
                                final CommandQueuedReceiptV1 commitReceipt =
                                        PulsarClientArtifactLargePayloadGatewaySmoke.requireQueued(commitResponse,
                                                "P->K CommitLargeSchedule", preparePosition);
                                final PulsarSourcePosition commitPosition =
                                        (PulsarSourcePosition) commitReceipt.sourcePosition();
                                PulsarClientArtifactLargePayloadGatewaySmoke.requireApplied(
                                        PulsarClientArtifactLargePayloadGatewaySmoke.runUntilApplied(runtime),
                                        "P->K CommitLargeSchedule");
                                final MessageRecord message = Optional.ofNullable(
                                        delayShard.getMessage(prepareReceipt.command().delayMessageId())).orElseThrow();
                                if (delayShard.getReservation(reservationId).status()
                                        != PayloadReservationStatus.COMMITTED || message.status() != MessageStatus.SCHEDULED
                                        || message.payloadReference() == null
                                        || !Arrays.equals(message.payloadReference().immutableObjectVersion(),
                                        proof.immutableObjectVersion())
                                        || !Arrays.equals(message.payloadReference().proofId(), proof.proofId())) {
                                    throw new IllegalStateException("P->K Worker did not persist exact Object Store reference");
                                }
                                final byte[] objectPayload = payloadStore.readPayload(message.payloadReference());
                                if (!Arrays.equals(payload, objectPayload)) {
                                    throw new IllegalStateException("P->K Object Store readback mismatch");
                                }
                                final LaneRecord lane = Optional.ofNullable(delayShard.getLane(message.laneId()))
                                        .orElseThrow(() -> new IllegalStateException("P->K target Lane missing"));
                                final ShardLogMutationAppender sourceAppender =
                                        new PulsarClientArtifactShardLogMutationAppender(
                                                PulsarClientArtifactProducerFactory.create(pulsarClient,
                                                        PULSAR_CLUSTER, PULSAR_SOURCE_INCARNATION, sourcePhysicalTopic,
                                                        PULSAR_SOURCE_CREATED_AT, "cross-p-to-k-mutation"), nativeConsumer,
                                                shard, PULSAR_CLUSTER, PULSAR_SOURCE_INCARNATION, sourcePhysicalTopic,
                                                PULSAR_SOURCE_CREATED_AT, Duration.ofSeconds(20));
                                physicalBridge = new KafkaTargetBridge(
                                        KafkaClientArtifactWorkerSmoke.createPhysicalPublishBridge(
                                                configuration.kafkaBootstrap(), clusterId, null, null, shard,
                                                commitPosition, destinationTopic, destinationTopicId, receiptTopic,
                                                receiptTopicId, store, ownedShard, ownerIdentity, ownerAuthority,
                                                workClasses, controlKeys, destinationProfile,
                                                KafkaClientArtifactWorkerSmoke.capabilityProfile(), message.laneId(),
                                                lane.laneIncarnation(), WORK_CLASS_BYTES, null, 0, sourceAppender),
                                        configuration.kafkaBootstrap(), clusterId, destinationTopic, destinationTopicId);
                                runtime.bindPhysicalPublishExecutor(physicalBridge.executor());
                                runCrossPhysical(runtime, delayShard, ownedShard, ownerIdentity, ownerAuthority,
                                        store, workClasses, controlKeys, physicalBridge,
                                        prepareReceipt.command().delayMessageId(), commitPosition, objectPayload);
                                final byte[] duplicate = gateway.prepareLargeSchedule(prepareRequest).toByteArray();
                                if (!Arrays.equals(prepareResponse.toByteArray(), duplicate)) {
                                    throw new IllegalStateException("P->K Oxia Gateway idempotency did not return exact bytes");
                                }
                                drainAndRelease(runtime, root, ownerAuthority, shard, "cross-p-to-k-final-checkpoint");
                                runtime.close();
                                assignmentWithdrawn = assignmentAuthority.withdraw(placement.publication());
                                if (!assignmentWithdrawn) {
                                    throw new IllegalStateException("P->K assignment withdrawal was not exact");
                                }
                                System.out.println("P_TO_K Gateway + real Pulsar source + real Kafka target + real Oxia + MinIO passed: "
                                        + "prepare=" + preparePosition.ledgerId() + "/" + preparePosition.entryId()
                                        + ", commit=" + commitPosition.ledgerId() + "/" + commitPosition.entryId()
                                        + ", targetEvidence=KAFKA_TRANSACTIONAL_RECEIPT, exactPayload=true, idempotency=true");
                            }
                        }
                    } finally {
                        if (physicalBridge != null) {
                            physicalBridge.close();
                        }
                        if (runtime != null) {
                            try {
                                runtime.close();
                            } catch (RuntimeException ignored) {
                                // Keep the primary cross-adapter assertion authoritative.
                            }
                        }
                        if (!assignmentWithdrawn) {
                            assignmentAuthority.withdraw(placement.publication());
                        }
                        deleteTree(root);
                    }
                }
            deleteKafkaTopic(admin, destinationTopic);
            deleteKafkaTopic(admin, receiptTopic);
        } finally {
            PulsarClientArtifactLargePayloadGatewaySmoke.deletePartitionedTopic(pulsarAdmin,
                    configuration.pulsarAdminUrls(), sourceBase);
            try (Admin cleanupAdmin = Admin.create(adminConfiguration)) {
                deleteKafkaTopic(cleanupAdmin, destinationTopic);
                deleteKafkaTopic(cleanupAdmin, receiptTopic);
            } catch (Exception cleanupFailure) {
                System.err.println("P->K Kafka topic cleanup could not reconnect: " + cleanupFailure.getMessage());
            }
        }
    }

    private static AuthenticatedTenantContext tenant() {
        return new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
    }

    private static KeyPair ed25519() throws GeneralSecurityException {
        return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    }

    private static PayloadProofTrustSetSemanticV1 trustSet(final KeyPair proofKeys) {
        final long now = System.currentTimeMillis();
        return new PayloadProofTrustSetSemanticV1(1, List.of(PayloadProofVerifierKeyV1.fromPublicKey(
                7, proofKeys.getPublic(), Math.max(0, now - 60_000), now + 3_600_000)));
    }

    private static ProfileSemanticEnvelopeV1 objectStoreProfile(final CrossConfig configuration) {
        final io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1 semantic =
                new io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1(
                        io.nereusstream.delay.protocol.ObjectStoreProviderKindV1.S3_COMPATIBLE,
                        S3CompatiblePayloadObjectStore.endpointConfigDigest(configuration.minioEndpoint(),
                                configuration.minioRegion(), configuration.minioBucket()),
                        S3CompatiblePayloadObjectStore.credentialAuthorizationScopeDigest(
                                configuration.minioAccessKey(), configuration.minioRegion(), configuration.minioBucket()),
                        1, true, true, true, true, bytes(32, 20), 8L << 20,
                        io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1.SINGLE_PUT, 1, bytes(32, 21));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.OBJECT_STORE, Bytes.utf8("large-payload-store"), 1,
                semantic);
    }

    private static CredentialBindingKey credentialBinding(final RouteSnapshotV1 snapshot) {
        final IngressCredentialBindingRefV1 binding = snapshot.credentialBinding();
        return new CredentialBindingKey(binding.generation(), new Digest32(binding.bindingDigest()),
                new Digest32(binding.resolvedCredentialFingerprintDigest()));
    }

    private static S3CompatiblePayloadObjectStore newPayloadStore(final CrossConfig configuration,
                                                                   final AuthenticatedTenantContext tenant,
                                                                   final PayloadProofTrustSetSemanticV1 trustSet,
                                                                   final ProfileSemanticEnvelopeV1 objectStoreProfile,
                                                                   final KeyPair proofKeys) {
        return new S3CompatiblePayloadObjectStore(objectStoreProfile, configuration.minioEndpoint(),
                configuration.minioRegion(), configuration.minioBucket(), configuration.minioAccessKey(),
                configuration.minioSecretKey(), configuration.minioSessionToken(), tenant.tenantRoutingScope(),
                trustSet, 7, Long.MAX_VALUE, proofKeys.getPrivate(), null,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
                        .followRedirects(HttpClient.Redirect.NEVER).build(), Clock.systemUTC(),
                configuration.minioRequestTimeout());
    }

    private static byte[] payload() {
        final byte[] value = new byte[Math.toIntExact(PAYLOAD_BYTES)];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (index * 31 + 7);
        }
        return value;
    }

    private static PreparedCommand sourceCommand(final ShardId shard, final String identity,
                                                 final AdapterKindV1 destinationAdapter) {
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final ScheduleIntentV1 intent;
        if (destinationAdapter == AdapterKindV1.KAFKA) {
            intent = ScheduleIntentV1.create(KafkaClientArtifactLargePayloadGatewaySmoke.destinationProfile(),
                    KafkaClientArtifactLargePayloadGatewaySmoke.retryPolicy(), deliverAt, deliverAt + 10_000,
                    DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], Bytes.utf8(identity), null,
                    AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        } else if (destinationAdapter == AdapterKindV1.PULSAR) {
            intent = ScheduleIntentV1.create(PulsarClientArtifactLargePayloadGatewaySmoke.destinationProfile(),
                    PulsarClientArtifactLargePayloadGatewaySmoke.retryPolicy(), deliverAt, deliverAt + 10_000,
                    DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], Bytes.utf8(identity), null,
                    AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of())), null, null);
        } else {
            throw new IllegalArgumentException("cross destination adapter must be Kafka or Pulsar");
        }
        return PreparedCommand.scheduleV1(shard, intent, deliverAt + 20_000);
    }

    private static WorkClassExecutionRegistry workClasses() {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, 8, WORK_CLASS_BYTES, 1, WORK_CLASS_BYTES,
                    1_000_000, protectedClass ? 1 : 0, protectedClass ? 1 : 0,
                    workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies,
                TimeUnit.SECONDS.toNanos(5), TimeUnit.SECONDS.toNanos(30), 16, 8_000_000), System::nanoTime);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) seed);
        return result;
    }

    private static byte[] digest(final int seed) {
        return bytes(32, seed);
    }

    private static UUID toUuid(final Uuid value) {
        return new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private static void ensureKafkaTopic(final Admin admin, final String topic, final int partitions) throws Exception {
        try {
            final TopicDescription description = describeKafkaTopic(admin, topic);
            if (description.partitions().size() != partitions) {
                throw new IllegalStateException("Kafka cross topic partition count mismatch: " + topic);
            }
            return;
        } catch (Exception ignored) {
            // The exact generated topic is created below.
        }
        try {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all()
                    .get(15, TimeUnit.SECONDS);
        } catch (Exception failure) {
            final TopicDescription existing = describeKafkaTopic(admin, topic);
            if (existing.partitions().size() != partitions) {
                throw failure;
            }
        }
    }

    private static TopicDescription describeKafkaTopic(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
    }

    private static void deleteKafkaTopic(final Admin admin, final String topic) {
        try {
            admin.deleteTopics(List.of(topic)).all().get(15, TimeUnit.SECONDS);
        } catch (Exception failure) {
            System.err.println("Kafka cross topic cleanup failed for " + topic + ": " + failure.getMessage());
        }
    }

    private static void drainAndRelease(final WorkerShardRuntime runtime, final Path root,
                                        final OxiaOwnerLeaseStore ownerAuthority, final ShardId shard,
                                        final String checkpointName) throws Exception {
        final Path checkpointPath = root.resolve(checkpointName);
        final byte[] checkpointId = Arrays.copyOf(Bytes.sha256(Bytes.utf8(checkpointName)), 16);
        final var drain = runtime.drain(new io.nereusstream.delay.ownership.OwnerDrainCoordinator.DrainRequest(
                System.currentTimeMillis() + 30_000, 0, checkpointPath, checkpointId),
                System::currentTimeMillis, () -> { });
        if (drain.pendingCheckpointTask() != null || drain.finalCheckpointPath() == null
                || !Files.isDirectory(checkpointPath) || CheckpointFileInventory.collect(checkpointPath).isEmpty()
                || !ownerAuthority.current(shard).isEmpty()) {
            throw new IllegalStateException("cross Worker drain did not publish checkpoint and release Owner");
        }
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

    private static String requiredEnv(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required for the cross-adapter authority smoke");
        }
        return value;
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static String configuredNullable(final String name) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? null : value;
    }

    private static Duration configuredDuration(final String name, final long fallbackMillis) {
        final String value = configuredNullable(name);
        final long millis = value == null ? fallbackMillis : Long.parseLong(value);
        if (millis <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return Duration.ofMillis(millis);
    }

    private static Path regularFile(final String value, final String name) {
        final Path path = Path.of(value).toAbsolutePath().normalize();
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException(name + " is not a regular file: " + path);
        }
        return path;
    }

    private static final class GatewayFixture implements AutoCloseable {
        private final GatewayGrpcServer server;
        private final OxiaGatewayAdmissionController admission;
        private final OxiaGatewayIdempotencyStore idempotency;
        private final OxiaGatewayAuditSink audit;
        private final OxiaSyncOwnerLeaseBackend.ClientHandle admissionHandle;
        private final OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyHandle;
        private final OxiaSyncOwnerLeaseBackend.ClientHandle auditHandle;
        private final KeyPair jwtKeys;

        private GatewayFixture(final GatewayGrpcServer server, final OxiaGatewayAdmissionController admission,
                               final OxiaGatewayIdempotencyStore idempotency, final OxiaGatewayAuditSink audit,
                               final OxiaSyncOwnerLeaseBackend.ClientHandle admissionHandle,
                               final OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyHandle,
                               final OxiaSyncOwnerLeaseBackend.ClientHandle auditHandle, final KeyPair jwtKeys) {
            this.server = server;
            this.admission = admission;
            this.idempotency = idempotency;
            this.audit = audit;
            this.admissionHandle = admissionHandle;
            this.idempotencyHandle = idempotencyHandle;
            this.auditHandle = auditHandle;
            this.jwtKeys = jwtKeys;
        }

        private static GatewayFixture open(final CrossConfig configuration, final String gatewayPrefix,
                                           final AuthenticatedTenantContext tenant,
                                           final OxiaSignedRouteSnapshotProvider provider,
                                           final DefaultDelaySemanticCore core,
                                           final DefaultSubmissionCoordinator submissions,
                                           final S3CompatiblePayloadObjectStore payloadStore) {
            final OxiaSyncOwnerLeaseBackend.ClientHandle admissionHandle = OxiaSyncOwnerLeaseBackend
                    .connectUnchecked(configuration.oxiaEndpoint(), configuration.namespace(),
                            "cross-gateway-admission-" + UUID.randomUUID(), Duration.ofSeconds(15),
                            gatewayPrefix + "/admission-client");
            final OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyHandle = OxiaSyncOwnerLeaseBackend
                    .connectUnchecked(configuration.oxiaEndpoint(), configuration.namespace(),
                            "cross-gateway-idempotency-" + UUID.randomUUID(), Duration.ofSeconds(15),
                            gatewayPrefix + "/idempotency-client");
            final OxiaSyncOwnerLeaseBackend.ClientHandle auditHandle = OxiaSyncOwnerLeaseBackend
                    .connectUnchecked(configuration.oxiaEndpoint(), configuration.namespace(),
                            "cross-gateway-audit-" + UUID.randomUUID(), Duration.ofSeconds(15),
                            gatewayPrefix + "/audit-client");
            try {
                final OxiaGatewayAdmissionController admission = new OxiaGatewayAdmissionController(admissionHandle,
                        gatewayPrefix + "/admission", System::currentTimeMillis,
                        new OxiaGatewayAdmissionController.Limits(4, 8_000_000, 2, 2, 30_000, 8));
                final OxiaGatewayIdempotencyStore idempotency = new OxiaGatewayIdempotencyStore(idempotencyHandle,
                        gatewayPrefix + "/idempotency", System::currentTimeMillis, 10_000, 30_000);
                final OxiaGatewayAuditSink audit = new OxiaGatewayAuditSink(auditHandle, gatewayPrefix + "/audit");
                final GatewayScheduleService schedule = new GatewayScheduleService(core, idempotency, submissions,
                        System::currentTimeMillis);
                final KeyPair jwtKeys = rsa();
                final MutualTlsJwtGatewayTenantAuthority tenantAuthority = new MutualTlsJwtGatewayTenantAuthority(
                        new RsaSha256GatewayJwtVerifier(jwtKeys.getPublic(), "nereus-delay-gateway-e2e-issuer",
                                "nereus-delay-gateway-e2e", "gateway-e2e-key", Clock.systemUTC(), 30, 600));
                final GatewayIngressService ingress = new GatewayIngressService(schedule, tenantAuthority, admission,
                        audit, System::currentTimeMillis);
                final GatewayPayloadStoreAuthority payloadAuthority = new GatewayPayloadStoreAuthority(
                        tenant.tenantRoutingScope(),
                        (receipt, kind, now) -> payloadStore.issueUploadHandle(receipt, kind, now),
                        (receipt, handle, now) -> payloadStore.attest(receipt, handle, now));
                final GatewayPayloadIngressService payloadIngress = new GatewayPayloadIngressService(payloadAuthority,
                        tenantAuthority, admission, audit, System::currentTimeMillis);
                final GatewayGrpcServer server = GatewayGrpcServer.mutualTls(configuration.gatewayPort(),
                        configuration.gatewayServerCertificate(), configuration.gatewayServerKey(),
                        configuration.gatewayCaCertificate(), new io.nereusstream.delay.gateway.GatewayGrpcService(
                                ingress, GatewayGrpcContext.provider(), payloadIngress));
                return new GatewayFixture(server, admission, idempotency, audit, admissionHandle, idempotencyHandle,
                        auditHandle, jwtKeys);
            } catch (RuntimeException | Error failure) {
                closeQuietly(auditHandle);
                closeQuietly(idempotencyHandle);
                closeQuietly(admissionHandle);
                throw failure;
            } catch (Exception failure) {
                closeQuietly(auditHandle);
                closeQuietly(idempotencyHandle);
                closeQuietly(admissionHandle);
                throw new IllegalStateException("cross Gateway fixture construction failed", failure);
            }
        }

        private void start() throws Exception {
            server.start();
        }

        private KeyPair jwtKeys() {
            return jwtKeys;
        }

        @Override
        public void close() {
            RuntimeException first = null;
            try {
                server.close();
            } catch (RuntimeException failure) {
                first = failure;
            }
            try {
                audit.close();
            } catch (RuntimeException failure) {
                first = first == null ? failure : first;
            }
            try {
                idempotency.close();
            } catch (RuntimeException failure) {
                first = first == null ? failure : first;
            }
            try {
                admission.close();
            } catch (RuntimeException failure) {
                first = first == null ? failure : first;
            }
            closeQuietly(auditHandle);
            closeQuietly(idempotencyHandle);
            closeQuietly(admissionHandle);
            if (first != null) {
                throw first;
            }
        }
    }

    private static KeyPair rsa() throws GeneralSecurityException {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2_048);
        return generator.generateKeyPair();
    }

    private static void closeQuietly(final AutoCloseable resource) {
        try {
            resource.close();
        } catch (Exception ignored) {
            // The main E2E failure remains authoritative; cleanup is best effort.
        }
    }

    private static ManagedChannel tlsChannel(final int port, final Path ca, final Path clientCertificate,
                                             final Path clientPrivateKey) throws SSLException {
        final io.grpc.netty.shaded.io.netty.handler.ssl.SslContext sslContext = GrpcSslContexts.forClient()
                .trustManager(ca.toFile()).keyManager(clientCertificate.toFile(), clientPrivateKey.toFile()).build();
        return NettyChannelBuilder.forAddress("127.0.0.1", port).sslContext(sslContext).build();
    }

    private static DelayGatewayV1Grpc.DelayGatewayV1BlockingStub gatewayStub(final ManagedChannel channel,
                                                                               final String token) {
        final Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
        return DelayGatewayV1Grpc.newBlockingStub(ClientInterceptors.intercept(channel,
                MetadataUtils.newAttachHeadersInterceptor(headers)));
    }

    private static String gatewayToken(final KeyPair keyPair, final AuthenticatedTenantContext tenant,
                                       final byte[] certificateFingerprint) throws GeneralSecurityException {
        final long now = Instant.now().getEpochSecond();
        final String header = "{\"alg\":\"RS256\",\"typ\":\"JWT\",\"kid\":\"gateway-e2e-key\"}";
        final String claims = "{\"iss\":\"nereus-delay-gateway-e2e-issuer\",\"aud\":\"nereus-delay-gateway-e2e\","
                + "\"sub\":\"gateway-e2e-client\",\"tenant\":\"tenant-e2e\",\"tenant_scope_hash\":\""
                + encode(tenant.authenticatedTenantScopeHash()) + "\",\"tenant_routing_scope\":\""
                + encode(tenant.tenantRoutingScope()) + "\",\"iat\":" + (now - 100) + ",\"nbf\":"
                + (now - 100) + ",\"exp\":" + (now + 300) + ",\"jti\":\"gateway-e2e-jwt-"
                + UUID.randomUUID() + "\",\"cnf\":{\"x5t#S256\":\"" + encode(certificateFingerprint)
                + "\"}}";
        final String input = encode(header.getBytes(StandardCharsets.UTF_8)) + "."
                + encode(claims.getBytes(StandardCharsets.UTF_8));
        final Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(keyPair.getPrivate());
        signature.update(input.getBytes(StandardCharsets.US_ASCII));
        return input + "." + encode(signature.sign());
    }

    private static byte[] certificateFingerprint(final Path certificate) throws Exception {
        final CertificateFactory factory = CertificateFactory.getInstance("X.509");
        try (InputStream input = Files.newInputStream(certificate)) {
            return Bytes.sha256(((X509Certificate) factory.generateCertificate(input)).getEncoded());
        }
    }

    private static String encode(final byte[] value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
    }

    private abstract static class CrossTargetBridge {
        abstract WorkerPhysicalPublishExecutor executor();

        abstract ShardLogMutationAppender appender();

        abstract DestinationLaneId laneId();

        abstract byte[] laneIncarnation();

        abstract ChannelResourceIdentityV1 channel();

        abstract ReadyCertificateV1 readyCertificate();

        abstract List<EvidenceCursorV1> evidenceCursors();

        abstract PublishEvidenceKindV1 expectedEvidenceKind();

        abstract void bindGraph(WorkerShardRuntime runtime, OwnedDelayShard ownedShard,
                                OwnerIdentityV1 ownerIdentity, OxiaOwnerLeaseStore authority, ShardStore store,
                                WorkClassExecutionRegistry workClasses, KeyPair verificationKey);

        abstract void waitForCompletion(WorkerPhysicalPublishExecutor.Submission submission) throws Exception;

        abstract void verifyDestinationPayload(byte[] expectedPayload) throws Exception;

        abstract void requireDestinationResponseLossResolved(DestinationPublishResult result);

        abstract void close();
    }

    private static final class ManagedChannelHandle implements AutoCloseable {
        private final ManagedChannel channel;

        private ManagedChannelHandle(final ManagedChannel channel) {
            this.channel = channel;
        }

        private ManagedChannel channel() {
            return channel;
        }

        @Override
        public void close() {
            channel.shutdownNow();
            try {
                channel.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while closing Gateway channel", interrupted);
            }
        }
    }

    private static final class PulsarTargetBridge extends CrossTargetBridge {
        private final PulsarClientArtifactWorkerSmoke.PhysicalPublishBridge delegate;
        private final PulsarClient client;
        private final String destinationTopic;

        private PulsarTargetBridge(final PulsarClientArtifactWorkerSmoke.PhysicalPublishBridge delegate,
                                   final PulsarClient client, final String destinationTopic) {
            this.delegate = delegate;
            this.client = client;
            this.destinationTopic = destinationTopic;
        }

        @Override
        WorkerPhysicalPublishExecutor executor() {
            return delegate.executor();
        }

        @Override
        ShardLogMutationAppender appender() {
            return delegate.appender();
        }

        @Override
        DestinationLaneId laneId() {
            return delegate.laneId();
        }

        @Override
        byte[] laneIncarnation() {
            return delegate.laneIncarnation();
        }

        @Override
        ChannelResourceIdentityV1 channel() {
            return delegate.channel();
        }

        @Override
        ReadyCertificateV1 readyCertificate() {
            return delegate.readyCertificate();
        }

        @Override
        List<EvidenceCursorV1> evidenceCursors() {
            return delegate.evidenceCursors();
        }

        @Override
        PublishEvidenceKindV1 expectedEvidenceKind() {
            return PublishEvidenceKindV1.PULSAR_SEND_ACK;
        }

        @Override
        void bindGraph(final WorkerShardRuntime runtime, final OwnedDelayShard ownedShard,
                       final OwnerIdentityV1 ownerIdentity, final OxiaOwnerLeaseStore authority, final ShardStore store,
                       final WorkClassExecutionRegistry workClasses, final KeyPair verificationKey) {
            PulsarClientArtifactWorkerSmoke.bindActiveOwnerPublishGraph(runtime, ownedShard, ownerIdentity,
                    authority, store, workClasses, verificationKey, delegate, WORK_CLASS_BYTES);
        }

        @Override
        void waitForCompletion(final WorkerPhysicalPublishExecutor.Submission submission) throws Exception {
            PulsarClientArtifactWorkerSmoke.waitForPhysicalCompletion(submission);
        }

        @Override
        void verifyDestinationPayload(final byte[] expectedPayload) throws Exception {
            final TopicResourceGuard guard = new TopicResourceGuard(PULSAR_CLUSTER,
                    PULSAR_DESTINATION_INCARNATION, PULSAR_DESTINATION_CREATED_AT);
            final org.apache.pulsar.client.api.GuardedConsumer<byte[]> consumer =
                    PulsarClientArtifactSourceConsumerFactory.create(client, guard, destinationTopic,
                            "cross-pulsar-target-readback-" + UUID.randomUUID());
            try {
                final Message<byte[]> message = consumer.receive(15, TimeUnit.SECONDS);
                if (message == null || !Arrays.equals(expectedPayload, message.getValue())) {
                    throw new IllegalStateException("cross Pulsar target payload did not read back exactly");
                }
                consumer.acknowledge(message);
            } finally {
                consumer.close();
            }
        }

        @Override
        void requireDestinationResponseLossResolved(final DestinationPublishResult result) {
            if (delegate.destinationResponseLoss() && !delegate.destinationResponseEvidenceResolved()) {
                throw new IllegalStateException("cross Pulsar target response-loss evidence did not resolve");
            }
            if (delegate.destinationResponseLoss()) {
                final PublishEvidenceV1 evidence = PublishEvidenceV1.decode(result.evidence());
                if (evidence.evidenceKind() != PublishEvidenceKindV1.PULSAR_SEND_ACK
                        || evidence.verificationStatus() != EvidenceVerificationStatusV1.VERIFIED_PUBLISHED) {
                    throw new IllegalStateException("cross Pulsar target response-loss evidence branch is not verified");
                }
            }
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class KafkaTargetBridge extends CrossTargetBridge {
        private final KafkaClientArtifactWorkerSmoke.PhysicalPublishBridge delegate;
        private final String bootstrap;
        private final String clusterId;
        private final String destinationTopic;
        private final UUID destinationTopicId;

        private KafkaTargetBridge(final KafkaClientArtifactWorkerSmoke.PhysicalPublishBridge delegate,
                                  final String bootstrap, final String clusterId, final String destinationTopic,
                                  final UUID destinationTopicId) {
            this.delegate = delegate;
            this.bootstrap = bootstrap;
            this.clusterId = clusterId;
            this.destinationTopic = destinationTopic;
            this.destinationTopicId = destinationTopicId;
        }

        @Override
        WorkerPhysicalPublishExecutor executor() {
            return delegate.executor();
        }

        @Override
        ShardLogMutationAppender appender() {
            return delegate.appender();
        }

        @Override
        DestinationLaneId laneId() {
            return delegate.laneId();
        }

        @Override
        byte[] laneIncarnation() {
            return delegate.laneIncarnation();
        }

        @Override
        ChannelResourceIdentityV1 channel() {
            return delegate.channel();
        }

        @Override
        ReadyCertificateV1 readyCertificate() {
            return delegate.readyCertificate();
        }

        @Override
        List<EvidenceCursorV1> evidenceCursors() {
            return delegate.evidenceCursors();
        }

        @Override
        PublishEvidenceKindV1 expectedEvidenceKind() {
            return PublishEvidenceKindV1.KAFKA_TRANSACTIONAL_RECEIPT;
        }

        @Override
        void bindGraph(final WorkerShardRuntime runtime, final OwnedDelayShard ownedShard,
                       final OwnerIdentityV1 ownerIdentity, final OxiaOwnerLeaseStore authority, final ShardStore store,
                       final WorkClassExecutionRegistry workClasses, final KeyPair verificationKey) {
            KafkaClientArtifactWorkerSmoke.bindActiveOwnerPublishGraph(runtime, ownedShard, ownerIdentity, authority,
                    store, workClasses, verificationKey, delegate, WORK_CLASS_BYTES);
        }

        @Override
        void waitForCompletion(final WorkerPhysicalPublishExecutor.Submission submission) throws Exception {
            KafkaClientArtifactWorkerSmoke.waitForPhysicalCompletion(submission);
        }

        @Override
        void verifyDestinationPayload(final byte[] expectedPayload) {
            final ConsumerResourceGuard guard = new ConsumerResourceGuard(clusterId, destinationTopic,
                    new Uuid(destinationTopicId.getMostSignificantBits(), destinationTopicId.getLeastSignificantBits()), 0);
            final org.apache.kafka.clients.consumer.GuardedConsumer<byte[], byte[]> consumer =
                    KafkaClientArtifactSourceConsumerFactory.create(
                            KafkaClientArtifactLargePayloadGatewaySmoke.consumerConfiguration(bootstrap,
                                    "cross-kafka-target-readback-" + UUID.randomUUID()), clusterId, destinationTopic,
                            destinationTopicId, 0);
            final TopicPartition partition = new TopicPartition(destinationTopic, 0);
            try {
                consumer.assign(List.of(partition));
                consumer.seek(partition, 0);
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (System.nanoTime() < deadline) {
                    final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(Duration.ofMillis(250));
                    final org.apache.kafka.clients.consumer.GuardedFetchEvidence evidence =
                            KafkaClientArtifactFetchEvidence.requireBatch(records, guard);
                    if (evidence == null || records.isEmpty()) {
                        continue;
                    }
                    for (org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record
                            : records.records(partition)) {
                        KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, guard);
                        if (!Arrays.equals(expectedPayload, record.value())) {
                            throw new IllegalStateException("cross Kafka target payload did not read back exactly");
                        }
                        return;
                    }
                }
                throw new IllegalStateException("cross Kafka target payload did not become visible");
            } finally {
                consumer.close();
            }
        }

        @Override
        void requireDestinationResponseLossResolved(final DestinationPublishResult result) {
            delegate.requireDestinationResponseLossResolved(result);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static void runCrossPhysical(final WorkerShardRuntime runtime, final DelayShard delayShard,
                                         final OwnedDelayShard ownedShard, final OwnerIdentityV1 ownerIdentity,
                                         final OxiaOwnerLeaseStore authority, final ShardStore store,
                                         final WorkClassExecutionRegistry workClasses, final KeyPair verificationKey,
                                         final CrossTargetBridge bridge, final DelayMessageId physicalMessageId,
                                         final SourcePosition physicalSchedulePosition,
                                         final byte[] expectedPayload) throws Exception {
        final MessageRecord message = Optional.ofNullable(delayShard.getMessage(physicalMessageId))
                .orElseThrow(() -> new IllegalStateException("cross physical Schedule message is missing"));
        if (message.status() != MessageStatus.SCHEDULED || !message.laneId().equals(bridge.laneId())) {
            throw new IllegalStateException("cross physical Schedule did not resolve the target Lane");
        }
        delayShard.activateLaneReadiness(bridge.laneId(), bridge.laneIncarnation(), bridge.channel(),
                bridge.readyCertificate(), bridge.evidenceCursors());
        final LaneRecord lane = delayShard.getLane(bridge.laneId());
        if (lane == null || !lane.schedulable()) {
            throw new IllegalStateException("cross physical Lane did not become schedulable");
        }
        bridge.bindGraph(runtime, ownedShard, ownerIdentity, authority, store, workClasses, verificationKey);
        waitUntil(message.deliverAtEpochMs());
        final byte[] payload = Bytes.copy(expectedPayload);
        WorkerShardRuntime.DueClaimPublishPhysicalTurn dueClaimPublish = null;
        final long schedulerBudgetBytes = Math.max(900_000L, payload.length);
        for (int schedulerTurn = 0; schedulerTurn < 32; schedulerTurn++) {
            final long dueEarliest = Math.max(System.currentTimeMillis(), message.deliverAtEpochMs());
            dueClaimPublish = runtime.runDueClaimPublishPhysicalTurn(
                    evidence(dueEarliest, dueEarliest + 500, "cross-worker-due-clock"),
                    new SchedulerBudget(1, schedulerBudgetBytes, TimeUnit.SECONDS.toNanos(2)),
                    message.expireAtEpochMs() - 1, claimCharge(payload.length), System::currentTimeMillis,
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)), 16,
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)), 16,
                    ignored -> Optional.of(payload));
            if (dueClaimPublish.dueClaimPublishTurn().claimResult().isPresent()) {
                break;
            }
        }
        if (dueClaimPublish == null) {
            throw new IllegalStateException("cross Worker scheduler did not run");
        }
        final var dueClaim = dueClaimPublish.dueClaimPublishTurn();
        final var claimResult = dueClaim.claimResult().orElseThrow(
                () -> new IllegalStateException("cross Worker did not return Claim result"));
        if (claimResult.kind() != io.nereusstream.delay.ownership.ClaimHandoffWorkClassExecutor.ResultKind.CLAIMED) {
            throw new IllegalStateException("cross Worker Claim was not admitted: " + claimResult.kind());
        }
        final var admissionSubmission = dueClaim.publishSubmission().orElseThrow(
                () -> new IllegalStateException("cross Worker did not queue Publish Admission"));
        final var admissionResult = admissionSubmission.result().orElseThrow(
                () -> new IllegalStateException("cross Worker Publish Admission has no result"));
        if (admissionResult.kind() != io.nereusstream.delay.ownership.PublishAdmissionWorkClassExecutor.ResultKind.ENQUEUED
                || admissionResult.sourcePosition() == null
                || admissionResult.sourcePosition().kind() != physicalSchedulePosition.kind()
                || admissionResult.sourcePosition().compareTo(physicalSchedulePosition) <= 0) {
            throw new IllegalStateException("cross Worker Publish Admission did not retain the source adapter position: "
                    + admissionResult.sourcePosition());
        }
        final PublishAdmissionBody admissionBody = PublishAdmissionBody.decode(
                admissionResult.mutation().canonicalBody());
        final byte[] publishAttemptId = admissionBody.publishAttemptId();
        final WorkerShardRuntime.SourceBoundPhysicalPublishTurn physicalTurn = dueClaimPublish.physicalTurn()
                .orElseThrow(() -> new IllegalStateException("cross Worker did not start physical publish"));
        if (physicalTurn.status() != WorkerShardRuntime.SourceBoundPhysicalPublishStatus.PHYSICAL_SUBMITTED) {
            throw new IllegalStateException("cross Worker did not submit physical publish: " + physicalTurn.status()
                    + "/" + physicalTurn.failure());
        }
        final WorkerPhysicalPublishExecutor.Submission submission = physicalTurn.physicalSubmission().orElseThrow();
        bridge.waitForCompletion(submission);
        final DestinationPublishResult physicalResult = submission.physicalResult().orElseThrow();
        if (physicalResult.disposition() != DestinationPublishResult.Disposition.PUBLISHED
                || physicalResult.stableCode() != StableCode.OK || physicalResult.evidence() == null) {
            throw new IllegalStateException("cross Worker target did not return typed PUBLISHED evidence: "
                    + physicalResult.disposition() + "/" + physicalResult.stableCode());
        }
        final PublishEvidenceV1 physicalEvidence = PublishEvidenceV1.decode(physicalResult.evidence());
        if (physicalEvidence.evidenceKind() != bridge.expectedEvidenceKind()
                || physicalEvidence.verificationStatus() != EvidenceVerificationStatusV1.VERIFIED_PUBLISHED) {
            throw new IllegalStateException("cross Worker target evidence kind/status mismatch: "
                    + physicalEvidence.evidenceKind() + "/" + physicalEvidence.verificationStatus());
        }
        physicalEvidence.requireBusinessMutation(publishAttemptId, true);

        SourceReplayMutation outcomeRecord = null;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            workClasses.runTurn(new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)));
            final io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult turn = runtime.runSourceTurn(
                    new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)), System::currentTimeMillis);
            if (turn.status() == io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                if (turn.entry() instanceof SourceReplayMutation mutation
                        && mutation.mutation().type() == SystemMutationType.PUBLISH_OUTCOME) {
                    outcomeRecord = mutation;
                    break;
                }
                continue;
            }
            if (turn.status() != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && turn.status() != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException("cross Worker source outcome turn failed: " + turn.status(),
                        turn.failure());
            }
            TimeUnit.MILLISECONDS.sleep(25);
        }
        if (outcomeRecord == null) {
            throw new IllegalStateException("cross Worker PUBLISH_OUTCOME did not become visible");
        }
        final PublishOutcomeBody outcome = PublishOutcomeBody.decode(outcomeRecord.mutation().canonicalBody());
        if (outcome.sideEffect() != 1 || outcome.stableCode() != StableCode.OK
                || !Arrays.equals(outcome.publishAttemptId(), publishAttemptId)) {
            throw new IllegalStateException("cross Worker Publish Outcome was not definitive PUBLISHED");
        }
        final PublishEvidenceV1 outcomeEvidence = PublishEvidenceV1.decode(outcome.evidence());
        if (outcomeEvidence.evidenceKind() != bridge.expectedEvidenceKind()
                || outcomeEvidence.verificationStatus() != EvidenceVerificationStatusV1.VERIFIED_PUBLISHED) {
            throw new IllegalStateException("cross Worker source-applied outcome evidence mismatch");
        }
        outcomeEvidence.requireBusinessMutation(publishAttemptId, true);
        if (outcomeRecord.position().kind() != physicalSchedulePosition.kind()
                || outcomeRecord.position().compareTo(physicalSchedulePosition) <= 0) {
            throw new IllegalStateException("cross Worker source-applied outcome lost source position identity");
        }
        final MessageRecord finalMessage = delayShard.getMessage(physicalMessageId);
        if (finalMessage == null || finalMessage.status() != MessageStatus.PUBLISHED
                || delayShard.findOpenPublishAttempt(publishAttemptId) != null) {
            throw new IllegalStateException("cross Worker source apply did not close the PUBLISHED attempt");
        }
        bridge.verifyDestinationPayload(payload);
        bridge.requireDestinationResponseLossResolved(physicalResult);
        System.out.println("Cross Worker due->Claim->Admission->target publish->source Outcome passed: source="
                + physicalSchedulePosition.kind() + ", targetEvidence=" + bridge.expectedEvidenceKind());
    }

    private static byte[] claimCharge(final long payloadBytes) {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 1, payloadBytes,
                0, 0, 0, 0, 0, 0, 0, 0, 0).canonicalBytes();
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
}
