package io.nereusstream.delay.transport;

import com.google.protobuf.ByteString;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
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
import io.nereusstream.delay.ownership.OwnerRecoveryTurn;
import io.nereusstream.delay.ownership.OwnedDelayShard;
import io.nereusstream.delay.ownership.ReplayTurnBudget;
import io.nereusstream.delay.ownership.RouteWorkerAssignmentCoordinator;
import io.nereusstream.delay.ownership.ShardLifecycleState;
import io.nereusstream.delay.ownership.SourceAssignment;
import io.nereusstream.delay.ownership.SourceReplayCursor;
import io.nereusstream.delay.ownership.SourceReplayEntry;
import io.nereusstream.delay.ownership.SourceReplayMutation;
import io.nereusstream.delay.ownership.SourceReplayRecord;
import io.nereusstream.delay.ownership.SourceReplaySuccessor;
import io.nereusstream.delay.ownership.WorkerAssignment;
import io.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import io.nereusstream.delay.ownership.WorkerAssignmentCoordinator;
import io.nereusstream.delay.ownership.WorkerShardRuntime;
import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CapacityDimensionV1;
import io.nereusstream.delay.protocol.CapacityVectorV1;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.ControlRef;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.EnqueueOutcomeKindV1;
import io.nereusstream.delay.protocol.IngressCredentialBindingRefV1;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import io.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import io.nereusstream.delay.protocol.OrderingMode;
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
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteLifecycleV1;
import io.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.RoutingHashVersionV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
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
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.DefaultDelaySemanticCore;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.semantic.SecureLogicalUuidV7Generator;
import io.nereusstream.delay.submission.DefaultSubmissionCoordinator;
import io.nereusstream.delay.submission.KafkaManagedSubmissionOutcomeProjector;
import io.nereusstream.delay.submission.RouteBoundSubmissionTransportPlanResolver;
import io.nereusstream.delay.submission.SubmissionOutcomeProjectorRegistry;
import io.nereusstream.delay.store.CheckpointFileInventory;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import io.nereusstream.delay.store.WorkerLoadVector;
import io.nereusstream.delay.store.WorkerPlacementPolicy;
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

import javax.net.ssl.SSLException;
import java.io.InputStream;
import java.net.URI;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Opt-in production-authority vertical proof for the V1 large-payload path.
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

    private KafkaClientArtifactLargePayloadGatewaySmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <large-payload-topic>");
        }
        final String bootstrap = arguments[0];
        final String topic = arguments[1] + "-" + UUID.randomUUID();
        final String destinationPhysicalTopic = configuredNullable(
                "NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC");
        final String receiptPhysicalTopic = destinationPhysicalTopic == null
                ? null : destinationPhysicalTopic + "-receipt";
        final String oxiaEndpoint = requiredEnv("NEREUS_DELAY_OXIA_ENDPOINT");
        final String minioEndpoint = requiredEnv("NEREUS_DELAY_MINIO_ENDPOINT");
        final String minioAccessKey = requiredEnv("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String minioSecretKey = requiredEnv("NEREUS_DELAY_MINIO_SECRET_KEY");
        final String minioBucket = requiredEnv("NEREUS_DELAY_MINIO_BUCKET");
        final String minioRegion = configured("NEREUS_DELAY_MINIO_REGION", "us-east-1");
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
        final AuthenticatedTenantContext tenant = new AuthenticatedTenantContext(
                bytes(32, 1), bytes(32, 2), bytes(32, 3));
        final KeyPair proofKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final long proofNow = System.currentTimeMillis();
        final PayloadProofVerifierKeyV1 proofVerifierKey = PayloadProofVerifierKeyV1.fromPublicKey(
                7, proofKeys.getPublic(), Math.max(0, proofNow - 60_000), proofNow + 3_600_000);
        final PayloadProofTrustSetSemanticV1 trustSet = new PayloadProofTrustSetSemanticV1(1,
                List.of(proofVerifierKey));
        final ProfileSemanticEnvelopeV1 objectStoreProfile = objectStoreProfile(
                minioUri, minioRegion, minioBucket, minioAccessKey);
        final byte[] payload = payload();
        final byte[] payloadHash = Bytes.sha256(payload);
        final RouteIncarnation routeIncarnation = RouteIncarnation.random();
        final ShardId shard = new ShardId(routeIncarnation, 0);
        final RouteSelectionHint routeHint = new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));
        final KeyPair controlKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final SystemMutation trustActivation = trustActivation(shard, trustSet.ref(), tenant, controlKeys);
        final PreparedCommand beforeRoute = command(shard, "large-payload-before-route");
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

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
                    ? null : toUuid(describe(admin, destinationPhysicalTopic).topicId());
            final UUID receiptTopicId = receiptPhysicalTopic == null
                    ? null : toUuid(describe(admin, receiptPhysicalTopic).topicId());
            appendFrame(bootstrap, clusterId, topic, topicId, trustActivation.encodeFrame(), 0);
            appendFrame(bootstrap, clusterId, topic, topicId, CommandCodec.encodeFrameV1(beforeRoute), 1);

            final org.apache.kafka.clients.consumer.GuardedFetchEvidence fetchEvidence = fetchEvidence(
                    bootstrap, clusterId, topic, nativeTopicId, shard);
            if (fetchEvidence.firstRecordOffset() != 0 || fetchEvidence.lastRecordOffset() != 1
                    || fetchEvidence.lastStableOffset() < 2) {
                throw new IllegalStateException("Kafka large-payload barrier proof did not cover activation and pre-route records: "
                        + fetchEvidence);
            }
            final long barrierOffset = 2;
            final RouteSnapshotV1 snapshot = routeSnapshot(clusterId, topic, nativeTopicId, routeIncarnation,
                    fetchEvidence, tenant, controlKeys);
            final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
            final String routePrefix = "nereus-delay/kafka-large-payload-route/" + UUID.randomUUID();
            final String assignmentPrefix = "nereus-delay/kafka-large-payload-assignment/" + UUID.randomUUID();
            final String gatewayPrefix = "nereus-delay/kafka-large-payload-gateway/" + UUID.randomUUID();

            try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                         oxiaEndpoint, namespace, "nereus-delay-large-route-publisher-" + UUID.randomUUID(),
                         Duration.ofSeconds(15), routePrefix);
                 OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                         oxiaEndpoint, namespace, "nereus-delay-large-route-provider-" + UUID.randomUUID(),
                         Duration.ofSeconds(15), routePrefix);
                 OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle = OxiaSyncOwnerLeaseBackend.connectUnchecked(
                         oxiaEndpoint, namespace, "nereus-delay-large-assignment-" + UUID.randomUUID(),
                         Duration.ofSeconds(15), assignmentPrefix);
                 OxiaSyncOwnerLeaseBackend.ClientHandle admissionHandle = OxiaSyncOwnerLeaseBackend.connectUnchecked(
                         oxiaEndpoint, namespace, "nereus-delay-large-admission-" + UUID.randomUUID(),
                         Duration.ofSeconds(15), gatewayPrefix + "/admission-client");
                 OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyHandle = OxiaSyncOwnerLeaseBackend.connectUnchecked(
                         oxiaEndpoint, namespace, "nereus-delay-large-idempotency-" + UUID.randomUUID(),
                         Duration.ofSeconds(15), gatewayPrefix + "/idempotency-client");
                 OxiaSyncOwnerLeaseBackend.ClientHandle auditHandle = OxiaSyncOwnerLeaseBackend.connectUnchecked(
                         oxiaEndpoint, namespace, "nereus-delay-large-audit-" + UUID.randomUUID(),
                         Duration.ofSeconds(15), gatewayPrefix + "/audit-client")) {
                final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(
                        publisherSession, routePrefix, controlKeys.getPublic());
                final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                        providerSession, routePrefix, controlKeys.getPublic(), System::currentTimeMillis);
                publisher.publish(routeHint, snapshot, 0);
                provider.refresh().toCompletableFuture().join();

                final WorkerAssignmentAuthority assignmentAuthority = new OxiaSyncWorkerAssignmentBackend(
                        assignmentHandle, assignmentPrefix);
                final RouteWorkerAssignmentCoordinator routeCoordinator = new RouteWorkerAssignmentCoordinator(
                        provider, new WorkerAssignmentCoordinator(new WorkerPlacementPolicy(
                                new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), assignmentAuthority));
                final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement = routeCoordinator.placeActive(
                        tenant, routeHint, placementRequest(System.currentTimeMillis()));
                final WorkerAssignment accepted = routeCoordinator.requireAccepted(tenant,
                        placement.publication().revision(), placement.publication().assignment());
                requireRouteAssignment(accepted, snapshot, clusterId, nativeTopicId, barrierOffset);

                final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(assignmentHandle.backend());
                final OwnerLease lease = ownerAuthority.acquire(accepted.sourceAssignment(),
                        "kafka-large-payload-worker", assignmentHandle.sessionIdentity(),
                        System.currentTimeMillis(), LEASE_DURATION_MS).orElseThrow();
                final WorkClassExecutionRegistry workClasses = workClasses();
                final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshot(shard,
                        destinationProfile());
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
                        final V1ScheduleResolver resolver = destinationPhysicalTopic == null
                                ? scheduleResolver()
                                : scheduleResolver(clusterId, destinationTopicId, destinationPhysicalTopic);
                        final DelayShard delayShard = new DelayShard(store, DelayShardConfig.defaults(), null, null,
                                resolver, trustCatalog);
                        final io.nereusstream.delay.protocol.OwnerIdentityV1 ownerIdentity =
                                new io.nereusstream.delay.protocol.OwnerIdentityV1(bytes(16, 70), bytes(16, 71),
                                        lease.ownerEpoch(), Bytes.sha256(Bytes.utf8("large-payload-worker-fence")));
                        final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease, ownerIdentity);

                        final List<SourceReplayEntry> recoveryEntries = recoveryEntries(bootstrap, clusterId, topic,
                                nativeTopicId, accepted.sourceAssignment(), trustActivation, beforeRoute);
                        recover(accepted, ownerAuthority, ownedShard, recoveryEntries, controlKeys,
                                controlSnapshot, workClasses);
                        if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                || !(ownedShard.lastCatchupPosition() instanceof KafkaSourcePosition position)
                                || position.offset() != 1) {
                            throw new IllegalStateException("large-payload Worker recovery did not apply the source-ordered pre-route records");
                        }

                        final KafkaCommandTransportKey transportKey = new KafkaCommandTransportKey(clusterId, topic,
                                nativeTopicId, 0, new CredentialBindingKey(1, new Digest32(bytes(32, 41)),
                                new Digest32(bytes(32, 42))));
                        final KafkaProducer<byte[], byte[]> gatewayProducer = kafkaProducer(bootstrap);
                        transports.register(new ProductionKafkaProduceTransport(transportKey,
                                new ProductionKafkaProduceTransport.Configuration(-1, true, true,
                                        "large-payload-gateway-kafka-client"),
                                new KafkaClientArtifactProduceTransport(
                                        (GuardedProducer<byte[], byte[]>) gatewayProducer)));
                        final DefaultSubmissionCoordinator submissions = new DefaultSubmissionCoordinator(
                                new RouteBoundSubmissionTransportPlanResolver(provider, System::currentTimeMillis),
                                transports, SubmissionOutcomeProjectorRegistry.of(
                                        new KafkaManagedSubmissionOutcomeProjector(transportKey)));
                        final DefaultDelaySemanticCore core = new DefaultDelaySemanticCore(provider,
                                new SecureLogicalUuidV7Generator(), System::currentTimeMillis);
                        final S3CompatiblePayloadObjectStore payloadStore = new S3CompatiblePayloadObjectStore(
                                objectStoreProfile, minioUri, minioRegion, minioBucket, minioAccessKey,
                                minioSecretKey, minioSessionToken, tenant.tenantRoutingScope(), trustSet, 7,
                                proofKeys.getPrivate());

                        final OxiaGatewayAdmissionController admission = new OxiaGatewayAdmissionController(
                                admissionHandle, gatewayPrefix + "/admission", System::currentTimeMillis,
                                new OxiaGatewayAdmissionController.Limits(4, 8_000_000, 2, 2, 30_000, 8));
                        final OxiaGatewayIdempotencyStore idempotency = new OxiaGatewayIdempotencyStore(
                                idempotencyHandle, gatewayPrefix + "/idempotency", System::currentTimeMillis,
                                10_000, 30_000);
                        final OxiaGatewayAuditSink audit = new OxiaGatewayAuditSink(auditHandle,
                                gatewayPrefix + "/audit");
                        final GatewayScheduleService schedule = new GatewayScheduleService(core, idempotency,
                                submissions, System::currentTimeMillis);
                        final KeyPair jwtKeys = gatewayJwtKeys();
                        final io.nereusstream.delay.gateway.MutualTlsJwtGatewayTenantAuthority tenantAuthority =
                                new MutualTlsJwtGatewayTenantAuthority(new RsaSha256GatewayJwtVerifier(
                                        jwtKeys.getPublic(), "nereus-delay-gateway-e2e-issuer",
                                        "nereus-delay-gateway-e2e", "gateway-e2e-key", Clock.systemUTC(), 30, 600));
                        final GatewayIngressService ingress = new GatewayIngressService(schedule, tenantAuthority,
                                admission, audit, System::currentTimeMillis);
                        final GatewayPayloadStoreAuthority payloadAuthority = new GatewayPayloadStoreAuthority(
                                tenant.tenantRoutingScope(),
                                (receipt, kind, now) -> payloadStore.issueUploadHandle(receipt, kind, now),
                                (receipt, handle, now) -> payloadStore.attest(receipt, handle, now));
                        final GatewayPayloadIngressService payloadIngress = new GatewayPayloadIngressService(
                                payloadAuthority, tenantAuthority, admission, audit, System::currentTimeMillis);
                        final GatewayGrpcServer server = GatewayGrpcServer.mutualTls(gatewayPort,
                                serverCertificate, serverPrivateKey, trustedClientCertificates,
                                new io.nereusstream.delay.gateway.GatewayGrpcService(ingress,
                                        GatewayGrpcContext.provider(), payloadIngress));
                        final String token = token(jwtKeys, tenant, certificateFingerprint(clientCertificate));
                        WorkerShardRuntime runtime = null;
                        KafkaClientArtifactWorkerSmoke.PhysicalPublishBridge physicalBridge = null;
                        boolean runtimeClosed = false;
                        try {
                            server.start();
                            final ManagedChannel channel = channel(gatewayPort, trustedClientCertificates,
                                    clientCertificate, clientPrivateKey);
                            try {
                                final DelayGatewayV1Grpc.DelayGatewayV1BlockingStub gateway = stub(channel, token);
                                final ScheduleIntentV1 intent = largeScheduleIntent(System.currentTimeMillis());
                                final GatewayPrepareLargeScheduleRequestV1 prepareRequest = prepareRequest(intent,
                                        payload.length, payloadHash, trustSet.ref(), objectStoreProfile.ref());
                                final GatewaySubmissionOutcomeV1 prepareResponse = gateway.prepareLargeSchedule(
                                        prepareRequest);
                                final CommandQueuedReceiptV1 prepareReceipt = requireQueued(prepareResponse,
                                        "PrepareLargeSchedule", barrierOffset);
                                final KafkaSourcePosition preparePosition = (KafkaSourcePosition)
                                        prepareReceipt.sourcePosition();
                                final byte[] reservationId = reservationId(prepareReceipt);
                                final DestinationLaneId physicalLaneId = destinationPhysicalTopic == null ? null
                                        : laneId(clusterId, destinationTopicId, destinationPhysicalTopic);
                                runtime = KafkaClientArtifactWorkerSourceFactory.create(workerConsumer(bootstrap,
                                                "nereus-delay-large-worker-" + UUID.randomUUID(), clusterId, topic,
                                                nativeTopicId, shard), topic, POLL_TIMEOUT, accepted.sourceAssignment(),
                                        workClasses, ownedShard, store, resources, ownerAuthority,
                                        controlKeys.getPublic(), null, null, null, null,
                                        null);
                                requireApplied(runUntilApplied(runtime), "PrepareLargeSchedule");
                                final PayloadReservation reservation = Optional.ofNullable(
                                        delayShard.getReservation(reservationId)).orElseThrow(
                                                () -> new IllegalStateException("Worker did not persist the payload reservation"));
                                if (reservation.status() != PayloadReservationStatus.RESERVED) {
                                    throw new IllegalStateException("Prepare did not leave a RESERVED payload reservation: "
                                            + reservation.status());
                                }
                                payloadStore.register(reservation, trustSet.ref(), objectStoreProfile.ref());
                                final PayloadReservationReceiptV1 receipt = payloadStore.reservationReceipt(reservation);

                                final GatewayPayloadUploadHandleResponseV1 handleResponse = gateway
                                        .issuePayloadUploadHandle(GatewayIssuePayloadUploadHandleRequestV1.newBuilder()
                                                .setPayloadReservationReceiptV1(ByteString.copyFrom(receipt.payload()))
                                                .setUploadHandleKind(UploadHandleKindV1.OPAQUE_SINGLE_PUT.wireValue())
                                                .build());
                                final PayloadUploadHandleResponseV1 handleDomain =
                                        PayloadUploadHandleResponseV1.decode(
                                                handleResponse.getPayloadUploadHandleResponseV1().toByteArray());
                                if (handleDomain.outcome() != PayloadUploadHandleOutcomeV1.ISSUED) {
                                    throw new IllegalStateException("Gateway did not issue the payload upload handle: "
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
                                        || attestation.proof() == null
                                        || new String(attestation.proof().immutableObjectVersion(),
                                        StandardCharsets.UTF_8).startsWith("sha256-")) {
                                    throw new IllegalStateException("Gateway/MinIO did not return a provider-issued immutable payload proof");
                                }
                                final PayloadCommitProofV1 proof = attestation.proof();
                                final GatewayCommitLargeScheduleRequestV1 commitRequest =
                                        commitRequest(receipt, proof);
                                final GatewaySubmissionOutcomeV1 commitResponse = gateway.commitLargeSchedule(
                                        commitRequest);
                                final CommandQueuedReceiptV1 commitReceipt = requireQueued(commitResponse,
                                        "CommitLargeSchedule", barrierOffset + 1);
                                final KafkaSourcePosition commitPosition = (KafkaSourcePosition)
                                        commitReceipt.sourcePosition();
                                requireApplied(runUntilApplied(runtime), "CommitLargeSchedule");
                                final PayloadReservation committed = Optional.ofNullable(
                                        delayShard.getReservation(reservationId)).orElseThrow();
                                final MessageRecord message = Optional.ofNullable(
                                        delayShard.getMessage(prepareReceipt.command().delayMessageId())).orElseThrow();
                                if (committed.status() != PayloadReservationStatus.COMMITTED
                                        || message.status() != MessageStatus.SCHEDULED
                                        || message.payloadReference() == null
                                        || !Arrays.equals(message.payloadReference().immutableObjectVersion(),
                                        proof.immutableObjectVersion())
                                        || !Arrays.equals(message.payloadReference().proofId(), proof.proofId())) {
                                    throw new IllegalStateException("Worker did not persist the exact committed Object Store reference");
                                }
                                final byte[] objectPayload = payloadStore.readPayload(message.payloadReference());
                                if (!Arrays.equals(objectPayload, payload)) {
                                    throw new IllegalStateException("Worker Object Store readback did not match the committed payload");
                                }
                                if (!commitReceipt.command().commandType().name().equals("COMMIT_LARGE_SCHEDULE")) {
                                    throw new IllegalStateException("Commit receipt does not identify COMMIT_LARGE_SCHEDULE");
                                }
                                awaitConfiguredBrokerCut();
                                if (physicalLaneId != null) {
                                    final LaneRecord physicalLane = Optional.ofNullable(
                                            delayShard.getLane(physicalLaneId)).orElseThrow(
                                            () -> new IllegalStateException(
                                                    "Worker did not persist the physical destination Lane"));
                                    physicalBridge = KafkaClientArtifactWorkerSmoke.createPhysicalPublishBridge(
                                            bootstrap, clusterId, topic, nativeTopicId, shard,
                                            preparePosition, destinationPhysicalTopic, destinationTopicId,
                                            receiptPhysicalTopic, receiptTopicId, store, ownedShard, ownerIdentity,
                                            ownerAuthority, workClasses, controlKeys, destinationProfile(),
                                            capabilityProfile(), physicalLaneId, physicalLane.laneIncarnation(),
                                            LARGE_PAYLOAD_WORK_CLASS_BYTES);
                                    runtime.bindPhysicalPublishExecutor(physicalBridge.executor());
                                }
                                if (physicalBridge != null) {
                                    KafkaClientArtifactWorkerSmoke.runSourceAppliedPhysicalPublish(runtime, delayShard,
                                            ownedShard, ownerIdentity, ownerAuthority, store, workClasses, controlKeys,
                                            physicalBridge, prepareReceipt.command().delayMessageId(),
                                            commitPosition, objectPayload, bootstrap, clusterId,
                                            LARGE_PAYLOAD_WORK_CLASS_BYTES);
                                }
                                final byte[] duplicate = gateway.prepareLargeSchedule(prepareRequest).toByteArray();
                                if (!Arrays.equals(prepareResponse.toByteArray(), duplicate)) {
                                    throw new IllegalStateException("real Oxia Gateway idempotency did not return exact Prepare bytes");
                                }
                                final long latest = latestOffset(admin, topic, 0);
                                final long expectedLatest = barrierOffset + (physicalBridge == null ? 2 : 4);
                                if (latest != expectedLatest) {
                                    throw new IllegalStateException("duplicate Prepare appended an unexpected Kafka record: latest="
                                            + latest + ", expected=" + expectedLatest);
                                }
                                final Path checkpointPath = root.resolve("large-payload-final-checkpoint");
                                final byte[] checkpointId = java.util.Arrays.copyOf(
                                        Bytes.sha256(Bytes.utf8("large-payload-final-checkpoint")), 16);
                                final var drain = runtime.drain(
                                        new io.nereusstream.delay.ownership.OwnerDrainCoordinator.DrainRequest(
                                                System.currentTimeMillis() + 30_000, 0, checkpointPath, checkpointId),
                                        System::currentTimeMillis, () -> { });
                                if (drain.pendingCheckpointTask() != null || drain.finalCheckpointPath() == null
                                        || !Files.isDirectory(checkpointPath)
                                        || CheckpointFileInventory.collect(checkpointPath).isEmpty()
                                        || !ownerAuthority.current(shard).isEmpty()) {
                                    throw new IllegalStateException("large-payload Worker drain did not publish the final checkpoint or release the owner lease");
                                }
                                runtime.close();
                                runtimeClosed = true;
                                if (!assignmentAuthority.withdraw(placement.publication())) {
                                    throw new IllegalStateException("large-payload Worker assignment was not withdrawn exactly");
                                }
                                assignmentWithdrawn = true;
                                System.out.println("Kafka + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker "
                                        + "+ MinIO large-payload authority E2E passed: activationOffset=0, "
                                        + "barrierOffset=" + barrierOffset + ", prepareOffset="
                                        + prepareReceipt.sourcePosition() + ", commitOffset=" + commitReceipt.sourcePosition()
                                        + ", providerVersion=" + new String(proof.immutableObjectVersion(),
                                        StandardCharsets.UTF_8) + ", exactGatewayIdempotency=true");
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

    private static KafkaProducer<byte[], byte[]> kafkaProducer(final String bootstrap) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(configuration, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static org.apache.kafka.clients.consumer.GuardedFetchEvidence fetchEvidence(final String bootstrap,
                                                                                         final String clusterId,
                                                                                         final String topic,
                                                                                         final UUID topicId,
                                                                                         final ShardId shard) {
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, "nereus-delay-large-barrier-" + UUID.randomUUID()), clusterId,
                topic, topicId, shard.partition());
        final TopicPartition topicPartition = new TopicPartition(topic, shard.partition());
        final ConsumerResourceGuard guard = new ConsumerResourceGuard(clusterId, topic,
                new Uuid(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits()), shard.partition());
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

    private static List<SourceReplayEntry> recoveryEntries(final String bootstrap, final String clusterId,
                                                            final String topic, final UUID topicId,
                                                            final SourceAssignment assignment,
                                                            final SystemMutation activation,
                                                            final PreparedCommand beforeRoute) {
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, "nereus-delay-large-recovery-" + UUID.randomUUID()), clusterId,
                topic, topicId, assignment.shardId().partition());
        final List<SourceReplayEntry> entries = new ArrayList<>();
        try (KafkaClientArtifactRecoverySourceCursor cursor = new KafkaClientArtifactRecoverySourceCursor(
                consumer, assignment, topic, 0, POLL_TIMEOUT)) {
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
                || first.offset() != 0 || second.offset() != 1) {
            throw new IllegalStateException("Kafka recovery did not return the exact trust activation and pre-route records");
        }
        return List.copyOf(entries);
    }

    private static void recover(final WorkerAssignment accepted, final OxiaOwnerLeaseStore authority,
                                final OwnedDelayShard ownedShard, final List<SourceReplayEntry> entries,
                                final KeyPair verificationKeys, final CompatibleControlSnapshotV1 controlSnapshot,
                                final WorkClassExecutionRegistry workClasses) {
        final OwnerRecoveryCoordinator recovery = new OwnerRecoveryCoordinator(ownedShard, authority,
                accepted.sourceAssignment(), SourceReplaySuccessor.strictKafka(),
                SourceReplayCursor.of(entries.iterator()), verificationKeys.getPublic(), controlSnapshot,
                System::currentTimeMillis, new ReplayTurnBudget(2, 1_000_000, TimeUnit.SECONDS.toNanos(10)),
                workClasses);
        OwnerRecoveryTurn turn;
        do {
            turn = recovery.runTurn();
        } while (!turn.complete());
        if (!recovery.complete() || turn.outcomes().size() != 2) {
            throw new IllegalStateException("Kafka large-payload Worker recovery did not apply exactly two source records");
        }
    }

    private static io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult runUntilApplied(
            final WorkerShardRuntime runtime) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result;
        do {
            result = runtime.runSourceTurn(new SchedulerBudget(1, 2_000_000, TimeUnit.SECONDS.toNanos(2)),
                    System::currentTimeMillis);
            if (result.status() == io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                return result;
            }
            if (result.status() != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status() != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                    .WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException("Kafka large-payload Worker source turn failed: " + result.status(),
                        result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Kafka large-payload Worker source record did not become visible");
    }

    private static void requireApplied(final io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result,
                                       final String operation) {
        if (result.status() != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
            throw new IllegalStateException(operation + " was not applied and ACKed: " + result.status(),
                    result.failure());
        }
    }

    private static CommandQueuedReceiptV1 requireQueued(final GatewaySubmissionOutcomeV1 response,
                                                         final String operation, final long expectedOffset) {
        if (!response.hasSubmissionOutcomeNdr1()) {
            final StableErrorV1 error = StableErrorV1.decode(response.getPreparationErrorV1().toByteArray());
            throw new IllegalStateException(operation + " returned preparation error: stage=" + error.stage()
                    + ", code=" + error.code() + ", retryability=" + error.retryability()
                    + ", retryAtEpochMs=" + error.retryAtEpochMs()
                    + ", diagnosticCode=" + error.diagnosticCode());
        }
        final SubmissionOutcomeMessageV1 outcome = SubmissionOutcomeMessageV1.decode(
                response.getSubmissionOutcomeNdr1().toByteArray());
        if (outcome.kind() != io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.MANAGED
                || outcome.managed().kind() != EnqueueOutcomeKindV1.QUEUED) {
            throw new IllegalStateException(operation + " did not produce a managed QUEUED outcome: " + outcome);
        }
        final CommandQueuedReceiptV1 receipt = outcome.managed().queued();
        if (!(receipt.sourcePosition() instanceof KafkaSourcePosition position)
                || position.offset() != expectedOffset) {
            throw new IllegalStateException(operation + " Kafka offset mismatch: expected=" + expectedOffset
                    + ", actual=" + receipt.sourcePosition());
        }
        return receipt;
    }

    private static byte[] reservationId(final CommandQueuedReceiptV1 receipt) {
        return Bytes.sha256(Bytes.utf8("nereus-delay-reservation-id-v1\0"), receipt.command().commandId().bytes(),
                receipt.command().delayMessageId().bytes(), receipt.command().commandHash());
    }

    private static GatewayPrepareLargeScheduleRequestV1 prepareRequest(final ScheduleIntentV1 intent,
                                                                         final long payloadLength,
                                                                         final byte[] payloadHash,
                                                                         final PayloadProofTrustSetRefV1 trustSet,
                                                                         final ProfileRefV1 objectStoreProfile) {
        return GatewayPrepareLargeScheduleRequestV1.newBuilder()
                .setIdempotencyKey(ByteString.copyFrom(bytes(16, 80)))
                .setRoute(GatewayRouteSelectorV1.newBuilder().setIngressAdapterKind(AdapterKindV1.KAFKA.wireValue())
                        .setRouteAliasUtf8Nfc(ByteString.copyFromUtf8("primary")))
                .setScheduleIntentV1(ByteString.copyFrom(intent.canonicalBytes()))
                .setExpectedPayloadLength(payloadLength)
                .setPayloadSha256(ByteString.copyFrom(payloadHash))
                .setReservationTtlMs(120_000)
                .setPayloadProofTrustSetRefV1(ByteString.copyFrom(trustSet.canonicalBytes()))
                .setObjectStoreProfileRefV1(ByteString.copyFrom(objectStoreProfile.canonicalBytes()))
                .setRetryUntilEpochMs(System.currentTimeMillis() + 120_000)
                .build();
    }

    private static GatewayCommitLargeScheduleRequestV1 commitRequest(final PayloadReservationReceiptV1 receipt,
                                                                       final PayloadCommitProofV1 proof) {
        return GatewayCommitLargeScheduleRequestV1.newBuilder()
                .setIdempotencyKey(ByteString.copyFrom(bytes(16, 81)))
                .setPayloadReservationReceiptV1(ByteString.copyFrom(receipt.payload()))
                .setPayloadCommitProofV1(ByteString.copyFrom(proof.canonicalBytes()))
                .setRetryUntilEpochMs(System.currentTimeMillis() + 120_000)
                .build();
    }

    private static ScheduleIntentV1 largeScheduleIntent(final long now) {
        final long deliverAt = now + 15_000;
        return ScheduleIntentV1.forPrepare(destinationProfile(), retryPolicy(), deliverAt, deliverAt + 120_000,
                DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, Bytes.utf8("large-payload-key"),
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destinationProfile(), retryPolicy(), deliverAt,
                deliverAt + 10_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0],
                Bytes.utf8(identity), null, AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        return PreparedCommand.scheduleV1(shard, intent, deliverAt + 20_000);
    }

    private static ProfileRefV1 destinationProfile() {
        return new ProfileRefV1(Bytes.utf8("destination-large-payload"), 1,
                Bytes.sha256(Bytes.utf8("destination-large-payload-semantic")), ProfileKindV1.DESTINATION);
    }

    private static RetryPolicyRefV1 retryPolicy() {
        return new RetryPolicyRefV1(Bytes.utf8("retry-large-payload"), 1,
                Bytes.sha256(Bytes.utf8("retry-large-payload-semantic")));
    }

    private static ProfileSemanticEnvelopeV1 objectStoreProfile(final URI endpoint, final String region,
                                                                 final String bucket, final String accessKey) {
        final ObjectStoreProfileSemanticV1 semantic = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3_COMPATIBLE,
                S3CompatiblePayloadObjectStore.endpointConfigDigest(endpoint, region, bucket),
                S3CompatiblePayloadObjectStore.credentialAuthorizationScopeDigest(accessKey, region, bucket), 1,
                true, true, true, true, bytes(32, 20), 8L << 20, ObjectStoreProfileSemanticV1.SINGLE_PUT, 1,
                bytes(32, 21));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.OBJECT_STORE, Bytes.utf8("large-payload-store"), 1,
                semantic);
    }

    private static byte[] payload() {
        final byte[] value = new byte[(int) PAYLOAD_BYTES];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (index * 31 + 7);
        }
        return value;
    }

    private static SystemMutation trustActivation(final ShardId shard, final PayloadProofTrustSetRefV1 trustSet,
                                                  final AuthenticatedTenantContext tenant,
                                                  final KeyPair signingKeys) {
        final long retryUntil = System.currentTimeMillis() + 300_000;
        final ControlRef controlRef = new ControlRef(Bytes.sha256(Bytes.utf8("large-payload-trust-op")),
                Bytes.sha256(Bytes.utf8("large-payload-trust-request")), 1);
        final byte[] body = trustSetControlBody(shard, controlRef, trustSet, retryUntil);
        return SystemMutation.signed(shard, SystemMutationType.APPLY_SHARD_CONTROL, retryUntil,
                controlRef.logicalOperationIdentity(12), body,
                AuthorIdentity.control(Bytes.sha256(Bytes.utf8("large-payload-control-actor")),
                        Bytes.sha256(Bytes.utf8("large-payload-control-role")), tenant.authenticatedTenantScopeHash())
                        .canonicalBytes(), 1, signingKeys.getPrivate());
    }

    private static byte[] trustSetControlBody(final ShardId shard, final ControlRef controlRef,
                                              final PayloadProofTrustSetRefV1 trustSet, final long retryUntil) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] payload = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 12,
                new PayloadProofTrustSetActivatePayloadV1(trustSet).canonicalBytes()));
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

    private static V1ScheduleResolver scheduleResolver() {
        final byte[] tuple = Bytes.utf8("large-payload-kafka-canonical-lane-tuple-v1");
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        return new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(final ShardId shard, final io.nereusstream.delay.protocol.DelayMessageId message,
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

    private static V1ScheduleResolver scheduleResolver(final String clusterId, final UUID destinationTopicId,
                                                       final String destinationPhysicalTopic) {
        final ProfileRefV1 destination = destinationProfile();
        final ProfileRefV1 capability = capabilityProfile();
        final byte[] tuple = canonicalLaneTuple(clusterId, destinationTopicId, destinationPhysicalTopic,
                destination, capability);
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        final V1ScheduleResolver compatibilityResolver = scheduleResolver();
        return new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(final ShardId shard,
                                                     final io.nereusstream.delay.protocol.DelayMessageId message,
                                                     final ScheduleIntentV1 intent,
                                                     final io.nereusstream.delay.protocol.SourcePosition source) {
                // The pre-route Schedule is a recovery/barrier fixture. Keep
                // it on the legacy compatibility lane so a physical
                // Large-Payload Prepare lane cannot claim that older work.
                return compatibilityResolver.resolveSchedule(shard, message, intent, source);
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

    private static DestinationLaneId laneId(final String clusterId, final UUID destinationTopicId,
                                            final String destinationPhysicalTopic) {
        return DestinationLaneId.derive(canonicalLaneTuple(clusterId, destinationTopicId,
                destinationPhysicalTopic, destinationProfile(), capabilityProfile()));
    }

    private static ProfileRefV1 capabilityProfile() {
        return new ProfileRefV1(Bytes.utf8("kafka-worker-capability"), 1,
                Bytes.sha256(Bytes.utf8("kafka-worker-capability-semantic")),
                ProfileKindV1.DELIVERY_CAPABILITY);
    }

    private static byte[] canonicalLaneTuple(final String clusterId, final UUID topicId,
                                              final String physicalTopic, final ProfileRefV1 destination,
                                              final ProfileRefV1 capability) {
        if (physicalTopic == null || physicalTopic.isBlank()) {
            throw new IllegalArgumentException("Kafka physical topic must be nonblank");
        }
        final byte[] topicUuid = uuidBytes(topicId);
        return Bytes.concat(
                Bytes.sha256(Bytes.utf8("kafka-worker-tenant-routing-scope")),
                Bytes.u8(AdapterKindV1.KAFKA.wireValue()),
                Bytes.lp32(Bytes.utf8(clusterId)),
                Bytes.u8(1),
                topicUuid,
                Bytes.lp32(topicUuid),
                Bytes.u32be(0),
                Bytes.lp32(destination.profileId()),
                Bytes.u64beBits(destination.version()),
                destination.semanticHash(),
                Bytes.lp32(capability.profileId()),
                Bytes.u64beBits(capability.version()),
                capability.semanticHash(),
                Bytes.u8(1),
                Bytes.sha256(Bytes.utf8("kafka-worker-ordering-domain")));
    }

    private static RouteSnapshotV1 routeSnapshot(final String clusterId, final String topic, final UUID topicId,
                                                 final RouteIncarnation incarnation,
                                                 final org.apache.kafka.clients.consumer.GuardedFetchEvidence evidence,
                                                 final AuthenticatedTenantContext tenant, final KeyPair signingKeys) {
        final long now = System.currentTimeMillis();
        final BrokerResourceIdentityV1 broker = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1(clusterId, topicId));
        final RoutePartitionPolicyV1 policy = new RoutePartitionPolicyV1(0,
                ActivationBarrierV1.kafka(broker, 0, 2, evidence.lastStableOffset()), zeroQuota(), 1,
                Bytes.sha256(Bytes.utf8("large-payload-fetch-proof-v1\0"), evidence.fetchResponseBodySha256()));
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(now - 100, now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("large-payload-route-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("large-payload-route-issued-at")), 0, null);
        return RouteSnapshotV1.create(incarnation, tenant.authenticatedTenantScopeHash(), tenant.tenantRoutingScope(),
                RouteLifecycleV1.ACTIVE_FOR_NEW, now + 30_000,
                new io.nereusstream.delay.protocol.KafkaIngressRouteResourceV1(clusterId, topic, topicId, 1),
                RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1), 1, List.of(policy),
                100, 200, 1 << 20, 2 << 20, 10, 8 << 20, 180_000, now - 1_000, now + 300_000,
                new IngressCredentialBindingRefV1(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                Bytes.sha256(Bytes.utf8("large-payload-route-prerequisite")), issuedAt, 1,
                signingKeys.getPrivate());
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest placementRequest(final long now) {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(0,
                Bytes.sha256(Bytes.utf8("large-payload-worker-assignment")), 1,
                Bytes.sha256(Bytes.utf8("large-payload-worker-capacity")), 1,
                List.of(new WorkerPlacementPolicy.WorkerCandidate("kafka-large-payload-worker", capacity(2),
                        CapacityVectorV1.empty(), 0, 16, 0, 16, WorkerLoadVector.empty(), WorkerLoadVector.empty(),
                        now, true, 0)), capacity(1), CapacityVectorV1.empty(), CapacityVectorV1.empty(), null,
                now, 0, 0);
    }

    private static void requireRouteAssignment(final WorkerAssignment assignment, final RouteSnapshotV1 snapshot,
                                               final String clusterId, final UUID topicId, final long barrierOffset) {
        if (!assignment.routeBound() || !Arrays.equals(snapshot.snapshotDigest(), assignment.routeSnapshotDigest())
                || !(assignment.sourceAssignment().activationBarrier() instanceof KafkaActivationBarrier barrier)
                || !clusterId.equals(barrier.authenticatedClusterId()) || !topicId.equals(barrier.nativeTopicUuid())
                || barrier.exclusiveOffset() != barrierOffset) {
            throw new IllegalStateException("Oxia assignment did not retain the signed Kafka activation barrier");
        }
    }

    private static CompatibleControlSnapshotV1 controlSnapshot(final ShardId shard,
                                                                final ProfileRefV1 destinationProfile) {
        return new CompatibleControlSnapshotV1(new ShardSubjectV1(shard),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(destinationProfile), zeroQuota());
    }

    private static WorkClassExecutionRegistry workClasses() {
        final java.util.EnumMap<WorkClass, WorkClassPolicy> policies = new java.util.EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, 8, LARGE_PAYLOAD_WORK_CLASS_BYTES, 1,
                    LARGE_PAYLOAD_WORK_CLASS_BYTES, 1_000_000,
                    protectedClass ? 1 : 0, protectedClass ? 1 : 0, workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies,
                TimeUnit.SECONDS.toNanos(5), TimeUnit.SECONDS.toNanos(30), 16, 8_000_000), System::nanoTime);
    }

    private static CapacityVectorV1 capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimensionV1.COUNT];
        values[CapacityDimensionV1.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVectorV1(values);
    }

    private static QuotaGrantRefV1 zeroQuota() {
        return new QuotaGrantRefV1(bytes(32, 50), 1, new PublishAdmissionBody.ChargeVector(
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    }

    private static void appendFrame(final String bootstrap, final String clusterId, final String topic,
                                     final Uuid topicId, final byte[] frame, final long expectedOffset)
            throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = kafkaProducer(bootstrap)) {
            final GuardedProducer<byte[], byte[]> guarded = (GuardedProducer<byte[], byte[]>) producer;
            final var metadata = guarded.sendGuarded(new ProducerRecord<>(topic, 0, null, frame),
                    new org.apache.kafka.clients.producer.ProducerResourceGuard(clusterId, topic, topicId, 0))
                    .get(10, TimeUnit.SECONDS);
            if (metadata.recordMetadata().offset() != expectedOffset) {
                throw new IllegalStateException("Kafka large-payload append offset mismatch: expected="
                        + expectedOffset + ", actual=" + metadata.recordMetadata().offset());
            }
        }
    }

    private static Map<String, Object> consumerConfiguration(final String bootstrap, final String groupId) {
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

    private static GuardedConsumer<byte[], byte[]> workerConsumer(final String bootstrap, final String groupId,
                                                                   final String clusterId, final String topic,
                                                                   final UUID topicId, final ShardId shard) {
        return KafkaClientArtifactSourceConsumerFactory.create(consumerConfiguration(bootstrap, groupId), clusterId,
                topic, topicId, shard.partition());
    }

    private static long latestOffset(final Admin admin, final String topic, final int partition) throws Exception {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        return admin.listOffsets(Map.of(topicPartition, OffsetSpec.latest())).all().get(10, TimeUnit.SECONDS)
                .get(topicPartition).offset();
    }

    private static void ensureTopic(final Admin admin, final String topic) throws Exception {
        try {
            if (describe(admin, topic) != null) {
                return;
            }
        } catch (Exception missing) {
            // Create below.
        }
        final NewTopic newTopic = new NewTopic(topic, 1, (short) 3);
        newTopic.configs(Map.of("message.timestamp.type", "LogAppendTime"));
        admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                if (describe(admin, topic) != null) {
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
        return admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
    }

    private static KeyPair gatewayJwtKeys() throws GeneralSecurityException {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2_048);
        return generator.generateKeyPair();
    }

    private static String token(final KeyPair keyPair, final AuthenticatedTenantContext tenant,
                                final byte[] certificateFingerprint) throws GeneralSecurityException {
        final long now = Instant.now().getEpochSecond();
        final String header = "{\"alg\":\"RS256\",\"typ\":\"JWT\",\"kid\":\"gateway-e2e-key\"}";
        final String claims = "{" + "\"iss\":\"nereus-delay-gateway-e2e-issuer\"," +
                "\"aud\":\"nereus-delay-gateway-e2e\"," + "\"sub\":\"gateway-e2e-client\"," +
                "\"tenant\":\"tenant-e2e\",\"tenant_scope_hash\":\"" + encode(
                        tenant.authenticatedTenantScopeHash()) + "\",\"tenant_routing_scope\":\"" + encode(
                        tenant.tenantRoutingScope()) + "\",\"iat\":" + (now - 100) + ",\"nbf\":"
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

    private static DelayGatewayV1Grpc.DelayGatewayV1BlockingStub stub(final ManagedChannel channel,
                                                                        final String token) {
        final Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
        return DelayGatewayV1Grpc.newBlockingStub(ClientInterceptors.intercept(channel,
                MetadataUtils.newAttachHeadersInterceptor(headers)));
    }

    private static ManagedChannel channel(final int port, final Path ca, final Path clientCertificate,
                                          final Path clientPrivateKey) throws SSLException {
        final io.grpc.netty.shaded.io.netty.handler.ssl.SslContext sslContext = GrpcSslContexts.forClient()
                .trustManager(ca.toFile()).keyManager(clientCertificate.toFile(), clientPrivateKey.toFile()).build();
        return NettyChannelBuilder.forAddress("127.0.0.1", port).sslContext(sslContext).build();
    }

    private static byte[] certificateFingerprint(final Path certificate) throws Exception {
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
        final long timeoutSeconds = Long.parseLong(configured(
                "NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER_TIMEOUT_SECONDS", "180"));
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

    private static String configuredNullable(final String name) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? null : value;
    }

    private static UUID toUuid(final Uuid value) {
        return new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
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
}
