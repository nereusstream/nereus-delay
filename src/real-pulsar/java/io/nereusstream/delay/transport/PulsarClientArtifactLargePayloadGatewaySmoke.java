package io.nereusstream.delay.transport;

import com.google.protobuf.ByteString;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.nereusstream.delay.adapter.S3CompatiblePayloadObjectStore;
import io.nereusstream.delay.adapter.PulsarSendRequest;
import io.nereusstream.delay.adapter.PulsarSendResult;
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
import io.nereusstream.delay.ownership.PulsarSourceReactivationCoordinator;
import io.nereusstream.delay.ownership.PulsarSourceReactivationV1;
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
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.ControlRef;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.EnqueueOutcomeKindV1;
import io.nereusstream.delay.protocol.IngressCredentialBindingRefV1;
import io.nereusstream.delay.protocol.IngressRouteResourceV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
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
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PulsarIngressRouteResourceV1;
import io.nereusstream.delay.protocol.PulsarMetadataV1;
import io.nereusstream.delay.protocol.PulsarPhysicalPartitionIdentityV1;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
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
import io.nereusstream.delay.protocol.SubmissionOutcomeKindV1;
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
import io.nereusstream.delay.runtime.MessageStatus;
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
import io.nereusstream.delay.submission.PulsarManagedSubmissionOutcomeProjector;
import io.nereusstream.delay.submission.RouteBoundSubmissionTransportPlanResolver;
import io.nereusstream.delay.submission.SubmissionOutcomeProjectorRegistry;
import io.nereusstream.delay.store.CheckpointFileInventory;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import io.nereusstream.delay.store.WorkerLoadVector;
import io.nereusstream.delay.store.WorkerPlacementPolicy;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.PulsarClient;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Real Pulsar Gateway/Oxia/Worker/MinIO large-payload authority smoke. */
public final class PulsarClientArtifactLargePayloadGatewaySmoke {
    private static final String CLUSTER = "standalone";
    private static final byte[] SOURCE_INCARNATION = digest(43);
    private static final long SOURCE_CREATION_TIMESTAMP = 2001L;
    private static final byte[] DESTINATION_INCARNATION = digest(17);
    private static final long DESTINATION_CREATION_TIMESTAMP = 1001L;
    private static final long LEASE_DURATION_MS = 60_000;
    private static final long PAYLOAD_BYTES = (1L << 20) + 4_096;
    private static final long WORK_CLASS_BYTES = 2_000_000;
    private static final Duration RECEIVE_TIMEOUT = Duration.ofMillis(250);

    private PulsarClientArtifactLargePayloadGatewaySmoke() {
    }

    private static RouteSnapshotV1 routeSnapshot(final String physicalTopicBase, final String physicalTopic,
                                                  final RouteIncarnation incarnation,
                                                  final PulsarSourcePosition position,
                                                  final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof,
                                                  final KeyPair signingKeys) {
        final long now = System.currentTimeMillis();
        final BrokerResourceIdentityV1 broker = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1(CLUSTER, SOURCE_INCARNATION, physicalTopic,
                        SOURCE_CREATION_TIMESTAMP));
        final ActivationBarrierV1 barrier = ActivationBarrierV1.pulsar(broker, 0, position.ledgerId(),
                position.entryId(), position.normalizedBatchIndex(), position.batchSize(),
                proof.connectionGeneration(), proof.attestationDigest());
        final RoutePartitionPolicyV1 policy = new RoutePartitionPolicyV1(0, barrier, zeroQuota(),
                proof.connectionGeneration(), proof.attestationDigest());
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(now - 100, now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("pulsar-large-route-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("pulsar-large-route-issued-at")), 0, null);
        final IngressRouteResourceV1 ingress = new PulsarIngressRouteResourceV1(CLUSTER, physicalTopicBase,
                List.of(new PulsarPhysicalPartitionIdentityV1(0, physicalTopic, SOURCE_INCARNATION,
                        SOURCE_CREATION_TIMESTAMP)));
        return RouteSnapshotV1.create(incarnation, bytes(32, 1), bytes(32, 2), RouteLifecycleV1.ACTIVE_FOR_NEW,
                now + 30_000, ingress, RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1), 1, List.of(policy),
                100, 200, 1 << 20, 2 << 20, 10, 8 << 20, 180_000, now - 1_000, now + 300_000,
                new IngressCredentialBindingRefV1(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                Bytes.sha256(Bytes.utf8("pulsar-large-route-prerequisite")), issuedAt, 1,
                signingKeys.getPrivate());
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest placementRequest(final long now) {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(0,
                Bytes.sha256(Bytes.utf8("pulsar-large-worker-assignment")), 1,
                Bytes.sha256(Bytes.utf8("pulsar-large-worker-capacity")), 1,
                List.of(new WorkerPlacementPolicy.WorkerCandidate("pulsar-large-payload-worker", capacity(2),
                        io.nereusstream.delay.protocol.CapacityVectorV1.empty(), 0, 16, 0, 16,
                        WorkerLoadVector.empty(), WorkerLoadVector.empty(), now, true, 0)),
                capacity(1), io.nereusstream.delay.protocol.CapacityVectorV1.empty(),
                io.nereusstream.delay.protocol.CapacityVectorV1.empty(), null, now, 0, 0);
    }

    private static void requireRouteAssignment(final WorkerAssignment assignment, final RouteSnapshotV1 snapshot,
                                               final PulsarSourcePosition barrierPosition,
                                               final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof) {
        if (!assignment.routeBound() || !Arrays.equals(snapshot.snapshotDigest(), assignment.routeSnapshotDigest())
                || !(assignment.sourceAssignment().activationBarrier() instanceof PulsarActivationBarrier barrier)
                || !Arrays.equals(barrier.brokerResourceIncarnation(), SOURCE_INCARNATION)
                || !barrier.physicalTopic().equals(barrierPosition.physicalTopic())
                || barrier.ledgerId() != barrierPosition.ledgerId() || barrier.entryId() != barrierPosition.entryId()
                || barrier.normalizedLastBatchIndex() != barrierPosition.normalizedBatchIndex()
                || barrier.batchSize() != barrierPosition.batchSize()
                || barrier.guardedSourceConnectionGeneration() != proof.connectionGeneration()
                || !Arrays.equals(barrier.resourceGuardAttestationDigest(), proof.attestationDigest())) {
            throw new IllegalStateException("Oxia Pulsar assignment did not retain the signed source barrier");
        }
    }

    private static CompatibleControlSnapshotV1 controlSnapshot(final ShardId shard,
                                                                final ProfileRefV1 destinationProfile) {
        return new CompatibleControlSnapshotV1(new ShardSubjectV1(shard),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(destinationProfile), zeroQuota());
    }

    private static QuotaGrantRefV1 zeroQuota() {
        return new QuotaGrantRefV1(bytes(32, 50), 1, new PublishAdmissionBody.ChargeVector(
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    }

    private static CapacityVectorV1 capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimensionV1.COUNT];
        values[CapacityDimensionV1.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVectorV1(values);
    }

    private static WorkClassExecutionRegistry workClasses() {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, 8, WORK_CLASS_BYTES, 1, WORK_CLASS_BYTES,
                    WORK_CLASS_BYTES, protectedClass ? 1 : 0, protectedClass ? 1 : 0,
                    workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies,
                TimeUnit.SECONDS.toNanos(5), TimeUnit.SECONDS.toNanos(30), 16, 8_000_000),
                System::nanoTime);
    }

    private static PulsarSourcePosition sendFrameAndPosition(final PulsarClient client,
                                                               final TopicResourceGuard guard,
                                                               final String physicalTopic,
                                                               final ShardId shard,
                                                               final byte[] frame,
                                                               final String producerName) throws Exception {
        final PulsarClientArtifactSendTransport transport = new PulsarClientArtifactSendTransport(
                PulsarClientArtifactProducerFactory.create(client, CLUSTER, SOURCE_INCARNATION, physicalTopic,
                        SOURCE_CREATION_TIMESTAMP, producerName), CLUSTER, SOURCE_INCARNATION, physicalTopic,
                SOURCE_CREATION_TIMESTAMP, 0);
        try {
            final PulsarSendResult result = transport.send(new PulsarSendRequest(CLUSTER, SOURCE_INCARNATION,
                    physicalTopic, SOURCE_CREATION_TIMESTAMP, 0, CommandId.random(shard), frame))
                    .toCompletableFuture().get(15, TimeUnit.SECONDS);
            if (result.disposition() != PulsarSendResult.Disposition.PERSISTED) {
                throw new IllegalStateException("Pulsar large-payload source frame was not persisted: "
                        + result.disposition() + "/" + result.stableCode());
            }
            return new PulsarSourcePosition(shard, SOURCE_INCARNATION, physicalTopic, result.ledgerId(),
                    result.entryId(), result.batchIndex(), result.batchSize(), result.batched()
                    ? PulsarSourcePosition.EntryKind.BATCH : PulsarSourcePosition.EntryKind.NON_BATCH,
                    result.brokerEntryTimestampEpochMs());
        } finally {
            transport.close();
        }
    }

    private static int countSourceRecords(final PulsarClient client, final TopicResourceGuard guard,
                                          final String physicalTopic) throws Exception {
        final GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(client, guard,
                physicalTopic, "nereus-delay-pulsar-large-count-" + UUID.randomUUID());
        int count = 0;
        try {
            while (true) {
                final var message = consumer.receive(750, TimeUnit.MILLISECONDS);
                if (message == null) {
                    return count;
                }
                count++;
                consumer.acknowledge(message);
            }
        } finally {
            closeNative(consumer);
        }
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
        final String claims = "{" + "\"iss\":\"nereus-delay-gateway-e2e-issuer\","
                + "\"aud\":\"nereus-delay-gateway-e2e\",\"sub\":\"gateway-e2e-client\","
                + "\"tenant\":\"tenant-e2e\",\"tenant_scope_hash\":\""
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

    private static void createPartitionedTopic(final HttpClient client, final String adminUrl, final String topicBase,
                                               final String physicalTopic, final byte[] incarnation,
                                               final long creationTimestamp) throws Exception {
        final String partitionsPath = adminUrl + "/admin/v2/persistent/public/default/" + topicBase + "/partitions";
        for (int attempt = 0; attempt < 120; attempt++) {
            final HttpResponse<String> response = request(client, partitionsPath, "PUT", "1");
            if (response.statusCode() >= 200 && response.statusCode() < 300 || response.statusCode() == 409) {
                break;
            }
            if (response.statusCode() != 404 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw failure("create Pulsar large-payload partitioned topic", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        stampGuard(client, adminUrl, physicalTopic, incarnation, creationTimestamp);
    }

    private static void createTopic(final HttpClient client, final String adminUrl, final String topic,
                                    final byte[] incarnation, final long creationTimestamp) throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic;
        final String body = guardBody(incarnation, creationTimestamp);
        for (int attempt = 0; attempt < 40; attempt++) {
            final HttpResponse<String> response = request(client, path, "PUT", body);
            if (response.statusCode() >= 200 && response.statusCode() < 300 || response.statusCode() == 409) {
                return;
            }
            if (response.statusCode() != 412 && response.statusCode() != 503) {
                throw failure("create Pulsar large-payload destination topic", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("Pulsar large-payload destination topic did not converge: " + topic);
    }

    private static void stampGuard(final HttpClient client, final String adminUrl, final String physicalTopic,
                                   final byte[] incarnation, final long creationTimestamp) throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + physicalTopic + "/resourceGuard";
        int lastStatus = -1;
        String lastBody = "";
        for (int attempt = 0; attempt < 120; attempt++) {
            final HttpResponse<String> response = request(client, path, "PUT", guardBody(incarnation, creationTimestamp));
            lastStatus = response.statusCode();
            lastBody = response.body();
            if (response.statusCode() >= 200 && response.statusCode() < 300 || response.statusCode() == 409) {
                return;
            }
            if (response.statusCode() != 404 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw failure("stamp Pulsar large-payload resource guard", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("Pulsar large-payload resource guard did not converge: " + physicalTopic
                + " lastStatus=" + lastStatus + " lastBody=" + lastBody);
    }

    private static String guardBody(final byte[] incarnation, final long creationTimestamp) {
        return "{\"nereus.resource.guard.version\":\"1\",\"nereus.resource.incarnation\":\""
                + encode(incarnation) + "\",\"nereus.resource.created-at\":\""
                + Long.toUnsignedString(creationTimestamp) + "\"}";
    }

    private static void deletePartitionedTopic(final HttpClient client, final String adminUrl, final String topic) {
        delete(client, adminUrl + "/admin/v2/persistent/public/default/" + topic + "/partitions?force=true");
    }

    private static void deleteTopic(final HttpClient client, final String adminUrl, final String topic) {
        delete(client, adminUrl + "/admin/v2/persistent/public/default/" + topic + "?force=true");
    }

    private static void delete(final HttpClient client, final String path) {
        try {
            final HttpResponse<String> response = request(client, path, "DELETE", "");
            if (response.statusCode() >= 300 && response.statusCode() != 404) {
                System.err.println("Pulsar large-payload cleanup returned HTTP " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("Pulsar large-payload cleanup failed: " + failure.getMessage());
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
        } catch (org.apache.pulsar.client.api.PulsarClientException failure) {
            throw new IllegalStateException("Pulsar large-payload guarded consumer close failed", failure);
        }
    }

    /**
     * Creates the successor source only after a fresh guarded proof is available.
     *
     * <p>The P1 generation is allocated by the Broker process that admits the
     * guarded SUBSCRIBE.  During a multi-Broker failover two independent Broker
     * JVM counters can therefore accidentally present the same raw value.  A
     * candidate with that value is not a successor proof: discard the exact
     * candidate and establish another guarded SUBSCRIBE.  The reactivation
     * contract still remains strict and never accepts an equal generation.</p>
     */
    private static SuccessorSource createSuccessorSource(
            final PulsarClient client,
            final TopicResourceGuard sourceGuard,
            final String sourcePhysicalTopic,
            final ShardId shard,
            final PulsarSourcePosition commitPosition,
            final long previousGeneration) throws Exception {
        RuntimeException lastCollision = null;
        for (int attempt = 1; attempt <= 8; attempt++) {
            final GuardedConsumer<byte[]> candidate = PulsarClientArtifactSourceConsumerFactory.create(
                    client, sourceGuard, sourcePhysicalTopic,
                    "nereus-delay-pulsar-large-reactivation-" + UUID.randomUUID());
            try {
                final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof =
                        PulsarClientArtifactRecoverySourcePositioner.seekAfter(candidate, sourceGuard,
                                sourcePhysicalTopic, shard, Optional.of(commitPosition), Duration.ofSeconds(15));
                if (proof.connectionGeneration() != previousGeneration) {
                    return new SuccessorSource(candidate, proof);
                }
                closeNative(candidate);
                lastCollision = new IllegalStateException("equal successor connection generation="
                        + proof.connectionGeneration() + ", attempt=" + attempt);
                System.out.println("Pulsar source successor discarded equal Broker-local connection generation: "
                        + proof.connectionGeneration() + "; retry=" + (attempt < 8));
            } catch (RuntimeException | Error failure) {
                try {
                    closeNative(candidate);
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }
        throw new IllegalStateException("Pulsar source successor did not obtain a distinct connection generation",
                lastCollision);
    }

    private record SuccessorSource(
            GuardedConsumer<byte[]> consumer,
            PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof) {
    }

    private static String requiredEnv(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required for the real Pulsar large-payload authority smoke");
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

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static boolean failoverRequested() {
        final String marker = System.getenv("NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER_MARKER");
        return marker != null && !marker.isBlank();
    }

    private static void signalFailoverCut() throws Exception {
        final String marker = System.getenv("NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER_MARKER");
        if (marker == null || marker.isBlank()) {
            return;
        }
        final Path markerPath = Path.of(marker);
        final Path parent = markerPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(markerPath, Bytes.utf8("gateway-commit-complete\n"));
        System.out.println("Pulsar Gateway large-payload failover cut marker written after Commit/readback");
        final Path releasePath = Path.of(marker + ".release");
        final long deadline = System.currentTimeMillis() + 180_000;
        while (!Files.exists(releasePath)) {
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException("Pulsar Gateway large-payload failover cut was not released");
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        System.out.println("Pulsar Gateway large-payload failover cut release acknowledged after broker-1 stop");
    }

    private static String encode(final byte[] value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
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
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        }
    }

    private static void recover(final GuardedConsumer<byte[]> consumer,
                                final TopicResourceGuard guard,
                                final WorkerAssignment accepted,
                                final OxiaOwnerLeaseStore ownerAuthority,
                                final OwnedDelayShard ownedShard,
                                final SystemMutation activation,
                                final PreparedCommand beforeRoute,
                                final KeyPair signingKeys,
                                final CompatibleControlSnapshotV1 controlSnapshot,
                                final WorkClassExecutionRegistry workClasses,
        final String physicalTopic) {
        final List<SourceReplayEntry> entries = new ArrayList<>();
        // The active Worker deliberately reuses this guarded SUBSCRIBE so its
        // connection generation remains the one pinned in the Route barrier.
        // Do not close the recovery cursor here: its close() owns the native
        // consumer, and the Worker runtime owns that consumer after handoff.
        final PulsarClientArtifactRecoverySourceCursor cursor = new PulsarClientArtifactRecoverySourceCursor(
                consumer, guard, accepted.sourceAssignment(), physicalTopic, RECEIVE_TIMEOUT);
        for (int index = 0; index < 2; index++) {
            if (!cursor.hasNext()) {
                throw new IllegalStateException("Pulsar large-payload recovery ended before the route barrier");
            }
            entries.add(cursor.next());
        }
        if (!(entries.get(0) instanceof SourceReplayMutation mutation)
                || mutation.mutation().type() != SystemMutationType.APPLY_SHARD_CONTROL
                || !(entries.get(1) instanceof SourceReplayRecord record)
                || !record.command().equals(beforeRoute)
                || !activation.shardId().equals(entries.get(0).position().shardId())
                || !(entries.get(0).position() instanceof PulsarSourcePosition first)
                || !(entries.get(1).position() instanceof PulsarSourcePosition second)
                || first.compareWithinShard(second) >= 0) {
            throw new IllegalStateException("Pulsar recovery did not return the exact activation and pre-route records");
        }
        final OwnerRecoveryCoordinator recovery = new OwnerRecoveryCoordinator(ownedShard, ownerAuthority,
                accepted.sourceAssignment(), strictPulsarFixtureSuccessor(),
                SourceReplayCursor.of(entries.iterator()), signingKeys.getPublic(), controlSnapshot,
                System::currentTimeMillis,
                new ReplayTurnBudget(2, WORK_CLASS_BYTES, TimeUnit.SECONDS.toNanos(10)), workClasses);
        OwnerRecoveryTurn turn;
        do {
            turn = recovery.runTurn();
        } while (!turn.complete());
        if (!recovery.complete() || turn.outcomes().size() != 2) {
            throw new IllegalStateException("Pulsar large-payload Worker recovery did not apply exactly two records");
        }
    }

    /**
     * The real fixture uses two non-batched records in one BookKeeper ledger.
     * Pulsar's native MessageId proves adjacency for that bounded case; a
     * ledger transition is deliberately rejected until the adapter supplies a
     * stronger broker continuity proof.
     */
    private static SourceReplaySuccessor strictPulsarFixtureSuccessor() {
        return (previous, current) -> {
            if (!(previous instanceof PulsarSourcePosition previousPulsar)
                    || !(current instanceof PulsarSourcePosition currentPulsar)
                    || previousPulsar.ledgerId() != currentPulsar.ledgerId()) {
                return false;
            }
            if (previousPulsar.entryId() == currentPulsar.entryId()) {
                return previousPulsar.batchSize() == currentPulsar.batchSize()
                        && previousPulsar.normalizedBatchIndex() != previousPulsar.batchSize() - 1
                        && currentPulsar.normalizedBatchIndex()
                        == previousPulsar.normalizedBatchIndex() + 1;
            }
            return previousPulsar.entryKind() == PulsarSourcePosition.EntryKind.NON_BATCH
                    && currentPulsar.entryKind() == PulsarSourcePosition.EntryKind.NON_BATCH
                    && previousPulsar.entryId() != Long.MAX_VALUE
                    && currentPulsar.entryId() == previousPulsar.entryId() + 1;
        };
    }

    private static io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult runUntilApplied(
            final WorkerShardRuntime runtime) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result;
        do {
            result = runtime.runSourceTurn(new SchedulerBudget(1, WORK_CLASS_BYTES,
                    TimeUnit.SECONDS.toNanos(2)), System::currentTimeMillis);
            if (result.status() == io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                return result;
            }
            if (result.status() != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status() != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException("Pulsar large-payload Worker source turn failed: " + result.status(),
                        result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Pulsar large-payload Worker source record did not become visible");
    }

    private static void requireApplied(
            final io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result,
            final String operation) {
        if (result.status() != io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
            throw new IllegalStateException(operation + " was not applied and ACKed: " + result.status(),
                    result.failure());
        }
    }

    private static CommandQueuedReceiptV1 requireQueued(final GatewaySubmissionOutcomeV1 response,
                                                         final String operation,
                                                         final PulsarSourcePosition previousPosition) {
        if (!response.hasSubmissionOutcomeNdr1()) {
            final StableErrorV1 error = StableErrorV1.decode(response.getPreparationErrorV1().toByteArray());
            throw new IllegalStateException(operation + " returned preparation error: stage=" + error.stage()
                    + ", code=" + error.code() + ", retryability=" + error.retryability()
                    + ", diagnosticCode=" + error.diagnosticCode());
        }
        final SubmissionOutcomeMessageV1 outcome = SubmissionOutcomeMessageV1.decode(
                response.getSubmissionOutcomeNdr1().toByteArray());
        if (outcome.kind() != SubmissionOutcomeKindV1.MANAGED
                || outcome.managed().kind() != EnqueueOutcomeKindV1.QUEUED) {
            throw new IllegalStateException(operation + " did not produce a managed QUEUED outcome: " + outcome);
        }
        final CommandQueuedReceiptV1 receipt = outcome.managed().queued();
        if (!(receipt.sourcePosition() instanceof PulsarSourcePosition position)
                || position.compareWithinShard(previousPosition) <= 0) {
            throw new IllegalStateException(operation + " Pulsar source position did not advance: "
                    + receipt.sourcePosition());
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
                .setRoute(GatewayRouteSelectorV1.newBuilder().setIngressAdapterKind(AdapterKindV1.PULSAR.wireValue())
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
                DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, Bytes.utf8("pulsar-large-payload-key"),
                AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of())), null, null);
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destinationProfile(), retryPolicy(), deliverAt,
                deliverAt + 10_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0],
                Bytes.utf8(identity), null,
                AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of())), null, null);
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
        final io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1 semantic =
                new io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1(
                        io.nereusstream.delay.protocol.ObjectStoreProviderKindV1.S3_COMPATIBLE,
                        S3CompatiblePayloadObjectStore.endpointConfigDigest(endpoint, region, bucket),
                        S3CompatiblePayloadObjectStore.credentialAuthorizationScopeDigest(accessKey, region, bucket),
                        1, true, true, true, true, bytes(32, 20), 8L << 20,
                        io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1.SINGLE_PUT, 1, bytes(32, 21));
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
        final ControlRef controlRef = new ControlRef(Bytes.sha256(Bytes.utf8("pulsar-large-trust-op")),
                Bytes.sha256(Bytes.utf8("pulsar-large-trust-request")), 1);
        final byte[] body = trustSetControlBody(shard, controlRef, trustSet, retryUntil);
        return SystemMutation.signed(shard, SystemMutationType.APPLY_SHARD_CONTROL, retryUntil,
                controlRef.logicalOperationIdentity(12), body,
                AuthorIdentity.control(Bytes.sha256(Bytes.utf8("pulsar-large-control-actor")),
                        Bytes.sha256(Bytes.utf8("pulsar-large-control-role")),
                        tenant.authenticatedTenantScopeHash()).canonicalBytes(), 1, signingKeys.getPrivate());
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

    private static V1ScheduleResolver scheduleResolver(final String destinationPhysicalTopic) {
        final ProfileRefV1 destination = destinationProfile();
        final ProfileRefV1 capability = PulsarClientArtifactWorkerSmoke.capabilityProfile();
        final byte[] tuple = PulsarClientArtifactWorkerSmoke.canonicalLaneTuple(destinationPhysicalTopic,
                destination, capability);
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        final byte[] compatibilityTuple = Bytes.utf8("pulsar-large-payload-compatibility-lane-v1");
        final DestinationLaneId compatibilityLane = DestinationLaneId.derive(compatibilityTuple);
        return new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(final ShardId shard,
                                                     final io.nereusstream.delay.protocol.DelayMessageId message,
                                                     final ScheduleIntentV1 intent,
                                                     final io.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedSchedule(compatibilityLane, compatibilityTuple, intent.inlinePayload(), null);
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

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <topic>");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String sourceBase = arguments[2] + "-" + UUID.randomUUID();
        final String sourcePhysicalName = sourceBase + "-partition-0";
        final String sourcePhysicalBase = "persistent://public/default/" + sourceBase;
        final String sourcePhysicalTopic = "persistent://public/default/" + sourcePhysicalName;
        final String destinationName = configured("NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC",
                sourceBase + "-destination");
        final String destinationPhysicalTopic = "persistent://public/default/" + destinationName;
        final String oxiaEndpoint = requiredEnv("NEREUS_DELAY_OXIA_ENDPOINT");
        final URI minioUri = URI.create(requiredEnv("NEREUS_DELAY_MINIO_ENDPOINT"));
        final String minioAccessKey = requiredEnv("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String minioSecretKey = requiredEnv("NEREUS_DELAY_MINIO_SECRET_KEY");
        final String minioBucket = requiredEnv("NEREUS_DELAY_MINIO_BUCKET");
        final String minioRegion = configured("NEREUS_DELAY_MINIO_REGION", "us-east-1");
        final Path serverCertificate = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_CERT");
        final Path serverPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_KEY");
        final Path trustedClientCertificates = requiredPath("NEREUS_DELAY_GATEWAY_CA_CERT");
        final Path clientCertificate = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_CERT");
        final Path clientPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_KEY");
        final int gatewayPort = Integer.parseInt(requiredEnv("NEREUS_DELAY_GATEWAY_PORT"));
        if (gatewayPort <= 0 || gatewayPort > 65_535) {
            throw new IllegalArgumentException("NEREUS_DELAY_GATEWAY_PORT must be 1..65535");
        }

        final HttpClient admin = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
        createPartitionedTopic(admin, adminUrl, sourceBase, sourcePhysicalName, SOURCE_INCARNATION,
                SOURCE_CREATION_TIMESTAMP);
        createTopic(admin, adminUrl, destinationName, DESTINATION_INCARNATION, DESTINATION_CREATION_TIMESTAMP);

        final AuthenticatedTenantContext tenant = new AuthenticatedTenantContext(
                bytes(32, 1), bytes(32, 2), bytes(32, 3));
        final byte[] payload = payload();
        final byte[] payloadHash = Bytes.sha256(payload);
        final KeyPair proofKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final long proofNow = System.currentTimeMillis();
        final PayloadProofVerifierKeyV1 verifierKey = PayloadProofVerifierKeyV1.fromPublicKey(
                7, proofKeys.getPublic(), Math.max(0, proofNow - 60_000), proofNow + 3_600_000);
        final PayloadProofTrustSetSemanticV1 trustSet = new PayloadProofTrustSetSemanticV1(1,
                List.of(verifierKey));
        final ProfileSemanticEnvelopeV1 objectStoreProfile = objectStoreProfile(
                minioUri, minioRegion, minioBucket, minioAccessKey);
        final RouteIncarnation routeIncarnation = RouteIncarnation.random();
        final ShardId shard = new ShardId(routeIncarnation, 0);
        final RouteSelectionHint routeHint = new RouteSelectionHint(AdapterKindV1.PULSAR, Bytes.utf8("primary"));
        final KeyPair controlKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final SystemMutation activation = trustActivation(shard, trustSet.ref(), tenant, controlKeys);
        final PreparedCommand beforeRoute = command(shard, "large-payload-before-route");
        final TopicResourceGuard sourceGuard = new TopicResourceGuard(CLUSTER, SOURCE_INCARNATION,
                SOURCE_CREATION_TIMESTAMP);
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String routePrefix = "nereus-delay/pulsar-large-payload-route/" + UUID.randomUUID();
        final String assignmentPrefix = "nereus-delay/pulsar-large-payload-assignment/" + UUID.randomUUID();
        final String gatewayPrefix = "nereus-delay/pulsar-large-payload-gateway/" + UUID.randomUUID();

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
            final PulsarSourcePosition activationPosition = sendFrameAndPosition(client, sourceGuard,
                    sourcePhysicalTopic, shard, activation.encodeFrame(), "pulsar-large-payload-activation");
            final PulsarSourcePosition beforeRoutePosition = PulsarClientArtifactWorkerSmoke.sendAndPosition(
                    client, sourceGuard, sourcePhysicalTopic, beforeRoute,
                    "pulsar-large-payload-before-route");
            if (activationPosition.compareWithinShard(beforeRoutePosition) >= 0) {
                throw new IllegalStateException("Pulsar large-payload source fixture order is not increasing");
            }
            final GuardedConsumer<byte[]> nativeConsumer = PulsarClientArtifactSourceConsumerFactory.create(
                    client, sourceGuard, sourcePhysicalTopic, "nereus-delay-pulsar-large-worker-" + UUID.randomUUID());
            GuardedConsumer<byte[]> activeConsumer = nativeConsumer;
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof =
                    PulsarClientArtifactRecoverySourcePositioner.seekAfter(nativeConsumer, sourceGuard,
                            sourcePhysicalTopic, shard, Optional.empty(), Duration.ofSeconds(5));
            final RouteSnapshotV1 snapshot = routeSnapshot(sourcePhysicalBase, sourcePhysicalTopic, routeIncarnation,
                    beforeRoutePosition, proof, controlKeys);
            boolean runtimeDrained = false;
            try {
                try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                             oxiaEndpoint, namespace, "nereus-delay-pulsar-large-route-publisher-" + UUID.randomUUID(),
                             Duration.ofSeconds(15), routePrefix);
                     OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                             oxiaEndpoint, namespace, "nereus-delay-pulsar-large-route-provider-" + UUID.randomUUID(),
                             Duration.ofSeconds(15), routePrefix);
                     OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle =
                             OxiaSyncOwnerLeaseBackend.connectUnchecked(oxiaEndpoint, namespace,
                                     "nereus-delay-pulsar-large-assignment-" + UUID.randomUUID(),
                                     Duration.ofSeconds(15), assignmentPrefix)) {
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
                                    new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)),
                                    assignmentAuthority));
                    final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement =
                            routeCoordinator.placeActive(tenant, routeHint, placementRequest(System.currentTimeMillis()));
                    final WorkerAssignment accepted = routeCoordinator.requireAccepted(tenant,
                            placement.publication().revision(), placement.publication().assignment());
                    requireRouteAssignment(accepted, snapshot, beforeRoutePosition, proof);
                    WorkerAssignmentAuthority.Publication activePublication = placement.publication();

                    final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(assignmentHandle.backend());
                    final OwnerLease lease = ownerAuthority.acquire(accepted.sourceAssignment(),
                            "pulsar-large-payload-worker", assignmentHandle.sessionIdentity(),
                            System.currentTimeMillis(), LEASE_DURATION_MS).orElseThrow();
                    final WorkClassExecutionRegistry workClasses =
                            PulsarClientArtifactWorkerSmoke.workClasses(WORK_CLASS_BYTES);
                    final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshot(shard,
                            destinationProfile());
                    final Path root = Files.createTempDirectory("nereus-delay-pulsar-large-payload-");
                    boolean assignmentWithdrawn = false;
                    final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(root);
                        try (SharedRocksDbResources resources = new SharedRocksDbResources(storeConfig);
                             ShardStore store = ShardStore.open(storeConfig, shard, resources);
                             InMemoryCommandTransportRegistry transports = new InMemoryCommandTransportRegistry()) {
                            resources.bindWorkClassExecutionRegistry(workClasses);
                            store.recordControlSnapshot(controlSnapshot);
                            final InMemoryPayloadProofTrustSetCatalog trustCatalog =
                                    new InMemoryPayloadProofTrustSetCatalog();
                            trustCatalog.publish(trustSet);
                            final DelayShard delayShard = new DelayShard(store, DelayShardConfig.defaults(), null, null,
                                    scheduleResolver(destinationPhysicalTopic), trustCatalog);
                            final io.nereusstream.delay.protocol.OwnerIdentityV1 ownerIdentity =
                                    new io.nereusstream.delay.protocol.OwnerIdentityV1(bytes(16, 70), bytes(16, 71),
                                            lease.ownerEpoch(), Bytes.sha256(Bytes.utf8("pulsar-large-worker-fence")));
                            final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease, ownerIdentity);
                            WorkerAssignment activeAssignment = accepted;
                            OwnedDelayShard activeOwnedShard = ownedShard;
                            io.nereusstream.delay.protocol.OwnerIdentityV1 activeOwnerIdentity = ownerIdentity;
                            recover(nativeConsumer, sourceGuard, accepted, ownerAuthority, ownedShard,
                                    activation, beforeRoute, controlKeys, controlSnapshot, workClasses, sourcePhysicalTopic);
                            if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                    || !(ownedShard.lastCatchupPosition() instanceof PulsarSourcePosition recovered)
                                    || recovered.compareWithinShard(beforeRoutePosition) != 0) {
                                throw new IllegalStateException("Pulsar large-payload Worker recovery did not apply the exact pre-route records");
                            }

                            final CredentialBindingKey binding = new CredentialBindingKey(1,
                                    new Digest32(bytes(32, 41)), new Digest32(bytes(32, 42)));
                            final PulsarCommandTransportKey transportKey = new PulsarCommandTransportKey(
                                    CLUSTER, sourcePhysicalTopic, new Bytes32(SOURCE_INCARNATION),
                                    SOURCE_CREATION_TIMESTAMP, 0, binding);
                            final PulsarClientArtifactSendTransport managedTransport = new PulsarClientArtifactSendTransport(
                                    PulsarClientArtifactProducerFactory.create(client, CLUSTER, SOURCE_INCARNATION,
                                            sourcePhysicalTopic, SOURCE_CREATION_TIMESTAMP,
                                            "pulsar-large-gateway-managed"), CLUSTER, SOURCE_INCARNATION,
                                    sourcePhysicalTopic, SOURCE_CREATION_TIMESTAMP, 0);
                            final PulsarClientArtifactSendTransport nativeTransport = new PulsarClientArtifactSendTransport(
                                    PulsarClientArtifactProducerFactory.create(client, CLUSTER, SOURCE_INCARNATION,
                                            sourcePhysicalTopic, SOURCE_CREATION_TIMESTAMP,
                                            "pulsar-large-gateway-native"), CLUSTER, SOURCE_INCARNATION,
                                    sourcePhysicalTopic, SOURCE_CREATION_TIMESTAMP, 0);
                            transports.register(new ProductionPulsarSendTransport(transportKey,
                                    new ProductionPulsarSendTransport.Configuration(true, true, true,
                                            "pulsar-large-gateway-client"), managedTransport, nativeTransport));
                            final DefaultSubmissionCoordinator submissions = new DefaultSubmissionCoordinator(
                                    new RouteBoundSubmissionTransportPlanResolver(provider, System::currentTimeMillis),
                                    transports, SubmissionOutcomeProjectorRegistry.of(
                                            new PulsarManagedSubmissionOutcomeProjector(transportKey)));
                            final DefaultDelaySemanticCore core = new DefaultDelaySemanticCore(provider,
                                    new SecureLogicalUuidV7Generator(), System::currentTimeMillis);
                            final S3CompatiblePayloadObjectStore payloadStore = new S3CompatiblePayloadObjectStore(
                                    objectStoreProfile, minioUri, minioRegion, minioBucket, minioAccessKey,
                                    minioSecretKey, null, tenant.tenantRoutingScope(), trustSet, 7,
                                    proofKeys.getPrivate());
                            final OxiaSyncOwnerLeaseBackend.ClientHandle admissionHandle =
                                    OxiaSyncOwnerLeaseBackend.connectUnchecked(oxiaEndpoint, namespace,
                                            "pulsar-large-admission-" + UUID.randomUUID(), Duration.ofSeconds(15),
                                            gatewayPrefix + "/admission-client");
                            final OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyHandle =
                                    OxiaSyncOwnerLeaseBackend.connectUnchecked(oxiaEndpoint, namespace,
                                            "pulsar-large-idempotency-" + UUID.randomUUID(), Duration.ofSeconds(15),
                                            gatewayPrefix + "/idempotency-client");
                            final OxiaSyncOwnerLeaseBackend.ClientHandle auditHandle =
                                    OxiaSyncOwnerLeaseBackend.connectUnchecked(oxiaEndpoint, namespace,
                                            "pulsar-large-audit-" + UUID.randomUUID(), Duration.ofSeconds(15),
                                            gatewayPrefix + "/audit-client");
                            try (admissionHandle; idempotencyHandle; auditHandle) {
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
                                final MutualTlsJwtGatewayTenantAuthority tenantAuthority =
                                        new MutualTlsJwtGatewayTenantAuthority(new RsaSha256GatewayJwtVerifier(
                                                jwtKeys.getPublic(), "nereus-delay-gateway-e2e-issuer",
                                                "nereus-delay-gateway-e2e", "gateway-e2e-key", Clock.systemUTC(),
                                                30, 600));
                                final GatewayIngressService ingress = new GatewayIngressService(schedule,
                                        tenantAuthority, admission, audit, System::currentTimeMillis);
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
                                WorkerShardRuntime runtime = null;
                                PulsarClientArtifactWorkerSmoke.PhysicalPublishBridge physicalBridge = null;
                                boolean runtimeClosed = false;
                                try {
                                    server.start();
                                    final ManagedChannel channel = channel(gatewayPort, trustedClientCertificates,
                                            clientCertificate, clientPrivateKey);
                                    try {
                                        final DelayGatewayV1Grpc.DelayGatewayV1BlockingStub gateway = stub(channel,
                                                token(jwtKeys, tenant, certificateFingerprint(clientCertificate)));
                                        final ScheduleIntentV1 intent = largeScheduleIntent(System.currentTimeMillis());
                                        final GatewayPrepareLargeScheduleRequestV1 prepareRequest = prepareRequest(intent,
                                                payload.length, payloadHash, trustSet.ref(), objectStoreProfile.ref());
                                        final GatewaySubmissionOutcomeV1 prepareResponse =
                                                gateway.prepareLargeSchedule(prepareRequest);
                                        final CommandQueuedReceiptV1 prepareReceipt = requireQueued(prepareResponse,
                                                "PrepareLargeSchedule", beforeRoutePosition);
                                        final PulsarSourcePosition preparePosition =
                                                (PulsarSourcePosition) prepareReceipt.sourcePosition();
                                        final byte[] reservationId = reservationId(prepareReceipt);
                                runtime = PulsarClientArtifactWorkerSourceFactory.create(activeConsumer,
                                        sourceGuard, sourcePhysicalTopic, RECEIVE_TIMEOUT,
                                                activeAssignment.sourceAssignment(), workClasses, activeOwnedShard, store, resources,
                                                ownerAuthority, controlKeys.getPublic(), null, null, null, null, null);
                                        requireApplied(runUntilApplied(runtime), "PrepareLargeSchedule");
                                        final var reservation = Optional.ofNullable(delayShard.getReservation(reservationId))
                                                .orElseThrow(() -> new IllegalStateException(
                                                        "Pulsar Worker did not persist the payload reservation"));
                                        if (reservation.status() != PayloadReservationStatus.RESERVED) {
                                            throw new IllegalStateException("Prepare did not leave a RESERVED payload reservation: "
                                                    + reservation.status());
                                        }
                                        payloadStore.register(reservation, trustSet.ref(), objectStoreProfile.ref());
                                        final PayloadReservationReceiptV1 receipt = payloadStore.reservationReceipt(reservation);
                                        final GatewayPayloadUploadHandleResponseV1 handleResponse =
                                                gateway.issuePayloadUploadHandle(GatewayIssuePayloadUploadHandleRequestV1.newBuilder()
                                                        .setPayloadReservationReceiptV1(ByteString.copyFrom(receipt.payload()))
                                                        .setUploadHandleKind(UploadHandleKindV1.OPAQUE_SINGLE_PUT.wireValue())
                                                        .build());
                                        final PayloadUploadHandleResponseV1 handleDomain = PayloadUploadHandleResponseV1.decode(
                                                handleResponse.getPayloadUploadHandleResponseV1().toByteArray());
                                        if (handleDomain.outcome() != PayloadUploadHandleOutcomeV1.ISSUED) {
                                            throw new IllegalStateException("Gateway did not issue the Pulsar payload upload handle");
                                        }
                                        final OpaquePayloadUploadHandleV1 handle = handleDomain.issued();
                                        payloadStore.upload(receipt, handle, payload, System.currentTimeMillis());
                                        final GatewayPayloadAttestationResponseV1 attestationResponse =
                                                gateway.attestPayloadUpload(GatewayAttestPayloadUploadRequestV1.newBuilder()
                                                        .setPayloadReservationReceiptV1(ByteString.copyFrom(receipt.payload()))
                                                        .setOpaquePayloadUploadHandleV1(ByteString.copyFrom(handle.canonicalBytes()))
                                                        .build());
                                        final PayloadAttestationResponseV1 attestation = PayloadAttestationResponseV1.decode(
                                                attestationResponse.getPayloadAttestationResponseV1().toByteArray());
                                        if (attestation.outcome() != PayloadAttestationOutcomeV1.ATTESTED
                                                || attestation.proof() == null
                                                || new String(attestation.proof().immutableObjectVersion(),
                                                StandardCharsets.UTF_8).startsWith("sha256-")) {
                                            throw new IllegalStateException("Gateway/MinIO did not return an immutable Pulsar payload proof");
                                        }
                                        final PayloadCommitProofV1 proofValue = attestation.proof();
                                        final GatewaySubmissionOutcomeV1 commitResponse = gateway.commitLargeSchedule(
                                                commitRequest(receipt, proofValue));
                                        final CommandQueuedReceiptV1 commitReceipt = requireQueued(commitResponse,
                                                "CommitLargeSchedule", preparePosition);
                                        final PulsarSourcePosition commitPosition =
                                                (PulsarSourcePosition) commitReceipt.sourcePosition();
                                        requireApplied(runUntilApplied(runtime), "CommitLargeSchedule");
                                        final var committed = Optional.ofNullable(delayShard.getReservation(reservationId))
                                                .orElseThrow();
                                        final var message = Optional.ofNullable(
                                                delayShard.getMessage(prepareReceipt.command().delayMessageId())).orElseThrow();
                                        if (committed.status() != PayloadReservationStatus.COMMITTED
                                                || message.status() != MessageStatus.SCHEDULED
                                                || message.payloadReference() == null
                                                || !Arrays.equals(message.payloadReference().immutableObjectVersion(),
                                                proofValue.immutableObjectVersion())
                                                || !Arrays.equals(message.payloadReference().proofId(), proofValue.proofId())) {
                                            throw new IllegalStateException("Pulsar Worker did not persist the exact committed Object Store reference");
                                        }
                                        final byte[] objectPayload = payloadStore.readPayload(message.payloadReference());
                                        if (!Arrays.equals(objectPayload, payload)) {
                                            throw new IllegalStateException("Pulsar Worker Object Store readback did not match the committed payload");
                                        }
                                        signalFailoverCut();
                                        if (failoverRequested()) {
                                            final PulsarActivationBarrier oldBarrier =
                                                    (PulsarActivationBarrier) activeAssignment.sourceAssignment()
                                                            .activationBarrier();
                                            final SuccessorSource successor = createSuccessorSource(client, sourceGuard,
                                                    sourcePhysicalTopic, shard, commitPosition,
                                                    oldBarrier.guardedSourceConnectionGeneration());
                                            final GuardedConsumer<byte[]> successorConsumer = successor.consumer();
                                            boolean successorInstalled = false;
                                            try {
                                                final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof successorProof =
                                                        successor.proof();
                                                final PulsarActivationBarrier successorBarrier = new PulsarActivationBarrier(
                                                        shard, oldBarrier.brokerResourceIncarnation(), oldBarrier.physicalTopic(),
                                                        oldBarrier.ledgerId(), oldBarrier.entryId(),
                                                        oldBarrier.normalizedLastBatchIndex(), oldBarrier.batchSize(),
                                                        successorProof.connectionGeneration(), successorProof.attestationDigest(),
                                                        oldBarrier.empty());
                                                final long successorAssignmentEpoch = Math.addExact(
                                                        activeAssignment.sourceAssignment().assignmentEpoch(), 1);
                                                final SourceAssignment successorSource = new SourceAssignment(shard,
                                                        Bytes.sha256(Bytes.utf8(
                                                                "nereus-delay-pulsar-source-reactivation-assignment-v1\0"),
                                                                activeAssignment.sourceAssignment().assignmentId(),
                                                                Bytes.u64be(successorAssignmentEpoch)),
                                                        successorAssignmentEpoch, successorBarrier);
                                                final WorkerAssignment successorAssignment = new WorkerAssignment(
                                                        activeAssignment.workerId(), successorSource,
                                                        Math.addExact(activeAssignment.placementEpoch(), 1),
                                                        activeAssignment.capacityEnvelopeDigest(), snapshot.snapshotDigest());
                                                final PulsarSourceReactivationV1 reactivation =
                                                        new PulsarSourceReactivationV1(snapshot.snapshotDigest(),
                                                                activeAssignment.sourceAssignment(), successorSource);
                                                final PulsarSourceReactivationCoordinator reactivationCoordinator =
                                                        new PulsarSourceReactivationCoordinator(provider,
                                                                assignmentAuthority, ownerAuthority);
                                                final PulsarSourceReactivationCoordinator.FencedPlan fencedPlan =
                                                        reactivationCoordinator.fenceForReactivation(tenant,
                                                                activePublication,
                                                                ownerAuthority.current(shard).orElseThrow(), reactivation,
                                                                System.currentTimeMillis());
                                                final WorkerShardRuntime oldRuntime = runtime;
                                                final OwnedDelayShard oldOwnedShard = activeOwnedShard;
                                                oldOwnedShard.fence();
                                                oldRuntime.closeForOwnerReactivation();
                                                runtimeClosed = true;
                                                final WorkerAssignmentAuthority.Publication successorPublication =
                                                        reactivationCoordinator.publishSuccessor(fencedPlan,
                                                                successorAssignment, () -> {
                                                                    if (!oldRuntime.sourcePaused()
                                                                            || oldRuntime.pendingSourceEntry().isPresent()) {
                                                                        throw new IllegalStateException(
                                                                                "old Pulsar Worker runtime is not quiescent");
                                                                    }
                                                                });
                                                final OwnerLease successorLease = reactivationCoordinator.acquireSuccessor(
                                                        fencedPlan, successorPublication, successorAssignment.workerId(),
                                                        assignmentHandle.sessionIdentity(), System.currentTimeMillis(),
                                                        LEASE_DURATION_MS);
                                                final io.nereusstream.delay.protocol.OwnerIdentityV1 successorOwnerIdentity =
                                                        new io.nereusstream.delay.protocol.OwnerIdentityV1(bytes(16, 70),
                                                                bytes(16, 71), successorLease.ownerEpoch(),
                                                                Bytes.sha256(Bytes.utf8(
                                                                        "pulsar-large-worker-reactivation-fence"),
                                                                        Bytes.u64beBits(successorLease.ownerEpoch())));
                                                final OwnedDelayShard successorOwnedShard = new OwnedDelayShard(delayShard,
                                                        successorLease, successorOwnerIdentity);
                                                successorOwnedShard.markCatchingUpForReactivation(ownerAuthority, successorSource,
                                                        strictPulsarFixtureSuccessor(), System.currentTimeMillis());
                                                successorOwnedShard.activateForReactivation(ownerAuthority,
                                                        controlSnapshot, System.currentTimeMillis());
                                                activeAssignment = successorAssignment;
                                                activePublication = successorPublication;
                                                activeConsumer = successorConsumer;
                                                activeOwnedShard = successorOwnedShard;
                                                activeOwnerIdentity = successorOwnerIdentity;
                                                successorInstalled = true;
                                                System.out.println("Pulsar source reactivation successor accepted: oldGeneration="
                                                        + oldBarrier.guardedSourceConnectionGeneration()
                                                        + ", newGeneration=" + successorProof.connectionGeneration()
                                                        + ", assignmentRevision=" + successorPublication.revision()
                                                        + ", ownerEpoch=" + successorLease.ownerEpoch());
                                            } finally {
                                                if (!successorInstalled) {
                                                    closeNative(successorConsumer);
                                                }
                                            }
                                            runtime = PulsarClientArtifactWorkerSourceFactory.create(activeConsumer,
                                                    sourceGuard, sourcePhysicalTopic, RECEIVE_TIMEOUT,
                                                    activeAssignment.sourceAssignment(), workClasses, activeOwnedShard,
                                                    store, resources, ownerAuthority, controlKeys.getPublic(), null, null,
                                                    null, null, null);
                                            runtimeClosed = false;
                                        }
                                        final LaneRecord physicalLane = Optional.ofNullable(
                                                delayShard.getLane(message.laneId())).orElseThrow(
                                                () -> new IllegalStateException(
                                                        "Pulsar Worker did not persist the physical destination Lane"));
                                        physicalBridge = PulsarClientArtifactWorkerSmoke.createPhysicalPublishBridge(
                                                client, activeConsumer, sourcePhysicalTopic, shard, commitPosition,
                                                destinationPhysicalTopic, store, activeOwnedShard, activeOwnerIdentity, ownerAuthority,
                                                workClasses, controlKeys, destinationProfile(),
                                                PulsarClientArtifactWorkerSmoke.capabilityProfile(), message.laneId(),
                                                physicalLane.laneIncarnation(), WORK_CLASS_BYTES);
                                        runtime.bindPhysicalPublishExecutor(physicalBridge.executor());
                                        PulsarClientArtifactWorkerSmoke.runSourceAppliedPhysicalPublish(runtime,
                                                delayShard, activeOwnedShard, activeOwnerIdentity, ownerAuthority, store, workClasses,
                                                controlKeys, physicalBridge, prepareReceipt.command().delayMessageId(), commitPosition,
                                                client, objectPayload, WORK_CLASS_BYTES);
                                        final byte[] duplicate = gateway.prepareLargeSchedule(prepareRequest).toByteArray();
                                        if (!Arrays.equals(prepareResponse.toByteArray(), duplicate)) {
                                            throw new IllegalStateException("real Oxia Pulsar Gateway idempotency did not return exact Prepare bytes");
                                        }
                                        final int sourceRecordCount = countSourceRecords(client, sourceGuard, sourcePhysicalTopic);
                                        if (sourceRecordCount != 6) {
                                            throw new IllegalStateException("duplicate Prepare changed the Pulsar source record count: "
                                                    + sourceRecordCount + " expected 6");
                                        }
                                        final Path checkpointPath = root.resolve("pulsar-large-payload-final-checkpoint");
                                        final byte[] checkpointId = Arrays.copyOf(
                                                Bytes.sha256(Bytes.utf8("pulsar-large-payload-final-checkpoint")), 16);
                                        final var drain = runtime.drain(
                                                new io.nereusstream.delay.ownership.OwnerDrainCoordinator.DrainRequest(
                                                        System.currentTimeMillis() + 30_000, 0, checkpointPath, checkpointId),
                                                System::currentTimeMillis, () -> { });
                                        if (drain.pendingCheckpointTask() != null || drain.finalCheckpointPath() == null
                                                || !Files.isDirectory(checkpointPath)
                                                || CheckpointFileInventory.collect(checkpointPath).isEmpty()
                                                || !ownerAuthority.current(shard).isEmpty()) {
                                            throw new IllegalStateException("Pulsar large-payload Worker drain did not publish the final checkpoint or release the owner");
                                        }
                                        runtime.close();
                                        runtimeClosed = true;
                                        runtimeDrained = true;
                                        if (!assignmentAuthority.withdraw(activePublication)) {
                                            throw new IllegalStateException("Pulsar large-payload Worker assignment was not withdrawn exactly");
                                        }
                                        assignmentWithdrawn = true;
                                        System.out.println("Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare="
                                                + preparePosition.ledgerId() + "/" + preparePosition.entryId() + ", commit="
                                                + commitPosition.ledgerId() + "/" + commitPosition.entryId()
                                                + ", exactGatewayIdempotency=true, sourceRecords=" + sourceRecordCount);
                                    } finally {
                                        channel.shutdownNow();
                                        channel.awaitTermination(10, TimeUnit.SECONDS);
                                    }
                                } finally {
                                    if (!runtimeClosed && runtime != null) {
                                        try {
                                            runtime.close();
                                        } catch (RuntimeException cleanupFailure) {
                                            System.err.println("Pulsar large-payload runtime cleanup deferred: "
                                                    + cleanupFailure.getMessage());
                                        }
                                    }
                                    if (physicalBridge != null) {
                                        physicalBridge.close();
                                    }
                                    server.close();
                                }
                            }
                        } finally {
                            if (!assignmentWithdrawn) {
                                assignmentAuthority.withdraw(activePublication);
                            }
                            deleteTree(root);
                        }
                }
            } finally {
                if (!runtimeDrained) {
                    closeNative(activeConsumer);
                }
            }
        } finally {
            deletePartitionedTopic(admin, adminUrl, sourceBase);
            deleteTopic(admin, adminUrl, destinationName);
        }
    }
}
