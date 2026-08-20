package io.nereusstream.delay.gateway;

import com.google.protobuf.ByteString;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import io.nereusstream.delay.gateway.v1.DelayGatewayV1Grpc;
import io.nereusstream.delay.gateway.v1.GatewayRouteSelectorV1;
import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.MessagePreconditionV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SubmissionModeV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.DelaySemanticCore;
import io.nereusstream.delay.semantic.LargeSchedulePreparationV1;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.submission.SubmissionCoordinator;
import io.nereusstream.delay.transport.TransportOwnershipPermit;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in network proof for the Gateway mTLS/JWT and durable Oxia composition. */
@Tag("real-service")
class OxiaRealGatewayGrpcSmokeTest {
    private static final long NOW_EPOCH_MS = 1_000_000;
    private static final long NOW_EPOCH_SECONDS = NOW_EPOCH_MS / 1_000;

    @Test
    void authenticatedScheduleIsNetworkBoundAndExactlyIdempotentAgainstRealOxia() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final Path serverCertificate = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_CERT");
        final Path serverPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_KEY");
        final Path trustedClientCertificates = requiredPath("NEREUS_DELAY_GATEWAY_CA_CERT");
        final Path clientCertificate = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_CERT");
        final Path clientPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_KEY");
        final int port = Integer.parseInt(requiredEnv("NEREUS_DELAY_GATEWAY_PORT"));
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("NEREUS_DELAY_GATEWAY_PORT must be 1..65535");
        }

        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-gateway-grpc/" + UUID.randomUUID();
        final MutableClock clock = new MutableClock(NOW_EPOCH_MS);
        final AuthenticatedTenantContext tenant = tenant(11);
        final KeyPair jwtKeyPair = rsaKeyPair();
        final byte[] certificateFingerprint = certificateFingerprint(clientCertificate);
        final RsaSha256GatewayJwtVerifier verifier = new RsaSha256GatewayJwtVerifier(
                jwtKeyPair.getPublic(), "nereus-delay-gateway-e2e-issuer", "nereus-delay-gateway-e2e",
                "gateway-e2e-key", Clock.fixed(Instant.ofEpochSecond(NOW_EPOCH_SECONDS), ZoneOffset.UTC),
                30, 600);
        final MutualTlsJwtGatewayTenantAuthority authority = new MutualTlsJwtGatewayTenantAuthority(verifier);

        final ScheduleIntentV1 intent = scheduleIntent();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, intent, 2_000_000);
        final PreparedSubmissionV1 prepared = PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command));
        final FixedCore core = new FixedCore(prepared);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final String token = token(jwtKeyPair, tenant, certificateFingerprint);
        final io.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1 request = request(intent);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle admissionClient = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, namespace, "nereus-delay-gateway-admission-" + UUID.randomUUID(), Duration.ofSeconds(15),
                prefix + "/admission-client");
             OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-idempotency-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix + "/idempotency-client");
             OxiaSyncOwnerLeaseBackend.ClientHandle auditClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-audit-" + UUID.randomUUID(), Duration.ofSeconds(15),
                     prefix + "/audit-client")) {
            final OxiaGatewayAdmissionController admission = new OxiaGatewayAdmissionController(
                    admissionClient, prefix + "/admission", clock,
                    new OxiaGatewayAdmissionController.Limits(1, 1_000_000, 1, 1, 10_000, 8));
            final OxiaGatewayIdempotencyStore idempotency = new OxiaGatewayIdempotencyStore(
                    idempotencyClient, prefix + "/idempotency", clock, 10_000, 10_000);
            final OxiaGatewayAuditSink audit = new OxiaGatewayAuditSink(auditClient, prefix + "/audit");
            final GatewayScheduleService schedule = new GatewayScheduleService(core, idempotency, coordinator, clock);
            final GatewayIngressService ingress = new GatewayIngressService(schedule, authority, admission, audit,
                    clock);
            final GatewayGrpcService grpc = new GatewayGrpcService(ingress, GatewayGrpcContext.provider());
            io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 firstResponse;

            try (GatewayGrpcServer server = GatewayGrpcServer.mutualTls(port, serverCertificate, serverPrivateKey,
                    trustedClientCertificates, grpc)) {
                server.start();
                final ManagedChannel channel = channel(port, trustedClientCertificates, clientCertificate,
                        clientPrivateKey);
                try {
                    final DelayGatewayV1Grpc.DelayGatewayV1BlockingStub authenticated = stub(channel, token);
                    firstResponse = authenticated.schedule(request);
                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 second =
                            authenticated.schedule(request);

                    assertEquals(firstResponse, second);
                    assertTrue(firstResponse.hasSubmissionOutcomeNdr1());
                    assertEquals(StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED.wireValue(),
                            SubmissionOutcomeMessageV1.decode(firstResponse.getSubmissionOutcomeNdr1().toByteArray())
                                    .managed().definitelyNotQueued().error().code().wireValue());
                    assertEquals(1, core.prepareCalls);
                    assertEquals(1, coordinator.submitCalls);

                    final DelayGatewayV1Grpc.DelayGatewayV1BlockingStub invalid = stub(channel,
                            mutateSignature(token));
                    final StatusRuntimeException authenticationFailure = assertThrows(StatusRuntimeException.class,
                            () -> invalid.schedule(request));
                    assertEquals(Status.Code.UNAUTHENTICATED, authenticationFailure.getStatus().getCode());
                    assertEquals(1, core.prepareCalls);
                } finally {
                    channel.shutdownNow();
                    assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
                }
            }

            try (GatewayGrpcServer restarted = GatewayGrpcServer.mutualTls(port, serverCertificate, serverPrivateKey,
                    trustedClientCertificates, grpc)) {
                restarted.start();
                final ManagedChannel channel = channel(port, trustedClientCertificates, clientCertificate,
                        clientPrivateKey);
                try {
                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 afterRestart =
                            stub(channel, token).schedule(request);
                    assertArrayEquals(firstResponse.toByteArray(), afterRestart.toByteArray());
                    assertEquals(1, core.prepareCalls);
                    assertEquals(1, coordinator.submitCalls);
                } finally {
                    channel.shutdownNow();
                    assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
                }
            }

            try (var admissionScan = admissionClient.client().rangeScan(
                    prefix + "/admission/admission/", prefix + "/admission/admission/\uffff");
                 var idempotencyScan = idempotencyClient.client().rangeScan(
                         prefix + "/idempotency/idempotency/", prefix + "/idempotency/idempotency/\uffff");
                 var auditScan = auditClient.client().rangeScan(
                         prefix + "/audit/audit/", prefix + "/audit/audit/\uffff")) {
                final List<io.oxia.client.api.GetResult> admissionRecords =
                        java.util.stream.StreamSupport.stream(admissionScan.spliterator(), false).toList();
                final List<io.oxia.client.api.GetResult> idempotencyRecords =
                        java.util.stream.StreamSupport.stream(idempotencyScan.spliterator(), false).toList();
                final List<io.oxia.client.api.GetResult> auditRecords =
                        java.util.stream.StreamSupport.stream(auditScan.spliterator(), false).toList();
                assertEquals(1, admissionRecords.size());
                assertEquals(0, GatewayAdmissionRecordV1.decode(admissionRecords.get(0).value()).leases().size());
                assertEquals(1, idempotencyRecords.size());
                final GatewayIdempotencyRecordV1 idempotencyRecord =
                        GatewayIdempotencyRecordV1.decode(idempotencyRecords.get(0).value());
                assertEquals(GatewayIdempotencyPhaseV1.QUIESCENT, idempotencyRecord.phase());
                assertEquals(1, idempotencyRecord.attempts().size());
                assertNotNull(idempotencyRecord.aggregateOutcomeBytes());
                assertEquals(2, auditRecords.size());
            }
        }
    }

    @Test
    void gatewayRecoversAfterCommittedOxiaAttemptResponseLoss() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final Path serverCertificate = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_CERT");
        final Path serverPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_KEY");
        final Path trustedClientCertificates = requiredPath("NEREUS_DELAY_GATEWAY_CA_CERT");
        final Path clientCertificate = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_CERT");
        final Path clientPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_KEY");
        final int port = Integer.parseInt(requiredEnv("NEREUS_DELAY_GATEWAY_PORT"));
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("NEREUS_DELAY_GATEWAY_PORT must be 1..65535");
        }

        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-gateway-response-loss/" + UUID.randomUUID();
        final MutableClock clock = new MutableClock(NOW_EPOCH_MS);
        final AuthenticatedTenantContext tenant = tenant(41);
        final KeyPair jwtKeyPair = rsaKeyPair();
        final byte[] certificateFingerprint = certificateFingerprint(clientCertificate);
        final RsaSha256GatewayJwtVerifier verifier = new RsaSha256GatewayJwtVerifier(
                jwtKeyPair.getPublic(), "nereus-delay-gateway-e2e-issuer", "nereus-delay-gateway-e2e",
                "gateway-e2e-key", Clock.fixed(Instant.ofEpochSecond(NOW_EPOCH_SECONDS), ZoneOffset.UTC),
                30, 600);
        final MutualTlsJwtGatewayTenantAuthority authority = new MutualTlsJwtGatewayTenantAuthority(verifier);
        final ScheduleIntentV1 intent = scheduleIntent();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, intent, 2_000_000);
        final PreparedSubmissionV1 prepared = PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command));
        final FixedCore core = new FixedCore(prepared);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final String token = token(jwtKeyPair, tenant, certificateFingerprint);
        final io.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1 request = request(intent);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle admissionClient = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, namespace, "nereus-delay-gateway-response-loss-admission-" + UUID.randomUUID(),
                Duration.ofSeconds(15), prefix + "/admission-client");
             OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-response-loss-idempotency-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix + "/idempotency-client");
             OxiaSyncOwnerLeaseBackend.ClientHandle auditClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-response-loss-audit-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix + "/audit-client")) {
            final ResponseLossOnceRecordClient responseLossIdempotencyRecords = new ResponseLossOnceRecordClient(
                    new SessionBoundOxiaGatewayRecordClient(idempotencyClient), 2);
            final GatewayGrpcService grpc = grpcService(
                    new SessionBoundOxiaGatewayRecordClient(admissionClient), responseLossIdempotencyRecords,
                    new SessionBoundOxiaGatewayRecordClient(auditClient), prefix, clock, core, coordinator,
                    authority);
            try (GatewayGrpcServer server = GatewayGrpcServer.mutualTls(port, serverCertificate, serverPrivateKey,
                    trustedClientCertificates, grpc)) {
                server.start();
                final ManagedChannel channel = channel(port, trustedClientCertificates, clientCertificate,
                        clientPrivateKey);
                try {
                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 firstResponse =
                            stub(channel, token).schedule(request);
                    assertTrue(firstResponse.hasSubmissionOutcomeNdr1());
                    assertEquals(io.nereusstream.delay.protocol.EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN,
                            SubmissionOutcomeMessageV1.decode(firstResponse.getSubmissionOutcomeNdr1().toByteArray())
                                    .managed().kind());
                    assertEquals(1, core.prepareCalls);
                    assertEquals(0, coordinator.submitCalls);

                    clock.set(NOW_EPOCH_MS + 10_000);
                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 recoveredResponse =
                            stub(channel, token).schedule(request);
                    assertArrayEquals(firstResponse.toByteArray(), recoveredResponse.toByteArray());
                    assertEquals(1, core.prepareCalls);
                    assertEquals(0, coordinator.submitCalls);
                    System.out.println("Gateway Oxia STARTED response-loss E2E passed: committed attempt was reread "
                            + "after deadline as exact UNCERTAIN without a second physical submission");
                } finally {
                    channel.shutdownNow();
                    assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
                }
            }

            try (var admissionScan = admissionClient.client().rangeScan(
                    prefix + "/admission/admission/", prefix + "/admission/admission/\uffff");
                 var idempotencyScan = idempotencyClient.client().rangeScan(
                         prefix + "/idempotency/idempotency/", prefix + "/idempotency/idempotency/\uffff");
                 var auditScan = auditClient.client().rangeScan(
                         prefix + "/audit/audit/", prefix + "/audit/audit/\uffff")) {
                final List<GetResult> admissionRecords =
                        java.util.stream.StreamSupport.stream(admissionScan.spliterator(), false).toList();
                final List<GetResult> idempotencyRecords =
                        java.util.stream.StreamSupport.stream(idempotencyScan.spliterator(), false).toList();
                final List<GetResult> auditRecords =
                        java.util.stream.StreamSupport.stream(auditScan.spliterator(), false).toList();
                assertEquals(1, admissionRecords.size());
                assertEquals(0, GatewayAdmissionRecordV1.decode(admissionRecords.get(0).value()).leases().size());
                assertEquals(1, idempotencyRecords.size());
                final GatewayIdempotencyRecordV1 idempotencyRecord =
                        GatewayIdempotencyRecordV1.decode(idempotencyRecords.get(0).value());
                assertEquals(GatewayIdempotencyPhaseV1.QUIESCENT, idempotencyRecord.phase());
                assertEquals(1, idempotencyRecord.attempts().size());
                assertEquals(GatewayPhysicalAttemptStateV1.UNCERTAIN,
                        idempotencyRecord.attempts().get(0).state());
                assertNotNull(idempotencyRecord.aggregateOutcomeBytes());
                assertEquals(4, auditRecords.size());
            }
        }
    }

    @Test
    void gatewayRecoversAfterCommittedOxiaRetryAttemptResponseLoss() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final Path serverCertificate = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_CERT");
        final Path serverPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_KEY");
        final Path trustedClientCertificates = requiredPath("NEREUS_DELAY_GATEWAY_CA_CERT");
        final Path clientCertificate = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_CERT");
        final Path clientPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_KEY");
        final int port = Integer.parseInt(requiredEnv("NEREUS_DELAY_GATEWAY_PORT"));
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("NEREUS_DELAY_GATEWAY_PORT must be 1..65535");
        }

        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-gateway-retry-response-loss/" + UUID.randomUUID();
        final MutableClock clock = new MutableClock(NOW_EPOCH_MS);
        final AuthenticatedTenantContext tenant = tenant(51);
        final KeyPair jwtKeyPair = rsaKeyPair();
        final byte[] certificateFingerprint = certificateFingerprint(clientCertificate);
        final RsaSha256GatewayJwtVerifier verifier = new RsaSha256GatewayJwtVerifier(
                jwtKeyPair.getPublic(), "nereus-delay-gateway-e2e-issuer", "nereus-delay-gateway-e2e",
                "gateway-e2e-key", Clock.fixed(Instant.ofEpochSecond(NOW_EPOCH_SECONDS), ZoneOffset.UTC),
                30, 600);
        final MutualTlsJwtGatewayTenantAuthority authority = new MutualTlsJwtGatewayTenantAuthority(verifier);
        final ScheduleIntentV1 intent = scheduleIntent();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, intent, 2_000_000);
        final PreparedSubmissionV1 prepared = PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command));
        final FixedCore core = new FixedCore(prepared);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final String token = token(jwtKeyPair, tenant, certificateFingerprint);
        final io.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1 request = request(intent);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle admissionClient = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, namespace, "nereus-delay-gateway-retry-response-loss-admission-" + UUID.randomUUID(),
                Duration.ofSeconds(15), prefix + "/admission-client");
             OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-retry-response-loss-idempotency-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix + "/idempotency-client");
             OxiaSyncOwnerLeaseBackend.ClientHandle auditClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-retry-response-loss-audit-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix + "/audit-client")) {
            final ResponseLossOnceRecordClient responseLossIdempotencyRecords = new ResponseLossOnceRecordClient(
                    new SessionBoundOxiaGatewayRecordClient(idempotencyClient), Set.of(2, 4));
            final GatewayGrpcService grpc = grpcService(
                    new SessionBoundOxiaGatewayRecordClient(admissionClient), responseLossIdempotencyRecords,
                    new SessionBoundOxiaGatewayRecordClient(auditClient), prefix, clock, core, coordinator,
                    authority);
            try (GatewayGrpcServer server = GatewayGrpcServer.mutualTls(port, serverCertificate, serverPrivateKey,
                    trustedClientCertificates, grpc)) {
                server.start();
                final ManagedChannel channel = channel(port, trustedClientCertificates, clientCertificate,
                        clientPrivateKey);
                try {
                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 firstResponse =
                            stub(channel, token).schedule(request);
                    assertEquals(io.nereusstream.delay.protocol.EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN,
                            SubmissionOutcomeMessageV1.decode(firstResponse.getSubmissionOutcomeNdr1().toByteArray())
                                    .managed().kind());
                    assertEquals(1, core.prepareCalls);
                    assertEquals(0, coordinator.submitCalls);

                    clock.set(NOW_EPOCH_MS + 10_000);
                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 recoveredResponse =
                            stub(channel, token).schedule(request);
                    assertArrayEquals(firstResponse.toByteArray(), recoveredResponse.toByteArray());

                    final GatewayIdempotencyRecordV1 recoveredRecord;
                    try (var idempotencyScan = idempotencyClient.client().rangeScan(
                            prefix + "/idempotency/idempotency/", prefix + "/idempotency/idempotency/\uffff")) {
                        final List<GetResult> records =
                                java.util.stream.StreamSupport.stream(idempotencyScan.spliterator(), false).toList();
                        assertEquals(1, records.size());
                        recoveredRecord = GatewayIdempotencyRecordV1.decode(records.get(0).value());
                    }
                    assertEquals(GatewayPhysicalAttemptStateV1.UNCERTAIN,
                            recoveredRecord.attempts().get(0).state());
                    final PhysicalEnqueueAttemptId priorAttemptId =
                            recoveredRecord.attempts().get(0).physicalAttemptId();
                    final PhysicalEnqueueAttemptId retryRequestId =
                            PhysicalEnqueueAttemptId.require(bytes(16, 151));
                    final io.nereusstream.delay.gateway.v1.GatewayRetryUncertainRequestV1 retryRequest =
                            io.nereusstream.delay.gateway.v1.GatewayRetryUncertainRequestV1.newBuilder()
                                    .setOriginalIdempotencyKey(request.getIdempotencyKey())
                                    .setExpectedPriorPhysicalAttemptId(ByteString.copyFrom(priorAttemptId.bytes()))
                                    .setRetryRequestId(ByteString.copyFrom(retryRequestId.bytes()))
                                    .build();

                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 retryResponse =
                            stub(channel, token).retryUncertain(retryRequest);
                    assertEquals(io.nereusstream.delay.protocol.EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN,
                            SubmissionOutcomeMessageV1.decode(retryResponse.getSubmissionOutcomeNdr1().toByteArray())
                                    .managed().kind());
                    assertEquals(1, core.prepareCalls);
                    assertEquals(0, coordinator.submitCalls);

                    clock.set(NOW_EPOCH_MS + 20_000);
                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 recoveredRetryResponse =
                            stub(channel, token).retryUncertain(retryRequest);
                    assertArrayEquals(retryResponse.toByteArray(), recoveredRetryResponse.toByteArray());
                    assertEquals(1, core.prepareCalls);
                    assertEquals(0, coordinator.submitCalls);
                    System.out.println("Gateway Oxia RETRY_UNCERTAIN response-loss E2E passed: committed retry "
                            + "attempt was reread after deadline as exact UNCERTAIN without a second physical submission");
                } finally {
                    channel.shutdownNow();
                    assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
                }
            }

            try (var admissionScan = admissionClient.client().rangeScan(
                    prefix + "/admission/admission/", prefix + "/admission/admission/\uffff");
                 var idempotencyScan = idempotencyClient.client().rangeScan(
                         prefix + "/idempotency/idempotency/", prefix + "/idempotency/idempotency/\uffff");
                 var auditScan = auditClient.client().rangeScan(
                         prefix + "/audit/audit/", prefix + "/audit/audit/\uffff")) {
                final List<GetResult> admissionRecords =
                        java.util.stream.StreamSupport.stream(admissionScan.spliterator(), false).toList();
                final List<GetResult> idempotencyRecords =
                        java.util.stream.StreamSupport.stream(idempotencyScan.spliterator(), false).toList();
                final List<GetResult> auditRecords =
                        java.util.stream.StreamSupport.stream(auditScan.spliterator(), false).toList();
                assertEquals(1, admissionRecords.size());
                assertEquals(0, GatewayAdmissionRecordV1.decode(admissionRecords.get(0).value()).leases().size());
                assertEquals(1, idempotencyRecords.size());
                final GatewayIdempotencyRecordV1 idempotencyRecord =
                        GatewayIdempotencyRecordV1.decode(idempotencyRecords.get(0).value());
                assertEquals(GatewayIdempotencyPhaseV1.QUIESCENT, idempotencyRecord.phase());
                assertEquals(2, idempotencyRecord.attempts().size());
                assertEquals(GatewayPhysicalAttemptStateV1.UNCERTAIN,
                        idempotencyRecord.attempts().get(0).state());
                assertEquals(GatewayPhysicalAttemptStateV1.UNCERTAIN,
                        idempotencyRecord.attempts().get(1).state());
                assertNotNull(idempotencyRecord.aggregateOutcomeBytes());
                assertEquals(8, auditRecords.size());
            }
        }
    }

    @Test
    void rotatedGatewayCertificatesRejectOldClientAndReuseDurableOutcome() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final Path serverCertificate = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_CERT");
        final Path serverPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_KEY");
        final Path trustedClientCertificates = requiredPath("NEREUS_DELAY_GATEWAY_CA_CERT");
        final Path clientCertificate = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_CERT");
        final Path clientPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_KEY");
        final Path rotatedServerCertificate = requiredPath("NEREUS_DELAY_GATEWAY_ROTATED_SERVER_CERT");
        final Path rotatedServerPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_ROTATED_SERVER_KEY");
        final Path rotatedTrustedClientCertificates = requiredPath("NEREUS_DELAY_GATEWAY_ROTATED_CA_CERT");
        final Path rotatedClientCertificate = requiredPath("NEREUS_DELAY_GATEWAY_ROTATED_CLIENT_CERT");
        final Path rotatedClientPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_ROTATED_CLIENT_KEY");
        final int port = Integer.parseInt(requiredEnv("NEREUS_DELAY_GATEWAY_PORT"));
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("NEREUS_DELAY_GATEWAY_PORT must be 1..65535");
        }

        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-gateway-rotation/" + UUID.randomUUID();
        final MutableClock clock = new MutableClock(NOW_EPOCH_MS);
        final AuthenticatedTenantContext tenant = tenant(23);
        final KeyPair jwtKeyPair = rsaKeyPair();
        final byte[] certificateFingerprint = certificateFingerprint(clientCertificate);
        final byte[] rotatedCertificateFingerprint = certificateFingerprint(rotatedClientCertificate);
        final RsaSha256GatewayJwtVerifier verifier = new RsaSha256GatewayJwtVerifier(
                jwtKeyPair.getPublic(), "nereus-delay-gateway-e2e-issuer", "nereus-delay-gateway-e2e",
                "gateway-e2e-key", Clock.fixed(Instant.ofEpochSecond(NOW_EPOCH_SECONDS), ZoneOffset.UTC),
                30, 600);
        final MutualTlsJwtGatewayTenantAuthority authority = new MutualTlsJwtGatewayTenantAuthority(verifier);
        final ScheduleIntentV1 intent = scheduleIntent();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, intent, 2_000_000);
        final PreparedSubmissionV1 prepared = PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command));
        final FixedCore core = new FixedCore(prepared);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final String token = token(jwtKeyPair, tenant, certificateFingerprint);
        final String rotatedToken = token(jwtKeyPair, tenant, rotatedCertificateFingerprint);
        final io.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1 request = request(intent);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle admissionClient = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, namespace, "nereus-delay-gateway-rotation-admission-" + UUID.randomUUID(),
                Duration.ofSeconds(15), prefix + "/admission-client");
             OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-rotation-idempotency-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix + "/idempotency-client");
             OxiaSyncOwnerLeaseBackend.ClientHandle auditClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-rotation-audit-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix + "/audit-client")) {
            final GatewayGrpcService grpc = grpcService(admissionClient, idempotencyClient, auditClient, prefix,
                    clock, core, coordinator, authority);
            final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 firstResponse;
            try (GatewayGrpcServer server = GatewayGrpcServer.mutualTls(port, serverCertificate, serverPrivateKey,
                    trustedClientCertificates, grpc)) {
                server.start();
                final ManagedChannel channel = channel(port, trustedClientCertificates, clientCertificate,
                        clientPrivateKey);
                try {
                    firstResponse = stub(channel, token).schedule(request);
                } finally {
                    channel.shutdownNow();
                    assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
                }
            }

            try (GatewayGrpcServer rotated = GatewayGrpcServer.mutualTls(port, rotatedServerCertificate,
                    rotatedServerPrivateKey, rotatedTrustedClientCertificates, grpc)) {
                rotated.start();
                final ManagedChannel oldChannel = channel(port, rotatedTrustedClientCertificates, clientCertificate,
                        clientPrivateKey);
                try {
                    final StatusRuntimeException rejection = assertThrows(StatusRuntimeException.class,
                            () -> stub(oldChannel, token).withDeadlineAfter(5, TimeUnit.SECONDS)
                                    .schedule(request));
                    assertEquals(Status.Code.UNAVAILABLE, rejection.getStatus().getCode());
                } finally {
                    oldChannel.shutdownNow();
                    assertTrue(oldChannel.awaitTermination(10, TimeUnit.SECONDS));
                }

                final ManagedChannel rotatedChannel = channel(port, rotatedTrustedClientCertificates,
                        rotatedClientCertificate, rotatedClientPrivateKey);
                try {
                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 rotatedResponse =
                            stub(rotatedChannel, rotatedToken).schedule(request);
                    assertArrayEquals(firstResponse.toByteArray(), rotatedResponse.toByteArray());
                    assertEquals(1, core.prepareCalls);
                    assertEquals(1, coordinator.submitCalls);
                    System.out.println("Gateway certificate rotation E2E passed: old mTLS client rejected and new "
                            + "certificate reread the exact durable outcome");
                } finally {
                    rotatedChannel.shutdownNow();
                    assertTrue(rotatedChannel.awaitTermination(10, TimeUnit.SECONDS));
                }
            }

            try (var admissionScan = admissionClient.client().rangeScan(
                    prefix + "/admission/admission/", prefix + "/admission/admission/\uffff");
                 var idempotencyScan = idempotencyClient.client().rangeScan(
                         prefix + "/idempotency/idempotency/", prefix + "/idempotency/idempotency/\uffff");
                 var auditScan = auditClient.client().rangeScan(
                         prefix + "/audit/audit/", prefix + "/audit/audit/\uffff")) {
                final List<io.oxia.client.api.GetResult> admissionRecords =
                        java.util.stream.StreamSupport.stream(admissionScan.spliterator(), false).toList();
                final List<io.oxia.client.api.GetResult> idempotencyRecords =
                        java.util.stream.StreamSupport.stream(idempotencyScan.spliterator(), false).toList();
                final List<io.oxia.client.api.GetResult> auditRecords =
                        java.util.stream.StreamSupport.stream(auditScan.spliterator(), false).toList();
                assertEquals(1, admissionRecords.size());
                assertEquals(0, GatewayAdmissionRecordV1.decode(admissionRecords.get(0).value()).leases().size());
                assertEquals(1, idempotencyRecords.size());
                final GatewayIdempotencyRecordV1 idempotencyRecord =
                        GatewayIdempotencyRecordV1.decode(idempotencyRecords.get(0).value());
                assertEquals(GatewayIdempotencyPhaseV1.QUIESCENT, idempotencyRecord.phase());
                assertEquals(1, idempotencyRecord.attempts().size());
                assertNotNull(idempotencyRecord.aggregateOutcomeBytes());
                assertEquals(2, auditRecords.size());
            }
        }
    }

    @Test
    void gatewayDurableRecordsRecoverAfterOxiaSessionChurn() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final Path serverCertificate = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_CERT");
        final Path serverPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_KEY");
        final Path trustedClientCertificates = requiredPath("NEREUS_DELAY_GATEWAY_CA_CERT");
        final Path clientCertificate = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_CERT");
        final Path clientPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_KEY");
        final int port = Integer.parseInt(requiredEnv("NEREUS_DELAY_GATEWAY_PORT"));
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("NEREUS_DELAY_GATEWAY_PORT must be 1..65535");
        }
        final Path restartGate = Path.of(requiredEnv("NEREUS_DELAY_GATEWAY_SESSION_CHURN_GATE"));
        final Path restartReady = Path.of(requiredEnv("NEREUS_DELAY_GATEWAY_SESSION_CHURN_READY"));
        final Path recoveryGate = Path.of(requiredEnv("NEREUS_DELAY_GATEWAY_SESSION_CHURN_RECOVERY_GATE"));
        final Path recoveryReady = Path.of(requiredEnv("NEREUS_DELAY_GATEWAY_SESSION_CHURN_RECOVERY_READY"));
        final Path stateDumpDirectory = optionalPath("NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN_STATE_DUMP_DIR");
        Files.deleteIfExists(restartGate);
        Files.deleteIfExists(restartReady);
        Files.deleteIfExists(recoveryGate);
        Files.deleteIfExists(recoveryReady);
        if (stateDumpDirectory != null) {
            Files.createDirectories(stateDumpDirectory);
            Files.deleteIfExists(stateDumpDirectory.resolve("before-oxia-restart.json"));
            Files.deleteIfExists(stateDumpDirectory.resolve("after-oxia-restart.json"));
        }

        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-gateway-session-churn/" + UUID.randomUUID();
        final MutableClock clock = new MutableClock(NOW_EPOCH_MS);
        final AuthenticatedTenantContext tenant = tenant(31);
        final KeyPair jwtKeyPair = rsaKeyPair();
        final byte[] certificateFingerprint = certificateFingerprint(clientCertificate);
        final RsaSha256GatewayJwtVerifier verifier = new RsaSha256GatewayJwtVerifier(
                jwtKeyPair.getPublic(), "nereus-delay-gateway-e2e-issuer", "nereus-delay-gateway-e2e",
                "gateway-e2e-key", Clock.fixed(Instant.ofEpochSecond(NOW_EPOCH_SECONDS), ZoneOffset.UTC),
                30, 600);
        final MutualTlsJwtGatewayTenantAuthority authority = new MutualTlsJwtGatewayTenantAuthority(verifier);
        final ScheduleIntentV1 intent = scheduleIntent();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, intent, 2_000_000);
        final PreparedSubmissionV1 prepared = PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command));
        final FixedCore core = new FixedCore(prepared);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final String token = token(jwtKeyPair, tenant, certificateFingerprint);
        final io.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1 request = request(intent);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle admissionClient = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, namespace, "nereus-delay-gateway-churn-admission-old-" + UUID.randomUUID(),
                Duration.ofSeconds(2), Duration.ofSeconds(1), prefix + "/admission-client");
             OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-churn-idempotency-old-" + UUID.randomUUID(),
                     Duration.ofSeconds(2), Duration.ofSeconds(1), prefix + "/idempotency-client");
             OxiaSyncOwnerLeaseBackend.ClientHandle auditClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-churn-audit-old-" + UUID.randomUUID(),
                     Duration.ofSeconds(2), Duration.ofSeconds(1), prefix + "/audit-client")) {
            final GatewayGrpcService oldGrpc = grpcService(admissionClient, idempotencyClient, auditClient, prefix,
                    clock, core, coordinator, authority);
            final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 firstResponse;
            try (GatewayGrpcServer oldServer = GatewayGrpcServer.mutualTls(port, serverCertificate, serverPrivateKey,
                    trustedClientCertificates, oldGrpc)) {
                oldServer.start();
                final ManagedChannel firstChannel = channel(port, trustedClientCertificates, clientCertificate,
                        clientPrivateKey);
                try {
                    firstResponse = stub(firstChannel, token).schedule(request);
                    assertTrue(firstResponse.hasSubmissionOutcomeNdr1());
                } finally {
                    firstChannel.shutdownNow();
                    assertTrue(firstChannel.awaitTermination(10, TimeUnit.SECONDS));
                }

                if (stateDumpDirectory != null) {
                    writeGatewaySessionChurnStateDump(stateDumpDirectory, "BEFORE_OXIA_RESTART", endpoint,
                            namespace, prefix, firstResponse.toByteArray(), core.prepareCalls,
                            coordinator.submitCalls, scanGatewayChurnRecords(admissionClient, idempotencyClient,
                                    auditClient, prefix), false, false);
                }

                Files.createFile(restartReady);
                awaitFile(restartGate);

                final GatewayAdmissionController staleAdmission = new OxiaGatewayAdmissionController(
                        admissionClient, prefix, clock,
                        new OxiaGatewayAdmissionController.Limits(2, 2_000_000, 1, 1, 10_000, 8));
                assertThrows(OxiaGatewaySessionUnavailableException.class,
                        () -> staleAdmission.reserve(new GatewayAdmissionRequestV1(
                                tenant, GatewayIngressOperationV1.SCHEDULE, 1)));
                final OxiaGatewayIdempotencyStore staleIdempotency = new OxiaGatewayIdempotencyStore(
                        idempotencyClient, prefix + "/idempotency", clock, 10_000, 10_000);
                assertThrows(OxiaGatewaySessionUnavailableException.class,
                        () -> staleIdempotency.exact(GatewayIdempotencyHashV1.keyHash(
                                tenant.authenticatedTenantScopeHash(), request.getIdempotencyKey().toByteArray())));

                final ManagedChannel staleChannel = channel(port, trustedClientCertificates, clientCertificate,
                        clientPrivateKey);
                try {
                    final StatusRuntimeException rejection = assertThrows(StatusRuntimeException.class,
                            () -> stub(staleChannel, token).withDeadlineAfter(5, TimeUnit.SECONDS)
                                    .schedule(request));
                    assertEquals(Status.Code.UNAVAILABLE, rejection.getStatus().getCode());
                } finally {
                    staleChannel.shutdownNow();
                    assertTrue(staleChannel.awaitTermination(10, TimeUnit.SECONDS));
                }

                Files.createFile(recoveryReady);
                awaitFile(recoveryGate);
            }

            try (OxiaSyncOwnerLeaseBackend.ClientHandle recoveredAdmissionClient = OxiaSyncOwnerLeaseBackend.connect(
                    endpoint, namespace, "nereus-delay-gateway-churn-admission-new-" + UUID.randomUUID(),
                    Duration.ofSeconds(15), prefix + "/recovered-admission-client");
                 OxiaSyncOwnerLeaseBackend.ClientHandle recoveredIdempotencyClient =
                         OxiaSyncOwnerLeaseBackend.connect(endpoint, namespace,
                                 "nereus-delay-gateway-churn-idempotency-new-" + UUID.randomUUID(),
                                 Duration.ofSeconds(15), prefix + "/recovered-idempotency-client");
                 OxiaSyncOwnerLeaseBackend.ClientHandle recoveredAuditClient = OxiaSyncOwnerLeaseBackend.connect(
                         endpoint, namespace, "nereus-delay-gateway-churn-audit-new-" + UUID.randomUUID(),
                         Duration.ofSeconds(15), prefix + "/recovered-audit-client")) {
                final GatewayGrpcService recoveredGrpc = grpcService(recoveredAdmissionClient,
                        recoveredIdempotencyClient, recoveredAuditClient, prefix, clock, core, coordinator,
                        authority);
                final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 recoveredResponse;
                try (GatewayGrpcServer recoveredServer = GatewayGrpcServer.mutualTls(port, serverCertificate,
                        serverPrivateKey, trustedClientCertificates, recoveredGrpc)) {
                    recoveredServer.start();
                    final ManagedChannel recoveredChannel = channel(port, trustedClientCertificates,
                            clientCertificate, clientPrivateKey);
                    try {
                        recoveredResponse = stub(recoveredChannel, token).schedule(request);
                        assertArrayEquals(firstResponse.toByteArray(), recoveredResponse.toByteArray());
                        assertEquals(1, core.prepareCalls);
                        assertEquals(1, coordinator.submitCalls);
                    } finally {
                        recoveredChannel.shutdownNow();
                        assertTrue(recoveredChannel.awaitTermination(10, TimeUnit.SECONDS));
                    }
                }

                try (var admissionScan = recoveredAdmissionClient.client().rangeScan(
                        prefix + "/admission/admission/", prefix + "/admission/admission/\uffff");
                     var idempotencyScan = recoveredIdempotencyClient.client().rangeScan(
                             prefix + "/idempotency/idempotency/", prefix + "/idempotency/idempotency/\uffff");
                     var auditScan = recoveredAuditClient.client().rangeScan(
                             prefix + "/audit/audit/", prefix + "/audit/audit/\uffff")) {
                    final List<io.oxia.client.api.GetResult> admissionRecords =
                            java.util.stream.StreamSupport.stream(admissionScan.spliterator(), false).toList();
                    final List<io.oxia.client.api.GetResult> idempotencyRecords =
                            java.util.stream.StreamSupport.stream(idempotencyScan.spliterator(), false).toList();
                    final List<io.oxia.client.api.GetResult> auditRecords =
                            java.util.stream.StreamSupport.stream(auditScan.spliterator(), false).toList();
                    assertEquals(1, admissionRecords.size());
                    assertEquals(0, GatewayAdmissionRecordV1.decode(admissionRecords.get(0).value()).leases().size());
                    assertEquals(1, idempotencyRecords.size());
                    final GatewayIdempotencyRecordV1 idempotencyRecord =
                            GatewayIdempotencyRecordV1.decode(idempotencyRecords.get(0).value());
                    assertEquals(GatewayIdempotencyPhaseV1.QUIESCENT, idempotencyRecord.phase());
                    assertEquals(1, idempotencyRecord.attempts().size());
                    assertNotNull(idempotencyRecord.aggregateOutcomeBytes());
                    assertEquals(2, auditRecords.size());
                }
                if (stateDumpDirectory != null) {
                    writeGatewaySessionChurnStateDump(stateDumpDirectory, "RECOVERED_AFTER_OXIA_RESTART", endpoint,
                            namespace, prefix, recoveredResponse.toByteArray(), core.prepareCalls,
                            coordinator.submitCalls, scanGatewayChurnRecords(recoveredAdmissionClient,
                                    recoveredIdempotencyClient, recoveredAuditClient, prefix), true, true);
                }
            }
            System.out.println("Gateway Oxia session churn E2E passed: stale admission/idempotency sessions failed "
                    + "closed and new sessions reread the exact durable outcome");
        }
    }

    @Test
    void gatewayRecoversAcrossRealOxiaDataServerFailover() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final Path serverCertificate = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_CERT");
        final Path serverPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_KEY");
        final Path trustedClientCertificates = requiredPath("NEREUS_DELAY_GATEWAY_CA_CERT");
        final Path clientCertificate = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_CERT");
        final Path clientPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_KEY");
        final int port = Integer.parseInt(requiredEnv("NEREUS_DELAY_GATEWAY_PORT"));
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("NEREUS_DELAY_GATEWAY_PORT must be 1..65535");
        }
        final Path failoverGate = Path.of(requiredEnv("NEREUS_DELAY_GATEWAY_MULTI_NODE_FAILOVER_GATE"));
        final Path failoverReady = Path.of(requiredEnv("NEREUS_DELAY_GATEWAY_MULTI_NODE_FAILOVER_READY"));
        Files.deleteIfExists(failoverGate);
        Files.deleteIfExists(failoverReady);

        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-gateway-multi-node-failover/" + UUID.randomUUID();
        final MutableClock clock = new MutableClock(NOW_EPOCH_MS);
        final AuthenticatedTenantContext tenant = tenant(37);
        final KeyPair jwtKeyPair = rsaKeyPair();
        final byte[] certificateFingerprint = certificateFingerprint(clientCertificate);
        final RsaSha256GatewayJwtVerifier verifier = new RsaSha256GatewayJwtVerifier(
                jwtKeyPair.getPublic(), "nereus-delay-gateway-e2e-issuer", "nereus-delay-gateway-e2e",
                "gateway-e2e-key", Clock.fixed(Instant.ofEpochSecond(NOW_EPOCH_SECONDS), ZoneOffset.UTC),
                30, 600);
        final MutualTlsJwtGatewayTenantAuthority authority = new MutualTlsJwtGatewayTenantAuthority(verifier);
        final ScheduleIntentV1 intent = scheduleIntent();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, intent, 2_000_000);
        final PreparedSubmissionV1 prepared = PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command));
        final FixedCore core = new FixedCore(prepared);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final String token = token(jwtKeyPair, tenant, certificateFingerprint);
        final io.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1 request = request(intent);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle admissionClient = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, namespace, "nereus-delay-gateway-multi-node-admission-" + UUID.randomUUID(),
                Duration.ofSeconds(30), prefix + "/admission-client");
             OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-multi-node-idempotency-" + UUID.randomUUID(),
                     Duration.ofSeconds(30), prefix + "/idempotency-client");
             OxiaSyncOwnerLeaseBackend.ClientHandle auditClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-multi-node-audit-" + UUID.randomUUID(),
                     Duration.ofSeconds(30), prefix + "/audit-client")) {
            final GatewayGrpcService grpc = grpcService(admissionClient, idempotencyClient, auditClient, prefix,
                    clock, core, coordinator, authority);
            final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 firstResponse;
            try (GatewayGrpcServer server = GatewayGrpcServer.mutualTls(port, serverCertificate, serverPrivateKey,
                    trustedClientCertificates, grpc)) {
                server.start();
                final ManagedChannel firstChannel = channel(port, trustedClientCertificates, clientCertificate,
                        clientPrivateKey);
                try {
                    firstResponse = stub(firstChannel, token).schedule(request);
                    assertTrue(firstResponse.hasSubmissionOutcomeNdr1());
                } finally {
                    firstChannel.shutdownNow();
                    assertTrue(firstChannel.awaitTermination(10, TimeUnit.SECONDS));
                }

                Files.createFile(failoverReady);
                awaitFile(failoverGate);
                admissionClient.backend().assertConnectedSession();
                idempotencyClient.backend().assertConnectedSession();
                auditClient.backend().assertConnectedSession();

                final ManagedChannel recoveredChannel = channel(port, trustedClientCertificates, clientCertificate,
                        clientPrivateKey);
                try {
                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 recoveredResponse =
                            stub(recoveredChannel, token).withDeadlineAfter(45, TimeUnit.SECONDS).schedule(request);
                    assertArrayEquals(firstResponse.toByteArray(), recoveredResponse.toByteArray());
                    assertEquals(1, core.prepareCalls);
                    assertEquals(1, coordinator.submitCalls);
                } finally {
                    recoveredChannel.shutdownNow();
                    assertTrue(recoveredChannel.awaitTermination(10, TimeUnit.SECONDS));
                }
            }

            try (var admissionScan = admissionClient.client().rangeScan(
                    prefix + "/admission/admission/", prefix + "/admission/admission/\uffff");
                 var idempotencyScan = idempotencyClient.client().rangeScan(
                         prefix + "/idempotency/idempotency/", prefix + "/idempotency/idempotency/\uffff");
                 var auditScan = auditClient.client().rangeScan(
                         prefix + "/audit/audit/", prefix + "/audit/audit/\uffff")) {
                final List<io.oxia.client.api.GetResult> admissionRecords =
                        java.util.stream.StreamSupport.stream(admissionScan.spliterator(), false).toList();
                final List<io.oxia.client.api.GetResult> idempotencyRecords =
                        java.util.stream.StreamSupport.stream(idempotencyScan.spliterator(), false).toList();
                final List<io.oxia.client.api.GetResult> auditRecords =
                        java.util.stream.StreamSupport.stream(auditScan.spliterator(), false).toList();
                assertEquals(1, admissionRecords.size());
                assertEquals(0, GatewayAdmissionRecordV1.decode(admissionRecords.get(0).value()).leases().size());
                assertEquals(1, idempotencyRecords.size());
                final GatewayIdempotencyRecordV1 idempotencyRecord =
                        GatewayIdempotencyRecordV1.decode(idempotencyRecords.get(0).value());
                assertEquals(GatewayIdempotencyPhaseV1.QUIESCENT, idempotencyRecord.phase());
                assertEquals(1, idempotencyRecord.attempts().size());
                assertNotNull(idempotencyRecord.aggregateOutcomeBytes());
                assertEquals(2, auditRecords.size());
            }
            System.out.println("Gateway multi-node Oxia failover E2E passed: session-bound clients preserved the "
                    + "exact durable outcome after the shard leader stopped");
        }
    }

    @Test
    void concurrentDuplicateRequestsAcrossTwoGatewayServersUseOneDurableAttempt() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final Path serverCertificate = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_CERT");
        final Path serverPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_KEY");
        final Path trustedClientCertificates = requiredPath("NEREUS_DELAY_GATEWAY_CA_CERT");
        final Path clientCertificate = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_CERT");
        final Path clientPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_KEY");
        final int port = Integer.parseInt(requiredEnv("NEREUS_DELAY_GATEWAY_PORT"));
        if (port <= 0 || port >= 65_534) {
            throw new IllegalArgumentException("NEREUS_DELAY_GATEWAY_PORT must leave one successor port");
        }

        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-gateway-race/" + UUID.randomUUID();
        final MutableClock clock = new MutableClock(NOW_EPOCH_MS);
        final AuthenticatedTenantContext tenant = tenant(17);
        final KeyPair jwtKeyPair = rsaKeyPair();
        final byte[] certificateFingerprint = certificateFingerprint(clientCertificate);
        final RsaSha256GatewayJwtVerifier verifier = new RsaSha256GatewayJwtVerifier(
                jwtKeyPair.getPublic(), "nereus-delay-gateway-e2e-issuer", "nereus-delay-gateway-e2e",
                "gateway-e2e-key", Clock.fixed(Instant.ofEpochSecond(NOW_EPOCH_SECONDS), ZoneOffset.UTC),
                30, 600);
        final MutualTlsJwtGatewayTenantAuthority authority = new MutualTlsJwtGatewayTenantAuthority(verifier);
        final ScheduleIntentV1 intent = scheduleIntent();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, intent, 2_000_000);
        final PreparedSubmissionV1 prepared = PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command));
        final FixedCore firstCore = new FixedCore(prepared);
        final FixedCore secondCore = new FixedCore(prepared);
        final CountingCoordinator firstCoordinator = new CountingCoordinator(command);
        final CountingCoordinator secondCoordinator = new CountingCoordinator(command);
        final String token = token(jwtKeyPair, tenant, certificateFingerprint);
        final io.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1 request = request(intent);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle firstAdmissionClient = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, namespace, "nereus-delay-gateway-race-admission-1-" + UUID.randomUUID(),
                Duration.ofSeconds(15), prefix + "/admission-client-1");
             OxiaSyncOwnerLeaseBackend.ClientHandle firstIdempotencyClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-race-idempotency-1-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix + "/idempotency-client-1");
             OxiaSyncOwnerLeaseBackend.ClientHandle firstAuditClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-race-audit-1-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix + "/audit-client-1");
             OxiaSyncOwnerLeaseBackend.ClientHandle secondAdmissionClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-race-admission-2-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix + "/admission-client-2");
             OxiaSyncOwnerLeaseBackend.ClientHandle secondIdempotencyClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-race-idempotency-2-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix + "/idempotency-client-2");
             OxiaSyncOwnerLeaseBackend.ClientHandle secondAuditClient = OxiaSyncOwnerLeaseBackend.connect(
                     endpoint, namespace, "nereus-delay-gateway-race-audit-2-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix + "/audit-client-2")) {
            final GatewayGrpcService firstGrpc = grpcService(firstAdmissionClient, firstIdempotencyClient,
                    firstAuditClient, prefix, clock, firstCore, firstCoordinator, authority);
            final GatewayGrpcService secondGrpc = grpcService(secondAdmissionClient, secondIdempotencyClient,
                    secondAuditClient, prefix, clock, secondCore, secondCoordinator, authority);
            try (GatewayGrpcServer firstServer = GatewayGrpcServer.mutualTls(port, serverCertificate, serverPrivateKey,
                    trustedClientCertificates, firstGrpc);
                 GatewayGrpcServer secondServer = GatewayGrpcServer.mutualTls(port + 1, serverCertificate,
                         serverPrivateKey, trustedClientCertificates, secondGrpc)) {
                firstServer.start();
                secondServer.start();
                final ManagedChannel firstChannel = channel(port, trustedClientCertificates, clientCertificate,
                        clientPrivateKey);
                final ManagedChannel secondChannel = channel(port + 1, trustedClientCertificates, clientCertificate,
                        clientPrivateKey);
                final ExecutorService executor = Executors.newFixedThreadPool(2);
                try {
                    final CompletableFuture<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1> firstCall =
                            CompletableFuture.supplyAsync(() -> stub(firstChannel, token).schedule(request), executor);
                    final CompletableFuture<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1> secondCall =
                            CompletableFuture.supplyAsync(() -> stub(secondChannel, token).schedule(request), executor);
                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 firstOutcome = firstCall.join();
                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 secondOutcome = secondCall.join();
                    assertTrue(firstOutcome.hasSubmissionOutcomeNdr1());
                    assertTrue(secondOutcome.hasSubmissionOutcomeNdr1());
                    assertEquals(1, firstCoordinator.submitCalls + secondCoordinator.submitCalls);

                    final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 settled =
                            stub(firstChannel, token).schedule(request);
                    assertTrue(settled.hasSubmissionOutcomeNdr1());
                    assertEquals(1, firstCoordinator.submitCalls + secondCoordinator.submitCalls);
                } finally {
                    executor.shutdownNow();
                    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
                    firstChannel.shutdownNow();
                    secondChannel.shutdownNow();
                    assertTrue(firstChannel.awaitTermination(10, TimeUnit.SECONDS));
                    assertTrue(secondChannel.awaitTermination(10, TimeUnit.SECONDS));
                }
            }

            try (var admissionScan = firstAdmissionClient.client().rangeScan(
                    prefix + "/admission/admission/", prefix + "/admission/admission/\uffff");
                 var idempotencyScan = firstIdempotencyClient.client().rangeScan(
                         prefix + "/idempotency/idempotency/", prefix + "/idempotency/idempotency/\uffff");
                 var auditScan = firstAuditClient.client().rangeScan(
                         prefix + "/audit/audit/", prefix + "/audit/audit/\uffff")) {
                final List<io.oxia.client.api.GetResult> admissionRecords =
                        java.util.stream.StreamSupport.stream(admissionScan.spliterator(), false).toList();
                final List<io.oxia.client.api.GetResult> idempotencyRecords =
                        java.util.stream.StreamSupport.stream(idempotencyScan.spliterator(), false).toList();
                final List<io.oxia.client.api.GetResult> auditRecords =
                        java.util.stream.StreamSupport.stream(auditScan.spliterator(), false).toList();
                assertEquals(1, admissionRecords.size());
                assertEquals(0, GatewayAdmissionRecordV1.decode(admissionRecords.get(0).value()).leases().size());
                assertEquals(1, idempotencyRecords.size());
                final GatewayIdempotencyRecordV1 idempotencyRecord =
                        GatewayIdempotencyRecordV1.decode(idempotencyRecords.get(0).value());
                assertEquals(1, idempotencyRecord.attempts().size());
                assertNotNull(idempotencyRecord.aggregateOutcomeBytes());
                assertTrue(auditRecords.size() >= 2 && auditRecords.size() <= 3);
            }
        }
    }

    private static GatewayGrpcService grpcService(final OxiaSyncOwnerLeaseBackend.ClientHandle admissionClient,
                                                   final OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyClient,
                                                   final OxiaSyncOwnerLeaseBackend.ClientHandle auditClient,
                                                   final String prefix, final MutableClock clock,
                                                   final FixedCore core, final CountingCoordinator coordinator,
                                                   final GatewayTenantAuthority authority) {
        return grpcService(new SessionBoundOxiaGatewayRecordClient(admissionClient),
                new SessionBoundOxiaGatewayRecordClient(idempotencyClient),
                new SessionBoundOxiaGatewayRecordClient(auditClient), prefix, clock, core, coordinator, authority);
    }

    private static GatewayGrpcService grpcService(final OxiaGatewayRecordClient admissionClient,
                                                   final OxiaGatewayRecordClient idempotencyClient,
                                                   final OxiaGatewayRecordClient auditClient,
                                                   final String prefix, final MutableClock clock,
                                                   final FixedCore core, final CountingCoordinator coordinator,
                                                   final GatewayTenantAuthority authority) {
        final OxiaGatewayAdmissionController admission = new OxiaGatewayAdmissionController(
                admissionClient, prefix + "/admission", clock,
                new OxiaGatewayAdmissionController.Limits(2, 2_000_000, 1, 1, 10_000, 8));
        final OxiaGatewayIdempotencyStore idempotency = new OxiaGatewayIdempotencyStore(
                idempotencyClient, prefix + "/idempotency", clock, 10_000, 10_000);
        final OxiaGatewayAuditSink audit = new OxiaGatewayAuditSink(auditClient, prefix + "/audit");
        final GatewayScheduleService schedule = new GatewayScheduleService(core, idempotency, coordinator, clock);
        final GatewayIngressService ingress = new GatewayIngressService(schedule, authority, admission, audit, clock);
        return new GatewayGrpcService(ingress, GatewayGrpcContext.provider());
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

    private static io.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1 request(
            final ScheduleIntentV1 intent) {
        return io.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1.newBuilder()
                .setIdempotencyKey(ByteString.copyFrom(bytes(16, 40)))
                .setRoute(GatewayRouteSelectorV1.newBuilder()
                        .setIngressAdapterKind(AdapterKindV1.KAFKA.wireValue())
                        .setRouteAliasUtf8Nfc(ByteString.copyFromUtf8("primary")))
                .setScheduleIntentV1(ByteString.copyFrom(intent.canonicalBytes()))
                .setRetryUntilEpochMs(2_000_000)
                .setSubmissionModeV1(SubmissionModeV1.MANAGED.wireValue())
                .build();
    }

    private static ScheduleIntentV1 scheduleIntent() {
        return ScheduleIntentV1.create(new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 60),
                        ProfileKindV1.DESTINATION), new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 61)),
                1_300, 1_800, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, Bytes.utf8("key"),
                Bytes.utf8("payload"), null, AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())),
                null, null);
    }

    private static AuthenticatedTenantContext tenant(final int seed) {
        return new AuthenticatedTenantContext(bytes(32, seed), bytes(32, seed + 1), bytes(32, seed + 2));
    }

    private static String token(final KeyPair keyPair, final AuthenticatedTenantContext tenant,
                                final byte[] certificateFingerprint) throws GeneralSecurityException {
        final String header = "{\"alg\":\"RS256\",\"typ\":\"JWT\",\"kid\":\"gateway-e2e-key\"}";
        final String claims = "{"
                + "\"iss\":\"nereus-delay-gateway-e2e-issuer\","
                + "\"aud\":\"nereus-delay-gateway-e2e\","
                + "\"sub\":\"gateway-e2e-client\","
                + "\"tenant\":\"tenant-e2e\","
                + "\"tenant_scope_hash\":\"" + encode(tenant.authenticatedTenantScopeHash()) + "\","
                + "\"tenant_routing_scope\":\"" + encode(tenant.tenantRoutingScope()) + "\","
                + "\"iat\":" + (NOW_EPOCH_SECONDS - 100) + ","
                + "\"nbf\":" + (NOW_EPOCH_SECONDS - 100) + ","
                + "\"exp\":" + (NOW_EPOCH_SECONDS + 200) + ","
                + "\"jti\":\"gateway-e2e-jwt\","
                + "\"cnf\":{\"x5t#S256\":\"" + encode(certificateFingerprint) + "\"}"
                + "}";
        final String encodedHeader = encode(header.getBytes(StandardCharsets.UTF_8));
        final String encodedClaims = encode(claims.getBytes(StandardCharsets.UTF_8));
        final String input = encodedHeader + "." + encodedClaims;
        final Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(keyPair.getPrivate());
        signature.update(input.getBytes(StandardCharsets.US_ASCII));
        return input + "." + encode(signature.sign());
    }

    private static String mutateSignature(final String token) {
        final int last = token.length() - 1;
        final char original = token.charAt(last);
        return token.substring(0, last) + (original == 'A' ? 'B' : 'A');
    }

    private static KeyPair rsaKeyPair() throws GeneralSecurityException {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2_048);
        return generator.generateKeyPair();
    }

    private static byte[] certificateFingerprint(final Path certificate) throws Exception {
        final CertificateFactory factory = CertificateFactory.getInstance("X.509");
        final X509Certificate parsed;
        try (InputStream input = Files.newInputStream(certificate)) {
            parsed = (X509Certificate) factory.generateCertificate(input);
        }
        return Bytes.sha256(parsed.getEncoded());
    }

    private static Path requiredPath(final String name) {
        final Path path = Path.of(requiredEnv(name));
        Assumptions.assumeTrue(Files.isRegularFile(path), name + " is not a regular file: " + path);
        return path;
    }

    private static Path optionalPath(final String name) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? null : Path.of(value);
    }

    private static GatewayChurnDurableSnapshot scanGatewayChurnRecords(
            final OxiaSyncOwnerLeaseBackend.ClientHandle admissionClient,
            final OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyClient,
            final OxiaSyncOwnerLeaseBackend.ClientHandle auditClient,
            final String prefix) throws Exception {
        try (var admissionScan = admissionClient.client().rangeScan(
                prefix + "/admission/admission/", prefix + "/admission/admission/\uffff");
             var idempotencyScan = idempotencyClient.client().rangeScan(
                     prefix + "/idempotency/idempotency/", prefix + "/idempotency/idempotency/\uffff");
             var auditScan = auditClient.client().rangeScan(
                     prefix + "/audit/audit/", prefix + "/audit/audit/\uffff")) {
            final List<GetResult> admissionRecords =
                    java.util.stream.StreamSupport.stream(admissionScan.spliterator(), false).toList();
            final List<GetResult> idempotencyRecords =
                    java.util.stream.StreamSupport.stream(idempotencyScan.spliterator(), false).toList();
            final List<GetResult> auditRecords =
                    java.util.stream.StreamSupport.stream(auditScan.spliterator(), false).toList();
            int activeLeases = 0;
            if (admissionRecords.size() == 1) {
                activeLeases = GatewayAdmissionRecordV1.decode(admissionRecords.get(0).value()).leases().size();
            }
            String idempotencyPhase = null;
            int idempotencyAttempts = 0;
            boolean aggregateOutcomePresent = false;
            if (idempotencyRecords.size() == 1) {
                final GatewayIdempotencyRecordV1 idempotencyRecord =
                        GatewayIdempotencyRecordV1.decode(idempotencyRecords.get(0).value());
                idempotencyPhase = idempotencyRecord.phase().name();
                idempotencyAttempts = idempotencyRecord.attempts().size();
                aggregateOutcomePresent = idempotencyRecord.aggregateOutcomeBytes() != null;
            }
            return new GatewayChurnDurableSnapshot(admissionRecords.size(), activeLeases,
                    idempotencyRecords.size(), idempotencyPhase, idempotencyAttempts,
                    aggregateOutcomePresent, auditRecords.size());
        }
    }

    private static void writeGatewaySessionChurnStateDump(final Path directory, final String phase,
                                                           final String endpoint, final String namespace,
                                                           final String prefix, final byte[] response,
                                                           final int prepareCalls, final int submitCalls,
                                                           final GatewayChurnDurableSnapshot snapshot,
                                                           final boolean staleSessionFailedClosed,
                                                           final boolean exactOutcomeRecovered) throws Exception {
        Files.createDirectories(directory);
        final Path target = directory.resolve("BEFORE_OXIA_RESTART".equals(phase)
                ? "before-oxia-restart.json" : "after-oxia-restart.json");
        final String json = "{\n"
                + "  \"schema\": \"nereus-delay-chaos-durable-state-dump-v1\",\n"
                + "  \"cell\": \"gateway-oxia-session-churn\",\n"
                + "  \"phase\": " + jsonString(phase) + ",\n"
                + "  \"process_pid\": " + ProcessHandle.current().pid() + ",\n"
                + "  \"oxia_endpoint\": " + jsonString(endpoint) + ",\n"
                + "  \"oxia_namespace\": " + jsonString(namespace) + ",\n"
                + "  \"prefix\": " + jsonString(prefix) + ",\n"
                + "  \"response_sha256\": " + jsonString(Bytes.hex(Bytes.sha256(response))) + ",\n"
                + "  \"response_bytes\": " + response.length + ",\n"
                + "  \"prepare_calls\": " + prepareCalls + ",\n"
                + "  \"submit_calls\": " + submitCalls + ",\n"
                + "  \"admission_records\": " + snapshot.admissionRecords() + ",\n"
                + "  \"admission_active_leases\": " + snapshot.activeLeases() + ",\n"
                + "  \"idempotency_records\": " + snapshot.idempotencyRecords() + ",\n"
                + "  \"idempotency_phase\": " + jsonNullable(snapshot.idempotencyPhase()) + ",\n"
                + "  \"idempotency_attempts\": " + snapshot.idempotencyAttempts() + ",\n"
                + "  \"aggregate_outcome_present\": " + snapshot.aggregateOutcomePresent() + ",\n"
                + "  \"audit_records\": " + snapshot.auditRecords() + ",\n"
                + "  \"stale_session_failed_closed\": " + staleSessionFailedClosed + ",\n"
                + "  \"exact_outcome_recovered\": " + exactOutcomeRecovered + ",\n"
                + "  \"oxia_process_restarted\": " + !"BEFORE_OXIA_RESTART".equals(phase) + ",\n"
                + "  \"durable_store_read\": true,\n"
                + "  \"dump_forced\": true\n"
                + "}\n";
        final ByteBuffer bytes = StandardCharsets.UTF_8.encode(json);
        try (FileChannel channel = FileChannel.open(target, java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.TRUNCATE_EXISTING, java.nio.file.StandardOpenOption.WRITE)) {
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
            channel.force(true);
        }
    }

    private static String jsonString(final String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String jsonNullable(final String value) {
        return value == null ? "null" : jsonString(value);
    }

    private record GatewayChurnDurableSnapshot(int admissionRecords, int activeLeases,
                                                int idempotencyRecords, String idempotencyPhase,
                                                int idempotencyAttempts, boolean aggregateOutcomePresent,
                                                int auditRecords) {
    }

    private static String requiredEnv(final String name) {
        final String value = System.getenv(name);
        Assumptions.assumeTrue(value != null && !value.isBlank(), name + " is not configured");
        return value;
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void awaitFile(final Path path) throws Exception {
        for (int attempt = 0; attempt < 120; attempt++) {
            if (Files.isRegularFile(path)) {
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Gateway Oxia session churn gate was not released: " + path);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static String encode(final byte[] value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
    }

    private static final class MutableClock implements TrustedClock {
        private long value;

        private MutableClock(final long value) {
            this.value = value;
        }

        @Override
        public long nowEpochMs() {
            return value;
        }

        private void set(final long next) {
            value = next;
        }
    }

    private static final class ResponseLossOnceRecordClient implements OxiaGatewayRecordClient {
        private final OxiaGatewayRecordClient delegate;
        private final Set<Integer> loseOnPutNumbers;
        private int putCalls;

        private ResponseLossOnceRecordClient(final OxiaGatewayRecordClient delegate, final int loseOnPutNumber) {
            this(delegate, Set.of(loseOnPutNumber));
        }

        private ResponseLossOnceRecordClient(final OxiaGatewayRecordClient delegate,
                                             final Set<Integer> loseOnPutNumbers) {
            this.delegate = delegate;
            this.loseOnPutNumbers = Set.copyOf(loseOnPutNumbers);
            if (this.loseOnPutNumbers.isEmpty() || this.loseOnPutNumbers.stream().anyMatch(number -> number <= 0)) {
                throw new IllegalArgumentException("loseOnPutNumbers must contain positive values");
            }
        }

        @Override
        public GetResult get(final String key) {
            return delegate.get(key);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            final int call = ++putCalls;
            final PutResult result = delegate.put(key, value, options);
            if (loseOnPutNumbers.contains(call)) {
                throw new IllegalStateException("simulated committed Oxia response loss");
            }
            return result;
        }

        @Override
        public void close() {
        }
    }

    private static final class FixedCore implements DelaySemanticCore {
        private final PreparedSubmissionV1 prepared;
        private int prepareCalls;

        private FixedCore(final PreparedSubmissionV1 prepared) {
            this.prepared = prepared;
        }

        @Override
        public PreparedSubmissionV1 prepareSchedule(final AuthenticatedTenantContext tenant,
                                                     final RouteSelectionHint route, final ScheduleIntentV1 intent,
                                                     final long retryUntilEpochMs,
                                                     final SubmissionModeV1 submissionMode) {
            prepareCalls++;
            return prepared;
        }

        @Override
        public PreparedCommand prepareLargeSchedule(final AuthenticatedTenantContext tenant,
                                                     final RouteSelectionHint route,
                                                     final LargeSchedulePreparationV1 request,
                                                     final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand preparePayloadCommit(final AuthenticatedTenantContext tenant,
                                                    final PayloadReservationReceiptV1 reservation,
                                                    final io.nereusstream.delay.protocol.PayloadCommitProofV1 proof,
                                                    final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareCancel(final AuthenticatedTenantContext tenant, final DelayMessageId messageId,
                                             final MessagePreconditionV1 precondition,
                                             final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareReschedule(final AuthenticatedTenantContext tenant,
                                                 final DelayMessageId messageId,
                                                 final MessagePreconditionV1 precondition,
                                                 final long deliverAtEpochMs, final long expireAtEpochMs,
                                                 final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedSubmissionV1 prepareManaged(final AuthenticatedTenantContext tenant,
                                                   final PreparedCommand command) {
            return prepared;
        }
    }

    private static final class CountingCoordinator implements SubmissionCoordinator {
        private final PreparedCommand command;
        private int submitCalls;

        private CountingCoordinator(final PreparedCommand command) {
            this.command = command;
        }

        @Override
        public CompletionStage<SubmissionOutcomeMessageV1> submit(final AuthenticatedTenantContext tenant,
                                                                    final PreparedSubmissionV1 submission,
                                                                    final TransportOwnershipPermit permit) {
            submitCalls++;
            return CompletableFuture.completedFuture(SubmissionOutcomeMessageV1.managed(
                    WireIngressOutcomeSupport.localDefinite(command, StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED)));
        }
    }
}
