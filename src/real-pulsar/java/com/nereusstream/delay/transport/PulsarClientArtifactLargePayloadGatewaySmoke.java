package com.nereusstream.delay.transport;

import com.google.protobuf.ByteString;
import com.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import com.nereusstream.delay.adapter.PulsarSendRequest;
import com.nereusstream.delay.adapter.PulsarSendResult;
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
import com.nereusstream.delay.ownership.PulsarSourceReactivation;
import com.nereusstream.delay.ownership.PulsarSourceReactivationCoordinator;
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
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.CompatibleControlSnapshot;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.EnqueueOutcomeKind;
import com.nereusstream.delay.protocol.IngressCredentialBindingRef;
import com.nereusstream.delay.protocol.IngressRouteResource;
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
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarIngressRouteResource;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPhysicalPartitionIdentity;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
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
import com.nereusstream.delay.protocol.SubmissionOutcomeKind;
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
import com.nereusstream.delay.submission.PulsarManagedSubmissionOutcomeProjector;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TopicResourceGuard;

/** Real Pulsar Gateway/Oxia/Worker/MinIO large-payload authority smoke. */
public final class PulsarClientArtifactLargePayloadGatewaySmoke {
    private static final String CLUSTER = PulsarClientArtifactClientBuilder.clusterId();
    private static final byte[] SOURCE_INCARNATION = digest(43);
    private static final long SOURCE_CREATION_TIMESTAMP = 2001L;
    private static final byte[] DESTINATION_INCARNATION = digest(17);
    private static final long DESTINATION_CREATION_TIMESTAMP = 1001L;
    private static final long LEASE_DURATION_MS = 60_000;
    private static final long PAYLOAD_BYTES = (1L << 20) + 4_096;
    private static final long WORK_CLASS_BYTES = 2_000_000;
    private static final Duration RECEIVE_TIMEOUT = Duration.ofMillis(250);

    private PulsarClientArtifactLargePayloadGatewaySmoke() {}

    static RouteSnapshot routeSnapshot(
            final String physicalTopicBase,
            final String physicalTopic,
            final RouteIncarnation incarnation,
            final PulsarSourcePosition position,
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof,
            final AuthenticatedTenantContext tenant,
            final KeyPair signingKeys) {
        final long now = System.currentTimeMillis();
        final BrokerResourceIdentity broker = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                CLUSTER, SOURCE_INCARNATION, physicalTopic, SOURCE_CREATION_TIMESTAMP));
        final ActivationBarrier barrier = ActivationBarrier.pulsar(
                broker,
                0,
                position.ledgerId(),
                position.entryId(),
                position.normalizedBatchIndex(),
                position.batchSize(),
                proof.connectionGeneration(),
                proof.attestationDigest());
        final RoutePartitionPolicy policy = new RoutePartitionPolicy(
                0, barrier, zeroQuota(), proof.connectionGeneration(), proof.attestationDigest());
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                now - 100,
                now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("pulsar-large-route-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("pulsar-large-route-issued-at")),
                0,
                null);
        final IngressRouteResource ingress = new PulsarIngressRouteResource(
                CLUSTER,
                physicalTopicBase,
                List.of(new PulsarPhysicalPartitionIdentity(
                        0, physicalTopic, SOURCE_INCARNATION, SOURCE_CREATION_TIMESTAMP)));
        return RouteSnapshot.create(
                incarnation,
                tenant.authenticatedTenantScopeHash(),
                tenant.tenantRoutingScope(),
                RouteLifecycle.ACTIVE_FOR_NEW,
                now + 30_000,
                ingress,
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
                Bytes.sha256(Bytes.utf8("pulsar-large-route-prerequisite")),
                issuedAt,
                1,
                signingKeys.getPrivate());
    }

    private static RouteSnapshot multiRouteSnapshot(
            final String physicalTopicBase,
            final RouteIncarnation incarnation,
            final List<LargeShardProbe> probes,
            final KeyPair signingKeys) {
        final long now = System.currentTimeMillis();
        final List<PulsarPhysicalPartitionIdentity> physicalPartitions = probes.stream()
                .map(probe -> new PulsarPhysicalPartitionIdentity(
                        probe.shard().partition(),
                        probe.physicalTopic(),
                        SOURCE_INCARNATION,
                        SOURCE_CREATION_TIMESTAMP))
                .toList();
        final List<RoutePartitionPolicy> policies = probes.stream()
                .map(probe -> {
                    final BrokerResourceIdentity broker =
                            BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                                    CLUSTER, SOURCE_INCARNATION, probe.physicalTopic(), SOURCE_CREATION_TIMESTAMP));
                    final PulsarSourcePosition position = probe.beforeRoutePosition();
                    final var proof = probe.proof();
                    final ActivationBarrier barrier = ActivationBarrier.pulsar(
                            broker,
                            probe.shard().partition(),
                            position.ledgerId(),
                            position.entryId(),
                            position.normalizedBatchIndex(),
                            position.batchSize(),
                            proof.connectionGeneration(),
                            proof.attestationDigest());
                    return new RoutePartitionPolicy(
                            probe.shard().partition(),
                            barrier,
                            zeroQuota(),
                            proof.connectionGeneration(),
                            proof.attestationDigest());
                })
                .toList();
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                now - 100,
                now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("pulsar-large-multi-route-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("pulsar-large-multi-route-issued-at")),
                0,
                null);
        return RouteSnapshot.create(
                incarnation,
                bytes(32, 1),
                bytes(32, 2),
                RouteLifecycle.ACTIVE_FOR_NEW,
                now + 30_000,
                new PulsarIngressRouteResource(CLUSTER, physicalTopicBase, physicalPartitions),
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
                Bytes.sha256(Bytes.utf8("pulsar-large-multi-route-prerequisite")),
                issuedAt,
                1,
                signingKeys.getPrivate());
    }

    static RouteWorkerAssignmentCoordinator.PlacementRequest placementRequest(final long now) {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(
                0,
                Bytes.sha256(Bytes.utf8("pulsar-large-worker-assignment")),
                1,
                Bytes.sha256(Bytes.utf8("pulsar-large-worker-capacity")),
                1,
                List.of(new WorkerPlacementPolicy.WorkerCandidate(
                        "pulsar-large-payload-worker",
                        capacity(2),
                        com.nereusstream.delay.protocol.CapacityVector.empty(),
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
                com.nereusstream.delay.protocol.CapacityVector.empty(),
                com.nereusstream.delay.protocol.CapacityVector.empty(),
                null,
                now,
                0,
                0);
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest placementRequest(
            final long now, final int partition, final String workerId) {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(
                partition,
                Bytes.sha256(Bytes.utf8("pulsar-large-multi-worker-assignment-" + partition + "-" + workerId)),
                1,
                Bytes.sha256(Bytes.utf8("pulsar-large-multi-worker-capacity-" + partition + "-" + workerId)),
                1,
                List.of(new WorkerPlacementPolicy.WorkerCandidate(
                        workerId,
                        capacity(2),
                        com.nereusstream.delay.protocol.CapacityVector.empty(),
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
                com.nereusstream.delay.protocol.CapacityVector.empty(),
                com.nereusstream.delay.protocol.CapacityVector.empty(),
                null,
                now,
                0,
                0);
    }

    private static byte[] orderingKeyForPartition(
            final RouteSnapshot snapshot, final AuthenticatedTenantContext tenant, final int partition) {
        for (int attempt = 0; attempt < 100_000; attempt++) {
            final byte[] candidate = Bytes.utf8("pulsar-large-multi-ordering-key-" + attempt);
            if (RouteHash.partition(snapshot, tenant.tenantRoutingScope(), candidate) == partition) {
                return candidate;
            }
        }
        throw new IllegalStateException("could not find deterministic ordering key for Pulsar partition " + partition);
    }

    static void requireRouteAssignment(
            final WorkerAssignment assignment,
            final RouteSnapshot snapshot,
            final PulsarSourcePosition barrierPosition,
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof) {
        if (!assignment.routeBound()
                || !Arrays.equals(snapshot.snapshotDigest(), assignment.routeSnapshotDigest())
                || !(assignment.sourceAssignment().activationBarrier() instanceof PulsarActivationBarrier barrier)
                || !Arrays.equals(barrier.brokerResourceIncarnation(), SOURCE_INCARNATION)
                || !barrier.physicalTopic().equals(barrierPosition.physicalTopic())
                || barrier.ledgerId() != barrierPosition.ledgerId()
                || barrier.entryId() != barrierPosition.entryId()
                || barrier.normalizedLastBatchIndex() != barrierPosition.normalizedBatchIndex()
                || barrier.batchSize() != barrierPosition.batchSize()
                || barrier.guardedSourceConnectionGeneration() != proof.connectionGeneration()
                || !Arrays.equals(barrier.resourceGuardAttestationDigest(), proof.attestationDigest())) {
            throw new IllegalStateException("Oxia Pulsar assignment did not retain the signed source barrier");
        }
    }

    static CompatibleControlSnapshot controlSnapshot(final ShardId shard, final ProfileRef destinationProfile) {
        return new CompatibleControlSnapshot(
                new ShardSubject(shard),
                List.of(new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1)),
                List.of(destinationProfile),
                zeroQuota());
    }

    static QuotaGrantRef zeroQuota() {
        return new QuotaGrantRef(
                bytes(32, 50),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    }

    static CapacityVector capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[CapacityDimension.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVector(values);
    }

    static WorkClassExecutionRegistry workClasses() {
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
                            WORK_CLASS_BYTES,
                            1,
                            WORK_CLASS_BYTES,
                            WORK_CLASS_BYTES,
                            protectedClass ? 1 : 0,
                            protectedClass ? 1 : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(
                new WorkClassRuntimeConfig(
                        policies, TimeUnit.SECONDS.toNanos(5), TimeUnit.SECONDS.toNanos(30), 16, 8_000_000),
                System::nanoTime);
    }

    static PulsarSourcePosition sendFrameAndPosition(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final ShardId shard,
            final byte[] frame,
            final String producerName)
            throws Exception {
        return sendFrameAndPosition(client, guard, physicalTopic, shard, frame, producerName, 0);
    }

    static PulsarSourcePosition sendFrameAndPosition(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final ShardId shard,
            final byte[] frame,
            final String producerName,
            final int partition)
            throws Exception {
        final PulsarClientArtifactSendTransport transport = new PulsarClientArtifactSendTransport(
                PulsarClientArtifactProducerFactory.create(
                        client, CLUSTER, SOURCE_INCARNATION, physicalTopic, SOURCE_CREATION_TIMESTAMP, producerName),
                CLUSTER,
                SOURCE_INCARNATION,
                physicalTopic,
                SOURCE_CREATION_TIMESTAMP,
                partition);
        try {
            final PulsarSendResult result = transport
                    .send(new PulsarSendRequest(
                            CLUSTER,
                            SOURCE_INCARNATION,
                            physicalTopic,
                            SOURCE_CREATION_TIMESTAMP,
                            partition,
                            CommandId.random(shard),
                            frame))
                    .toCompletableFuture()
                    .get(15, TimeUnit.SECONDS);
            if (result.disposition() != PulsarSendResult.Disposition.PERSISTED) {
                throw new IllegalStateException("Pulsar large-payload source frame was not persisted: "
                        + result.disposition() + "/" + result.stableCode());
            }
            return new PulsarSourcePosition(
                    shard,
                    SOURCE_INCARNATION,
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

    private static int countSourceRecords(
            final PulsarClient client, final TopicResourceGuard guard, final String physicalTopic) throws Exception {
        final GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(
                client, guard, physicalTopic, "nereus-delay-pulsar-large-count-" + UUID.randomUUID());
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

    private static void requireDestinationPayload(
            final GuardedConsumer<byte[]> consumer, final byte[] expectedPayload, final int sourcePartition)
            throws Exception {
        final var message = consumer.receive(15, TimeUnit.SECONDS);
        if (message == null || !Arrays.equals(expectedPayload, message.getValue())) {
            throw new IllegalStateException("Pulsar multi-shard destination payload was not read back exactly: "
                    + "sourcePartition=" + sourcePartition);
        }
        consumer.acknowledge(message);
    }

    private static PulsarSourcePosition sendCommandAndPosition(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final ShardId shard,
            final PreparedCommand command,
            final String producerName,
            final int partition)
            throws Exception {
        return sendFrameAndPosition(
                client,
                guard,
                physicalTopic,
                shard,
                com.nereusstream.delay.protocol.CommandCodec.encodeManagedFrame(command),
                producerName,
                partition);
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

    static void createPartitionedTopic(
            final HttpClient client,
            final String adminUrl,
            final String topicBase,
            final byte[] incarnation,
            final long creationTimestamp,
            final List<String> guardAdminUrls)
            throws Exception {
        createPartitionedTopic(client, adminUrl, topicBase, 1, incarnation, creationTimestamp, guardAdminUrls);
    }

    static void createPartitionedTopic(
            final HttpClient client,
            final String adminUrl,
            final String topicBase,
            final int partitionCount,
            final byte[] incarnation,
            final long creationTimestamp,
            final List<String> guardAdminUrls)
            throws Exception {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Pulsar large-payload partition count must be positive");
        }
        final String partitionsPath = adminUrl + "/admin/v2/persistent/public/default/" + topicBase + "/partitions";
        for (int attempt = 0; attempt < 120; attempt++) {
            final HttpResponse<String> response =
                    request(client, partitionsPath, "PUT", Integer.toString(partitionCount));
            if (response.statusCode() >= 200 && response.statusCode() < 300 || response.statusCode() == 409) {
                break;
            }
            if (response.statusCode() != 404 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw failure("create Pulsar large-payload partitioned topic", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        // Partition metadata does not eagerly load the physical PersistentTopic.
        // Ask the P1 admin surface to create any missing physical partitions before
        // the resource-controller endpoint applies its ordered guard update. The
        // normal non-partitioned create endpoint intentionally returns 409 for a
        // partitioned topic and cannot be used as materialization.
        createMissedPartitions(client, guardAdminUrls, topicBase);
        for (int partition = 0; partition < partitionCount; partition++) {
            stampGuard(client, guardAdminUrls, topicBase + "-partition-" + partition, incarnation, creationTimestamp);
        }
    }

    static void createMissedPartitions(final HttpClient client, final List<String> adminUrls, final String topicBase)
            throws Exception {
        int lastStatus = -1;
        String lastBody = "";
        for (int attempt = 0; attempt < 120; attempt++) {
            for (String adminUrl : adminUrls) {
                final String path =
                        adminUrl + "/admin/v2/persistent/public/default/" + topicBase + "/createMissedPartitions";
                final HttpResponse<String> response = request(client, path, "POST", "");
                lastStatus = response.statusCode();
                lastBody = response.body();
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    return;
                }
                if (response.statusCode() != 307
                        && response.statusCode() != 404
                        && response.statusCode() != 409
                        && response.statusCode() != 412
                        && response.statusCode() != 503) {
                    throw failure("materialize Pulsar large-payload physical partitions", response);
                }
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("Pulsar large-payload physical partitions did not converge: " + topicBase
                + " lastStatus=" + lastStatus + " lastBody=" + lastBody);
    }

    static void createTopic(
            final HttpClient client,
            final String adminUrl,
            final String topic,
            final byte[] incarnation,
            final long creationTimestamp)
            throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic;
        final String body = guardBody(incarnation, creationTimestamp);
        int lastStatus = -1;
        String lastBody = "";
        for (int attempt = 0; attempt < 40; attempt++) {
            final HttpResponse<String> response = request(client, path, "PUT", body);
            lastStatus = response.statusCode();
            lastBody = response.body();
            if (response.statusCode() >= 200 && response.statusCode() < 300 || response.statusCode() == 409) {
                if (topic.endsWith("-partition-0")) {
                    System.out.println("Pulsar large-payload physical topic materialization response: status="
                            + response.statusCode() + ", topic=" + topic);
                }
                return;
            }
            if (response.statusCode() != 412 && response.statusCode() != 503) {
                throw failure("create Pulsar large-payload destination topic", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("Pulsar large-payload topic did not converge: " + topic + " lastStatus="
                + lastStatus + " lastBody=" + lastBody);
    }

    static void stampGuard(
            final HttpClient client,
            final List<String> adminUrls,
            final String physicalTopic,
            final byte[] incarnation,
            final long creationTimestamp)
            throws Exception {
        int lastStatus = -1;
        String lastBody = "";
        for (int attempt = 0; attempt < 120; attempt++) {
            for (String adminUrl : adminUrls) {
                final String path =
                        adminUrl + "/admin/v2/persistent/public/default/" + physicalTopic + "/resourceGuard";
                final HttpResponse<String> response =
                        request(client, path, "PUT", guardBody(incarnation, creationTimestamp));
                lastStatus = response.statusCode();
                lastBody = response.body();
                if (response.statusCode() >= 200 && response.statusCode() < 300 || response.statusCode() == 409) {
                    return;
                }
                if (response.statusCode() != 307
                        && response.statusCode() != 404
                        && response.statusCode() != 412
                        && response.statusCode() != 503) {
                    throw failure("stamp Pulsar large-payload resource guard", response);
                }
                // A 307/404 from one Broker can mean that the physical topic is
                // owned or loaded by the other Broker. Try the next exact admin
                // endpoint before consuming the bounded retry delay.
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("Pulsar large-payload resource guard did not converge: " + physicalTopic
                + " lastStatus=" + lastStatus + " lastBody=" + lastBody);
    }

    static String guardBody(final byte[] incarnation, final long creationTimestamp) {
        return "{\"nereus.resource.guard.version\":\"1\",\"nereus.resource.incarnation\":\""
                + encode(incarnation) + "\",\"nereus.resource.created-at\":\""
                + Long.toUnsignedString(creationTimestamp) + "\"}";
    }

    static void deletePartitionedTopic(final HttpClient client, final List<String> adminUrls, final String topic) {
        deleteAcrossAdmins(
                client, adminUrls, "/admin/v2/persistent/public/default/" + topic + "/partitions?force=true");
    }

    static void deleteTopic(final HttpClient client, final List<String> adminUrls, final String topic) {
        deleteAcrossAdmins(client, adminUrls, "/admin/v2/persistent/public/default/" + topic + "?force=true");
    }

    static void deleteAcrossAdmins(final HttpClient client, final List<String> adminUrls, final String suffix) {
        Exception lastFailure = null;
        for (String adminUrl : adminUrls) {
            try {
                final HttpResponse<String> response = request(client, adminUrl + suffix, "DELETE", "");
                if (response.statusCode() < 300 || response.statusCode() == 404) {
                    return;
                }
                lastFailure = new IllegalStateException("HTTP " + response.statusCode());
            } catch (Exception failure) {
                lastFailure = failure;
            }
        }
        if (lastFailure != null) {
            System.err.println(
                    "Pulsar large-payload cleanup failed across all admin endpoints: " + lastFailure.getMessage());
        }
    }

    static HttpResponse<String> request(
            final HttpClient client, final String path, final String method, final String body) throws Exception {
        final HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(path)).header("Content-Type", "application/json");
        final HttpRequest request;
        if ("DELETE".equals(method)) {
            request = builder.DELETE().build();
        } else if ("POST".equals(method)) {
            request = builder.POST(HttpRequest.BodyPublishers.ofString(body)).build();
        } else {
            request = builder.PUT(HttpRequest.BodyPublishers.ofString(body)).build();
        }
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static IllegalStateException failure(final String operation, final HttpResponse<String> response) {
        return new IllegalStateException(
                operation + " failed with HTTP " + response.statusCode() + ": " + response.body());
    }

    private static void closeNative(final GuardedConsumer<byte[]> consumer) {
        try {
            consumer.close();
        } catch (org.apache.pulsar.client.api.PulsarClientException failure) {
            throw new IllegalStateException("Pulsar large-payload guarded consumer close failed", failure);
        }
    }

    private static void closeNativeQuietly(final GuardedConsumer<byte[]> consumer) {
        try {
            consumer.close();
        } catch (org.apache.pulsar.client.api.PulsarClientException | RuntimeException ignored) {
            // Teardown must not hide the primary multi-shard assertion.
        }
    }

    /**
     * A P1 seek can acknowledge the seek before its broker-side consumer
     * replacement notification has reached the client. Two immediate proof
     * reads are therefore not enough to bind a Route barrier: the old proof
     * can remain visible briefly while the guarded SUBSCRIBE is being
     * recreated. Require a bounded quiet window before publishing the
     * barrier, while still keeping the generation check strict.
     */
    private static PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof seekAfterAndSettle(
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard expectedGuard,
            final String physicalTopic,
            final ShardId shard,
            final Optional<PulsarSourcePosition> lastApplied,
            final Duration proofTimeout) {
        PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof previous =
                PulsarClientArtifactRecoverySourcePositioner.seekAfter(
                        consumer, expectedGuard, physicalTopic, shard, lastApplied, proofTimeout);
        int stableRounds = 0;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (stableRounds < 3) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Pulsar guarded recovery proof did not settle after seek");
            }
            try {
                TimeUnit.MILLISECONDS.sleep(250);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while settling Pulsar recovery proof", interrupted);
            }
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof current =
                    PulsarClientArtifactRecoverySourcePositioner.awaitStableProof(
                            consumer, expectedGuard, physicalTopic, shard.partition(), proofTimeout);
            if (previous.equals(current)) {
                stableRounds++;
            } else {
                previous = current;
                stableRounds = 0;
            }
        }
        return previous;
    }

    /**
     * Creates the successor source only after a fresh guarded proof is available.
     *
     * <p>The P1 generation is allocated by the Broker process that admits the
     * guarded SUBSCRIBE. During a multi-Broker failover two independent Broker
     * JVM counters can therefore accidentally present the same raw value. A
     * candidate with that value is not a successor proof: discard the exact
     * candidate and establish another guarded SUBSCRIBE. The reactivation
     * contract still remains strict and never accepts an equal generation.</p>
     */
    private static SuccessorSource createSuccessorSource(
            final PulsarClient client,
            final TopicResourceGuard sourceGuard,
            final String sourcePhysicalTopic,
            final ShardId shard,
            final PulsarSourcePosition commitPosition,
            final long previousGeneration)
            throws Exception {
        RuntimeException lastCollision = null;
        for (int attempt = 1; attempt <= 8; attempt++) {
            final GuardedConsumer<byte[]> candidate = PulsarClientArtifactSourceConsumerFactory.create(
                    client,
                    sourceGuard,
                    sourcePhysicalTopic,
                    "nereus-delay-pulsar-large-reactivation-" + UUID.randomUUID());
            try {
                final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof = seekAfterAndSettle(
                        candidate,
                        sourceGuard,
                        sourcePhysicalTopic,
                        shard,
                        Optional.of(commitPosition),
                        Duration.ofSeconds(15));
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
        throw new IllegalStateException(
                "Pulsar source successor did not obtain a distinct connection generation", lastCollision);
    }

    private record SuccessorSource(
            GuardedConsumer<byte[]> consumer,
            PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof) {}

    private record LargeShardProbe(
            ShardId shard,
            TopicResourceGuard guard,
            String physicalTopic,
            SystemMutation activation,
            PreparedCommand beforeRoute,
            PulsarSourcePosition activationPosition,
            PulsarSourcePosition beforeRoutePosition,
            PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof,
            GuardedConsumer<byte[]> consumer) {}

    private record LargeShardAdmission(
            LargeShardProbe probe,
            WorkerAssignmentAuthority.Publication publication,
            WorkerAssignment assignment,
            OwnerLease lease) {}

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

    private static List<String> configuredAdminUrls(final String value) {
        final List<String> urls = Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(candidate -> !candidate.isEmpty())
                .toList();
        if (urls.isEmpty()) {
            throw new IllegalArgumentException("Pulsar large-payload admin URL list must not be empty");
        }
        return urls;
    }

    private static boolean failoverRequested() {
        final String marker = System.getenv("NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER_MARKER");
        return marker != null && !marker.isBlank();
    }

    private static OwnerLease signalFailoverCut(final OxiaOwnerLeaseStore ownerAuthority, final OwnerLease lease)
            throws Exception {
        final String marker = System.getenv("NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER_MARKER");
        if (marker == null || marker.isBlank()) {
            return lease;
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
        OwnerLease currentLease = lease;
        long nextRenewalAt = System.currentTimeMillis() + 15_000;
        while (!Files.exists(releasePath)) {
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException("Pulsar Gateway large-payload failover cut was not released");
            }
            final long now = System.currentTimeMillis();
            if (now >= nextRenewalAt) {
                currentLease = ownerAuthority
                        .renew(currentLease, now, LEASE_DURATION_MS)
                        .orElseThrow(() -> new IllegalStateException(
                                "Pulsar Owner Lease renewal was rejected during failover cut"));
                System.out.println("Pulsar Owner Lease renewed during failover cut: ownerEpoch="
                        + currentLease.ownerEpoch() + ", expiresAt=" + currentLease.expiresAtEpochMs());
                nextRenewalAt = now + 15_000;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        System.out.println("Pulsar Gateway large-payload failover cut release acknowledged after broker-1 stop");
        return currentLease;
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

    static void recover(
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard guard,
            final WorkerAssignment accepted,
            final OxiaOwnerLeaseStore ownerAuthority,
            final OwnedDelayShard ownedShard,
            final SystemMutation activation,
            final PreparedCommand beforeRoute,
            final KeyPair signingKeys,
            final CompatibleControlSnapshot controlSnapshot,
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
            throw new IllegalStateException(
                    "Pulsar recovery did not return the exact activation and pre-route records");
        }
        final OwnerRecoveryCoordinator recovery = new OwnerRecoveryCoordinator(
                ownedShard,
                ownerAuthority,
                accepted.sourceAssignment(),
                strictPulsarFixtureSuccessor(),
                SourceReplayCursor.of(entries.iterator()),
                signingKeys.getPublic(),
                controlSnapshot,
                System::currentTimeMillis,
                new ReplayTurnBudget(2, WORK_CLASS_BYTES, TimeUnit.SECONDS.toNanos(10)),
                workClasses);
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
    static SourceReplaySuccessor strictPulsarFixtureSuccessor() {
        return (previous, current) -> {
            if (!(previous instanceof PulsarSourcePosition previousPulsar)
                    || !(current instanceof PulsarSourcePosition currentPulsar)
                    || previousPulsar.ledgerId() != currentPulsar.ledgerId()) {
                return false;
            }
            if (previousPulsar.entryId() == currentPulsar.entryId()) {
                return previousPulsar.batchSize() == currentPulsar.batchSize()
                        && previousPulsar.normalizedBatchIndex() != previousPulsar.batchSize() - 1
                        && currentPulsar.normalizedBatchIndex() == previousPulsar.normalizedBatchIndex() + 1;
            }
            return previousPulsar.entryKind() == PulsarSourcePosition.EntryKind.NON_BATCH
                    && currentPulsar.entryKind() == PulsarSourcePosition.EntryKind.NON_BATCH
                    && previousPulsar.entryId() != Long.MAX_VALUE
                    && currentPulsar.entryId() == previousPulsar.entryId() + 1;
        };
    }

    static com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult runUntilApplied(
            final WorkerShardRuntime runtime) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result;
        do {
            result = runtime.runSourceTurn(
                    new SchedulerBudget(1, WORK_CLASS_BYTES, TimeUnit.SECONDS.toNanos(2)), System::currentTimeMillis);
            if (result.status()
                    == com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                return result;
            }
            if (result.status() != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus
                                    .WAITING_FOR_WORK_CLASS) {
                throw new IllegalStateException(
                        "Pulsar large-payload Worker source turn failed: " + result.status(), result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Pulsar large-payload Worker source record did not become visible");
    }

    static com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult runUntilApplied(
            final WorkerShardFleetRuntime fleet, final ShardId expectedShard) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        do {
            final WorkerShardFleetRuntime.SourceTurn turn = fleet.runNextSourceTurn(
                    new SchedulerBudget(1, WORK_CLASS_BYTES, TimeUnit.SECONDS.toNanos(2)), System::currentTimeMillis);
            final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result = turn.result();
            if (turn.shardId().equals(expectedShard)
                    && result.status()
                            == com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                return result;
            }
            if (result.status() != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    && result.status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS
                    && result.status()
                            != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                throw new IllegalStateException(
                        "Pulsar multi-shard Worker source turn failed for " + turn.shardId() + ": " + result.status(),
                        result.failure());
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException(
                "Pulsar multi-shard Worker source record did not become visible for " + expectedShard);
    }

    static void requireApplied(
            final com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnResult result, final String operation) {
        if (result.status() != com.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
            throw new IllegalStateException(
                    operation + " was not applied and ACKed: " + result.status(), result.failure());
        }
    }

    private static void requirePrecommitFailure(final RuntimeException failure, final String faultMode) {
        final String message = failure.getMessage() == null ? failure.toString() : failure.getMessage();
        if ("PUT_503_BEFORE_COMMIT".equals(faultMode) && !message.contains("HTTP 503")) {
            throw new IllegalStateException("Pulsar pre-commit 503 did not retain the provider failure", failure);
        }
        if ("PUT_TIMEOUT_BEFORE_COMMIT".equals(faultMode) && !message.contains("S3 payload request failed")) {
            throw new IllegalStateException("Pulsar pre-commit timeout did not retain the provider failure", failure);
        }
    }

    static CanonicalCommandQueuedReceipt requireQueued(
            final GatewaySubmissionOutcome response,
            final String operation,
            final PulsarSourcePosition previousPosition) {
        if (!response.hasSubmissionOutcomeNdr1()) {
            final StableError error =
                    StableError.decode(response.getPreparationError().toByteArray());
            throw new IllegalStateException(operation + " returned preparation error: stage=" + error.stage()
                    + ", code=" + error.code() + ", retryability=" + error.retryability()
                    + ", diagnosticCode=" + error.diagnosticCode());
        }
        final SubmissionOutcomeMessage outcome = SubmissionOutcomeMessage.decode(
                response.getSubmissionOutcomeNdr1().toByteArray());
        if (outcome.kind() != SubmissionOutcomeKind.MANAGED || outcome.managed().kind() != EnqueueOutcomeKind.QUEUED) {
            throw new IllegalStateException(operation + " did not produce a managed QUEUED outcome: " + outcome);
        }
        final CanonicalCommandQueuedReceipt receipt = outcome.managed().queued();
        if (!(receipt.sourcePosition() instanceof PulsarSourcePosition position)
                || position.compareWithinShard(previousPosition) <= 0) {
            throw new IllegalStateException(
                    operation + " Pulsar source position did not advance: " + receipt.sourcePosition());
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
                .setIdempotencyKey(ByteString.copyFrom(bytes(16, 80 + partition * 2)))
                .setRoute(GatewayRouteSelector.newBuilder()
                        .setIngressAdapterKind(AdapterKind.PULSAR.wireValue())
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
                .setIdempotencyKey(ByteString.copyFrom(bytes(16, 81 + partition * 2)))
                .setPayloadReservationReceipt(ByteString.copyFrom(receipt.payload()))
                .setCanonicalPayloadCommitProof(ByteString.copyFrom(proof.canonicalBytes()))
                .setRetryUntilEpochMs(System.currentTimeMillis() + 120_000)
                .build();
    }

    static CanonicalScheduleIntent largeScheduleIntent(final long now) {
        return largeScheduleIntent(now, Bytes.utf8("pulsar-large-payload-key"));
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
                AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())),
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
                AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())),
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
        final com.nereusstream.delay.protocol.ObjectStoreProfileSemantic semantic =
                new com.nereusstream.delay.protocol.ObjectStoreProfileSemantic(
                        com.nereusstream.delay.protocol.ObjectStoreProviderKind.S3_COMPATIBLE,
                        S3CompatiblePayloadObjectStore.endpointConfigDigest(endpoint, region, bucket),
                        S3CompatiblePayloadObjectStore.credentialAuthorizationScopeDigest(accessKey, region, bucket),
                        1,
                        true,
                        true,
                        true,
                        true,
                        bytes(32, 20),
                        8L << 20,
                        com.nereusstream.delay.protocol.ObjectStoreProfileSemantic.SINGLE_PUT,
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
                Bytes.sha256(Bytes.utf8("pulsar-large-trust-op")),
                Bytes.sha256(Bytes.utf8("pulsar-large-trust-request")),
                1);
        final byte[] body = trustSetControlBody(shard, controlRef, trustSet, retryUntil);
        return SystemMutation.signed(
                shard,
                SystemMutationType.APPLY_SHARD_CONTROL,
                retryUntil,
                controlRef.logicalOperationIdentity(12),
                body,
                AuthorIdentity.control(
                                Bytes.sha256(Bytes.utf8("pulsar-large-control-actor")),
                                Bytes.sha256(Bytes.utf8("pulsar-large-control-role")),
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

    static ScheduleResolver scheduleResolver(final String destinationPhysicalTopic) {
        final ProfileRef destination = destinationProfile();
        final ProfileRef capability = PulsarClientArtifactWorkerSmoke.capabilityProfile();
        final byte[] tuple =
                PulsarClientArtifactWorkerSmoke.canonicalLaneTuple(destinationPhysicalTopic, destination, capability);
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        final byte[] compatibilityTuple = Bytes.utf8("pulsar-large-payload-compatibility-lane");
        final DestinationLaneId compatibilityLane = DestinationLaneId.derive(compatibilityTuple);
        return new ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(
                    final ShardId shard,
                    final com.nereusstream.delay.protocol.DelayMessageId message,
                    final CanonicalScheduleIntent intent,
                    final com.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedSchedule(compatibilityLane, compatibilityTuple, intent.inlinePayload(), null);
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
            final String destinationPhysicalTopicBase, final int destinationPartition) {
        if (destinationPartition < 0) {
            throw new IllegalArgumentException("Pulsar multi-shard destination partition must be non-negative");
        }
        final String destinationPhysicalTopic = destinationPhysicalTopicBase + "-partition-" + destinationPartition;
        final ProfileRef destination = destinationProfile();
        final ProfileRef capability = PulsarClientArtifactWorkerSmoke.capabilityProfile();
        final byte[] tuple = PulsarClientArtifactWorkerSmoke.canonicalLaneTuple(
                destinationPhysicalTopic, destination, capability, destinationPartition);
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        final byte[] compatibilityTuple = Bytes.utf8("pulsar-large-payload-compatibility-lane");
        final DestinationLaneId compatibilityLane = DestinationLaneId.derive(compatibilityTuple);
        return new ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(
                    final ShardId shard,
                    final com.nereusstream.delay.protocol.DelayMessageId message,
                    final CanonicalScheduleIntent intent,
                    final com.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedSchedule(compatibilityLane, compatibilityTuple, intent.inlinePayload(), null);
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

    /**
     * Runs the full Large Payload authority path over two independent Pulsar
     * source partitions and one guarded destination topic. Each source shard
     * retains source-ordered PUBLISH_OUTCOME evidence before the exact
     * destination payload is accepted.
     */
    private static void runMultiShard(
            final String serviceUrl,
            final List<String> adminUrls,
            final String topicPrefix,
            final String oxiaEndpoint,
            final URI minioUri,
            final String minioRegion,
            final String minioBucket,
            final String minioAccessKey,
            final String minioSecretKey,
            final Duration minioRequestTimeout,
            final String minioFaultMode,
            final Path serverCertificate,
            final Path serverPrivateKey,
            final Path trustedClientCertificates,
            final Path clientCertificate,
            final Path clientPrivateKey,
            final int gatewayPort)
            throws Exception {
        if (!"NONE".equals(minioFaultMode)) {
            throw new IllegalArgumentException("Pulsar multi-shard large-payload mode requires MinIO fault mode NONE");
        }
        final int shardCount = 2;
        final String sourceBase = topicPrefix + "-" + UUID.randomUUID();
        if (sourceBase.contains("-partition-")) {
            throw new IllegalArgumentException("Pulsar multi-shard topic base must not contain '-partition-'");
        }
        final String physicalTopicBase = "persistent://public/default/" + sourceBase;
        final String destinationName = sourceBase + "-destination";
        final String destinationPhysicalTopic = "persistent://public/default/" + destinationName;
        final HttpClient admin = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        createPartitionedTopic(
                admin,
                adminUrls.get(0),
                sourceBase,
                shardCount,
                SOURCE_INCARNATION,
                SOURCE_CREATION_TIMESTAMP,
                adminUrls);
        createPartitionedTopic(
                admin,
                adminUrls.get(0),
                destinationName,
                shardCount,
                DESTINATION_INCARNATION,
                DESTINATION_CREATION_TIMESTAMP,
                adminUrls);
        for (int partition = 0; partition < shardCount; partition++) {
            createTopic(
                    admin,
                    adminUrls.get(0),
                    destinationName + "-partition-" + partition + "-attempt-journal",
                    PulsarClientArtifactWorkerSmoke.attemptJournalIncarnation(),
                    PulsarClientArtifactWorkerSmoke.attemptJournalCreationTimestamp());
        }

        final AuthenticatedTenantContext tenant =
                new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
        final byte[] payload = payload();
        final byte[] payloadHash = Bytes.sha256(payload);
        final KeyPair proofKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final long proofNow = System.currentTimeMillis();
        final PayloadProofVerifierKey verifierKey = PayloadProofVerifierKey.fromPublicKey(
                7, proofKeys.getPublic(), Math.max(0, proofNow - 60_000), proofNow + 3_600_000);
        final PayloadProofTrustSetSemantic trustSet = new PayloadProofTrustSetSemantic(1, List.of(verifierKey));
        final ProfileSemanticEnvelope objectStoreProfile =
                objectStoreProfile(minioUri, minioRegion, minioBucket, minioAccessKey);
        final RouteIncarnation routeIncarnation = RouteIncarnation.random();
        final RouteSelectionHint routeHint = new RouteSelectionHint(AdapterKind.PULSAR, Bytes.utf8("primary"));
        final KeyPair controlKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String routePrefix = "nereus-delay/pulsar-large-payload-multi-route/" + UUID.randomUUID();
        final String assignmentPrefix = "nereus-delay/pulsar-large-payload-multi-assignment/" + UUID.randomUUID();
        final String gatewayPrefix = "nereus-delay/pulsar-large-payload-multi-gateway/" + UUID.randomUUID();

        try (PulsarClient client = PulsarClientArtifactClientBuilder.builder(serviceUrl).build();
                GuardedConsumer<byte[]> destinationConsumer0 = PulsarClientArtifactSourceConsumerFactory.create(
                        client,
                        new TopicResourceGuard(CLUSTER, DESTINATION_INCARNATION, DESTINATION_CREATION_TIMESTAMP),
                        destinationPhysicalTopic + "-partition-0",
                        "nereus-delay-pulsar-large-multi-destination-0-" + UUID.randomUUID());
                GuardedConsumer<byte[]> destinationConsumer1 = PulsarClientArtifactSourceConsumerFactory.create(
                        client,
                        new TopicResourceGuard(CLUSTER, DESTINATION_INCARNATION, DESTINATION_CREATION_TIMESTAMP),
                        destinationPhysicalTopic + "-partition-1",
                        "nereus-delay-pulsar-large-multi-destination-1-" + UUID.randomUUID())) {
            final List<LargeShardProbe> probes = new ArrayList<>(shardCount);
            for (int partition = 0; partition < shardCount; partition++) {
                final ShardId shard = new ShardId(routeIncarnation, partition);
                final String physicalTopic = physicalTopicBase + "-partition-" + partition;
                final TopicResourceGuard guard =
                        new TopicResourceGuard(CLUSTER, SOURCE_INCARNATION, SOURCE_CREATION_TIMESTAMP);
                final SystemMutation activation = trustActivation(shard, trustSet.ref(), tenant, controlKeys);
                final PreparedCommand beforeRoute = command(shard, "large-payload-multi-before-" + partition);
                final PulsarSourcePosition activationPosition = sendFrameAndPosition(
                        client,
                        guard,
                        physicalTopic,
                        shard,
                        activation.encodeFrame(),
                        "pulsar-large-payload-multi-activation-" + partition,
                        partition);
                final PulsarSourcePosition beforeRoutePosition = sendCommandAndPosition(
                        client,
                        guard,
                        physicalTopic,
                        shard,
                        beforeRoute,
                        "pulsar-large-payload-multi-before-" + partition,
                        partition);
                if (activationPosition.compareWithinShard(beforeRoutePosition) >= 0) {
                    throw new IllegalStateException(
                            "Pulsar multi-shard source fixture order is not increasing: partition=" + partition);
                }
                final GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(
                        client,
                        guard,
                        physicalTopic,
                        "nereus-delay-pulsar-large-multi-worker-" + partition + "-" + UUID.randomUUID());
                final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof = seekAfterAndSettle(
                        consumer, guard, physicalTopic, shard, Optional.empty(), Duration.ofSeconds(5));
                probes.add(new LargeShardProbe(
                        shard,
                        guard,
                        physicalTopic,
                        activation,
                        beforeRoute,
                        activationPosition,
                        beforeRoutePosition,
                        proof,
                        consumer));
            }

            final RouteSnapshot snapshot = multiRouteSnapshot(physicalTopicBase, routeIncarnation, probes, controlKeys);
            final boolean[] runtimeOwned = new boolean[shardCount];
            final boolean[] runtimeDrained = new boolean[shardCount];
            try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                            oxiaEndpoint,
                            namespace,
                            "nereus-delay-pulsar-large-multi-route-publisher-" + UUID.randomUUID(),
                            Duration.ofSeconds(15),
                            routePrefix);
                    OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                            oxiaEndpoint,
                            namespace,
                            "nereus-delay-pulsar-large-multi-route-provider-" + UUID.randomUUID(),
                            Duration.ofSeconds(15),
                            routePrefix);
                    OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle =
                            OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                    oxiaEndpoint,
                                    namespace,
                                    "nereus-delay-pulsar-large-multi-assignment-" + UUID.randomUUID(),
                                    Duration.ofSeconds(15),
                                    assignmentPrefix)) {
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
                    final String workerId = "pulsar-large-payload-worker-" + (char) ('a' + partition);
                    final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement = coordinator.placeActive(
                            tenant, routeHint, placementRequest(System.currentTimeMillis(), partition, workerId));
                    final WorkerAssignment accepted = coordinator.requireAccepted(
                            tenant,
                            placement.publication().revision(),
                            placement.publication().assignment());
                    requireRouteAssignment(accepted, snapshot, probe.beforeRoutePosition(), probe.proof());
                    if (!workerId.equals(accepted.workerId()) || !assignedWorkers.add(accepted.workerId())) {
                        throw new IllegalStateException(
                                "Pulsar multi-shard Large Payload placement did not retain a unique Worker: "
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
                            "Pulsar multi-shard Large Payload placement did not span two Worker identities");
                }

                final WorkClassExecutionRegistry workClasses = workClasses();
                final long fleetWorkClassBytes = Math.multiplyExact((long) shardCount, WORK_CLASS_BYTES);
                final ClaimExecutionAdmission claimAdmission =
                        new ClaimExecutionAdmission(shardCount, fleetWorkClassBytes);
                final DestinationPhysicalAdmission physicalAdmission =
                        new DestinationPhysicalAdmission(shardCount, fleetWorkClassBytes);
                physicalAdmission.registerTargetCluster(CLUSTER, shardCount, fleetWorkClassBytes);
                final Path root = Files.createTempDirectory("nereus-delay-pulsar-large-payload-multi-");
                final List<ShardStore> stores = new ArrayList<>(shardCount);
                final List<DelayShard> delayShards = new ArrayList<>(shardCount);
                final List<WorkerShardRuntime> runtimes = new ArrayList<>(shardCount);
                final List<OwnedDelayShard> ownedShards = new ArrayList<>(shardCount);
                final List<com.nereusstream.delay.protocol.OwnerIdentity> ownerIdentities = new ArrayList<>(shardCount);
                final List<PulsarClientArtifactWorkerSmoke.PhysicalPublishBridge> physicalBridges =
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
                                        destinationPhysicalTopic, probe.shard().partition()),
                                trustCatalog);
                        delayShards.add(delayShard);
                        final OwnerLease lease = admission.lease();
                        final com.nereusstream.delay.protocol.OwnerIdentity ownerIdentity =
                                new com.nereusstream.delay.protocol.OwnerIdentity(
                                        bytes(16, 70 + probe.shard().partition()),
                                        bytes(16, 90 + probe.shard().partition()),
                                        lease.ownerEpoch(),
                                        Bytes.sha256(Bytes.utf8("pulsar-large-payload-multi-worker-fence-"
                                                + probe.shard().partition())));
                        final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease, ownerIdentity);
                        ownedShards.add(ownedShard);
                        ownerIdentities.add(ownerIdentity);
                        recover(
                                probe.consumer(),
                                probe.guard(),
                                admission.assignment(),
                                ownerAuthority,
                                ownedShard,
                                probe.activation(),
                                probe.beforeRoute(),
                                controlKeys,
                                controlSnapshot(probe.shard(), destinationProfile()),
                                workClasses,
                                probe.physicalTopic());
                        if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                || !(ownedShard.lastCatchupPosition() instanceof PulsarSourcePosition recovered)
                                || recovered.compareWithinShard(probe.beforeRoutePosition()) != 0) {
                            throw new IllegalStateException(
                                    "Pulsar multi-shard Large Payload recovery did not apply partition "
                                            + probe.shard().partition() + " activation and pre-route records");
                        }
                        runtimes.add(PulsarClientArtifactWorkerSourceFactory.create(
                                probe.consumer(),
                                probe.guard(),
                                probe.physicalTopic(),
                                RECEIVE_TIMEOUT,
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
                        runtimeOwned[probe.shard().partition()] = true;
                        final CredentialBindingKey binding =
                                new CredentialBindingKey(1, new Digest32(bytes(32, 41)), new Digest32(bytes(32, 42)));
                        final PulsarCommandTransportKey transportKey = new PulsarCommandTransportKey(
                                CLUSTER,
                                probe.physicalTopic(),
                                new Bytes32(SOURCE_INCARNATION),
                                SOURCE_CREATION_TIMESTAMP,
                                probe.shard().partition(),
                                binding);
                        final PulsarClientArtifactSendTransport managedTransport =
                                new PulsarClientArtifactSendTransport(
                                        PulsarClientArtifactProducerFactory.create(
                                                client,
                                                CLUSTER,
                                                SOURCE_INCARNATION,
                                                probe.physicalTopic(),
                                                SOURCE_CREATION_TIMESTAMP,
                                                "pulsar-large-payload-multi-managed-"
                                                        + probe.shard().partition()),
                                        CLUSTER,
                                        SOURCE_INCARNATION,
                                        probe.physicalTopic(),
                                        SOURCE_CREATION_TIMESTAMP,
                                        probe.shard().partition());
                        final PulsarClientArtifactSendTransport nativeTransport = new PulsarClientArtifactSendTransport(
                                PulsarClientArtifactProducerFactory.create(
                                        client,
                                        CLUSTER,
                                        SOURCE_INCARNATION,
                                        probe.physicalTopic(),
                                        SOURCE_CREATION_TIMESTAMP,
                                        "pulsar-large-payload-multi-native-"
                                                + probe.shard().partition()),
                                CLUSTER,
                                SOURCE_INCARNATION,
                                probe.physicalTopic(),
                                SOURCE_CREATION_TIMESTAMP,
                                probe.shard().partition());
                        transports.register(new ProductionPulsarSendTransport(
                                transportKey,
                                new ProductionPulsarSendTransport.Configuration(
                                        true, true, true, "pulsar-large-payload-multi-client"),
                                managedTransport,
                                nativeTransport));
                    }
                    fleet = new WorkerShardFleetRuntime(workClasses, resources, runtimes);
                    final DefaultSubmissionCoordinator submissions = new DefaultSubmissionCoordinator(
                            new RouteBoundSubmissionTransportPlanResolver(provider, System::currentTimeMillis),
                            transports,
                            SubmissionOutcomeProjectorRegistry.of(new PulsarManagedSubmissionOutcomeProjector()));
                    final DefaultDelaySemanticCore core = new DefaultDelaySemanticCore(
                            provider, new SecureLogicalUuidV7Generator(), System::currentTimeMillis);
                    final S3CompatiblePayloadObjectStore payloadStore = new S3CompatiblePayloadObjectStore(
                            objectStoreProfile,
                            minioUri,
                            minioRegion,
                            minioBucket,
                            minioAccessKey,
                            minioSecretKey,
                            null,
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
                                            "pulsar-large-payload-multi-admission-" + UUID.randomUUID(),
                                            Duration.ofSeconds(15),
                                            gatewayPrefix + "/admission-client");
                            OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyHandle =
                                    OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                            oxiaEndpoint,
                                            namespace,
                                            "pulsar-large-payload-multi-idempotency-" + UUID.randomUUID(),
                                            Duration.ofSeconds(15),
                                            gatewayPrefix + "/idempotency-client");
                            OxiaSyncOwnerLeaseBackend.ClientHandle auditHandle =
                                    OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                            oxiaEndpoint,
                                            namespace,
                                            "pulsar-large-payload-multi-audit-" + UUID.randomUUID(),
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
                                        "PrepareLargeSchedule partition=" + partition,
                                        admissionRecord.probe().beforeRoutePosition());
                                final PulsarSourcePosition preparePosition =
                                        (PulsarSourcePosition) prepareReceipt.sourcePosition();
                                requireApplied(
                                        runUntilApplied(
                                                fleet, admissionRecord.probe().shard()),
                                        "PrepareLargeSchedule partition=" + partition);
                                final int runtimeIndex = partition;
                                final byte[] reservationId = reservationId(prepareReceipt);
                                final PayloadReservation reservation = Optional.ofNullable(
                                                delayShards.get(runtimeIndex).getReservation(reservationId))
                                        .orElseThrow();
                                if (reservation.status() != PayloadReservationStatus.RESERVED) {
                                    throw new IllegalStateException(
                                            "Pulsar multi-shard Prepare did not leave RESERVED partition=" + partition
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
                                            "Pulsar multi-shard Gateway did not issue upload handle partition="
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
                                            "Pulsar multi-shard Gateway/MinIO attestation failed partition=" + partition
                                                    + ": " + attestation.outcome());
                                }
                                final CanonicalPayloadCommitProof proof = attestation.proof();
                                final GatewaySubmissionOutcome commitResponse =
                                        gateway.commitLargeSchedule(commitRequest(receipt, proof, partition));
                                final CanonicalCommandQueuedReceipt commitReceipt = requireQueued(
                                        commitResponse, "CommitLargeSchedule partition=" + partition, preparePosition);
                                requireApplied(
                                        runUntilApplied(
                                                fleet, admissionRecord.probe().shard()),
                                        "CommitLargeSchedule partition=" + partition);
                                final PayloadReservation committed = Optional.ofNullable(
                                                delayShards.get(runtimeIndex).getReservation(reservationId))
                                        .orElseThrow();
                                final var message = Optional.ofNullable(delayShards
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
                                            "Pulsar multi-shard Worker did not persist exact Object Store "
                                                    + "reference/readback partition=" + partition);
                                }
                                if (!Arrays.equals(
                                        prepareResponse.toByteArray(),
                                        gateway.prepareLargeSchedule(prepareRequest)
                                                .toByteArray())) {
                                    throw new IllegalStateException(
                                            "Pulsar multi-shard Oxia Gateway idempotency changed Prepare bytes "
                                                    + "partition=" + partition);
                                }
                                if (countSourceRecords(
                                                client,
                                                admissionRecord.probe().guard(),
                                                admissionRecord.probe().physicalTopic())
                                        != 4) {
                                    throw new IllegalStateException(
                                            "Pulsar multi-shard duplicate Prepare changed source record count "
                                                    + "partition=" + partition);
                                }
                                final LaneRecord physicalLane = Optional.ofNullable(
                                                delayShards.get(runtimeIndex).getLane(message.laneId()))
                                        .orElseThrow(() -> new IllegalStateException(
                                                "Pulsar multi-shard Worker did not persist the physical "
                                                        + "destination Lane"));
                                final PulsarClientArtifactWorkerSmoke.PhysicalPublishBridge physicalBridge =
                                        PulsarClientArtifactWorkerSmoke.createPhysicalPublishBridge(
                                                client,
                                                admissionRecord.probe().consumer(),
                                                admissionRecord.probe().physicalTopic(),
                                                admissionRecord.probe().shard(),
                                                (PulsarSourcePosition) commitReceipt.sourcePosition(),
                                                destinationPhysicalTopic + "-partition-" + partition,
                                                stores.get(runtimeIndex),
                                                ownedShards.get(runtimeIndex),
                                                ownerIdentities.get(runtimeIndex),
                                                ownerAuthority,
                                                workClasses,
                                                controlKeys,
                                                destinationProfile(),
                                                PulsarClientArtifactWorkerSmoke.capabilityProfile(),
                                                message.laneId(),
                                                physicalLane.laneIncarnation(),
                                                WORK_CLASS_BYTES,
                                                physicalAdmission,
                                                partition);
                                physicalBridges.add(physicalBridge);
                                runtimes.get(runtimeIndex).bindPhysicalPublishExecutor(physicalBridge.executor());
                                PulsarClientArtifactWorkerSmoke.runSourceAppliedPhysicalPublish(
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
                                        (PulsarSourcePosition) commitReceipt.sourcePosition(),
                                        client,
                                        payload,
                                        WORK_CLASS_BYTES,
                                        claimAdmission);
                                requireDestinationPayload(
                                        partition == 0 ? destinationConsumer0 : destinationConsumer1,
                                        payload,
                                        partition);
                                final int publishedSourceRecordCount = countSourceRecords(
                                        client,
                                        admissionRecord.probe().guard(),
                                        admissionRecord.probe().physicalTopic());
                                if (publishedSourceRecordCount != 6) {
                                    throw new IllegalStateException(
                                            "Pulsar multi-shard physical publish did not append the exact "
                                                    + "Admission/Outcome pair partition=" + partition
                                                    + ", sourceRecords="
                                                    + publishedSourceRecordCount);
                                }
                                System.out.println("Pulsar multi-shard Large Payload partition=" + partition
                                        + " passed: prepare=" + prepareReceipt.sourcePosition() + ", commit="
                                        + commitReceipt.sourcePosition() + ", objectVersion="
                                        + new String(proof.immutableObjectVersion(), StandardCharsets.UTF_8)
                                        + ", destinationEgress=true, sourceRecords=" + publishedSourceRecordCount);
                            }
                            for (int index = 0; index < runtimes.size(); index++) {
                                final int partition =
                                        admissions.get(index).probe().shard().partition();
                                final Path checkpointPath =
                                        root.resolve("pulsar-large-payload-multi-final-checkpoint-" + partition);
                                final byte[] checkpointId = Arrays.copyOf(
                                        Bytes.sha256(
                                                Bytes.utf8("pulsar-large-payload-multi-final-checkpoint-" + partition)),
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
                                            "Pulsar multi-shard Large Payload drain did not release partition "
                                                    + partition + " checkpoint/owner");
                                }
                                runtimeDrained[index] = true;
                            }
                            fleet.close();
                            fleet = null;
                            for (LargeShardAdmission admissionRecord : admissions) {
                                if (!assignmentAuthority.withdraw(admissionRecord.publication())) {
                                    throw new IllegalStateException("Pulsar multi-shard assignment withdrawal failed: "
                                            + admissionRecord.probe().shard());
                                }
                            }
                            assignmentsWithdrawn = true;
                            System.out.println(
                                    "Pulsar signed Route -> two guarded SUBSCRIBE barriers -> Oxia multi-shard "
                                            + "Assignment/Owner -> one Worker fleet -> Gateway mTLS/JWT -> "
                                            + "two Large Payload reservations -> real MinIO "
                                            + "upload/attest/commit/readback -> two destination "
                                            + "PUBLISHED outcomes -> checkpoint passed: "
                                            + "subscribePartitions=" + shardCount + ", routeRevision=" + routeRevision
                                            + ", workers=" + assignedWorkers + ", sourceBarriers="
                                            + probes.stream()
                                                    .map(probe -> probe.beforeRoutePosition()
                                                                    .ledgerId() + "/"
                                                            + probe.beforeRoutePosition()
                                                                    .entryId())
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
                                    "Pulsar multi-shard Worker fleet cleanup deferred: " + cleanupFailure.getMessage());
                        }
                    }
                    for (PulsarClientArtifactWorkerSmoke.PhysicalPublishBridge physicalBridge : physicalBridges) {
                        try {
                            physicalBridge.close();
                        } catch (RuntimeException cleanupFailure) {
                            System.err.println("Pulsar multi-shard physical destination cleanup deferred: "
                                    + cleanupFailure.getMessage());
                        }
                    }
                    if (!assignmentsWithdrawn) {
                        for (LargeShardAdmission admission : admissions) {
                            assignmentAuthority.withdraw(admission.publication());
                        }
                    }
                    for (ShardStore store : stores) {
                        try {
                            store.close();
                        } catch (RuntimeException cleanupFailure) {
                            System.err.println(
                                    "Pulsar multi-shard Store cleanup deferred: " + cleanupFailure.getMessage());
                        }
                    }
                    deleteTree(root);
                }
            } finally {
                for (int index = 0; index < probes.size(); index++) {
                    if (!runtimeOwned[index] || !runtimeDrained[index]) {
                        closeNativeQuietly(probes.get(index).consumer());
                    }
                }
            }
        } finally {
            deletePartitionedTopic(admin, adminUrls, sourceBase);
            deletePartitionedTopic(admin, adminUrls, destinationName);
        }
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <topic>");
        }
        final String serviceUrl = arguments[0];
        final List<String> adminUrls = configuredAdminUrls(arguments[1]);
        final String adminUrl = adminUrls.get(0);
        final String sourceBase = arguments[2] + "-" + UUID.randomUUID();
        final String sourcePhysicalName = sourceBase + "-partition-0";
        final String sourcePhysicalBase = "persistent://public/default/" + sourceBase;
        final String sourcePhysicalTopic = "persistent://public/default/" + sourcePhysicalName;
        final String destinationName =
                configured("NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC", sourceBase + "-destination");
        final String destinationPhysicalTopic = "persistent://public/default/" + destinationName;
        final String oxiaEndpoint = requiredEnv("NEREUS_DELAY_OXIA_ENDPOINT");
        final URI minioUri = URI.create(requiredEnv("NEREUS_DELAY_MINIO_ENDPOINT"));
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
        final Path serverCertificate = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_CERT");
        final Path serverPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_SERVER_KEY");
        final Path trustedClientCertificates = requiredPath("NEREUS_DELAY_GATEWAY_CA_CERT");
        final Path clientCertificate = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_CERT");
        final Path clientPrivateKey = requiredPath("NEREUS_DELAY_GATEWAY_CLIENT_KEY");
        final int gatewayPort = Integer.parseInt(requiredEnv("NEREUS_DELAY_GATEWAY_PORT"));
        if (gatewayPort <= 0 || gatewayPort > 65_535) {
            throw new IllegalArgumentException("NEREUS_DELAY_GATEWAY_PORT must be 1..65535");
        }
        if ("1".equals(configured("NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MULTI_SHARD", "0"))) {
            runMultiShard(
                    serviceUrl,
                    adminUrls,
                    arguments[2],
                    oxiaEndpoint,
                    minioUri,
                    minioRegion,
                    minioBucket,
                    minioAccessKey,
                    minioSecretKey,
                    minioRequestTimeout,
                    minioFaultMode,
                    serverCertificate,
                    serverPrivateKey,
                    trustedClientCertificates,
                    clientCertificate,
                    clientPrivateKey,
                    gatewayPort);
            return;
        }

        final HttpClient admin = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        createPartitionedTopic(admin, adminUrl, sourceBase, SOURCE_INCARNATION, SOURCE_CREATION_TIMESTAMP, adminUrls);
        createTopic(admin, adminUrl, destinationName, DESTINATION_INCARNATION, DESTINATION_CREATION_TIMESTAMP);
        createTopic(
                admin,
                adminUrl,
                destinationName + "-attempt-journal",
                PulsarClientArtifactWorkerSmoke.attemptJournalIncarnation(),
                PulsarClientArtifactWorkerSmoke.attemptJournalCreationTimestamp());

        final AuthenticatedTenantContext tenant =
                new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
        final byte[] payload = payload();
        final byte[] payloadHash = Bytes.sha256(payload);
        final KeyPair proofKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final long proofNow = System.currentTimeMillis();
        final PayloadProofVerifierKey verifierKey = PayloadProofVerifierKey.fromPublicKey(
                7, proofKeys.getPublic(), Math.max(0, proofNow - 60_000), proofNow + 3_600_000);
        final PayloadProofTrustSetSemantic trustSet = new PayloadProofTrustSetSemantic(1, List.of(verifierKey));
        final ProfileSemanticEnvelope objectStoreProfile =
                objectStoreProfile(minioUri, minioRegion, minioBucket, minioAccessKey);
        final RouteIncarnation routeIncarnation = RouteIncarnation.random();
        final ShardId shard = new ShardId(routeIncarnation, 0);
        final RouteSelectionHint routeHint = new RouteSelectionHint(AdapterKind.PULSAR, Bytes.utf8("primary"));
        final KeyPair controlKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final SystemMutation activation = trustActivation(shard, trustSet.ref(), tenant, controlKeys);
        final PreparedCommand beforeRoute = command(shard, "large-payload-before-route");
        final TopicResourceGuard sourceGuard =
                new TopicResourceGuard(CLUSTER, SOURCE_INCARNATION, SOURCE_CREATION_TIMESTAMP);
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String routePrefix = "nereus-delay/pulsar-large-payload-route/" + UUID.randomUUID();
        final String assignmentPrefix = "nereus-delay/pulsar-large-payload-assignment/" + UUID.randomUUID();
        final String gatewayPrefix = "nereus-delay/pulsar-large-payload-gateway/" + UUID.randomUUID();

        try (PulsarClient client = PulsarClientArtifactClientBuilder.builder(serviceUrl).build()) {
            final PulsarSourcePosition activationPosition = sendFrameAndPosition(
                    client,
                    sourceGuard,
                    sourcePhysicalTopic,
                    shard,
                    activation.encodeFrame(),
                    "pulsar-large-payload-activation");
            final PulsarSourcePosition beforeRoutePosition = PulsarClientArtifactWorkerSmoke.sendAndPosition(
                    client, sourceGuard, sourcePhysicalTopic, beforeRoute, "pulsar-large-payload-before-route");
            if (activationPosition.compareWithinShard(beforeRoutePosition) >= 0) {
                throw new IllegalStateException("Pulsar large-payload source fixture order is not increasing");
            }
            final GuardedConsumer<byte[]> nativeConsumer = PulsarClientArtifactSourceConsumerFactory.create(
                    client, sourceGuard, sourcePhysicalTopic, "nereus-delay-pulsar-large-worker-" + UUID.randomUUID());
            GuardedConsumer<byte[]> activeConsumer = nativeConsumer;
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof = seekAfterAndSettle(
                    nativeConsumer, sourceGuard, sourcePhysicalTopic, shard, Optional.empty(), Duration.ofSeconds(5));
            final RouteSnapshot snapshot = routeSnapshot(
                    sourcePhysicalBase,
                    sourcePhysicalTopic,
                    routeIncarnation,
                    beforeRoutePosition,
                    proof,
                    tenant,
                    controlKeys);
            boolean runtimeDrained = false;
            try {
                try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                                oxiaEndpoint,
                                namespace,
                                "nereus-delay-pulsar-large-route-publisher-" + UUID.randomUUID(),
                                Duration.ofSeconds(15),
                                routePrefix);
                        OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                                oxiaEndpoint,
                                namespace,
                                "nereus-delay-pulsar-large-route-provider-" + UUID.randomUUID(),
                                Duration.ofSeconds(15),
                                routePrefix);
                        OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle =
                                OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                        oxiaEndpoint,
                                        namespace,
                                        "nereus-delay-pulsar-large-assignment-" + UUID.randomUUID(),
                                        Duration.ofSeconds(15),
                                        assignmentPrefix)) {
                    final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(
                            publisherSession, routePrefix, controlKeys.getPublic());
                    final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                            providerSession, routePrefix, controlKeys.getPublic(), System::currentTimeMillis);
                    publisher.publish(routeHint, snapshot, 0);
                    provider.refresh().toCompletableFuture().join();
                    final WorkerAssignmentAuthority assignmentAuthority =
                            new OxiaSyncWorkerAssignmentBackend(assignmentHandle, assignmentPrefix);
                    final RouteWorkerAssignmentCoordinator routeCoordinator = new RouteWorkerAssignmentCoordinator(
                            provider,
                            new WorkerAssignmentCoordinator(
                                    new WorkerPlacementPolicy(
                                            new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)),
                                    assignmentAuthority));
                    final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement =
                            routeCoordinator.placeActive(
                                    tenant, routeHint, placementRequest(System.currentTimeMillis()));
                    final WorkerAssignment accepted = routeCoordinator.requireAccepted(
                            tenant,
                            placement.publication().revision(),
                            placement.publication().assignment());
                    requireRouteAssignment(accepted, snapshot, beforeRoutePosition, proof);
                    WorkerAssignmentAuthority.Publication activePublication = placement.publication();

                    final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(assignmentHandle.backend());
                    final OwnerLease lease = ownerAuthority
                            .acquire(
                                    accepted.sourceAssignment(),
                                    "pulsar-large-payload-worker",
                                    assignmentHandle.sessionIdentity(),
                                    System.currentTimeMillis(),
                                    LEASE_DURATION_MS)
                            .orElseThrow();
                    final WorkClassExecutionRegistry workClasses =
                            PulsarClientArtifactWorkerSmoke.workClasses(WORK_CLASS_BYTES);
                    final CompatibleControlSnapshot controlSnapshot = controlSnapshot(shard, destinationProfile());
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
                        final DelayShard delayShard = new DelayShard(
                                store,
                                DelayShardConfig.defaults(),
                                null,
                                null,
                                scheduleResolver(destinationPhysicalTopic),
                                trustCatalog);
                        final com.nereusstream.delay.protocol.OwnerIdentity ownerIdentity =
                                new com.nereusstream.delay.protocol.OwnerIdentity(
                                        bytes(16, 70),
                                        bytes(16, 71),
                                        lease.ownerEpoch(),
                                        Bytes.sha256(Bytes.utf8("pulsar-large-worker-fence")));
                        final OwnedDelayShard ownedShard = new OwnedDelayShard(delayShard, lease, ownerIdentity);
                        WorkerAssignment activeAssignment = accepted;
                        OwnedDelayShard activeOwnedShard = ownedShard;
                        com.nereusstream.delay.protocol.OwnerIdentity activeOwnerIdentity = ownerIdentity;
                        recover(
                                nativeConsumer,
                                sourceGuard,
                                accepted,
                                ownerAuthority,
                                ownedShard,
                                activation,
                                beforeRoute,
                                controlKeys,
                                controlSnapshot,
                                workClasses,
                                sourcePhysicalTopic);
                        if (ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                || !(ownedShard.lastCatchupPosition() instanceof PulsarSourcePosition recovered)
                                || recovered.compareWithinShard(beforeRoutePosition) != 0) {
                            throw new IllegalStateException(
                                    "Pulsar large-payload Worker recovery did not apply the exact pre-route records");
                        }

                        final CredentialBindingKey binding =
                                new CredentialBindingKey(1, new Digest32(bytes(32, 41)), new Digest32(bytes(32, 42)));
                        final PulsarCommandTransportKey transportKey = new PulsarCommandTransportKey(
                                CLUSTER,
                                sourcePhysicalTopic,
                                new Bytes32(SOURCE_INCARNATION),
                                SOURCE_CREATION_TIMESTAMP,
                                0,
                                binding);
                        final PulsarClientArtifactSendTransport managedTransport =
                                new PulsarClientArtifactSendTransport(
                                        PulsarClientArtifactProducerFactory.create(
                                                client,
                                                CLUSTER,
                                                SOURCE_INCARNATION,
                                                sourcePhysicalTopic,
                                                SOURCE_CREATION_TIMESTAMP,
                                                "pulsar-large-gateway-managed"),
                                        CLUSTER,
                                        SOURCE_INCARNATION,
                                        sourcePhysicalTopic,
                                        SOURCE_CREATION_TIMESTAMP,
                                        0);
                        final PulsarClientArtifactSendTransport nativeTransport = new PulsarClientArtifactSendTransport(
                                PulsarClientArtifactProducerFactory.create(
                                        client,
                                        CLUSTER,
                                        SOURCE_INCARNATION,
                                        sourcePhysicalTopic,
                                        SOURCE_CREATION_TIMESTAMP,
                                        "pulsar-large-gateway-native"),
                                CLUSTER,
                                SOURCE_INCARNATION,
                                sourcePhysicalTopic,
                                SOURCE_CREATION_TIMESTAMP,
                                0);
                        transports.register(new ProductionPulsarSendTransport(
                                transportKey,
                                new ProductionPulsarSendTransport.Configuration(
                                        true, true, true, "pulsar-large-gateway-client"),
                                managedTransport,
                                nativeTransport));
                        final DefaultSubmissionCoordinator submissions = new DefaultSubmissionCoordinator(
                                new RouteBoundSubmissionTransportPlanResolver(provider, System::currentTimeMillis),
                                transports,
                                SubmissionOutcomeProjectorRegistry.of(
                                        new PulsarManagedSubmissionOutcomeProjector(transportKey)));
                        final DefaultDelaySemanticCore core = new DefaultDelaySemanticCore(
                                provider, new SecureLogicalUuidV7Generator(), System::currentTimeMillis);
                        final S3CompatiblePayloadObjectStore payloadStore = new S3CompatiblePayloadObjectStore(
                                objectStoreProfile,
                                minioUri,
                                minioRegion,
                                minioBucket,
                                minioAccessKey,
                                minioSecretKey,
                                null,
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
                        final OxiaSyncOwnerLeaseBackend.ClientHandle admissionHandle =
                                OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                        oxiaEndpoint,
                                        namespace,
                                        "pulsar-large-admission-" + UUID.randomUUID(),
                                        Duration.ofSeconds(15),
                                        gatewayPrefix + "/admission-client");
                        final OxiaSyncOwnerLeaseBackend.ClientHandle idempotencyHandle =
                                OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                        oxiaEndpoint,
                                        namespace,
                                        "pulsar-large-idempotency-" + UUID.randomUUID(),
                                        Duration.ofSeconds(15),
                                        gatewayPrefix + "/idempotency-client");
                        final OxiaSyncOwnerLeaseBackend.ClientHandle auditHandle =
                                OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                        oxiaEndpoint,
                                        namespace,
                                        "pulsar-large-audit-" + UUID.randomUUID(),
                                        Duration.ofSeconds(15),
                                        gatewayPrefix + "/audit-client");
                        try (admissionHandle;
                                idempotencyHandle;
                                auditHandle) {
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
                            final GatewayScheduleService schedule = new GatewayScheduleService(
                                    core, idempotency, submissions, System::currentTimeMillis);
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
                            WorkerShardRuntime runtime = null;
                            PulsarClientArtifactWorkerSmoke.PhysicalPublishBridge physicalBridge = null;
                            boolean runtimeClosed = false;
                            try {
                                server.start();
                                final ManagedChannel channel = channel(
                                        gatewayPort, trustedClientCertificates, clientCertificate, clientPrivateKey);
                                try {
                                    final DelayGatewayGrpc.DelayGatewayBlockingStub gateway = stub(
                                            channel, token(jwtKeys, tenant, certificateFingerprint(clientCertificate)));
                                    final CanonicalScheduleIntent intent =
                                            largeScheduleIntent(System.currentTimeMillis());
                                    final GatewayPrepareLargeScheduleRequest prepareRequest = prepareRequest(
                                            intent,
                                            payload.length,
                                            payloadHash,
                                            trustSet.ref(),
                                            objectStoreProfile.ref());
                                    final GatewaySubmissionOutcome prepareResponse =
                                            gateway.prepareLargeSchedule(prepareRequest);
                                    final CanonicalCommandQueuedReceipt prepareReceipt =
                                            requireQueued(prepareResponse, "PrepareLargeSchedule", beforeRoutePosition);
                                    final PulsarSourcePosition preparePosition =
                                            (PulsarSourcePosition) prepareReceipt.sourcePosition();
                                    final byte[] reservationId = reservationId(prepareReceipt);
                                    runtime = PulsarClientArtifactWorkerSourceFactory.create(
                                            activeConsumer,
                                            sourceGuard,
                                            sourcePhysicalTopic,
                                            RECEIVE_TIMEOUT,
                                            activeAssignment.sourceAssignment(),
                                            workClasses,
                                            activeOwnedShard,
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
                                    final var reservation = Optional.ofNullable(
                                                    delayShard.getReservation(reservationId))
                                            .orElseThrow(() -> new IllegalStateException(
                                                    "Pulsar Worker did not persist the payload reservation"));
                                    if (reservation.status() != PayloadReservationStatus.RESERVED) {
                                        throw new IllegalStateException(
                                                "Prepare did not leave a RESERVED payload reservation: "
                                                        + reservation.status());
                                    }
                                    payloadStore.register(reservation, trustSet.ref(), objectStoreProfile.ref());
                                    final PayloadReservationReceipt receipt =
                                            payloadStore.reservationReceipt(reservation);
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
                                                "Gateway did not issue the Pulsar payload upload handle");
                                    }
                                    final OpaquePayloadUploadHandle handle = handleDomain.issued();
                                    try {
                                        payloadStore.upload(receipt, handle, payload, System.currentTimeMillis());
                                    } catch (RuntimeException failure) {
                                        if (!expectPrecommitFailure) {
                                            throw failure;
                                        }
                                        requirePrecommitFailure(failure, minioFaultMode);
                                        final var retained = Optional.ofNullable(
                                                        delayShard.getReservation(reservationId))
                                                .orElseThrow();
                                        if (retained.status() != PayloadReservationStatus.RESERVED
                                                || delayShard.getMessage(prepareReceipt
                                                                .command()
                                                                .delayMessageId())
                                                        != null
                                                || countSourceRecords(client, sourceGuard, sourcePhysicalTopic) != 3) {
                                            throw new IllegalStateException(
                                                    "Pulsar pre-commit payload failure crossed the Commit boundary");
                                        }
                                        final PayloadAttestationResponse absent =
                                                payloadStore.attest(receipt, handle, System.currentTimeMillis());
                                        if (absent.outcome() != PayloadAttestationOutcome.OBJECT_NOT_READY_RETRYABLE) {
                                            throw new IllegalStateException(
                                                    "Pulsar pre-commit payload failure did not leave the object "
                                                            + "absent: "
                                                            + absent.outcome());
                                        }
                                        final Path checkpointPath =
                                                root.resolve("pulsar-large-payload-precommit-checkpoint");
                                        final byte[] checkpointId = Arrays.copyOf(
                                                Bytes.sha256(Bytes.utf8("pulsar-large-payload-precommit-checkpoint")),
                                                16);
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
                                                || !ownerAuthority
                                                        .current(shard)
                                                        .isEmpty()) {
                                            throw new IllegalStateException(
                                                    "Pulsar pre-commit payload failure did not drain and release "
                                                            + "the Owner");
                                        }
                                        runtime.close();
                                        runtimeClosed = true;
                                        runtimeDrained = true;
                                        if (!assignmentAuthority.withdraw(activePublication)) {
                                            throw new IllegalStateException(
                                                    "Pulsar pre-commit Worker assignment was not withdrawn exactly");
                                        }
                                        assignmentWithdrawn = true;
                                        System.out.println(
                                                "Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker "
                                                        + "+ MinIO large-payload pre-commit "
                                                        + "fail-closed E2E passed: fault="
                                                        + minioFaultMode
                                                        + ", Prepare retained RESERVED, Commit absent, "
                                                        + "payload object absent, owner released");
                                        return;
                                    }
                                    final GatewayPayloadAttestationResponse attestationResponse =
                                            gateway.attestPayloadUpload(GatewayAttestPayloadUploadRequest.newBuilder()
                                                    .setPayloadReservationReceipt(
                                                            ByteString.copyFrom(receipt.payload()))
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
                                                "Gateway/MinIO did not return an immutable Pulsar payload proof");
                                    }
                                    final CanonicalPayloadCommitProof proofValue = attestation.proof();
                                    final GatewaySubmissionOutcome commitResponse =
                                            gateway.commitLargeSchedule(commitRequest(receipt, proofValue));
                                    final CanonicalCommandQueuedReceipt commitReceipt =
                                            requireQueued(commitResponse, "CommitLargeSchedule", preparePosition);
                                    final PulsarSourcePosition commitPosition =
                                            (PulsarSourcePosition) commitReceipt.sourcePosition();
                                    requireApplied(runUntilApplied(runtime), "CommitLargeSchedule");
                                    final var committed = Optional.ofNullable(delayShard.getReservation(reservationId))
                                            .orElseThrow();
                                    final var message = Optional.ofNullable(delayShard.getMessage(
                                                    prepareReceipt.command().delayMessageId()))
                                            .orElseThrow();
                                    if (committed.status() != PayloadReservationStatus.COMMITTED
                                            || message.status() != MessageStatus.SCHEDULED
                                            || message.payloadReference() == null
                                            || !Arrays.equals(
                                                    message.payloadReference().immutableObjectVersion(),
                                                    proofValue.immutableObjectVersion())
                                            || !Arrays.equals(
                                                    message.payloadReference().proofId(), proofValue.proofId())) {
                                        throw new IllegalStateException(
                                                "Pulsar Worker did not persist the exact committed Object Store "
                                                        + "reference");
                                    }
                                    final byte[] objectPayload = payloadStore.readPayload(message.payloadReference());
                                    if (!Arrays.equals(objectPayload, payload)) {
                                        throw new IllegalStateException(
                                                "Pulsar Worker Object Store readback did not match the committed "
                                                        + "payload");
                                    }
                                    final OwnerLease activeLeaseBeforeCut = ownerAuthority
                                            .current(shard)
                                            .orElseThrow(() -> new IllegalStateException(
                                                    "Pulsar active Owner Lease disappeared before failover cut"));
                                    if (activeLeaseBeforeCut.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                            || !activeLeaseBeforeCut.validAt(System.currentTimeMillis())) {
                                        throw new IllegalStateException(
                                                "Pulsar active Owner Lease is not live before failover cut");
                                    }
                                    final OwnerLease reactivationLease =
                                            signalFailoverCut(ownerAuthority, activeLeaseBeforeCut);
                                    if (failoverRequested()) {
                                        final PulsarActivationBarrier oldBarrier =
                                                (PulsarActivationBarrier) activeAssignment
                                                        .sourceAssignment()
                                                        .activationBarrier();
                                        final SuccessorSource successor = createSuccessorSource(
                                                client,
                                                sourceGuard,
                                                sourcePhysicalTopic,
                                                shard,
                                                commitPosition,
                                                oldBarrier.guardedSourceConnectionGeneration());
                                        final GuardedConsumer<byte[]> successorConsumer = successor.consumer();
                                        boolean successorInstalled = false;
                                        try {
                                            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof
                                                    successorProof = successor.proof();
                                            final PulsarActivationBarrier successorBarrier =
                                                    new PulsarActivationBarrier(
                                                            shard,
                                                            oldBarrier.brokerResourceIncarnation(),
                                                            oldBarrier.physicalTopic(),
                                                            oldBarrier.ledgerId(),
                                                            oldBarrier.entryId(),
                                                            oldBarrier.normalizedLastBatchIndex(),
                                                            oldBarrier.batchSize(),
                                                            successorProof.connectionGeneration(),
                                                            successorProof.attestationDigest(),
                                                            oldBarrier.empty());
                                            final long successorAssignmentEpoch = Math.addExact(
                                                    activeAssignment
                                                            .sourceAssignment()
                                                            .assignmentEpoch(),
                                                    1);
                                            final SourceAssignment successorSource = new SourceAssignment(
                                                    shard,
                                                    Bytes.sha256(
                                                            Bytes.utf8("nereus-delay-pulsar-source-"
                                                                    + "reactivation-assignment\0"),
                                                            activeAssignment
                                                                    .sourceAssignment()
                                                                    .assignmentId(),
                                                            Bytes.u64be(successorAssignmentEpoch)),
                                                    successorAssignmentEpoch,
                                                    successorBarrier);
                                            final WorkerAssignment successorAssignment = new WorkerAssignment(
                                                    activeAssignment.workerId(),
                                                    successorSource,
                                                    Math.addExact(activeAssignment.placementEpoch(), 1),
                                                    activeAssignment.capacityEnvelopeDigest(),
                                                    snapshot.snapshotDigest());
                                            final PulsarSourceReactivation reactivation = new PulsarSourceReactivation(
                                                    snapshot.snapshotDigest(),
                                                    activeAssignment.sourceAssignment(),
                                                    successorSource);
                                            final PulsarSourceReactivationCoordinator reactivationCoordinator =
                                                    new PulsarSourceReactivationCoordinator(
                                                            provider, assignmentAuthority, ownerAuthority);
                                            final PulsarSourceReactivationCoordinator.FencedPlan fencedPlan =
                                                    reactivationCoordinator.fenceForReactivation(
                                                            tenant,
                                                            activePublication,
                                                            reactivationLease,
                                                            reactivation,
                                                            System.currentTimeMillis());
                                            final WorkerShardRuntime oldRuntime = runtime;
                                            final OwnedDelayShard oldOwnedShard = activeOwnedShard;
                                            oldOwnedShard.fence();
                                            oldRuntime.closeForOwnerReactivation();
                                            runtimeClosed = true;
                                            final WorkerAssignmentAuthority.Publication successorPublication =
                                                    reactivationCoordinator.publishSuccessor(
                                                            fencedPlan, successorAssignment, () -> {
                                                                if (!oldRuntime.sourcePaused()
                                                                        || oldRuntime
                                                                                .pendingSourceEntry()
                                                                                .isPresent()) {
                                                                    throw new IllegalStateException(
                                                                            "old Pulsar Worker runtime is not "
                                                                                    + "quiescent");
                                                                }
                                                            });
                                            final OwnerLease successorLease = reactivationCoordinator.acquireSuccessor(
                                                    fencedPlan,
                                                    successorPublication,
                                                    successorAssignment.workerId(),
                                                    assignmentHandle.sessionIdentity(),
                                                    System.currentTimeMillis(),
                                                    LEASE_DURATION_MS);
                                            final com.nereusstream.delay.protocol.OwnerIdentity successorOwnerIdentity =
                                                    new com.nereusstream.delay.protocol.OwnerIdentity(
                                                            bytes(16, 70),
                                                            bytes(16, 71),
                                                            successorLease.ownerEpoch(),
                                                            Bytes.sha256(
                                                                    Bytes.utf8("pulsar-large-worker-"
                                                                            + "reactivation-fence"),
                                                                    Bytes.u64beBits(successorLease.ownerEpoch())));
                                            final OwnedDelayShard successorOwnedShard = new OwnedDelayShard(
                                                    delayShard, successorLease, successorOwnerIdentity);
                                            successorOwnedShard.markCatchingUpForReactivation(
                                                    ownerAuthority,
                                                    successorSource,
                                                    strictPulsarFixtureSuccessor(),
                                                    System.currentTimeMillis());
                                            successorOwnedShard.activateForReactivation(
                                                    ownerAuthority, controlSnapshot, System.currentTimeMillis());
                                            activeAssignment = successorAssignment;
                                            activePublication = successorPublication;
                                            activeConsumer = successorConsumer;
                                            activeOwnedShard = successorOwnedShard;
                                            activeOwnerIdentity = successorOwnerIdentity;
                                            successorInstalled = true;
                                            System.out.println(
                                                    "Pulsar source reactivation successor accepted: oldGeneration="
                                                            + oldBarrier.guardedSourceConnectionGeneration()
                                                            + ", newGeneration=" + successorProof.connectionGeneration()
                                                            + ", assignmentRevision=" + successorPublication.revision()
                                                            + ", ownerEpoch=" + successorLease.ownerEpoch());
                                        } finally {
                                            if (!successorInstalled) {
                                                closeNative(successorConsumer);
                                            }
                                        }
                                        runtime = PulsarClientArtifactWorkerSourceFactory.create(
                                                activeConsumer,
                                                sourceGuard,
                                                sourcePhysicalTopic,
                                                RECEIVE_TIMEOUT,
                                                activeAssignment.sourceAssignment(),
                                                workClasses,
                                                activeOwnedShard,
                                                store,
                                                resources,
                                                ownerAuthority,
                                                controlKeys.getPublic(),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null);
                                        runtimeClosed = false;
                                    }
                                    final LaneRecord physicalLane = Optional.ofNullable(
                                                    delayShard.getLane(message.laneId()))
                                            .orElseThrow(() -> new IllegalStateException(
                                                    "Pulsar Worker did not persist the physical destination Lane"));
                                    physicalBridge = PulsarClientArtifactWorkerSmoke.createPhysicalPublishBridge(
                                            client,
                                            activeConsumer,
                                            sourcePhysicalTopic,
                                            shard,
                                            commitPosition,
                                            destinationPhysicalTopic,
                                            store,
                                            activeOwnedShard,
                                            activeOwnerIdentity,
                                            ownerAuthority,
                                            workClasses,
                                            controlKeys,
                                            destinationProfile(),
                                            PulsarClientArtifactWorkerSmoke.capabilityProfile(),
                                            message.laneId(),
                                            physicalLane.laneIncarnation(),
                                            WORK_CLASS_BYTES);
                                    runtime.bindPhysicalPublishExecutor(physicalBridge.executor());
                                    PulsarClientArtifactWorkerSmoke.runSourceAppliedPhysicalPublish(
                                            runtime,
                                            delayShard,
                                            activeOwnedShard,
                                            activeOwnerIdentity,
                                            ownerAuthority,
                                            store,
                                            workClasses,
                                            controlKeys,
                                            physicalBridge,
                                            prepareReceipt.command().delayMessageId(),
                                            commitPosition,
                                            client,
                                            objectPayload,
                                            WORK_CLASS_BYTES);
                                    final byte[] duplicate = gateway.prepareLargeSchedule(prepareRequest)
                                            .toByteArray();
                                    if (!Arrays.equals(prepareResponse.toByteArray(), duplicate)) {
                                        throw new IllegalStateException(
                                                "real Oxia Pulsar Gateway idempotency did not return exact "
                                                        + "Prepare bytes");
                                    }
                                    final int sourceRecordCount =
                                            countSourceRecords(client, sourceGuard, sourcePhysicalTopic);
                                    if (sourceRecordCount != 6) {
                                        throw new IllegalStateException(
                                                "duplicate Prepare changed the Pulsar source record count: "
                                                        + sourceRecordCount + " expected 6");
                                    }
                                    final Path checkpointPath = root.resolve("pulsar-large-payload-final-checkpoint");
                                    final byte[] checkpointId = Arrays.copyOf(
                                            Bytes.sha256(Bytes.utf8("pulsar-large-payload-final-checkpoint")), 16);
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
                                                "Pulsar large-payload Worker drain did not publish the final "
                                                        + "checkpoint or release the owner");
                                    }
                                    runtime.close();
                                    runtimeClosed = true;
                                    runtimeDrained = true;
                                    if (!assignmentAuthority.withdraw(activePublication)) {
                                        throw new IllegalStateException(
                                                "Pulsar large-payload Worker assignment was not withdrawn exactly");
                                    }
                                    assignmentWithdrawn = true;
                                    System.out.println(
                                            "Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker "
                                                    + "+ MinIO large-payload authority E2E passed: prepare="
                                                    + preparePosition.ledgerId() + "/" + preparePosition.entryId()
                                                    + ", commit="
                                                    + commitPosition.ledgerId() + "/" + commitPosition.entryId()
                                                    + ", exactGatewayIdempotency=true, sourceRecords="
                                                    + sourceRecordCount);
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
            deletePartitionedTopic(admin, adminUrls, sourceBase);
            deleteTopic(admin, adminUrls, destinationName);
        }
    }
}
