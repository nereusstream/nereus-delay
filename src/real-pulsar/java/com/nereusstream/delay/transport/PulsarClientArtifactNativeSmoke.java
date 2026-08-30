package com.nereusstream.delay.transport;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.nereusstream.delay.adapter.PulsarNativePreparedRecordValidator;
import com.nereusstream.delay.adapter.PulsarSendResult;
import com.nereusstream.delay.adapter.PulsarTargetResource;
import com.nereusstream.delay.assessment.DeploymentSafetyGate;
import com.nereusstream.delay.assessment.DisposableEnvironmentAttestation;
import com.nereusstream.delay.assessment.PersistentStagingActivation;
import com.nereusstream.delay.assessment.PersistentStagingEvidence;
import com.nereusstream.delay.assessment.PersistentStagingNativeCanaryIdentity;
import com.nereusstream.delay.assessment.PhysicalSendActivationGate;
import com.nereusstream.delay.client.AutoFastSchedule;
import com.nereusstream.delay.client.EmbeddedDelayService;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshot;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SubmissionOutcomeKind;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.route.RouteSnapshotProvider;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.submission.DefaultSubmissionCoordinator;
import com.nereusstream.delay.submission.PulsarNativeSubmissionOutcomeProjector;
import com.nereusstream.delay.submission.RouteBoundSubmissionTransportPlanResolver;
import com.nereusstream.delay.submission.SubmissionOutcomeProjectorRegistry;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicResourceGuard;

/** Real-service smoke for the final native record encoder and deliverAt path. */
public final class PulsarClientArtifactNativeSmoke {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final String CLUSTER = PulsarClientArtifactClientBuilder.clusterId();
    private static final byte[] INCARNATION = digest(17);
    private static final long CREATION_TIMESTAMP = 1001L;

    private PulsarClientArtifactNativeSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <topic>");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String topic = arguments[2];
        final String physicalTopic = "persistent://public/default/" + topic;
        final HttpClient admin = HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(5))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        try (PersistentStagingActivation.Loaded persistentActivation = PersistentStagingActivation.loadIfConfigured()) {
            if (persistentActivation == null
                    && "STAGING".equals(System.getenv(PersistentStagingActivation.CLASSIFICATION_ENV))) {
                throw new IllegalStateException(
                        "STAGING native physical send requires Gate C, SHADOW, and an ENABLED policy");
            }
            createTopic(admin, adminUrl, topic);
            try {
                runNativeSendAndRead(serviceUrl, physicalTopic, persistentActivation);
            } finally {
                if (persistentActivation == null) {
                    deleteTopicIfPresent(admin, adminUrl, topic);
                }
            }
        }
    }

    private static void runNativeSendAndRead(
            final String serviceUrl,
            final String physicalTopic,
            final PersistentStagingActivation.Loaded persistentActivation)
            throws Exception {
        final byte[] payload = Bytes.utf8("nereus-delay-native-p1-smoke");
        final long nowEpochMs = System.currentTimeMillis();
        final long deliverAtEpochMs = nowEpochMs + 7_000L;
        final KeyPair capabilityKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ArtifactGenerationSet artifacts = persistentActivation == null
                ? ArtifactGenerationSet.current(
                        1, PulsarSourceLock.digest(), Bytes.sha256(Bytes.utf8("native-real-smoke-schema")))
                : persistentActivation.artifacts();
        final String producerName = "nereus-delay-p1-native-smoke";
        final PersistentStagingNativeCanaryIdentity.Identity identity = PersistentStagingNativeCanaryIdentity.create(
                CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, artifacts);
        final PulsarBrokerResourceIdentity target = identity.target();
        final PulsarTargetResource resource = new PulsarTargetResource(
                target.authenticatedClusterId(),
                target.resourceIncarnation(),
                target.physicalTopic(),
                target.physicalTopicCreationTimestamp(),
                0);
        final TrustedUtcIntervalEvidence issuedAt = evidence(nowEpochMs - 3_000L);
        final NativeCapabilitySnapshot capabilitySnapshot = NativeCapabilitySnapshot.create(
                identity.destination().ref(),
                identity.capability().ref(),
                target,
                0,
                Bytes.sha256(Bytes.utf8("native-smoke-resource-guard")),
                1,
                1,
                Bytes.sha256(Bytes.utf8("native-smoke-credential-binding")),
                Bytes.sha256(Bytes.utf8("native-smoke-credential-fingerprint")),
                identity.principalScopeDigest(),
                issuedAt,
                nowEpochMs + 60_000L,
                1,
                capabilityKeys.getPrivate());
        final HandoffPolicySnapshot handoff;
        if (persistentActivation == null) {
            handoff = HandoffPolicySnapshot.create(
                    identity.policyScopeDigest(),
                    1,
                    HandoffPolicyMode.ENABLED,
                    1_000,
                    nowEpochMs - 1_000L,
                    nowEpochMs + 60_000L,
                    HandoffPath.AUTO_FAST,
                    issuedAt,
                    1,
                    artifacts.setDigest(),
                    capabilityKeys.getPrivate());
        } else {
            handoff = persistentActivation.policyPublication().head().snapshot();
            if (!Arrays.equals(identity.policyScopeDigest(), persistentActivation.policyScopeDigest())
                    || !Arrays.equals(identity.policyScopeDigest(), handoff.policyScopeDigest())) {
                throw new IllegalStateException("persistent policy is not scoped to the exact native canary");
            }
        }
        final PulsarMetadata metadata = new PulsarMetadata(
                Bytes.utf8("native-smoke-key"),
                PulsarMetadata.KeyEncoding.UTF8,
                Bytes.utf8("native-smoke-ordering"),
                List.of(new PulsarMetadata.Property("native-smoke", "coordinator")));
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                identity.destination().ref(),
                new RetryPolicyRef(
                        Bytes.utf8("native-smoke-retry"), 1, Bytes.sha256(Bytes.utf8("native-smoke-retry-semantic"))),
                deliverAtEpochMs,
                deliverAtEpochMs + 30_000L,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                payload,
                null,
                AdapterMetadata.pulsar(metadata),
                null,
                nowEpochMs - 10_000L,
                NativeDeliveryPolicy.ALLOW_AUTO_FAST_AND_MANAGED_HANDOFF);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand managed = PreparedCommand.schedule(shard, intent, deliverAtEpochMs + 30_000L);
        final PreparedSubmission submission;
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(selectionStatePath(persistentActivation)), shard, Clock.systemUTC())) {
            submission = service.prepareAutoFast(AutoFastSchedule.withNativeCandidate(
                    managed,
                    new AutoFastSchedule.NativeCandidate(
                            identity.destination(),
                            identity.capability(),
                            target,
                            PersistentStagingNativeCanaryIdentity.PHYSICAL_PARTITION,
                            payload,
                            metadata,
                            intent.eventTimeEpochMs(),
                            deliverAtEpochMs,
                            PersistentStagingNativeCanaryIdentity.MAX_HANDOFF_LEAD_MS,
                            capabilitySnapshot,
                            capabilityKeys.getPublic(),
                            true,
                            handoff)));
        }
        if (submission.isManaged() || !submission.isNativeRecordReady()) {
            throw new IllegalStateException("actual AUTO_FAST selection did not produce a native prepared record");
        }
        final PhysicalSendActivationGate activationGate = persistentActivation == null
                ? PhysicalSendActivationGate.disposableLocal(
                        DeploymentSafetyGate.GateBStatus.PASS,
                        new DisposableEnvironmentAttestation(
                                "native-smoke-disposable",
                                physicalTopic,
                                true,
                                true,
                                true,
                                true,
                                Bytes.sha256(Bytes.utf8("native-smoke-disposable-evidence"))),
                        artifacts)
                : persistentActivation.physicalGate();
        final PulsarNativePreparedRecordValidator validator = persistentActivation == null
                ? new PulsarNativePreparedRecordValidator(
                        resource, capabilityKeys.getPublic(), Clock.systemUTC(), null, activationGate)
                : new PulsarNativePreparedRecordValidator(
                        resource,
                        capabilityKeys.getPublic(),
                        Clock.systemUTC(),
                        null,
                        activationGate,
                        persistentActivation::requireFrozenHandoffPolicy);
        final PulsarPreparedRecord expectedRecord = validator.materialize(submission);
        final PulsarCommandTransportKey transportKey = new PulsarCommandTransportKey(
                target.authenticatedClusterId(),
                target.physicalTopic(),
                new Bytes32(target.resourceIncarnation()),
                target.physicalTopicCreationTimestamp(),
                0,
                new CredentialBindingKey(
                        capabilitySnapshot.credentialBindingGeneration(),
                        new Digest32(capabilitySnapshot.credentialBindingDigest()),
                        new Digest32(capabilitySnapshot.resolvedCredentialFingerprintDigest())));
        final AuthenticatedTenantContext tenant = new AuthenticatedTenantContext(
                identity.tenantRouteScopeDigest(),
                Bytes.sha256(Bytes.utf8("native-smoke-routing")),
                identity.principalScopeDigest());

        try (PulsarClient client =
                        PulsarClientArtifactClientBuilder.builder(serviceUrl).build();
                Producer<byte[]> producer = PulsarClientArtifactProducerFactory.create(
                        client, CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, producerName)) {
            final PulsarClientArtifactSendTransport recordSender = new PulsarClientArtifactSendTransport(
                    producer, CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, 0, true);
            final AtomicReference<PulsarSendResult> physicalResult = new AtomicReference<>();
            final com.nereusstream.delay.adapter.PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport
                    nativeSender =
                            new com.nereusstream.delay.adapter.PinnedPulsarNativeSubmissionAdapter
                                    .PulsarNativeSendTransport() {
                                @Override
                                public CompletableFuture<PulsarSendResult> send(
                                        final com.nereusstream.delay.adapter.PulsarNativeSendRequest ignored) {
                                    return CompletableFuture.failedFuture(
                                            new AssertionError("coordinator used the envelope-only native sender"));
                                }

                                @Override
                                public CompletableFuture<PulsarSendResult> sendPreparedRecord(
                                        final PulsarPreparedRecord record,
                                        final ArtifactGenerationSet candidateArtifacts) {
                                    if (!expectedRecord.equals(record) || !artifacts.equals(candidateArtifacts)) {
                                        return CompletableFuture.failedFuture(new AssertionError(
                                                "coordinator changed the validated prepared record"));
                                    }
                                    return recordSender
                                            .sendPreparedRecord(record, candidateArtifacts)
                                            .toCompletableFuture()
                                            .thenApply(result -> {
                                                physicalResult.set(result);
                                                return result;
                                            });
                                }
                            };
            final ProductionPulsarSendTransport transport = new ProductionPulsarSendTransport(
                    transportKey,
                    new ProductionPulsarSendTransport.Configuration(true, true, true, "source-locked-p1-smoke"),
                    ignored -> CompletableFuture.failedFuture(
                            new AssertionError("coordinator selected the managed sender for a native branch")),
                    nativeSender,
                    validator);
            final CommandTransportRegistry transports = key -> transportKey.equals(key) ? transport : null;
            final DefaultSubmissionCoordinator coordinator = new DefaultSubmissionCoordinator(
                    new RouteBoundSubmissionTransportPlanResolver(routes(), System::currentTimeMillis, validator),
                    transports,
                    SubmissionOutcomeProjectorRegistry.of(new PulsarNativeSubmissionOutcomeProjector()));
            final var outcome =
                    coordinator.submit(tenant, submission).toCompletableFuture().get(20, TimeUnit.SECONDS);
            if (outcome.kind() != SubmissionOutcomeKind.NATIVE_RECEIPT) {
                throw new IllegalStateException("native coordinator did not produce a receipt: " + outcome.kind());
            }
            final PulsarSendResult result = physicalResult.get();
            if (result == null) {
                throw new IllegalStateException("native coordinator did not reach the prepared-record sender");
            }
            if (result.disposition() != PulsarSendResult.Disposition.PERSISTED
                    || result.responseEvidenceBytes() == null
                    || result.brokerEntryTimestampEpochMs() < 0) {
                throw new IllegalStateException("native P1 SEND did not return persisted typed evidence: " + result);
            }
            final PublishEvidence evidence = PublishEvidence.decode(result.responseEvidenceBytes());
            com.nereusstream.delay.adapter.PulsarSendAckEvidence.requireExactRecordBinding(
                    evidence,
                    expectedRecord,
                    artifacts,
                    result.ledgerId(),
                    result.entryId(),
                    result.batchIndex(),
                    result.batchSize(),
                    result.brokerEntryTimestampEpochMs());
            final AckFields ack = AckFields.from(evidence);

            final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
            try (GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(
                    client, guard, physicalTopic, "nereus-delay-p1-native-smoke-sub", SubscriptionType.Shared)) {
                final Message<byte[]> early = consumer.receive(1, TimeUnit.SECONDS);
                if (early != null) {
                    throw new IllegalStateException("native deliverAt released a message before its exact target time");
                }
                final Message<byte[]> delivered = consumer.receive(12, TimeUnit.SECONDS);
                if (delivered == null || !Arrays.equals(payload, delivered.getValue())) {
                    throw new IllegalStateException("native deliverAt payload was not delivered exactly");
                }
                consumer.acknowledge(delivered);
            }
            writePersistentEvidence(
                    persistentActivation, identity, handoff, expectedRecord, result, ack, deliverAtEpochMs);
            System.out.println("Pulsar native coordinator typed-evidence smoke passed: topic="
                    + physicalTopic
                    + ", deliverAt="
                    + deliverAtEpochMs
                    + ", ledger="
                    + result.ledgerId()
                    + ", entry="
                    + result.entryId()
                    + ", sequence="
                    + evidenceSequence(evidence));
        }
    }

    private static Path selectionStatePath(final PersistentStagingActivation.Loaded persistentActivation)
            throws Exception {
        if (persistentActivation == null) {
            return Files.createTempDirectory("nereus-delay-native-selection-");
        }
        final String configured = System.getenv("NEREUS_DELAY_PERSISTENT_STAGING_NATIVE_STATE_DIR");
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException("persistent native selection requires a durable state directory");
        }
        final Path path = Path.of(configured).toAbsolutePath().normalize();
        Files.createDirectories(path);
        return path;
    }

    private static void writePersistentEvidence(
            final PersistentStagingActivation.Loaded activation,
            final PersistentStagingNativeCanaryIdentity.Identity identity,
            final HandoffPolicySnapshot handoff,
            final PulsarPreparedRecord record,
            final PulsarSendResult result,
            final AckFields ack,
            final long deliverAtEpochMs)
            throws Exception {
        if (activation == null) {
            return;
        }
        final String configured = System.getenv("NEREUS_DELAY_PERSISTENT_STAGING_NATIVE_EVIDENCE");
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException("persistent native canary requires an immutable evidence path");
        }
        final JsonObject value = new JsonObject();
        value.addProperty("schema", "nereus-delay.persistent-native-canary");
        value.addProperty("schemaGeneration", 1);
        value.addProperty("environmentId", activation.environmentId());
        value.addProperty("candidateCommit", activation.candidateCommit());
        value.addProperty("nativeAdmission", 1);
        value.addProperty("nativeSend", 1);
        value.addProperty("handedOff", 0);
        value.addProperty("targetPhysicalTopic", identity.target().physicalTopic());
        value.addProperty("policyScopeDigest", Bytes.hex(identity.policyScopeDigest()));
        value.addProperty("policyGeneration", Long.toUnsignedString(handoff.generation()));
        value.addProperty("policySnapshotDigest", Bytes.hex(handoff.snapshotDigest()));
        value.addProperty("deliverAtEpochMs", deliverAtEpochMs);
        value.addProperty("ledgerId", result.ledgerId());
        value.addProperty("entryId", result.entryId());
        value.addProperty("batchIndex", result.batchIndex());
        value.addProperty("batchSize", result.batchSize());
        value.addProperty("sequenceId", ack.actualSequenceId());
        value.addProperty("preparedRecordHash", Bytes.hex(record.preparedRecordHash()));
        value.addProperty("sendCommandSha256", Bytes.hex(ack.sendCommandSha256()));
        value.addProperty("authenticatedResponseCommandSha256", Bytes.hex(ack.authenticatedResponseSha256()));
        value.addProperty("verdict", "PASS");
        PersistentStagingEvidence.writeNew(
                Path.of(configured), (GSON.toJson(value) + "\n").getBytes(StandardCharsets.UTF_8));
    }

    private static long evidenceSequence(final PublishEvidence evidence) {
        final var fields = new com.nereusstream.delay.protocol.CanonicalProtobuf.Reader(evidence.branch());
        long sequence = -1;
        while (fields.hasRemaining()) {
            final var field = fields.next();
            if (field.number() == 13) {
                sequence = field.unsignedValue();
            }
        }
        if (sequence < 0) {
            throw new IllegalStateException("native ACK evidence did not contain a sequence");
        }
        return sequence;
    }

    private static RouteSnapshotProvider routes() {
        return new RouteSnapshotProvider() {
            @Override
            public com.nereusstream.delay.protocol.RouteSnapshot activeForNewSchedule(
                    final AuthenticatedTenantContext context, final RouteSelectionHint hint) {
                return null;
            }

            @Override
            public com.nereusstream.delay.protocol.RouteSnapshot exact(
                    final RouteIncarnation incarnation, final AuthenticatedTenantContext context) {
                return null;
            }

            @Override
            public long publishedRevision() {
                return 1;
            }
        };
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(
                time,
                time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("native-smoke-clock-" + time),
                1,
                time,
                time,
                Bytes.sha256(Bytes.utf8("native-smoke-time-" + time)),
                0,
                null);
    }

    private static void createTopic(final HttpClient client, final String adminUrl, final String topic)
            throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic;
        final String body = "{\"nereus.resource.guard.version\":\"1\","
                + "\"nereus.resource.incarnation\":\""
                + Base64.getUrlEncoder().withoutPadding().encodeToString(INCARNATION)
                + "\",\"nereus.resource.created-at\":\""
                + Long.toUnsignedString(CREATION_TIMESTAMP)
                + "\"}";
        for (int attempt = 0; attempt < 40; attempt++) {
            final HttpResponse<String> response = request(client, path, "PUT", body);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return;
            }
            if (response.statusCode() != 409 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw failure("create native smoke topic", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("native smoke topic create did not converge: " + topic);
    }

    private static void deleteTopicIfPresent(final HttpClient client, final String adminUrl, final String topic) {
        try {
            final HttpResponse<String> response = request(
                    client, adminUrl + "/admin/v2/persistent/public/default/" + topic + "?force=true", "DELETE", "");
            if (response.statusCode() >= 300 && response.statusCode() != 404) {
                System.err.println("P1 native smoke cleanup could not delete topic: " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("P1 native smoke cleanup failed: " + failure.getMessage());
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

    private static byte[] digest(final int seed) {
        final byte[] result = new byte[32];
        Arrays.fill(result, (byte) seed);
        return result;
    }

    private record AckFields(long actualSequenceId, byte[] sendCommandSha256, byte[] authenticatedResponseSha256) {
        private static AckFields from(final PublishEvidence evidence) {
            final var reader = new com.nereusstream.delay.protocol.CanonicalProtobuf.Reader(evidence.branch());
            final com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field[] fields =
                    new com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field[22];
            int count = 0;
            while (reader.hasRemaining()) {
                if (count >= fields.length) {
                    throw new IllegalArgumentException("native ACK has too many fields");
                }
                fields[count++] = reader.next();
            }
            if (count != fields.length) {
                throw new IllegalArgumentException("native ACK has incomplete fields");
            }
            for (int index = 0; index < fields.length; index++) {
                if (fields[index].number() != index + 1) {
                    throw new IllegalArgumentException("native ACK field order is not canonical");
                }
            }
            return new AckFields(fields[12].unsignedValue(), fields[18].rawValue(), fields[19].rawValue());
        }

        @Override
        public byte[] sendCommandSha256() {
            return Bytes.copy(sendCommandSha256);
        }

        @Override
        public byte[] authenticatedResponseSha256() {
            return Bytes.copy(authenticatedResponseSha256);
        }
    }
}
