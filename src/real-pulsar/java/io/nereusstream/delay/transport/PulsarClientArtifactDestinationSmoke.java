package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.DestinationPublishRequest;
import io.nereusstream.delay.adapter.DestinationPublishResult;
import io.nereusstream.delay.adapter.PinnedPulsarDestinationAdapter;
import io.nereusstream.delay.adapter.PulsarDestinationRequest;
import io.nereusstream.delay.adapter.PulsarSendAckEvidence;
import io.nereusstream.delay.adapter.PulsarTargetResource;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.PublishEvidenceKindV1;
import io.nereusstream.delay.protocol.PublishEvidenceV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.GuardedMessageId;
import org.apache.pulsar.client.api.GuardedSendSuccessEvidence;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Real-service smoke for source-bound Pulsar destination publication evidence. */
public final class PulsarClientArtifactDestinationSmoke {
    private static final String CLUSTER = "standalone";
    private static final byte[] INCARNATION = digest(17);
    private static final long CREATION_TIMESTAMP = 1001L;

    private PulsarClientArtifactDestinationSmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <topic>");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String topic = arguments[2];
        final String physicalTopic = "persistent://public/default/" + topic;
        final HttpClient admin = HttpClient.newHttpClient();
        createTopic(admin, adminUrl, topic, INCARNATION, CREATION_TIMESTAMP);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
            final String producerName = "nereus-delay-p1-destination-" + topic;
            final byte[] producerNameHash = Bytes.sha256(Bytes.utf8(producerName));
            final boolean responseLoss = hasResponseLoss();
            final AtomicReference<GuardedMessageId> responseLostMessage = new AtomicReference<>();
            final AtomicBoolean responseEvidenceResolved = new AtomicBoolean();
            final Producer<byte[]> producer = PulsarClientArtifactProducerFactory.create(client, CLUSTER, INCARNATION,
                    physicalTopic, CREATION_TIMESTAMP, producerName);
            final Producer<byte[]> transportProducer = responseLoss
                    ? responseLossProducer(producer, responseLostMessage) : producer;
            final PulsarClientArtifactDestinationTransport.PublishEvidenceProvider evidenceProvider = responseLoss
                    ? (request, preparedHash, failure) -> resolveResponseLoss(request, preparedHash,
                    producerNameHash, responseLostMessage, responseEvidenceResolved)
                    : null;
            try (PulsarClientArtifactDestinationTransport transport = new PulsarClientArtifactDestinationTransport(
                    transportProducer, CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, 0,
                    producerNameHash, evidenceProvider)) {
                try (PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(
                        new PulsarTargetResource(CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, 0),
                        transport)) {
                    final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
                    final DestinationPublishRequest request = request(shard, topic);
                    final PulsarSourcePosition source = new PulsarSourcePosition(shard, digest(19),
                            "persistent://public/default/source-" + topic, 7, 11, 0, 1,
                            PulsarSourcePosition.EntryKind.NON_BATCH, 1_002);
                    final byte[] preparedHash = Bytes.sha256(Bytes.utf8("prepared-" + topic));
                    final DestinationPublishResult result = adapter.publish(request, source, preparedHash)
                            .toCompletableFuture().get(20, TimeUnit.SECONDS);
                    requireTypedPublished(result, request, "source-bound Pulsar destination publish");
                    requirePayload(client, physicalTopic, request.payload());
                    if (responseLoss) {
                        if (!responseEvidenceResolved.get()) {
                            throw new IllegalStateException("Pulsar response-loss provider did not resolve evidence");
                        }
                        System.out.println("Pulsar committed response-loss smoke passed: real SEND persisted the "
                                + "exact payload, the local response was discarded, and typed PULSAR_SEND_ACK "
                                + "evidence resolved PUBLISHED");
                        return;
                    }
                    final PublishEvidenceV1 evidence = PublishEvidenceV1.decode(result.evidence());
                    System.out.println("Pulsar destination typed-evidence smoke passed: topic=" + physicalTopic
                            + ", ledger=" + branchNumber(evidence, 3) + ", entry=" + branchNumber(evidence, 4)
                            + ", batchIndex=" + branchNumber(evidence, 5) + ", sequence=" + branchNumber(evidence, 8)
                            + ", brokerPersistenceTime=" + result.brokerPersistenceTimeEpochMs());
                }
            }
        } finally {
            deleteTopicIfPresent(admin, adminUrl, topic);
        }
    }

    private static boolean hasResponseLoss() {
        return "1".equals(System.getenv("NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS"));
    }

    private static Optional<PulsarClientArtifactDestinationTransport.ResolvedPublish> resolveResponseLoss(
            final PulsarDestinationRequest request, final byte[] preparedPublishHash,
            final byte[] producerNameHash, final AtomicReference<GuardedMessageId> responseLostMessage,
            final AtomicBoolean responseEvidenceResolved) {
        final GuardedMessageId messageId = responseLostMessage.get();
        if (messageId == null) {
            return Optional.empty();
        }
        final TopicResourceGuard expectedGuard = new TopicResourceGuard(request.authenticatedClusterId(),
                request.resourceIncarnation(), request.physicalTopicCreationTimestamp());
        if (!expectedGuard.equals(messageId.resourceGuard()) || !request.physicalTopic().equals(messageId.physicalTopic())
                || request.partition() != messageId.partition() || !(messageId instanceof MessageIdAdv advanced)
                || advanced.getLedgerId() < 0 || advanced.getEntryId() < 0
                || advanced.getPartitionIndex() != request.partition()) {
            return Optional.empty();
        }
        final GuardedSendSuccessEvidence evidence = messageId.responseEvidence();
        final TopicResourceGuardAttestation expectedAttestation = new TopicResourceGuardAttestation(
                expectedGuard, request.physicalTopic(), request.partition());
        if (evidence == null || !expectedAttestation.equals(evidence.attestation())
                || evidence.ledgerId() != advanced.getLedgerId() || evidence.entryId() != advanced.getEntryId()
                || evidence.brokerEntryTimestamp() != messageId.brokerEntryTimestamp()) {
            return Optional.empty();
        }
        final int rawBatchIndex = advanced.getBatchIndex();
        final int rawBatchSize = advanced.getBatchSize();
        final int normalizedBatchIndex = rawBatchIndex < 0 ? 0 : rawBatchIndex;
        if (rawBatchIndex >= 0 && (rawBatchSize <= 0
                || Integer.compareUnsigned(rawBatchIndex, rawBatchSize) >= 0)) {
            return Optional.empty();
        }
        final PublishEvidenceV1 typed = PulsarSendAckEvidence.published(request, preparedPublishHash,
                producerNameHash, advanced.getLedgerId(), advanced.getEntryId(), normalizedBatchIndex,
                evidence.brokerEntryTimestamp(), evidence.sequenceId(), evidence.authenticatedResponseCommandSha256());
        typed.requireBusinessMutation(request.publishAttemptId(), true);
        responseEvidenceResolved.set(true);
        return Optional.of(new PulsarClientArtifactDestinationTransport.ResolvedPublish(
                typed, evidence.brokerEntryTimestamp()));
    }

    @SuppressWarnings("unchecked")
    private static Producer<byte[]> responseLossProducer(final Producer<byte[]> delegate,
                                                          final AtomicReference<GuardedMessageId> responseLostMessage) {
        return (Producer<byte[]>) Proxy.newProxyInstance(
                PulsarClientArtifactDestinationSmoke.class.getClassLoader(), new Class<?>[]{Producer.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("newMessage") && method.getParameterCount() == 0) {
                        final TypedMessageBuilder<byte[]> builder = (TypedMessageBuilder<byte[]>) invoke(
                                delegate, method, arguments);
                        return responseLossBuilder(builder, responseLostMessage);
                    }
                    return invoke(delegate, method, arguments);
                });
    }

    @SuppressWarnings("unchecked")
    private static TypedMessageBuilder<byte[]> responseLossBuilder(
            final TypedMessageBuilder<byte[]> delegate, final AtomicReference<GuardedMessageId> responseLostMessage) {
        return (TypedMessageBuilder<byte[]>) Proxy.newProxyInstance(
                PulsarClientArtifactDestinationSmoke.class.getClassLoader(), new Class<?>[]{TypedMessageBuilder.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("value") && method.getParameterCount() == 1) {
                        invoke(delegate, method, arguments);
                        return proxy;
                    }
                    if (method.getName().equals("sendAsync") && method.getParameterCount() == 0) {
                        final CompletableFuture<MessageId> sent = (CompletableFuture<MessageId>) invoke(
                                delegate, method, arguments);
                        return sent.thenCompose(messageId -> {
                            if (!(messageId instanceof GuardedMessageId guarded)) {
                                return CompletableFuture.failedFuture(new IllegalStateException(
                                        "Pulsar response-loss wrapper observed an unguarded MessageId"));
                            }
                            responseLostMessage.set(guarded);
                            return CompletableFuture.failedFuture(new IllegalStateException(
                                    "simulated committed Pulsar SEND response loss"));
                        });
                    }
                    return invoke(delegate, method, arguments);
                });
    }

    private static Object invoke(final Object target, final java.lang.reflect.Method method,
                                 final Object[] arguments) throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    private static DestinationPublishRequest request(final ShardId shard, final String topic) {
        final long now = System.currentTimeMillis();
        return new DestinationPublishRequest(DestinationLaneId.derive(Bytes.utf8("destination-lane-" + topic)),
                Arrays.copyOf(digest(23), 16), DelayMessageId.random(shard), 0, digest(29), now, now,
                Bytes.utf8("pulsar-destination-payload-" + topic), new byte[0]);
    }

    private static void requireTypedPublished(final DestinationPublishResult result,
                                              final DestinationPublishRequest request,
                                              final String label) {
        if (result.disposition() != DestinationPublishResult.Disposition.PUBLISHED
                || result.evidence() == null || result.brokerPersistenceTimeEpochMs() < 0) {
            throw new IllegalStateException(label + " did not return PUBLISHED: "
                    + result.disposition() + "/" + result.stableCode());
        }
        final PublishEvidenceV1 evidence = PublishEvidenceV1.decode(result.evidence());
        if (evidence.evidenceKind() != PublishEvidenceKindV1.PULSAR_SEND_ACK
                || evidence.verificationStatus() != EvidenceVerificationStatusV1.VERIFIED_PUBLISHED) {
            throw new IllegalStateException(label + " returned the wrong evidence branch");
        }
        evidence.requireBusinessMutation(request.publishAttemptId(), true);
    }

    private static long branchNumber(final PublishEvidenceV1 evidence, final int number) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(evidence.branch());
        while (reader.hasRemaining()) {
            final CanonicalProtobuf.Reader.Field field = reader.next();
            if (field.number() == number) {
                return field.unsignedValue();
            }
        }
        throw new IllegalStateException("Pulsar SEND ACK branch is missing field " + number);
    }

    private static void requirePayload(final PulsarClient client, final String physicalTopic,
                                       final byte[] expectedPayload) throws Exception {
        final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
        final GuardedConsumer<byte[]> guarded = PulsarClientArtifactSourceConsumerFactory.create(client, guard,
                physicalTopic, "nereus-delay-p1-destination-evidence-" + physicalTopic.hashCode());
        try {
            final Message<byte[]> message = guarded.receive(15, TimeUnit.SECONDS);
            if (message == null || !Arrays.equals(expectedPayload, message.getValue())) {
                throw new IllegalStateException("typed destination publish payload was not read back exactly");
            }
            guarded.acknowledge(message);
        } finally {
            guarded.close();
        }
    }

    private static void createTopic(final HttpClient client, final String adminUrl, final String topic,
                                    final byte[] incarnation, final long creationTimestamp) throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic;
        final String body = "{\"nereus.resource.guard.version\":\"1\","
                + "\"nereus.resource.incarnation\":\""
                + Base64.getUrlEncoder().withoutPadding().encodeToString(incarnation) + "\","
                + "\"nereus.resource.created-at\":\""
                + Long.toUnsignedString(creationTimestamp) + "\"}";
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
                System.err.println("P1 destination cleanup could not delete topic: " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("P1 destination cleanup failed: " + failure.getMessage());
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

    private static byte[] digest(final int seed) {
        final byte[] result = new byte[32];
        Arrays.fill(result, (byte) seed);
        return result;
    }
}
