package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.DestinationPublishRequest;
import io.nereusstream.delay.adapter.DestinationPublishResult;
import io.nereusstream.delay.adapter.PinnedPulsarDestinationAdapter;
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
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TopicResourceGuard;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

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
            final Producer<byte[]> producer = PulsarClientArtifactProducerFactory.create(client, CLUSTER, INCARNATION,
                    physicalTopic, CREATION_TIMESTAMP, producerName);
            try (PulsarClientArtifactDestinationTransport transport = new PulsarClientArtifactDestinationTransport(
                    producer, CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, 0,
                    Bytes.sha256(Bytes.utf8(producerName)))) {
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
