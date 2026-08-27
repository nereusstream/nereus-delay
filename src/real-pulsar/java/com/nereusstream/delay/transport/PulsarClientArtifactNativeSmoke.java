package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PulsarSendResult;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentity;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarKey;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarRecordTemplate;
import com.nereusstream.delay.protocol.PulsarReservedProperties;
import com.nereusstream.delay.protocol.PulsarSequenceAuthority;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.protocol.ReservedPublishMetadata;
import com.nereusstream.delay.protocol.ResolvedPayload;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicResourceGuard;

/** Real-service smoke for the final native record encoder and deliverAt path. */
public final class PulsarClientArtifactNativeSmoke {
    private static final String CLUSTER = "standalone";
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
        final HttpClient admin = HttpClient.newHttpClient();
        createTopic(admin, adminUrl, topic);
        try {
            runNativeSendAndRead(serviceUrl, physicalTopic);
        } finally {
            deleteTopicIfPresent(admin, adminUrl, topic);
        }
    }

    private static void runNativeSendAndRead(final String serviceUrl, final String physicalTopic) throws Exception {
        final byte[] payload = Bytes.utf8("nereus-delay-native-p1-smoke");
        final long deliverAtEpochMs = System.currentTimeMillis() + 6_000L;
        final RouteIncarnation routeIncarnation = RouteIncarnation.random();
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(routeIncarnation, 0));
        final byte[] publishAttemptId = digest(31);
        final byte[] nativeDeliveryId = digest(32);
        final ArtifactGenerationSet artifacts = ArtifactGenerationSet.current(
                1, PulsarSourceLock.digest(), Bytes.sha256(Bytes.utf8("native-real-smoke-schema")));
        final String producerName = "nereus-delay-p1-native-smoke";
        final PulsarBrokerResourceIdentity target =
                new PulsarBrokerResourceIdentity(CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP);
        final ReservedPublishMetadata reserved = new ReservedPublishMetadata(
                routeIncarnation,
                0,
                messageId,
                1,
                publishAttemptId,
                Bytes.sha256(Bytes.utf8("native-smoke-destination")),
                Bytes.sha256(Bytes.utf8("native-smoke-capability")),
                deliverAtEpochMs,
                DeliveryMode.MANAGED);
        final PulsarRecordTemplate template = new PulsarRecordTemplate(
                BrokerResourceIdentity.pulsar(target),
                0,
                PulsarKey.none(),
                null,
                List.<PulsarMetadata.Property>of(),
                System.currentTimeMillis(),
                reserved,
                DeliveryContract.PULSAR_NATIVE_DELIVERY,
                deliverAtEpochMs,
                com.nereusstream.delay.protocol.PayloadForPublish.inline(payload),
                artifacts.setDigest());
        final byte[] preparedIdentityHash = Bytes.sha256(Bytes.utf8("native-smoke-prepared-identity"));
        final PulsarPreparedRecord record = new PulsarPreparedRecord(
                template,
                template.recordTemplateHash(),
                ResolvedPayload.of(payload),
                PulsarSequenceAuthority.producerAssigned(),
                ExternalDeliveryIdentity.nativeDelivery(nativeDeliveryId),
                preparedIdentityHash,
                PulsarReservedProperties.all(reserved, publishAttemptId, preparedIdentityHash),
                artifacts.setDigest());

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
                Producer<byte[]> producer = PulsarClientArtifactProducerFactory.create(
                        client, CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, producerName);
                PulsarClientArtifactSendTransport transport = new PulsarClientArtifactSendTransport(
                        producer, CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, 0, true)) {
            final PulsarSendResult result = transport
                    .sendPreparedRecord(record, artifacts)
                    .toCompletableFuture()
                    .get(20, TimeUnit.SECONDS);
            if (result.disposition() != PulsarSendResult.Disposition.PERSISTED
                    || result.responseEvidenceBytes() == null
                    || result.brokerEntryTimestampEpochMs() < 0) {
                throw new IllegalStateException("native P1 SEND did not return persisted typed evidence: " + result);
            }
            final PublishEvidence evidence = PublishEvidence.decode(result.responseEvidenceBytes());
            com.nereusstream.delay.adapter.PulsarSendAckEvidence.requireExactRecordBinding(
                    evidence,
                    record,
                    artifacts,
                    result.ledgerId(),
                    result.entryId(),
                    result.batchIndex(),
                    result.batchSize(),
                    result.brokerEntryTimestampEpochMs());

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
            System.out.println("Pulsar native typed-evidence smoke passed: topic="
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
        final HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(path)).header("Content-Type", "application/json");
        final HttpRequest request = "DELETE".equals(method)
                ? builder.DELETE().build()
                : builder.PUT(HttpRequest.BodyPublishers.ofString(body)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
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
}
