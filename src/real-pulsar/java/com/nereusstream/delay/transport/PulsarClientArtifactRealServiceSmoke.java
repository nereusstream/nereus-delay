package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PulsarSendRequest;
import com.nereusstream.delay.adapter.PulsarSendResult;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardException;

/** Real-service smoke for the source-locked P1 producer and broker guard. */
public final class PulsarClientArtifactRealServiceSmoke {
    private static final String CLUSTER = PulsarClientArtifactClientBuilder.clusterId();
    private static final byte[] OLD_INCARNATION = digest(17);
    private static final byte[] NEW_INCARNATION = digest(29);
    private static final long OLD_CREATION_TIMESTAMP = 1001L;
    private static final long NEW_CREATION_TIMESTAMP = 1002L;

    private PulsarClientArtifactRealServiceSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <topic>");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String topic = arguments[2];
        final String physicalTopic = "persistent://public/default/" + topic;
        final HttpClient admin = HttpClient.newHttpClient();
        createTopic(admin, adminUrl, topic, OLD_INCARNATION, OLD_CREATION_TIMESTAMP);

        try (PulsarClient client = PulsarClientArtifactClientBuilder.builder(serviceUrl).build()) {
            final Producer<byte[]> oldProducer = PulsarClientArtifactProducerFactory.create(
                    client,
                    CLUSTER,
                    OLD_INCARNATION,
                    physicalTopic,
                    OLD_CREATION_TIMESTAMP,
                    "nereus-delay-p1-real-smoke-old");
            final PulsarClientArtifactSendTransport oldTransport = new PulsarClientArtifactSendTransport(
                    oldProducer, CLUSTER, OLD_INCARNATION, physicalTopic, OLD_CREATION_TIMESTAMP, 0);
            final PulsarSendRequest oldRequest = request(OLD_INCARNATION, physicalTopic, OLD_CREATION_TIMESTAMP, 1);
            final PulsarSendResult first =
                    oldTransport.send(oldRequest).toCompletableFuture().join();
            requirePersisted(first, "initial guarded P1 send");

            oldTransport.close();
            deleteTopic(admin, adminUrl, topic);
            createTopic(admin, adminUrl, topic, NEW_INCARNATION, NEW_CREATION_TIMESTAMP);
            final TopicResourceGuardException stale = requireStaleProducerRejected(client, physicalTopic);

            final Producer<byte[]> replacementProducer = PulsarClientArtifactProducerFactory.create(
                    client,
                    CLUSTER,
                    NEW_INCARNATION,
                    physicalTopic,
                    NEW_CREATION_TIMESTAMP,
                    "nereus-delay-p1-real-smoke-new");
            final PulsarClientArtifactSendTransport replacementTransport = new PulsarClientArtifactSendTransport(
                    replacementProducer, CLUSTER, NEW_INCARNATION, physicalTopic, NEW_CREATION_TIMESTAMP, 0);
            final PulsarSendResult replacement = replacementTransport
                    .send(request(NEW_INCARNATION, physicalTopic, NEW_CREATION_TIMESTAMP, 2))
                    .toCompletableFuture()
                    .join();
            requirePersisted(replacement, "replacement guarded P1 send");
            replacementTransport.close();
            System.out.println("P1 Delay real-service smoke passed: initial=" + first.disposition()
                    + ", stale=DEFINITIVELY_NOT_PERSISTED, replacement=" + replacement.disposition()
                    + ", staleGuard=" + stale.expectedGuard());
        } finally {
            deleteTopicIfPresent(admin, adminUrl, topic);
        }
    }

    private static TopicResourceGuardException requireStaleProducerRejected(
            final PulsarClient client, final String physicalTopic) throws Exception {
        final TopicResourceGuard expected = new TopicResourceGuard(CLUSTER, OLD_INCARNATION, OLD_CREATION_TIMESTAMP);
        try {
            final Producer<byte[]> staleProducer = PulsarClientArtifactProducerFactory.create(
                    client,
                    CLUSTER,
                    OLD_INCARNATION,
                    physicalTopic,
                    OLD_CREATION_TIMESTAMP,
                    "nereus-delay-p1-real-smoke-stale");
            staleProducer.close();
            throw new IllegalStateException("stale P1 producer was created after topic replacement");
        } catch (PulsarClientException failure) {
            final TopicResourceGuardException guardFailure = findGuardFailure(failure);
            if (guardFailure == null
                    || !expected.equals(guardFailure.expectedGuard())
                    || !guardFailure.definitelyNotPersisted()
                    || guardFailure.responseEvidence().isPresent()) {
                throw new IllegalStateException(
                        "stale P1 producer rejection was not the expected typed boundary", failure);
            }
            return guardFailure;
        }
    }

    private static TopicResourceGuardException findGuardFailure(final Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof TopicResourceGuardException guardFailure) {
                return guardFailure;
            }
            current = current.getCause();
        }
        return null;
    }

    private static PulsarSendRequest request(
            final byte[] incarnation, final String topic, final long creationTimestamp, final int sequence) {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        return new PulsarSendRequest(
                CLUSTER,
                incarnation,
                topic,
                creationTimestamp,
                0,
                CommandId.random(shard),
                Bytes.utf8("nereus-delay-p1-real-smoke-" + sequence));
    }

    private static void requirePersisted(final PulsarSendResult result, final String label) {
        if (result.disposition() != PulsarSendResult.Disposition.PERSISTED
                || result.ledgerId() < 0
                || result.entryId() < 0
                || result.brokerEntryTimestampEpochMs() < 0
                || result.requestEvidenceBytes() == null
                || result.responseEvidenceBytes() == null) {
            throw new IllegalStateException(
                    label + " was not an evidenced persistence: " + result.disposition() + "/" + result.stableCode());
        }
    }

    private static void createTopic(
            final HttpClient client,
            final String adminUrl,
            final String topic,
            final byte[] incarnation,
            final long creationTimestamp)
            throws Exception {
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
            if (response.statusCode() != 409 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw failure("create topic", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("topic create did not converge: " + topic);
    }

    private static void deleteTopic(final HttpClient client, final String adminUrl, final String topic)
            throws Exception {
        final HttpResponse<String> response =
                request(client, adminUrl + "/admin/v2/persistent/public/default/" + topic, "DELETE", "");
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw failure("delete topic", response);
        }
    }

    private static void deleteTopicIfPresent(final HttpClient client, final String adminUrl, final String topic) {
        try {
            final HttpResponse<String> response = request(
                    client, adminUrl + "/admin/v2/persistent/public/default/" + topic + "?force=true", "DELETE", "");
            if (response.statusCode() >= 300 && response.statusCode() != 404) {
                System.err.println("P1 smoke cleanup could not delete topic: " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("P1 smoke cleanup failed: " + failure.getMessage());
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
