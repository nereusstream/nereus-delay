package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceAcknowledgement;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;

/** Real guarded append/replay/ACK smoke for one signed Pulsar System Mutation. */
public final class PulsarClientArtifactMutationSmoke {
    private static final String CLUSTER = PulsarClientArtifactClientBuilder.clusterId();
    private static final byte[] INCARNATION = digest(53);
    private static final long CREATION_TIMESTAMP = 3001L;

    private PulsarClientArtifactMutationSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <mutation-topic>");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String topic = arguments[2] + "-" + UUID.randomUUID();
        final String physicalTopic = "persistent://public/default/" + topic;
        final HttpClient admin = HttpClient.newHttpClient();
        createTopic(admin, adminUrl, topic);
        try {
            final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
            final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
            final SystemMutation mutation = timeFence(shard);
            final PulsarSourcePosition appendedPosition;
            try (PulsarClient client =
                    PulsarClientArtifactClientBuilder.builder(serviceUrl).build()) {
                final GuardedConsumer<byte[]> appendProofConsumer = PulsarClientArtifactSourceConsumerFactory.create(
                        client, guard, physicalTopic, "nereus-delay-mutation-append-" + UUID.randomUUID());
                try (PulsarClientArtifactShardLogMutationAppender appender =
                        new PulsarClientArtifactShardLogMutationAppender(
                                PulsarClientArtifactProducerFactory.create(
                                        client,
                                        CLUSTER,
                                        INCARNATION,
                                        physicalTopic,
                                        CREATION_TIMESTAMP,
                                        "nereus-delay-mutation-producer"),
                                appendProofConsumer,
                                shard,
                                CLUSTER,
                                INCARNATION,
                                physicalTopic,
                                CREATION_TIMESTAMP,
                                Duration.ofSeconds(15))) {
                    final var outcome = appender.append(mutation);
                    if (outcome.disposition()
                            != com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendDisposition.PERSISTED) {
                        throw new IllegalStateException(
                                "Pulsar mutation append was not persisted: " + outcome.disposition());
                    }
                    appendedPosition = (PulsarSourcePosition) outcome.sourcePosition();
                } finally {
                    closeNative(appendProofConsumer);
                }

                final GuardedConsumer<byte[]> recoveryNative = PulsarClientArtifactSourceConsumerFactory.create(
                        client, guard, physicalTopic, "nereus-delay-mutation-recovery-" + UUID.randomUUID());
                final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof =
                        PulsarClientArtifactRecoverySourcePositioner.seekAfter(
                                recoveryNative, guard, physicalTopic, shard, Optional.empty(), Duration.ofSeconds(5));
                final SourceAssignment assignment = new SourceAssignment(
                        shard,
                        Bytes.sha256(Bytes.utf8("pulsar-mutation-assignment")),
                        1,
                        PulsarActivationBarrier.empty(
                                shard,
                                INCARNATION,
                                physicalTopic,
                                proof.connectionGeneration(),
                                proof.attestationDigest()));
                try (PulsarClientArtifactRecoverySourceCursor recovery = new PulsarClientArtifactRecoverySourceCursor(
                        recoveryNative, guard, assignment, physicalTopic, Duration.ofMillis(250))) {
                    final SourceReplayEntry recovered = recovery.next();
                    if (!(recovered instanceof SourceReplayMutation replayed)) {
                        throw new IllegalStateException("Pulsar recovery returned a non-mutation entry: "
                                + recovered.getClass().getName());
                    }
                    if (!mutation.equals(replayed.mutation())) {
                        throw new IllegalStateException("Pulsar recovery mutation bytes changed");
                    }
                    requireReplayPosition(appendedPosition, replayed.position(), "recovery");
                    if (recovery.hasNext()) {
                        throw new IllegalStateException("Pulsar mutation recovery exposed an unexpected second entry");
                    }
                }

                final GuardedConsumer<byte[]> activeNative = PulsarClientArtifactSourceConsumerFactory.create(
                        client, guard, physicalTopic, "nereus-delay-mutation-source-" + UUID.randomUUID());
                try (PulsarClientArtifactSourceRecordConsumer source = new PulsarClientArtifactSourceRecordConsumer(
                        activeNative, guard, shard, physicalTopic, Duration.ofMillis(250))) {
                    final SourceRecordConsumer.PolledSourceRecord polled = poll(source);
                    if (!(polled.entry() instanceof SourceReplayMutation replayed)
                            || !mutation.equals(replayed.mutation())) {
                        throw new IllegalStateException(
                                "Pulsar active source did not expose the exact System Mutation");
                    }
                    requireReplayPosition(appendedPosition, replayed.position(), "active source");
                    final SourceAcknowledgement.AcknowledgementResult ack =
                            polled.acknowledgement().acknowledge(polled.entry(), null);
                    if (ack.disposition() != SourceAcknowledgement.Disposition.ACKED) {
                        throw new IllegalStateException(
                                "Pulsar mutation ACK was not durable: " + ack.disposition(), ack.failure());
                    }
                }
            }
            System.out.println("Pulsar Shard Log mutation append/replay/ACK smoke passed: physicalTopic="
                    + physicalTopic + ", ledger=" + appendedPosition.ledgerId() + ", entry="
                    + appendedPosition.entryId() + ", record=TIME_FENCE, guarded Producer, ordered mutation replay, "
                    + "ack receipt ACK");
        } finally {
            deleteTopicIfPresent(admin, adminUrl, topic);
        }
    }

    private static void requireReplayPosition(
            final PulsarSourcePosition appended, final SourcePosition replayedPosition, final String phase) {
        if (!(replayedPosition instanceof PulsarSourcePosition replayed)
                || !appended.shardId().equals(replayed.shardId())
                || !appended.sameSourceIdentity(replayed)
                || appended.ledgerId() != replayed.ledgerId()
                || appended.entryId() != replayed.entryId()
                || appended.normalizedBatchIndex() != replayed.normalizedBatchIndex()
                || appended.batchSize() != replayed.batchSize()
                || appended.brokerEntryTimestampEpochMs() != replayed.brokerEntryTimestampEpochMs()) {
            throw new IllegalStateException(
                    "Pulsar " + phase + " position changed: appended=" + appended + ", replayed=" + replayedPosition);
        }
    }

    private static SystemMutation timeFence(final ShardId shard) throws Exception {
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(
                3_000,
                3_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("pulsar-clock"),
                1,
                2,
                3,
                Bytes.sha256(Bytes.utf8("pulsar-mutation-evidence")),
                0,
                null);
        final int keyVersion = 1;
        final long closeThrough = 1_000;
        final byte[] proofId = Bytes.sha256(
                Bytes.utf8("nereus-delay-time-fence-proof\0"),
                shard.routeIncarnation().bytes(),
                Bytes.u32beBits(shard.partition()),
                Bytes.i64be(closeThrough),
                Bytes.u32beBits(keyVersion),
                Bytes.lp32(evidence.canonicalBytes()));
        final byte[] body = com.nereusstream.delay.protocol.CanonicalProtobuf.message(output -> {
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(
                    output, 1, new ShardSubject(shard).canonicalBytes());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(
                    output, 2, SystemMutationType.TIME_FENCE.wireValue());
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(output, 3, 9_000);
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(output, 10, closeThrough);
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32Bits(output, 11, keyVersion);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 12, proofId);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 13, evidence.canonicalBytes());
        });
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        return SystemMutation.signed(
                shard,
                SystemMutationType.TIME_FENCE,
                9_000,
                proofId,
                body,
                AuthorIdentity.fence(Bytes.utf8("pulsar-mutation-fence"), keyVersion)
                        .canonicalBytes(),
                keyVersion,
                keyPair.getPrivate());
    }

    private static SourceRecordConsumer.PolledSourceRecord poll(final PulsarClientArtifactSourceRecordConsumer source) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            final Optional<SourceRecordConsumer.PolledSourceRecord> polled = source.poll();
            if (polled.isPresent()) {
                return polled.orElseThrow();
            }
        }
        throw new IllegalStateException("Pulsar mutation source record did not become visible");
    }

    private static void createTopic(final HttpClient client, final String adminUrl, final String topic)
            throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic;
        final String body = "{\"nereus.resource.guard.version\":\"1\","
                + "\"nereus.resource.incarnation\":\""
                + Base64.getUrlEncoder().withoutPadding().encodeToString(INCARNATION) + "\","
                + "\"nereus.resource.created-at\":\""
                + Long.toUnsignedString(CREATION_TIMESTAMP) + "\"}";
        for (int attempt = 0; attempt < 40; attempt++) {
            final HttpResponse<String> response = request(client, path, "PUT", body);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return;
            }
            if (response.statusCode() != 409 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw new IllegalStateException(
                        "create topic failed with HTTP " + response.statusCode() + ": " + response.body());
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("topic create did not converge: " + topic);
    }

    private static void deleteTopicIfPresent(final HttpClient client, final String adminUrl, final String topic) {
        try {
            final HttpResponse<String> response = request(
                    client, adminUrl + "/admin/v2/persistent/public/default/" + topic + "?force=true", "DELETE", "");
            if (response.statusCode() >= 300 && response.statusCode() != 404) {
                System.err.println("Pulsar mutation smoke cleanup could not delete topic: " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("Pulsar mutation smoke cleanup failed: " + failure.getMessage());
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

    private static void closeNative(final GuardedConsumer<byte[]> consumer) {
        try {
            consumer.close();
        } catch (PulsarClientException failure) {
            throw new IllegalStateException("Pulsar mutation proof consumer close failed", failure);
        }
    }

    private static byte[] digest(final int seed) {
        final byte[] value = new byte[32];
        Arrays.fill(value, (byte) seed);
        return value;
    }
}
