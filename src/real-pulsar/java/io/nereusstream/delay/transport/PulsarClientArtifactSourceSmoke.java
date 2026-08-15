package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.PulsarSendRequest;
import io.nereusstream.delay.adapter.PulsarSendResult;
import io.nereusstream.delay.ownership.SourceAcknowledgement;
import io.nereusstream.delay.ownership.SourceAssignment;
import io.nereusstream.delay.ownership.SourceRecordConsumer;
import io.nereusstream.delay.ownership.SourceReplayCursor;
import io.nereusstream.delay.ownership.SourceReplayEntry;
import io.nereusstream.delay.ownership.SourceReplayRecord;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Real-service guarded SUBSCRIBE, replay, Broker-timestamp, and ACK smoke. */
public final class PulsarClientArtifactSourceSmoke {
    private static final String CLUSTER = "standalone";
    private static final byte[] INCARNATION = digest(43);
    private static final long CREATION_TIMESTAMP = 2001L;

    private PulsarClientArtifactSourceSmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <topic>");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String topic = arguments[2] + "-source-" + UUID.randomUUID();
        final String physicalTopic = "persistent://public/default/" + topic;
        final HttpClient admin = HttpClient.newHttpClient();
        createTopic(admin, adminUrl, topic);
        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
            final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
            final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
            final PreparedCommand firstCommand = command(shard, "source-one");
            final PreparedCommand secondCommand = command(shard, "source-two");
            send(client, guard, physicalTopic, firstCommand, "producer-first");
            send(client, guard, physicalTopic, secondCommand, "producer-second");

            final String recoverySubscription = "nereus-delay-recovery-" + UUID.randomUUID();
            final GuardedConsumer<byte[]> recoveryNative = PulsarClientArtifactSourceConsumerFactory.create(
                    client, guard, physicalTopic, recoverySubscription);
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof recoveryProof =
                    PulsarClientArtifactRecoverySourcePositioner.seekAfter(recoveryNative, guard, physicalTopic,
                            shard, Optional.empty(), java.time.Duration.ofSeconds(5));
            final SourceAssignment recoveryAssignment = new SourceAssignment(shard,
                    Bytes.sha256(Bytes.utf8("pulsar-recovery-assignment")), 1,
                    PulsarActivationBarrier.empty(shard, INCARNATION, physicalTopic,
                            recoveryProof.connectionGeneration(), recoveryProof.attestationDigest()));
            final PulsarSourcePosition recoveredFirstPosition;
            try (PulsarClientArtifactRecoverySourceCursor recovery =
                         new PulsarClientArtifactRecoverySourceCursor(recoveryNative, guard, recoveryAssignment,
                                 physicalTopic, java.time.Duration.ofMillis(250))) {
                final SourceReplayCursor<SourceReplayEntry> cursor = SourceReplayCursor.of(recovery);
                final SourceReplayEntry recoveredFirst = cursor.peek();
                if (!(recoveredFirst instanceof SourceReplayRecord recoveredRecord)
                        || !recoveredRecord.command().equals(firstCommand)
                        || cursor.peek() != recoveredFirst) {
                    throw new IllegalStateException("Pulsar recovery cursor did not retain the exact first entry");
                }
                if (!(recoveredFirst.position() instanceof PulsarSourcePosition position)) {
                    throw new IllegalStateException("Pulsar recovery cursor returned a non-Pulsar position");
                }
                recoveredFirstPosition = position;
                cursor.next();
                final SourceReplayEntry recoveredSecond = cursor.peek();
                if (!(recoveredSecond instanceof SourceReplayRecord recoveredSecondRecord)
                        || !recoveredSecondRecord.command().equals(secondCommand)) {
                    throw new IllegalStateException("Pulsar recovery cursor did not expose the second entry");
                }
                cursor.next();
                if (cursor.hasNext()) {
                    throw new IllegalStateException("Pulsar recovery cursor exposed an unexpected third entry");
                }
            }

            final String positionedRecoverySubscription = "nereus-delay-recovery-positioned-" + UUID.randomUUID();
            final GuardedConsumer<byte[]> positionedRecoveryNative = PulsarClientArtifactSourceConsumerFactory.create(
                    client, guard, physicalTopic, positionedRecoverySubscription);
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof positionedRecoveryProof =
                    PulsarClientArtifactRecoverySourcePositioner.seekAfter(positionedRecoveryNative, guard,
                            physicalTopic, shard, Optional.of(recoveredFirstPosition),
                            java.time.Duration.ofSeconds(5));
            final SourceAssignment positionedAssignment = new SourceAssignment(shard,
                    Bytes.sha256(Bytes.utf8("pulsar-positioned-recovery-assignment")), 1,
                    PulsarActivationBarrier.empty(shard, INCARNATION, physicalTopic,
                            positionedRecoveryProof.connectionGeneration(),
                            positionedRecoveryProof.attestationDigest()));
            try (PulsarClientArtifactRecoverySourceCursor positionedRecovery =
                         new PulsarClientArtifactRecoverySourceCursor(positionedRecoveryNative, guard,
                                 positionedAssignment, physicalTopic, java.time.Duration.ofSeconds(2))) {
                final SourceReplayCursor<SourceReplayEntry> cursor = SourceReplayCursor.of(positionedRecovery);
                final SourceReplayEntry positionedSecond = cursor.peek();
                if (!(positionedSecond instanceof SourceReplayRecord positionedSecondRecord)
                        || !positionedSecondRecord.command().equals(secondCommand)) {
                    throw new IllegalStateException("Pulsar positioned recovery did not skip the durable first entry");
                }
                cursor.next();
                if (cursor.hasNext()) {
                    throw new IllegalStateException("Pulsar positioned recovery exposed an unexpected third entry");
                }
            }
            System.out.println("Pulsar positioned recovery passed: skipped=" + recoveredFirstPosition.ledgerId()
                    + "/" + recoveredFirstPosition.entryId()
                    + ", returned=" + secondCommand.commandId()
                    + ", connectionGeneration=" + positionedRecoveryProof.connectionGeneration());

            final String subscription = "nereus-delay-source-" + UUID.randomUUID();
            final GuardedConsumer<byte[]> firstNative = PulsarClientArtifactSourceConsumerFactory.create(
                    client, guard, physicalTopic, subscription);
            final PulsarClientArtifactSourceRecordConsumer firstSource =
                    new PulsarClientArtifactSourceRecordConsumer(firstNative, guard, shard, physicalTopic,
                            java.time.Duration.ofMillis(250));
            final SourceRecordConsumer.PolledSourceRecord first = pollUntil(firstSource, firstCommand, true);
            final SourceReplayRecord firstEntry = sourceRecord(first);
            final PulsarSourcePosition firstPosition = position(firstEntry);
            requirePosition(firstPosition, shard, INCARNATION, physicalTopic);
            final long firstGeneration = requireProof(firstEntry);
            // Close the native consumer directly: the source adapter correctly
            // refuses to close while this record remains the retry authority.
            closeNative(firstNative);

            final GuardedConsumer<byte[]> replayNative = PulsarClientArtifactSourceConsumerFactory.create(
                    client, guard, physicalTopic, subscription);
            final PulsarClientArtifactSourceRecordConsumer replaySource =
                    new PulsarClientArtifactSourceRecordConsumer(replayNative, guard, shard, physicalTopic,
                            java.time.Duration.ofMillis(250));
            PulsarSourcePosition secondPosition = null;
            long secondGeneration = 0;
            boolean replayClosed = false;
            try {
                final SourceRecordConsumer.PolledSourceRecord replayed = pollUntil(replaySource, firstCommand, true);
                final SourceReplayRecord replayEntry = sourceRecord(replayed);
                if (!firstPosition.equals(position(replayEntry))
                        || !firstEntry.command().equals(replayEntry.command())
                        || firstGeneration == requireProof(replayEntry)) {
                    throw new IllegalStateException("Pulsar source replay did not retain exact position/proof boundary");
                }
                secondGeneration = replayEntry.sourceConnectionGeneration();
                requireAcked(replayed.acknowledgement().acknowledge(replayed.entry(), null), "first source record");

                final SourceRecordConsumer.PolledSourceRecord second = pollUntil(replaySource, secondCommand, true);
                final SourceReplayRecord secondEntry = sourceRecord(second);
                secondPosition = position(secondEntry);
                requirePosition(secondPosition, shard, INCARNATION, physicalTopic);
                if (secondPosition.equals(firstPosition)) {
                    throw new IllegalStateException("Pulsar source cursor did not move after ACK");
                }
                requireAcked(second.acknowledgement().acknowledge(second.entry(), null), "second source record");
                replaySource.close();
                replayClosed = true;
            } finally {
                if (!replayClosed) {
                    closeNative(replayNative);
                }
            }

            final GuardedConsumer<byte[]> afterAckNative = PulsarClientArtifactSourceConsumerFactory.create(
                    client, guard, physicalTopic, subscription);
            final PulsarClientArtifactSourceRecordConsumer afterAck =
                    new PulsarClientArtifactSourceRecordConsumer(afterAckNative, guard, shard, physicalTopic,
                            java.time.Duration.ofMillis(250));
            try {
                if (pollUntil(afterAck, null, false) != null) {
                    throw new IllegalStateException("Pulsar source replay returned a record after both ACKs");
                }
                afterAck.close();
            } catch (RuntimeException | Error failure) {
                closeNative(afterAckNative);
                throw failure;
            }
            System.out.println("Pulsar source ACK smoke passed: physicalTopic=" + physicalTopic
                    + ", firstLedger=" + firstPosition.ledgerId() + ", firstEntry=" + firstPosition.entryId()
                    + ", secondLedger=" + secondPosition.ledgerId() + ", secondEntry=" + secondPosition.entryId()
                    + ", firstConnectionGeneration=" + firstGeneration
                    + ", secondConnectionGeneration=" + secondGeneration);
        } finally {
            deleteTopicIfPresent(admin, adminUrl, topic);
        }
    }

    private static void send(final PulsarClient client, final TopicResourceGuard guard, final String physicalTopic,
                             final PreparedCommand command, final String producerName) throws Exception {
        final PulsarClientArtifactSendTransport transport = new PulsarClientArtifactSendTransport(
                PulsarClientArtifactProducerFactory.create(client, guard.authenticatedClusterId(),
                        guard.resourceIncarnation(), physicalTopic, guard.topicCreationTimestamp(), producerName),
                CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, 0);
        try {
            final PulsarSendResult result = transport.send(new PulsarSendRequest(CLUSTER, INCARNATION, physicalTopic,
                    CREATION_TIMESTAMP, 0, command.commandId(), CommandCodec.encodeFrameV1(command)))
                    .toCompletableFuture().get(15, TimeUnit.SECONDS);
            if (result.disposition() != PulsarSendResult.Disposition.PERSISTED) {
                throw new IllegalStateException("guarded Pulsar source producer did not persist: "
                        + result.disposition());
            }
        } finally {
            transport.close();
        }
    }

    private static SourceRecordConsumer.PolledSourceRecord pollUntil(
            final PulsarClientArtifactSourceRecordConsumer source, final PreparedCommand expected,
            final boolean requireRecord) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            final Optional<SourceRecordConsumer.PolledSourceRecord> polled = source.poll();
            if (polled.isPresent()) {
                if (expected != null && !sourceRecord(polled.get()).command().equals(expected)) {
                    throw new IllegalStateException("Pulsar source returned an unexpected command");
                }
                return polled.get();
            }
        }
        if (requireRecord) {
            throw new IllegalStateException("Pulsar source record did not become visible");
        }
        return null;
    }

    private static SourceReplayRecord sourceRecord(final SourceRecordConsumer.PolledSourceRecord record) {
        return (SourceReplayRecord) record.entry();
    }

    private static PulsarSourcePosition position(final SourceReplayRecord entry) {
        return (PulsarSourcePosition) entry.position();
    }

    private static long requireProof(final SourceReplayRecord entry) {
        if (entry.sourceConnectionGeneration() == null || entry.sourceConnectionGeneration() == 0
                || entry.guardAttestationDigest() == null || allZero(entry.guardAttestationDigest())) {
            throw new IllegalStateException("Pulsar source record lacks guarded connection proof");
        }
        return entry.sourceConnectionGeneration();
    }

    private static void requirePosition(final PulsarSourcePosition position, final ShardId shard,
                                        final byte[] incarnation, final String physicalTopic) {
        if (!position.shardId().equals(shard) || !Arrays.equals(position.brokerResourceIncarnation(), incarnation)
                || !position.physicalTopic().equals(physicalTopic) || position.ledgerId() < 0
                || position.entryId() < 0 || position.brokerEntryTimestampEpochMs() < 0) {
            throw new IllegalStateException("Pulsar source position did not retain exact broker identity");
        }
    }

    private static void requireAcked(final SourceAcknowledgement.AcknowledgementResult result,
                                     final String label) {
        if (result.disposition() != SourceAcknowledgement.Disposition.ACKED) {
            throw new IllegalStateException(label + " was not ACKED: " + result.disposition(), result.failure());
        }
    }

    private static void closeNative(final GuardedConsumer<byte[]> consumer) {
        try {
            consumer.close();
        } catch (PulsarClientException failure) {
            throw new IllegalStateException("Pulsar source native consumer close failed", failure);
        }
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("destination-" + identity), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)), ProfileKindV1.DESTINATION);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(Bytes.utf8("retry-" + identity), 1,
                Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination, retryPolicy, deliverAt,
                deliverAt + 10_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0],
                Bytes.utf8("source-" + identity), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        return PreparedCommand.scheduleV1(shard, intent, deliverAt + 20_000);
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
                System.err.println("Pulsar source smoke cleanup could not delete topic: " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("Pulsar source smoke cleanup failed: " + failure.getMessage());
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

    private static boolean allZero(final byte[] value) {
        for (byte element : value) {
            if (element != 0) {
                return false;
            }
        }
        return true;
    }

    private static byte[] digest(final int seed) {
        final byte[] result = new byte[32];
        Arrays.fill(result, (byte) seed);
        return result;
    }
}
