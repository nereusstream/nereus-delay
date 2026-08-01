package io.nereusstream.delay.adapter;

import io.nereusstream.delay.client.EnqueueStatus;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdapterIngressTest {
    @Test
    void kafkaAdapterReturnsQueuedOnlyForPinnedPersistedResult() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final UUID topic = UUID.randomUUID();
        final KafkaIngressResource resource = new KafkaIngressResource(shard, "cluster-a", topic, 2);
        final PreparedCommand command = command(shard);
        final PinnedKafkaCommandIngress.KafkaProduceTransport transport = request -> {
            assertEquals(topic, request.nativeTopicUuid());
            assertEquals(2, request.partition());
            assertEquals(command.commandId(), request.commandId());
            assertEquals(command, CommandCodec.decodeFrame(request.frame()));
            return CompletableFuture.completedFuture(KafkaProduceResult.persisted("cluster-a", topic, 2, 41,
                    7, 1000, Bytes.utf8("ack")));
        };
        try (PinnedKafkaCommandIngress adapter = new PinnedKafkaCommandIngress(resource, transport)) {
            final var outcome = adapter.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.QUEUED, outcome.status());
            assertEquals(41, ((io.nereusstream.delay.protocol.KafkaSourcePosition) outcome.receipt()
                    .sourcePosition()).offset());
        }
    }

    @Test
    void kafkaTransportExceptionIsUncertainAndNotDefinitelyRejected() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final KafkaIngressResource resource = new KafkaIngressResource(shard, "cluster", UUID.randomUUID(), 0);
        final PreparedCommand command = command(shard);
        final PinnedKafkaCommandIngress.KafkaProduceTransport transport = request -> {
            throw new IllegalStateException("connection lost after ownership");
        };
        try (PinnedKafkaCommandIngress adapter = new PinnedKafkaCommandIngress(resource, transport)) {
            final var outcome = adapter.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.ENQUEUE_UNCERTAIN, outcome.status());
            assertEquals(StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue(), outcome.stableCode());
        }
    }

    @Test
    void pulsarGuardRejectionIsDefinitelyNotQueued() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final byte[] token = Bytes.sha256(Bytes.utf8("token"));
        final PulsarIngressResource resource = new PulsarIngressResource(shard, "cluster", token,
                "persistent://tenant/ns/command-1", 7001, 1);
        final PreparedCommand command = command(shard);
        final PinnedPulsarCommandIngress.PulsarSendTransport transport = request -> CompletableFuture.completedFuture(
                PulsarSendResult.definitelyNotPersisted(StableCode.BROKER_DEFINITIVE_NOT_PERSISTED.wireValue(),
                        Bytes.utf8("guard")));
        try (PinnedPulsarCommandIngress adapter = new PinnedPulsarCommandIngress(resource, transport)) {
            final var outcome = adapter.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.DEFINITELY_NOT_QUEUED, outcome.status());
            assertEquals(StableCode.BROKER_DEFINITIVE_NOT_PERSISTED.wireValue(), outcome.stableCode());
        }
    }

    @Test
    void persistedPulsarResultCarriesBatchAwareSourcePosition() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final byte[] token = Bytes.sha256(Bytes.utf8("token-2"));
        final PulsarIngressResource resource = new PulsarIngressResource(shard, "cluster", token,
                "persistent://tenant/ns/command-3", 7003, 3);
        final PreparedCommand command = command(shard);
        final PinnedPulsarCommandIngress.PulsarSendTransport transport = request -> {
            assertEquals(resource.physicalTopicCreationTimestamp(), request.physicalTopicCreationTimestamp());
            return CompletableFuture.completedFuture(PulsarSendResult.persisted("cluster", token,
                    resource.physicalTopic(), resource.physicalTopicCreationTimestamp(), 3, 8, 9, 1, 2, true,
                    1001, null));
        };
        try (PinnedPulsarCommandIngress adapter = new PinnedPulsarCommandIngress(resource, transport)) {
            final var outcome = adapter.enqueue(command).toCompletableFuture().join();
            assertTrue(outcome.status() == EnqueueStatus.QUEUED);
            final PulsarSourcePosition position = (PulsarSourcePosition) outcome.receipt().sourcePosition();
            assertEquals(1, position.normalizedBatchIndex());
            assertEquals(PulsarSourcePosition.EntryKind.BATCH, position.entryKind());
            assertFalse(position.canonicalBytes().length == 0);
        }
    }

    @Test
    void pulsarCreationIdentityMismatchIsUncertain() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final byte[] token = Bytes.sha256(Bytes.utf8("token-3"));
        final PulsarIngressResource resource = new PulsarIngressResource(shard, "cluster", token,
                "persistent://tenant/ns/command-4", 7004, 4);
        final PreparedCommand command = command(shard);
        final PinnedPulsarCommandIngress.PulsarSendTransport transport = request -> CompletableFuture.completedFuture(
                PulsarSendResult.persisted("cluster", token, resource.physicalTopic(), 7005, 4, 8, 9, 0, 1, false,
                        1001, null));
        try (PinnedPulsarCommandIngress adapter = new PinnedPulsarCommandIngress(resource, transport)) {
            final var outcome = adapter.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.ENQUEUE_UNCERTAIN, outcome.status());
            assertEquals(StableCode.RESOURCE_INCARNATION_MISMATCH.wireValue(), outcome.stableCode());
        }
    }

    private static PreparedCommand command(final ShardId shard) {
        return PreparedCommand.schedule(shard, new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("adapter-lane")),
                2000, 5000, OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
    }
}
