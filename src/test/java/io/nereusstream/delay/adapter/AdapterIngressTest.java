package io.nereusstream.delay.adapter;

import io.nereusstream.delay.client.EnqueueStatus;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.EnqueueOutcomeKindV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void kafkaWireBridgeCarriesQueuedReceiptAndAckEvidence() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final UUID topic = UUID.randomUUID();
        final KafkaIngressResource resource = new KafkaIngressResource(shard, "cluster-wire", topic, 5);
        final PreparedCommand command = command(shard);
        final byte[] evidence = Bytes.utf8("kafka-response");
        final byte[] attempt = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("wire-attempt")), 16);
        final PinnedKafkaCommandIngress.KafkaProduceTransport transport = request ->
                CompletableFuture.completedFuture(KafkaProduceResult.persisted("cluster-wire", topic, 5, 12,
                        3, 2_000, evidence));
        try (PinnedKafkaCommandIngress adapter = new PinnedKafkaCommandIngress(resource, transport)) {
            final var wire = adapter.enqueueOutcomeV1(command, 5_000, attempt).toCompletableFuture().join();
            assertEquals(EnqueueOutcomeKindV1.QUEUED, wire.kind());
            final var ack = (io.nereusstream.delay.protocol.CommandQueuedReceiptV1.KafkaQueuedAck)
                    wire.queued().brokerAck();
            assertArrayEquals(Bytes.sha256(evidence), ack.responseSha256());
            assertEquals(wire, io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1.decode(
                    wire.canonicalBytes()));
        }
    }

    @Test
    void kafkaWireBridgeCarriesAuthenticatedDefinitiveProof() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final UUID topic = UUID.randomUUID();
        final KafkaIngressResource resource = new KafkaIngressResource(shard, "cluster-proof", topic, 6);
        final PreparedCommand command = command(shard);
        final byte[] attempt = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("proof-attempt")), 16);
        final PinnedKafkaCommandIngress.KafkaProduceTransport transport = request ->
                CompletableFuture.completedFuture(KafkaProduceResult.definitelyNotPersisted(
                        StableCode.BROKER_DEFINITIVE_NOT_PERSISTED.wireValue(), Bytes.utf8("rejection")));
        try (PinnedKafkaCommandIngress adapter = new PinnedKafkaCommandIngress(resource, transport)) {
            final var wire = adapter.enqueueOutcomeV1(command, 5_000, attempt).toCompletableFuture().join();
            assertEquals(EnqueueOutcomeKindV1.DEFINITELY_NOT_QUEUED, wire.kind());
            assertEquals(NonPersistenceProofKindV1.KAFKA_DEFINITIVE_REJECTION,
                    wire.definitelyNotQueued().proof().kind());
            assertEquals(wire, io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1.decode(
                    wire.canonicalBytes()));
        }
    }

    @Test
    void kafkaWireBridgeKeepsTransportExceptionUncertain() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final KafkaIngressResource resource = new KafkaIngressResource(shard, "cluster-unknown", UUID.randomUUID(), 7);
        final PreparedCommand command = command(shard);
        final byte[] attempt = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("unknown-attempt")), 16);
        final PinnedKafkaCommandIngress.KafkaProduceTransport transport = request -> {
            throw new IllegalStateException("lost after ownership");
        };
        try (PinnedKafkaCommandIngress adapter = new PinnedKafkaCommandIngress(resource, transport)) {
            final var wire = adapter.enqueueOutcomeV1(command, 5_000, attempt).toCompletableFuture().join();
            assertEquals(EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN, wire.kind());
            assertEquals(StableCode.ENQUEUE_RESULT_UNCERTAIN, wire.uncertain().error().code());
        }
    }

    @Test
    void kafkaWireBridgeDoesNotInventProofWithoutResponseEvidence() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final KafkaIngressResource resource = new KafkaIngressResource(shard, "cluster-no-proof", UUID.randomUUID(), 8);
        final PreparedCommand command = command(shard);
        final byte[] attempt = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("no-proof-attempt")), 16);
        final PinnedKafkaCommandIngress.KafkaProduceTransport transport = request ->
                CompletableFuture.completedFuture(KafkaProduceResult.definitelyNotPersisted(
                        StableCode.BROKER_DEFINITIVE_NOT_PERSISTED.wireValue(), null));
        try (PinnedKafkaCommandIngress adapter = new PinnedKafkaCommandIngress(resource, transport)) {
            final var wire = adapter.enqueueOutcomeV1(command, 5_000, attempt).toCompletableFuture().join();
            assertEquals(EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN, wire.kind());
            assertEquals(StableCode.ENQUEUE_RESULT_UNCERTAIN, wire.uncertain().error().code());
        }
    }

    @Test
    void kafkaV1WireRejectsLegacyBodyBeforeTransportOwnership() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 25);
        final KafkaIngressResource resource = new KafkaIngressResource(shard, "cluster-v1", UUID.randomUUID(), 25);
        final PreparedCommand legacy = PreparedCommand.schedule(shard,
                new io.nereusstream.delay.protocol.ScheduleIntent(DestinationLaneId.derive(
                        Bytes.utf8("legacy-v1-lane")), 2_000, 5_000, OrderingMode.BEST_EFFORT,
                        Bytes.utf8("legacy")), 10_000);
        final java.util.concurrent.atomic.AtomicBoolean transportCalled =
                new java.util.concurrent.atomic.AtomicBoolean();
        final PinnedKafkaCommandIngress.KafkaProduceTransport transport = request -> {
            transportCalled.set(true);
            return CompletableFuture.failedFuture(new AssertionError("legacy V1 body reached transport"));
        };
        try (PinnedKafkaCommandIngress adapter = new PinnedKafkaCommandIngress(resource, transport)) {
            final var failure = assertThrows(java.util.concurrent.CompletionException.class,
                    () -> adapter.enqueueOutcomeV1(legacy, 5_000,
                            java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("legacy-v1-attempt")), 16))
                            .toCompletableFuture().join());
            assertTrue(failure.getCause() instanceof IllegalArgumentException);
            assertFalse(transportCalled.get());
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
    void pulsarManagedTransportFailureUsesManagedUncertainCode() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 20);
        final byte[] token = Bytes.sha256(Bytes.utf8("managed-uncertain-token"));
        final PulsarIngressResource resource = new PulsarIngressResource(shard, "cluster-managed-uncertain", token,
                "persistent://tenant/ns/command-20", 7020, 20);
        final PreparedCommand command = command(shard);
        final PinnedPulsarCommandIngress.PulsarSendTransport transport = request -> {
            throw new IllegalStateException("connection lost after producer ownership");
        };
        try (PinnedPulsarCommandIngress adapter = new PinnedPulsarCommandIngress(resource, transport)) {
            final var outcome = adapter.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.ENQUEUE_UNCERTAIN, outcome.status());
            assertEquals(StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue(), outcome.stableCode());
        }
    }

    @Test
    void pulsarManagedNullResultUsesManagedUncertainCode() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 21);
        final PulsarIngressResource resource = new PulsarIngressResource(shard, "cluster-managed-null",
                Bytes.sha256(Bytes.utf8("managed-null-token")), "persistent://tenant/ns/command-21", 7021, 21);
        final PreparedCommand command = command(shard);
        final PinnedPulsarCommandIngress.PulsarSendTransport transport = request ->
                CompletableFuture.completedFuture(null);
        try (PinnedPulsarCommandIngress adapter = new PinnedPulsarCommandIngress(resource, transport)) {
            final var outcome = adapter.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.ENQUEUE_UNCERTAIN, outcome.status());
            assertEquals(StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue(), outcome.stableCode());
        }
    }

    @Test
    void managedIngressDoesNotLeakNativeUncertainCode() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 22);
        final UUID topic = UUID.randomUUID();
        final KafkaIngressResource resource = new KafkaIngressResource(shard, "cluster-managed-code", topic, 22);
        final PreparedCommand command = command(shard);
        final PinnedKafkaCommandIngress.KafkaProduceTransport transport = request ->
                CompletableFuture.completedFuture(KafkaProduceResult.unknown(
                        StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN.wireValue(), Bytes.utf8("native-code")));
        try (PinnedKafkaCommandIngress adapter = new PinnedKafkaCommandIngress(resource, transport)) {
            final var outcome = adapter.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.ENQUEUE_UNCERTAIN, outcome.status());
            assertEquals(StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue(), outcome.stableCode());
            final var wire = adapter.enqueueOutcomeV1(command, 5_000,
                    java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("managed-code-attempt")), 16))
                    .toCompletableFuture().join();
            assertEquals(EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN, wire.kind());
            assertEquals(StableCode.ENQUEUE_RESULT_UNCERTAIN, wire.uncertain().error().code());
        }
    }

    @Test
    void managedIngressNormalizesNativeGuardCode() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 23);
        final UUID topic = UUID.randomUUID();
        final KafkaIngressResource resource = new KafkaIngressResource(shard, "cluster-managed-guard", topic, 23);
        final PreparedCommand command = command(shard);
        final PinnedKafkaCommandIngress.KafkaProduceTransport transport = request ->
                CompletableFuture.completedFuture(KafkaProduceResult.definitelyNotPersisted(
                        StableCode.NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED.wireValue(), Bytes.utf8("guard-code")));
        try (PinnedKafkaCommandIngress adapter = new PinnedKafkaCommandIngress(resource, transport)) {
            final var wire = adapter.enqueueOutcomeV1(command, 5_000,
                    java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("managed-guard-attempt")), 16))
                    .toCompletableFuture().join();
            assertEquals(EnqueueOutcomeKindV1.DEFINITELY_NOT_QUEUED, wire.kind());
            assertEquals(StableCode.BROKER_DEFINITIVE_NOT_PERSISTED,
                    wire.definitelyNotQueued().error().code());
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

    @Test
    void pulsarWireBridgeCarriesBatchAwareReceiptAndEvidence() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final byte[] token = Bytes.sha256(Bytes.utf8("wire-pulsar-token"));
        final PulsarIngressResource resource = new PulsarIngressResource(shard, "cluster-pulsar-wire", token,
                "persistent://tenant/ns/command-9", 9009, 9);
        final PreparedCommand command = command(shard);
        final byte[] evidence = Bytes.utf8("pulsar-response");
        final byte[] attempt = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("pulsar-attempt")), 16);
        final PinnedPulsarCommandIngress.PulsarSendTransport transport = request -> {
            assertEquals(resource.physicalTopicCreationTimestamp(), request.physicalTopicCreationTimestamp());
            return CompletableFuture.completedFuture(PulsarSendResult.persisted("cluster-pulsar-wire", token,
                    resource.physicalTopic(), resource.physicalTopicCreationTimestamp(), 9, 12, 13, 2, 3, true,
                    2_001, evidence));
        };
        try (PinnedPulsarCommandIngress adapter = new PinnedPulsarCommandIngress(resource, transport)) {
            final var wire = adapter.enqueueOutcomeV1(command, 5_000, attempt).toCompletableFuture().join();
            assertEquals(EnqueueOutcomeKindV1.QUEUED, wire.kind());
            final var ack = (io.nereusstream.delay.protocol.CommandQueuedReceiptV1.PulsarQueuedAck)
                    wire.queued().brokerAck();
            assertArrayEquals(Bytes.sha256(evidence), ack.sendReceiptSha256());
            assertEquals(2, ack.normalizedBatchIndex());
            assertEquals(wire, io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1.decode(
                    wire.canonicalBytes()));
        }
    }

    @Test
    void pulsarWireBridgeCarriesGuardRejectionProof() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 10);
        final byte[] token = Bytes.sha256(Bytes.utf8("guard-wire-token"));
        final PulsarIngressResource resource = new PulsarIngressResource(shard, "cluster-pulsar-proof", token,
                "persistent://tenant/ns/command-10", 9010, 10);
        final PreparedCommand command = command(shard);
        final byte[] attempt = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("pulsar-proof-attempt")), 16);
        final PinnedPulsarCommandIngress.PulsarSendTransport transport = request ->
                CompletableFuture.completedFuture(PulsarSendResult.definitelyNotPersisted(
                        StableCode.BROKER_DEFINITIVE_NOT_PERSISTED.wireValue(), Bytes.utf8("guard-rejection")));
        try (PinnedPulsarCommandIngress adapter = new PinnedPulsarCommandIngress(resource, transport)) {
            final var wire = adapter.enqueueOutcomeV1(command, 5_000, attempt).toCompletableFuture().join();
            assertEquals(EnqueueOutcomeKindV1.DEFINITELY_NOT_QUEUED, wire.kind());
            assertEquals(NonPersistenceProofKindV1.PULSAR_GUARD_REJECTION,
                    wire.definitelyNotQueued().proof().kind());
            assertEquals(wire, io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1.decode(
                    wire.canonicalBytes()));
        }
    }

    private static PreparedCommand command(final ShardId shard) {
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("adapter-destination"), 1,
                Bytes.sha256(Bytes.utf8("adapter-destination-semantic")), ProfileKindV1.DESTINATION);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(Bytes.utf8("adapter-retry"), 1,
                Bytes.sha256(Bytes.utf8("adapter-retry-semantic")));
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination, retryPolicy, 2_000, 5_000,
                DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], Bytes.utf8("payload"), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        return PreparedCommand.scheduleV1(shard, intent, 10_000);
    }
}
