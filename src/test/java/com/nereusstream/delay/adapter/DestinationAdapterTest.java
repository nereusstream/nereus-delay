package com.nereusstream.delay.adapter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DestinationAdapterTest {
    @Test
    void destinationCloseFailureCanBeRetriedWhileAdapterRemainsFenced() {
        final KafkaTargetResource resource = new KafkaTargetResource("cluster", UUID.randomUUID(), 0);
        final AtomicInteger closeCalls = new AtomicInteger();
        final PinnedKafkaDestinationAdapter.KafkaDestinationTransport transport =
                new PinnedKafkaDestinationAdapter.KafkaDestinationTransport() {
                    @Override
                    public CompletableFuture<DestinationPublishResult> publish(final KafkaDestinationRequest request) {
                        return CompletableFuture.completedFuture(
                                DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
                    }

                    @Override
                    public void close() {
                        if (closeCalls.getAndIncrement() == 0) {
                            throw new IllegalStateException("close failed");
                        }
                    }
                };
        final PinnedKafkaDestinationAdapter adapter = new PinnedKafkaDestinationAdapter(resource, transport);

        assertThrows(IllegalStateException.class, adapter::close);
        assertEquals(
                DestinationPublishResult.Disposition.UNKNOWN,
                adapter.publish(request(100, 100)).toCompletableFuture().join().disposition());
        adapter.close();
        adapter.close();
        assertEquals(2, closeCalls.get());
    }

    @Test
    void kafkaDestinationPreservesAttemptAndBusinessTiming() {
        final KafkaTargetResource resource = new KafkaTargetResource("cluster", UUID.randomUUID(), 4);
        final DestinationPublishRequest request = request(2_000, 2_000);
        final PinnedKafkaDestinationAdapter.KafkaDestinationTransport transport = actual -> {
            assertEquals(resource.nativeTopicUuid(), actual.nativeTopicUuid());
            assertArrayEquals(request.publishAttemptId(), actual.publishAttemptId());
            assertEquals(2_000, actual.deliverAtEpochMs());
            return CompletableFuture.completedFuture(DestinationPublishResult.published(
                    BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1(
                            resource.authenticatedClusterId(), resource.nativeTopicUuid())),
                    resource.partition(),
                    Bytes.utf8("record-identity"),
                    2_001,
                    Bytes.utf8("kafka-evidence")));
        };
        try (PinnedKafkaDestinationAdapter adapter = new PinnedKafkaDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result =
                    adapter.publish(request).toCompletableFuture().join();
            assertEquals(DestinationPublishResult.Disposition.PUBLISHED, result.disposition());
            assertEquals(StableCode.OK, result.stableCode());
        }
    }

    @Test
    void kafkaDestinationDoesNotInvokeTransportForEarlyActionAt() {
        final KafkaTargetResource resource = new KafkaTargetResource("cluster", UUID.randomUUID(), 4);
        final AtomicInteger calls = new AtomicInteger();
        final PinnedKafkaDestinationAdapter.KafkaDestinationTransport transport = actual -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
        };
        try (PinnedKafkaDestinationAdapter adapter = new PinnedKafkaDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result =
                    adapter.publish(request(1_999, 2_000)).toCompletableFuture().join();
            assertEquals(DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED, result.disposition());
            assertEquals(StableCode.INVALID_METADATA, result.stableCode());
            assertEquals(0, calls.get());
        }
    }

    @Test
    void targetTransportFailureIsUnknown() {
        final byte[] token = Bytes.sha256(Bytes.utf8("target-token"));
        final PulsarTargetResource resource =
                new PulsarTargetResource("cluster", token, "persistent://tenant/ns/topic", Long.MIN_VALUE, 0);
        final PinnedPulsarDestinationAdapter.PulsarDestinationTransport transport = actual -> {
            assertEquals(resource.physicalTopicCreationTimestamp(), actual.physicalTopicCreationTimestamp());
            throw new IllegalStateException("connection closed after send ownership");
        };
        try (PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(
                resource, transport, PulsarDestinationTimingPolicy.certifiedHandoff(100))) {
            final DestinationPublishResult result =
                    adapter.publish(request(900, 1_000)).toCompletableFuture().join();
            assertEquals(DestinationPublishResult.Disposition.UNKNOWN, result.disposition());
            assertEquals(StableCode.DESTINATION_OUTCOME_UNKNOWN, result.stableCode());
        }
    }

    @Test
    void pulsarSourceBoundTransportReceivesSourcePositionAndPreparedHash() {
        final byte[] resourceIncarnation = Bytes.sha256(Bytes.utf8("pulsar-source-bound-resource"));
        final PulsarTargetResource resource = new PulsarTargetResource(
                "cluster", resourceIncarnation, "persistent://tenant/ns/source-bound", 8_100, 0);
        final DestinationPublishRequest request = request(1_000, 1_000);
        final SourcePosition source = new PulsarSourcePosition(
                request.delayMessageId().routingId().shardId(),
                Bytes.sha256(Bytes.utf8("source-resource")),
                "persistent://tenant/ns/source",
                4,
                9,
                0,
                1,
                PulsarSourcePosition.EntryKind.NON_BATCH,
                1_001);
        final byte[] preparedHash = Bytes.sha256(Bytes.utf8("prepared-source-bound"));
        final AtomicReference<SourcePosition> receivedSource = new AtomicReference<>();
        final AtomicReference<byte[]> receivedPreparedHash = new AtomicReference<>();
        final PinnedPulsarDestinationAdapter.PulsarDestinationTransport transport =
                new PinnedPulsarDestinationAdapter.PulsarDestinationTransport() {
                    @Override
                    public CompletableFuture<DestinationPublishResult> publish(final PulsarDestinationRequest ignored) {
                        throw new AssertionError("source-bound transport overload was not used");
                    }

                    @Override
                    public CompletableFuture<DestinationPublishResult> publish(
                            final PulsarDestinationRequest ignored,
                            final SourcePosition exactSource,
                            final byte[] exactPreparedHash) {
                        receivedSource.set(exactSource);
                        receivedPreparedHash.set(exactPreparedHash);
                        return CompletableFuture.completedFuture(
                                DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
                    }
                };
        try (PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result = adapter.publish(request, source, preparedHash)
                    .toCompletableFuture()
                    .join();
            assertEquals(DestinationPublishResult.Disposition.UNKNOWN, result.disposition());
        }
        assertEquals(source, receivedSource.get());
        assertArrayEquals(preparedHash, receivedPreparedHash.get());
    }

    @Test
    void pulsarSourceBoundTransportAcceptsKafkaSourcePositionFromTheSameDelayShard() {
        final byte[] resourceIncarnation = Bytes.sha256(Bytes.utf8("pulsar-cross-source-resource"));
        final PulsarTargetResource resource = new PulsarTargetResource(
                "cluster", resourceIncarnation, "persistent://tenant/ns/cross-source", 8_101, 0);
        final DestinationPublishRequest request = request(1_000, 1_000);
        final SourcePosition source = new KafkaSourcePosition(
                request.delayMessageId().routingId().shardId(), "source-cluster", UUID.randomUUID(), 4, 1, 1_001);
        final byte[] preparedHash = Bytes.sha256(Bytes.utf8("prepared-cross-source"));
        final AtomicReference<SourcePosition> receivedSource = new AtomicReference<>();
        final PinnedPulsarDestinationAdapter.PulsarDestinationTransport transport =
                new PinnedPulsarDestinationAdapter.PulsarDestinationTransport() {
                    @Override
                    public CompletableFuture<DestinationPublishResult> publish(final PulsarDestinationRequest ignored) {
                        throw new AssertionError("source-bound transport overload was not used");
                    }

                    @Override
                    public CompletableFuture<DestinationPublishResult> publish(
                            final PulsarDestinationRequest ignored,
                            final SourcePosition exactSource,
                            final byte[] ignoredPreparedHash) {
                        receivedSource.set(exactSource);
                        return CompletableFuture.completedFuture(
                                DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
                    }
                };
        try (PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result = adapter.publish(request, source, preparedHash)
                    .toCompletableFuture()
                    .join();
            assertEquals(DestinationPublishResult.Disposition.UNKNOWN, result.disposition());
        }
        assertEquals(source, receivedSource.get());
    }

    @Test
    void pulsarDefaultTimingPolicyRejectsEarlyActionBeforeTransport() {
        final PulsarTargetResource resource = new PulsarTargetResource(
                "cluster",
                Bytes.sha256(Bytes.utf8("pulsar-default-timing")),
                "persistent://tenant/ns/default-timing",
                8_100,
                0);
        final AtomicInteger calls = new AtomicInteger();
        final PinnedPulsarDestinationAdapter.PulsarDestinationTransport transport = actual -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
        };
        try (PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result =
                    adapter.publish(request(900, 1_000)).toCompletableFuture().join();
            assertEquals(DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED, result.disposition());
            assertEquals(StableCode.INVALID_METADATA, result.stableCode());
            assertEquals(0, calls.get());
        }
    }

    @Test
    void pulsarCertifiedHandoffRequiresTheExactFixedLead() {
        final PulsarTargetResource resource = new PulsarTargetResource(
                "cluster",
                Bytes.sha256(Bytes.utf8("pulsar-certified-timing")),
                "persistent://tenant/ns/certified-timing",
                8_101,
                0);
        final AtomicInteger calls = new AtomicInteger();
        final PinnedPulsarDestinationAdapter.PulsarDestinationTransport transport = actual -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
        };
        try (PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(
                resource, transport, PulsarDestinationTimingPolicy.certifiedHandoff(100))) {
            final DestinationPublishResult accepted =
                    adapter.publish(request(900, 1_000)).toCompletableFuture().join();
            assertEquals(DestinationPublishResult.Disposition.UNKNOWN, accepted.disposition());
            final DestinationPublishResult rejected =
                    adapter.publish(request(899, 1_000)).toCompletableFuture().join();
            assertEquals(DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED, rejected.disposition());
            assertEquals(StableCode.INVALID_METADATA, rejected.stableCode());
            assertEquals(1, calls.get());
        }
    }

    @Test
    void kafkaPublishedIdentityMismatchIsUnknown() {
        final KafkaTargetResource resource = new KafkaTargetResource("cluster", UUID.randomUUID(), 4);
        final PinnedKafkaDestinationAdapter.KafkaDestinationTransport transport =
                actual -> CompletableFuture.completedFuture(DestinationPublishResult.published(
                        BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster", UUID.randomUUID())),
                        resource.partition(),
                        Bytes.utf8("record"),
                        2_001,
                        Bytes.utf8("evidence")));
        try (PinnedKafkaDestinationAdapter adapter = new PinnedKafkaDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result =
                    adapter.publish(request(2_000, 2_000)).toCompletableFuture().join();
            assertEquals(DestinationPublishResult.Disposition.UNKNOWN, result.disposition());
            assertEquals(StableCode.RESOURCE_INCARNATION_MISMATCH, result.stableCode());
        }
    }

    @Test
    void pulsarPublishedIdentityMustMatchCreationIdentity() {
        final byte[] token = Bytes.sha256(Bytes.utf8("pulsar-target-token"));
        final PulsarTargetResource resource =
                new PulsarTargetResource("cluster", token, "persistent://tenant/ns/published", 8100, 0);
        final PinnedPulsarDestinationAdapter.PulsarDestinationTransport transport =
                actual -> CompletableFuture.completedFuture(DestinationPublishResult.published(
                        BrokerResourceIdentityV1.pulsar(new PulsarBrokerResourceIdentityV1(
                                resource.authenticatedClusterId(), resource.resourceIncarnation(),
                                resource.physicalTopic(), resource.physicalTopicCreationTimestamp())),
                        resource.partition(),
                        Bytes.utf8("record"),
                        2_001,
                        Bytes.utf8("pulsar-evidence")));
        try (PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result =
                    adapter.publish(request(2_000, 2_000)).toCompletableFuture().join();
            assertEquals(DestinationPublishResult.Disposition.PUBLISHED, result.disposition());
        }
    }

    @Test
    void missingTransportResultIsUnknown() {
        final KafkaTargetResource resource = new KafkaTargetResource("cluster", UUID.randomUUID(), 0);
        final PinnedKafkaDestinationAdapter.KafkaDestinationTransport transport = actual -> null;
        try (PinnedKafkaDestinationAdapter adapter = new PinnedKafkaDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result =
                    adapter.publish(request(100, 100)).toCompletableFuture().join();
            assertTrue(result.disposition() == DestinationPublishResult.Disposition.UNKNOWN);
            assertEquals(StableCode.DESTINATION_OUTCOME_UNKNOWN, result.stableCode());
        }
    }

    @Test
    void kafkaCallbackRegistrationFailureRemainsUnknown() {
        final KafkaTargetResource resource = new KafkaTargetResource("cluster", UUID.randomUUID(), 0);
        final PinnedKafkaDestinationAdapter.KafkaDestinationTransport transport =
                actual -> new HandleRegistrationFailureFuture<>();
        try (PinnedKafkaDestinationAdapter adapter = new PinnedKafkaDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result =
                    adapter.publish(request(100, 100)).toCompletableFuture().join();
            assertEquals(DestinationPublishResult.Disposition.UNKNOWN, result.disposition());
            assertEquals(StableCode.DESTINATION_OUTCOME_UNKNOWN, result.stableCode());
        }
    }

    @Test
    void pulsarCallbackRegistrationFailureRemainsUnknown() {
        final PulsarTargetResource resource = new PulsarTargetResource(
                "cluster",
                Bytes.sha256(Bytes.utf8("callback-registration-resource")),
                "persistent://tenant/ns/callback-registration",
                8_200,
                0);
        final PinnedPulsarDestinationAdapter.PulsarDestinationTransport transport =
                actual -> new HandleRegistrationFailureFuture<>();
        try (PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result =
                    adapter.publish(request(100, 100)).toCompletableFuture().join();
            assertEquals(DestinationPublishResult.Disposition.UNKNOWN, result.disposition());
            assertEquals(StableCode.DESTINATION_OUTCOME_UNKNOWN, result.stableCode());
        }
    }

    @Test
    void publishedResultRequiresIdentityAndEvidence() {
        assertThrows(
                IllegalArgumentException.class,
                () -> DestinationPublishResult.published(null, 2_001, Bytes.utf8("evidence")));
        assertThrows(
                IllegalArgumentException.class,
                () -> DestinationPublishResult.published(Bytes.utf8("record"), 2_001, null));
    }

    @Test
    void nonPublishedResultCannotPretendSuccess() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DestinationPublishResult(
                        DestinationPublishResult.Disposition.UNKNOWN, StableCode.OK, null, -1, null, null, -1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DestinationPublishResult(
                        DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED,
                        StableCode.INVALID_METADATA,
                        Bytes.utf8("delivery"),
                        -1,
                        null,
                        null,
                        -1));
    }

    @Test
    void targetResourcesRejectNonCanonicalBrokerIdentityText() {
        assertThrows(
                IllegalArgumentException.class, () -> new KafkaTargetResource("cluster\u0301", UUID.randomUUID(), 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarTargetResource(
                        "cluster",
                        Bytes.sha256(Bytes.utf8("target-token")),
                        "persistent://tenant/ns/topic\u0301",
                        2_001,
                        0));
    }

    private static DestinationPublishRequest request(final long actionAt, final long deliverAt) {
        return new DestinationPublishRequest(
                DestinationLaneId.derive(Bytes.utf8("target-lane")),
                new byte[16],
                DelayMessageId.random(new ShardId(RouteIncarnation.random(), 0)),
                0,
                new byte[32],
                actionAt,
                deliverAt,
                Bytes.utf8("payload"),
                new byte[0]);
    }

    private static final class HandleRegistrationFailureFuture<T> extends CompletableFuture<T> {
        @Override
        public <U> CompletableFuture<U> handle(
                final java.util.function.BiFunction<? super T, Throwable, ? extends U> function) {
            throw new IllegalStateException("completion callback registration failed");
        }
    }
}
