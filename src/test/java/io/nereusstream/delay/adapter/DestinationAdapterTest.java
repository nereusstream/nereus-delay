package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DestinationAdapterTest {
    @Test
    void kafkaDestinationPreservesAttemptAndBusinessTiming() {
        final KafkaTargetResource resource = new KafkaTargetResource("cluster", UUID.randomUUID(), 4);
        final DestinationPublishRequest request = request(2_000, 2_000);
        final PinnedKafkaDestinationAdapter.KafkaDestinationTransport transport = actual -> {
            assertEquals(resource.nativeTopicUuid(), actual.nativeTopicUuid());
            assertArrayEquals(request.publishAttemptId(), actual.publishAttemptId());
            assertEquals(2_000, actual.deliverAtEpochMs());
            return CompletableFuture.completedFuture(DestinationPublishResult.published(
                    Bytes.utf8("record-identity"), 2_001, null));
        };
        try (PinnedKafkaDestinationAdapter adapter = new PinnedKafkaDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result = adapter.publish(request).toCompletableFuture().join();
            assertEquals(DestinationPublishResult.Disposition.PUBLISHED, result.disposition());
            assertEquals(StableCode.OK, result.stableCode());
        }
    }

    @Test
    void targetTransportFailureIsUnknown() {
        final byte[] token = Bytes.sha256(Bytes.utf8("target-token"));
        final PulsarTargetResource resource = new PulsarTargetResource("cluster", token,
                "persistent://tenant/ns/topic", 0);
        final PinnedPulsarDestinationAdapter.PulsarDestinationTransport transport = actual -> {
            throw new IllegalStateException("connection closed after send ownership");
        };
        try (PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result = adapter.publish(request(900, 1_000))
                    .toCompletableFuture().join();
            assertEquals(DestinationPublishResult.Disposition.UNKNOWN, result.disposition());
            assertEquals(StableCode.DESTINATION_OUTCOME_UNKNOWN, result.stableCode());
        }
    }

    @Test
    void missingTransportResultIsUnknown() {
        final KafkaTargetResource resource = new KafkaTargetResource("cluster", UUID.randomUUID(), 0);
        final PinnedKafkaDestinationAdapter.KafkaDestinationTransport transport = actual -> null;
        try (PinnedKafkaDestinationAdapter adapter = new PinnedKafkaDestinationAdapter(resource, transport)) {
            final DestinationPublishResult result = adapter.publish(request(100, 100)).toCompletableFuture().join();
            assertTrue(result.disposition() == DestinationPublishResult.Disposition.UNKNOWN);
            assertEquals(StableCode.DESTINATION_OUTCOME_UNKNOWN, result.stableCode());
        }
    }

    private static DestinationPublishRequest request(final long actionAt, final long deliverAt) {
        return new DestinationPublishRequest(DestinationLaneId.derive(Bytes.utf8("target-lane")), new byte[16],
                DelayMessageId.random(new ShardId(RouteIncarnation.random(), 0)), 0, new byte[32], actionAt,
                deliverAt, Bytes.utf8("payload"), new byte[0]);
    }
}
