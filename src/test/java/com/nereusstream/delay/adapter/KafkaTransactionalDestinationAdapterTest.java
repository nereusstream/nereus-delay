package com.nereusstream.delay.adapter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
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

class KafkaTransactionalDestinationAdapterTest {
    @Test
    void mapsBeforeTransportAndCarriesExactTargetReceiptPair() {
        final Fixture fixture = new Fixture();
        final AtomicReference<KafkaTransactionalDestinationRequest> sent = new AtomicReference<>();
        final KafkaTransactionalDestinationAdapter adapter = fixture.adapter(request -> {
            sent.set(request);
            return CompletableFuture.completedFuture(DestinationPublishResult.published(
                    BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(
                            fixture.target.authenticatedClusterId(), fixture.target.nativeTopicUuid())),
                    fixture.target.partition(),
                    request.mapping().publishAttemptId(),
                    2_001,
                    Bytes.utf8("target-and-receipt-commit-evidence")));
        });

        final DestinationPublishResult result = adapter.publish(fixture.request, fixture.source, fixture.preparedHash)
                .toCompletableFuture()
                .join();

        assertEquals(DestinationPublishResult.Disposition.PUBLISHED, result.disposition());
        assertArrayEquals(fixture.request.publishAttemptId(), result.externalDeliveryIdentity());
        final KafkaTransactionalDestinationRequest transaction = sent.get();
        assertEquals("target-topic", transaction.targetPhysicalTopic());
        assertEquals("receipt-topic", transaction.receiptPhysicalTopic());
        assertEquals(fixture.receiptResource, transaction.receiptResource());
        assertArrayEquals(fixture.preparedHash, transaction.mapping().preparedPublishHash());
        assertArrayEquals(
                fixture.request.publishAttemptId(), transaction.mapping().publishAttemptId());
        assertEquals(1, fixture.journal.records().size());
        assertArrayEquals(fixture.mappingId(), transaction.mapping().mappingId());
    }

    @Test
    void sourcePositionMismatchIsDefinitiveAndDoesNotInvokeTransport() {
        final Fixture fixture = new Fixture();
        final AtomicInteger calls = new AtomicInteger();
        final KafkaTransactionalDestinationAdapter adapter = fixture.adapter(request -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        });
        final SourcePosition foreign = new KafkaSourcePosition(
                new ShardId(RouteIncarnation.random(), 4), "source", UUID.randomUUID(), 9, null, 1_000);

        final DestinationPublishResult result = adapter.publish(fixture.request, foreign, fixture.preparedHash)
                .toCompletableFuture()
                .join();

        assertEquals(DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED, result.disposition());
        assertEquals(StableCode.INVALID_METADATA, result.stableCode());
        assertEquals(0, calls.get());
        assertEquals(0, fixture.journal.records().size());
    }

    @Test
    void acceptsPulsarSourcePositionFromTheSameDelayShard() {
        final Fixture fixture = new Fixture();
        final AtomicReference<KafkaTransactionalDestinationRequest> sent = new AtomicReference<>();
        final KafkaTransactionalDestinationAdapter adapter = fixture.adapter(request -> {
            sent.set(request);
            return CompletableFuture.completedFuture(DestinationPublishResult.published(
                    BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(
                            fixture.target.authenticatedClusterId(), fixture.target.nativeTopicUuid())),
                    fixture.target.partition(),
                    request.mapping().publishAttemptId(),
                    2_001,
                    Bytes.utf8("cross-source-target-and-receipt-evidence")));
        });
        final SourcePosition source = new PulsarSourcePosition(
                fixture.shard,
                Bytes.sha256(Bytes.utf8("pulsar-source-resource")),
                "persistent://tenant/ns/source",
                4,
                9,
                0,
                1,
                PulsarSourcePosition.EntryKind.NON_BATCH,
                1_001);

        final DestinationPublishResult result = adapter.publish(fixture.request, source, fixture.preparedHash)
                .toCompletableFuture()
                .join();

        assertEquals(DestinationPublishResult.Disposition.PUBLISHED, result.disposition());
        assertEquals(1, fixture.journal.records().size());
        assertArrayEquals(source.canonicalBytes(), sent.get().mapping().sourcePosition());
    }

    @Test
    void missingPreparedHashAuthorityRemainsUnavailable() {
        final Fixture fixture = new Fixture();
        final AtomicInteger calls = new AtomicInteger();
        final KafkaTransactionalDestinationAdapter adapter = fixture.adapter(request -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        });

        final DestinationPublishResult result =
                adapter.publish(fixture.request).toCompletableFuture().join();

        assertEquals(DestinationPublishResult.Disposition.UNKNOWN, result.disposition());
        assertEquals(StableCode.CAPABILITY_UNAVAILABLE, result.stableCode());
        assertEquals(0, calls.get());
        assertTrue(fixture.journal.unresolved(fixture.producerKey()).isEmpty());
    }

    private static final class Fixture {
        private final RouteIncarnation route = RouteIncarnation.random();
        private final ShardId shard = new ShardId(route, 0);
        private final UUID targetTopic = UUID.randomUUID();
        private final UUID receiptTopic = UUID.randomUUID();
        private final KafkaTargetResource target = new KafkaTargetResource("target-cluster", targetTopic, 0);
        private final KafkaReceiptResource receiptResource =
                new KafkaReceiptResource("target-cluster", receiptTopic, route, 0, 0, 1, 1, 0);
        private final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("k2-lane"));
        private final byte[] laneIncarnation = bytes(16, 7);
        private final byte[] transactionHash = bytes(32, 9);
        private final byte[] preparedHash = bytes(32, 11);
        private final KafkaReceiptJournal journal = new KafkaReceiptJournal(shard, receiptResource);
        private final DestinationPublishRequest request = new DestinationPublishRequest(
                lane,
                laneIncarnation,
                DelayMessageId.random(shard),
                3,
                bytes(32, 13),
                2_000,
                2_000,
                Bytes.utf8("payload"),
                Bytes.utf8("adapter-metadata"));
        private final KafkaSourcePosition source =
                new KafkaSourcePosition(shard, "source-cluster", UUID.randomUUID(), 18, 2, 1_000);

        private KafkaTransactionalDestinationAdapter adapter(
                final KafkaTransactionalDestinationAdapter.KafkaTransactionalDestinationTransport transport) {
            return new KafkaTransactionalDestinationAdapter(
                    target,
                    receiptResource,
                    "target-topic",
                    "receipt-topic",
                    journal,
                    lane,
                    laneIncarnation,
                    transactionHash,
                    transport);
        }

        private KafkaReceiptJournal.ProducerKey producerKey() {
            return new KafkaReceiptJournal.ProducerKey(lane, laneIncarnation, transactionHash, target);
        }

        private byte[] mappingId() {
            final KafkaReceiptJournal.AttemptIdentity identity = new KafkaReceiptJournal.AttemptIdentity(
                    request.delayMessageId(),
                    request.generation(),
                    request.publishAttemptId(),
                    preparedHash,
                    source.brokerPersistenceTimeEpochMs(),
                    source.canonicalBytes());
            return KafkaReceiptJournal.Mapping.create(shard, producerKey(), 0, identity)
                    .mappingId();
        }

        private static byte[] bytes(final int length, final int seed) {
            final byte[] result = new byte[length];
            for (int index = 0; index < result.length; index++) {
                result[index] = (byte) (seed + index);
            }
            return result;
        }
    }
}
