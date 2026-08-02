package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SourceReplaySuccessorTest {
    @Test
    void strictKafkaRejectsAnOffsetGap() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition first = new KafkaSourcePosition(shard, "cluster", topic, 10,
                null, 100);
        final KafkaSourcePosition gap = new KafkaSourcePosition(shard, "cluster", topic, 12,
                null, 100);

        assertThrows(IllegalStateException.class,
                () -> SourceReplaySuccessor.strictKafka().validate(first, gap));
    }

    @Test
    void strictKafkaAcceptsTheImmediateSuccessorAndExactRedelivery() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition first = new KafkaSourcePosition(shard, "cluster", topic, 10,
                3, 100);
        final KafkaSourcePosition next = new KafkaSourcePosition(shard, "cluster", topic, 11,
                3, 101);

        assertDoesNotThrow(() -> SourceReplaySuccessor.strictKafka().validate(first, next));
        assertDoesNotThrow(() -> SourceReplaySuccessor.strictKafka().validate(first,
                new KafkaSourcePosition(shard, "cluster", topic, 10, 3, 100)));
    }

    @Test
    void strictPulsarBatchSuccessorDoesNotGuessAnEntryTransition() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final byte[] resource = Bytes.sha256(Bytes.utf8("resource"));
        final PulsarSourcePosition first = new PulsarSourcePosition(shard, resource, "persistent://t/a", 7,
                9, 0, 2, PulsarSourcePosition.EntryKind.BATCH, 100);
        final PulsarSourcePosition nextMember = new PulsarSourcePosition(shard, resource, "persistent://t/a", 7,
                9, 1, 2, PulsarSourcePosition.EntryKind.BATCH, 100);
        final PulsarSourcePosition nextEntry = new PulsarSourcePosition(shard, resource, "persistent://t/a", 7,
                10, 0, 1, PulsarSourcePosition.EntryKind.NON_BATCH, 101);

        assertDoesNotThrow(() -> SourceReplaySuccessor.strictPulsarBatchMember()
                .validate(first, nextMember));
        assertThrows(IllegalStateException.class, () -> SourceReplaySuccessor.strictPulsarBatchMember()
                .validate(nextMember, nextEntry));
    }
}
