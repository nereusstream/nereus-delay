package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SourceActivationBarrierTest {
    @Test
    void KafkaBarrierUsesExclusiveNextOffsetAndPinsTopicIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final UUID topic = UUID.randomUUID();
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shard, "cluster", topic, 10);
        assertFalse(barrier.reachedBy(new KafkaSourcePosition(shard, "cluster", topic, 8, null, 1)));
        assertTrue(barrier.reachedBy(new KafkaSourcePosition(shard, "cluster", topic, 9, null, 1)));
        assertTrue(new KafkaActivationBarrier(shard, "cluster", topic, 0).reachedBy(null));
        assertThrows(IllegalArgumentException.class,
                () -> barrier.reachedBy(new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9, null, 1)));
    }

    @Test
    void PulsarBarrierRequiresTheInclusiveFinalBatchMember() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final byte[] resource = new byte[32];
        resource[0] = 7;
        final PulsarActivationBarrier barrier = new PulsarActivationBarrier(shard, resource, "persistent://t/a",
                4, 8, 2, false);
        assertFalse(barrier.reachedBy(new PulsarSourcePosition(shard, resource, "persistent://t/a", 4, 8, 1, 3,
                PulsarSourcePosition.EntryKind.BATCH, 1)));
        assertTrue(barrier.reachedBy(new PulsarSourcePosition(shard, resource, "persistent://t/a", 4, 8, 2, 3,
                PulsarSourcePosition.EntryKind.BATCH, 1)));
        assertTrue(PulsarActivationBarrier.empty(shard, resource, "persistent://t/a").reachedBy(null));
        final byte[] replacement = resource.clone();
        replacement[1] = 8;
        assertThrows(IllegalArgumentException.class, () -> barrier.reachedBy(new PulsarSourcePosition(shard,
                replacement, "persistent://t/a", 4, 8, 2, 3, PulsarSourcePosition.EntryKind.BATCH, 1)));
    }

    @Test
    void emptyPulsarBarrierStillRejectsARecordFromAnotherResource() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final byte[] resource = Bytes.sha256(Bytes.utf8("empty-resource"));
        final PulsarActivationBarrier barrier = PulsarActivationBarrier.empty(shard, resource,
                "persistent://t/empty");
        assertThrows(IllegalArgumentException.class, () -> barrier.validatePosition(new PulsarSourcePosition(shard,
                Bytes.sha256(Bytes.utf8("replacement-resource")), "persistent://t/empty", 1, 1, 0, 1,
                PulsarSourcePosition.EntryKind.NON_BATCH, 1)));
    }
}
