package io.nereusstream.delay.protocol;

import io.nereusstream.delay.ownership.SourceAssignment;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void KafkaBarrierSaturatesExclusiveNextOffsetAtLongMaximum() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition lastOffset = new KafkaSourcePosition(shard, "cluster", topic,
                Long.MAX_VALUE, null, 1);
        assertTrue(new KafkaActivationBarrier(shard, "cluster", topic, Long.MAX_VALUE)
                .reachedBy(lastOffset));
    }

    @Test
    void PulsarBarrierRequiresTheInclusiveFinalBatchMember() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final byte[] resource = new byte[32];
        resource[0] = 7;
        final byte[] guardDigest = Bytes.sha256(Bytes.utf8("guard-1"));
        final PulsarActivationBarrier barrier = new PulsarActivationBarrier(shard, resource, "persistent://t/a",
                4, 8, 2, 11, guardDigest, false);
        barrier.validateSourceConnection(11, guardDigest);
        assertThrows(IllegalArgumentException.class, () -> barrier.validateSourceConnection(12, guardDigest));
        assertThrows(IllegalArgumentException.class,
                () -> barrier.validateSourceConnection(11, Bytes.sha256(Bytes.utf8("other-guard"))));
        assertFalse(barrier.reachedBy(new PulsarSourcePosition(shard, resource, "persistent://t/a", 4, 8, 1, 3,
                PulsarSourcePosition.EntryKind.BATCH, 1)));
        assertTrue(barrier.reachedBy(new PulsarSourcePosition(shard, resource, "persistent://t/a", 4, 8, 2, 3,
                PulsarSourcePosition.EntryKind.BATCH, 1)));
        assertTrue(PulsarActivationBarrier.empty(shard, resource, "persistent://t/a", 11, guardDigest)
                .reachedBy(null));
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
                "persistent://t/empty", 7, Bytes.sha256(Bytes.utf8("empty-guard")));
        assertThrows(IllegalArgumentException.class, () -> barrier.validatePosition(new PulsarSourcePosition(shard,
                Bytes.sha256(Bytes.utf8("replacement-resource")), "persistent://t/empty", 1, 1, 0, 1,
                PulsarSourcePosition.EntryKind.NON_BATCH, 1)));
        assertThrows(IllegalArgumentException.class, () -> barrier.reachedBy(new PulsarSourcePosition(shard,
                Bytes.sha256(Bytes.utf8("replacement-resource")), "persistent://t/empty", 1, 1, 0, 1,
                PulsarSourcePosition.EntryKind.NON_BATCH, 1)));
    }

    @Test
    void pulsarBarrierAndAssignmentUseValueEqualityForArrayFields() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final byte[] resource = Bytes.sha256(Bytes.utf8("assignment-resource"));
        final byte[] guard = Bytes.sha256(Bytes.utf8("assignment-guard"));
        final PulsarActivationBarrier first = new PulsarActivationBarrier(shard, resource,
                "persistent://t/value", 7, 9, 1, 13, guard, false);
        final PulsarActivationBarrier second = new PulsarActivationBarrier(shard, resource.clone(),
                "persistent://t/value", 7, 9, 1, 13, guard.clone(), false);
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        final SourceAssignment left = new SourceAssignment(shard, Bytes.sha256(Bytes.utf8("assignment-id")), 4,
                first);
        final SourceAssignment right = new SourceAssignment(shard, Bytes.sha256(Bytes.utf8("assignment-id")), 4,
                second);
        assertEquals(left, right);
        assertTrue(left.sameIdentity(right));
    }

    @Test
    void pulsarBarrierRejectsZeroResourceOrGuardIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final byte[] resource = Bytes.sha256(Bytes.utf8("non-zero-resource"));
        final byte[] zero = new byte[32];
        assertThrows(IllegalArgumentException.class, () -> new PulsarActivationBarrier(shard, zero,
                "persistent://t/zero", 0, 0, 0, 1, resource, true));
        assertThrows(IllegalArgumentException.class, () -> new PulsarActivationBarrier(shard, resource,
                "persistent://t/zero", 0, 0, 0, 1, zero, true));
    }
}
