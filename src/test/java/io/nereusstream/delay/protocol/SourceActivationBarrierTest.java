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
    void KafkaBarrierAcceptsUnsignedMaximumExclusiveOffset() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition lastOffset = new KafkaSourcePosition(shard, "cluster", topic,
                -1L, null, 1);
        assertTrue(new KafkaActivationBarrier(shard, "cluster", topic, -1L)
                .reachedBy(lastOffset));
    }

    @Test
    void KafkaBarrierOrdersUnsignedHighBitOffsets() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition highBit = new KafkaSourcePosition(shard, "cluster", topic,
                Long.MIN_VALUE, null, 1);
        assertTrue(new KafkaActivationBarrier(shard, "cluster", topic, Long.MIN_VALUE)
                .reachedBy(highBit));
        assertFalse(new KafkaActivationBarrier(shard, "cluster", topic, Long.MIN_VALUE)
                .reachedBy(new KafkaSourcePosition(shard, "cluster", topic, Long.MAX_VALUE - 1, null, 1)));
    }

    @Test
    void KafkaBarrierRejectsNonCanonicalClusterIdentityAtConstruction() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final UUID topic = UUID.randomUUID();
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaActivationBarrier(shard, "cluster\u0301", topic, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaActivationBarrier(shard, "cluster\0", topic, 1));
    }

    @Test
    void PulsarBarrierRequiresTheInclusiveFinalBatchMember() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final byte[] resource = new byte[32];
        resource[0] = 7;
        final byte[] guardDigest = Bytes.sha256(Bytes.utf8("guard-1"));
        final PulsarActivationBarrier barrier = new PulsarActivationBarrier(shard, resource, "persistent://t/a",
                4, 8, 2, 3, Long.MIN_VALUE, guardDigest, false);
        barrier.validateSourceConnection(Long.MIN_VALUE, guardDigest);
        assertThrows(IllegalArgumentException.class, () -> barrier.validateSourceConnection(Long.MAX_VALUE, guardDigest));
        assertThrows(IllegalArgumentException.class,
                () -> barrier.validateSourceConnection(11, Bytes.sha256(Bytes.utf8("other-guard"))));
        assertFalse(barrier.reachedBy(new PulsarSourcePosition(shard, resource, "persistent://t/a", 4, 8, 1, 3,
                PulsarSourcePosition.EntryKind.BATCH, 1)));
        assertTrue(barrier.reachedBy(new PulsarSourcePosition(shard, resource, "persistent://t/a", 4, 8, 2, 3,
                PulsarSourcePosition.EntryKind.BATCH, 1)));
        assertTrue(PulsarActivationBarrier.empty(shard, resource, "persistent://t/a", Long.MIN_VALUE, guardDigest)
                .reachedBy(null));
        final byte[] replacement = resource.clone();
        replacement[1] = 8;
        assertThrows(IllegalArgumentException.class, () -> barrier.reachedBy(new PulsarSourcePosition(shard,
                replacement, "persistent://t/a", 4, 8, 2, 3, PulsarSourcePosition.EntryKind.BATCH, 1)));
    }

    @Test
    @SuppressWarnings("deprecation")
    void legacyPulsarBarrierAllowsUnknownBatchShapeWithoutWeakeningIdentityFence() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final byte[] resource = Bytes.sha256(Bytes.utf8("legacy-batch-resource"));
        final byte[] guard = Bytes.sha256(Bytes.utf8("legacy-batch-guard"));
        final PulsarActivationBarrier barrier = new PulsarActivationBarrier(shard, resource,
                "persistent://t/legacy", 4, 8, 1, Long.MIN_VALUE, guard, false);

        assertTrue(barrier.reachedBy(new PulsarSourcePosition(shard, resource, "persistent://t/legacy",
                4, 8, 2, 3, PulsarSourcePosition.EntryKind.BATCH, 100)));
        assertThrows(IllegalArgumentException.class, () -> barrier.reachedBy(new PulsarSourcePosition(shard,
                Bytes.sha256(Bytes.utf8("replacement-resource")), "persistent://t/legacy", 4, 8, 2, 3,
                PulsarSourcePosition.EntryKind.BATCH, 100)));
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
                "persistent://t/value", 7, 9, 1, 13, 13, guard, false);
        final PulsarActivationBarrier second = new PulsarActivationBarrier(shard, resource.clone(),
                "persistent://t/value", 7, 9, 1, 13, 13, guard.clone(), false);
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
                "persistent://t/zero", 0, 0, 0, 0, 1, resource, true));
        assertThrows(IllegalArgumentException.class, () -> new PulsarActivationBarrier(shard, resource,
                "persistent://t/zero", 0, 0, 0, 0, 1, zero, true));
    }

    @Test
    void pulsarBarrierPinsBatchShapeForTheInclusiveEntry() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final byte[] resource = Bytes.sha256(Bytes.utf8("batch-shape-resource"));
        final byte[] guard = Bytes.sha256(Bytes.utf8("batch-shape-guard"));
        final PulsarActivationBarrier barrier = new PulsarActivationBarrier(shard, resource,
                "persistent://t/batch-shape", 9, 11, 2, 4, 3, guard, false);
        final PulsarSourcePosition sameEntryDifferentBatch = new PulsarSourcePosition(shard, resource,
                "persistent://t/batch-shape", 9, 11, 2, 5,
                PulsarSourcePosition.EntryKind.BATCH, 1);
        assertThrows(IllegalArgumentException.class, () -> barrier.reachedBy(sameEntryDifferentBatch));
        assertThrows(IllegalArgumentException.class, () -> barrier.validatePosition(sameEntryDifferentBatch));
        assertThrows(IllegalArgumentException.class, () -> new PulsarActivationBarrier(shard, resource,
                "persistent://t/batch-shape", 9, 11, 4, 4, 3, guard, false));
        assertThrows(IllegalArgumentException.class, () -> new PulsarActivationBarrier(shard, resource,
                "persistent://t/e\u0301", 9, 11, 2, 4, 3, guard, false));
        final PulsarSourcePosition sameEntryAtBarrier = new PulsarSourcePosition(shard, resource,
                "persistent://t/batch-shape", 9, 11, 2, 4,
                PulsarSourcePosition.EntryKind.BATCH, 1);
        assertTrue(barrier.reachedBy(sameEntryAtBarrier));
    }
}
