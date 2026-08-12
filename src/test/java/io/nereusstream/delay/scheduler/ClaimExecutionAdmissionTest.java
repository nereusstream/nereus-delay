package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClaimExecutionAdmissionTest {

    @Test
    void protectsOtherReadyLanesAcrossShardAndWorkerCaps() {
        final ShardId firstShard = new ShardId(RouteIncarnation.random(), 1);
        final ShardId secondShard = new ShardId(RouteIncarnation.random(), 2);
        final DestinationLaneId firstLane = lane("first");
        final DestinationLaneId secondLane = lane("second");
        final DestinationLaneId thirdLane = lane("third");
        final ClaimExecutionAdmission admission = new ClaimExecutionAdmission(3, 30);
        admission.registerShard(new ClaimExecutionAdmission.ShardSpec(firstShard, 3, 30));
        admission.registerShard(new ClaimExecutionAdmission.ShardSpec(secondShard, 2, 20));
        admission.registerLane(spec(firstShard, firstLane, 1));
        admission.registerLane(spec(firstShard, secondLane, 2));
        admission.registerLane(spec(secondShard, thirdLane, 3));
        admission.openReady(firstShard, firstLane, incarnation(1));
        admission.openReady(firstShard, secondLane, incarnation(2));
        admission.openReady(secondShard, thirdLane, incarnation(3));

        final DelayMessageId firstMessage = DelayMessageId.random(firstShard);
        final ClaimExecutionAdmission.AdmissionDecision first = admission.tryAcquire(firstShard, firstLane,
                incarnation(1), firstMessage, 1, 5);
        assertTrue(first.granted());
        assertEquals(1, admission.laneSnapshot(firstShard, firstLane).activeMessages());
        assertEquals(1, admission.shardSnapshot(firstShard).activeMessages());
        assertEquals(1, admission.workerSnapshot().activeMessages());

        final ClaimExecutionAdmission.AdmissionDecision protectedReadyLanes = admission.tryAcquire(firstShard,
                firstLane, incarnation(1), DelayMessageId.random(firstShard), 1, 5);
        assertFalse(protectedReadyLanes.granted());
        assertEquals(ClaimExecutionAdmission.Rejection.WORKER_CAPACITY, protectedReadyLanes.rejection());

        admission.closeReady(firstShard, secondLane, incarnation(2));
        final ClaimExecutionAdmission.AdmissionDecision second = admission.tryAcquire(firstShard,
                firstLane, incarnation(1), DelayMessageId.random(firstShard), 1, 5);
        assertTrue(second.granted());
        assertEquals(2, admission.workerSnapshot().activeMessages());

        assertTrue(first.reservation().release());
        assertFalse(first.reservation().release());
        assertTrue(second.reservation().release());
        assertEquals(0, admission.workerSnapshot().activeMessages());
        assertEquals(0, admission.workerSnapshot().activeBytes());
    }

    @Test
    void rejectsIdentityDriftDuplicatesAndUnprotectedReadyTransition() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final DestinationLaneId lane = lane("identity");
        final ClaimExecutionAdmission admission = new ClaimExecutionAdmission(1, 10);
        admission.registerShard(new ClaimExecutionAdmission.ShardSpec(shard, 1, 10));
        admission.registerLane(new ClaimExecutionAdmission.LaneSpec(shard, lane, incarnation(4),
                1, 10, 1, 10));
        admission.openReady(shard, lane, incarnation(4));
        final DelayMessageId message = DelayMessageId.random(shard);

        final ClaimExecutionAdmission.AdmissionDecision wrongIncarnation = admission.tryAcquire(shard, lane,
                incarnation(5), message, 1, 10);
        assertEquals(ClaimExecutionAdmission.Rejection.LANE_IDENTITY_MISMATCH,
                wrongIncarnation.rejection());

        final ClaimExecutionAdmission.AdmissionDecision granted = admission.tryAcquire(shard, lane,
                incarnation(4), message, 1, 10);
        assertTrue(granted.granted());
        final ClaimExecutionAdmission.AdmissionDecision duplicate = admission.tryAcquire(shard, lane,
                incarnation(4), message, 1, 10);
        assertEquals(ClaimExecutionAdmission.Rejection.MESSAGE_GENERATION_ALREADY_RESERVED,
                duplicate.rejection());
        assertThrows(IllegalArgumentException.class, () -> admission.tryAcquire(
                new ShardId(RouteIncarnation.random(), 3), lane, incarnation(4), message, 1, 10));

        admission.closeReady(shard, lane, incarnation(4));
        granted.reservation().release();
        assertEquals(ClaimExecutionAdmission.Rejection.LANE_NOT_READY,
                admission.tryAcquire(shard, lane, incarnation(4), DelayMessageId.random(shard), 1, 1)
                        .rejection());

        final ClaimExecutionAdmission insufficient = new ClaimExecutionAdmission(1, 9);
        insufficient.registerShard(new ClaimExecutionAdmission.ShardSpec(shard, 1, 9));
        insufficient.registerLane(new ClaimExecutionAdmission.LaneSpec(shard, lane, incarnation(4),
                1, 10, 1, 10));
        assertThrows(IllegalStateException.class,
                () -> insufficient.openReady(shard, lane, incarnation(4)));
    }

    @Test
    void enforcesShardAndLaneByteCapsIndependentlyOfWorkerCapacity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final DestinationLaneId firstLane = lane("shard-first");
        final DestinationLaneId secondLane = lane("shard-second");
        final ClaimExecutionAdmission admission = new ClaimExecutionAdmission(10, 1_000);
        admission.registerShard(new ClaimExecutionAdmission.ShardSpec(shard, 2, 15));
        admission.registerLane(new ClaimExecutionAdmission.LaneSpec(shard, firstLane, incarnation(6),
                0, 0, 2, 20));
        admission.registerLane(new ClaimExecutionAdmission.LaneSpec(shard, secondLane, incarnation(7),
                1, 5, 2, 20));
        admission.openReady(shard, firstLane, incarnation(6));
        admission.openReady(shard, secondLane, incarnation(7));

        final ClaimExecutionAdmission.AdmissionDecision first = admission.tryAcquire(shard, firstLane,
                incarnation(6), DelayMessageId.random(shard), 1, 10);
        assertTrue(first.granted());
        final ClaimExecutionAdmission.AdmissionDecision shardFull = admission.tryAcquire(shard, firstLane,
                incarnation(6), DelayMessageId.random(shard), 1, 1);
        assertEquals(ClaimExecutionAdmission.Rejection.SHARD_CAPACITY, shardFull.rejection());

        admission.closeReady(shard, secondLane, incarnation(7));
        final ClaimExecutionAdmission.AdmissionDecision second = admission.tryAcquire(shard, firstLane,
                incarnation(6), DelayMessageId.random(shard), 1, 6);
        assertEquals(ClaimExecutionAdmission.Rejection.SHARD_CAPACITY, second.rejection());
        first.reservation().release();
    }

    private static ClaimExecutionAdmission.LaneSpec spec(final ShardId shard,
                                                         final DestinationLaneId lane,
                                                         final int incarnation) {
        return new ClaimExecutionAdmission.LaneSpec(shard, lane, incarnation(incarnation),
                1, 5, 2, 20);
    }

    private static DestinationLaneId lane(final String value) {
        return DestinationLaneId.derive(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static byte[] incarnation(final int value) {
        final byte[] result = new byte[16];
        result[15] = (byte) value;
        return result;
    }
}
