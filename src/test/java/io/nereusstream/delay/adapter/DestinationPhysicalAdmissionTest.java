package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DestinationPhysicalAdmissionTest {
    @Test
    void protectsOtherReadyLaneMinimumsAtWorkerAndTargetCluster() {
        final DestinationPhysicalAdmission admission = new DestinationPhysicalAdmission(4, 100);
        admission.registerTargetCluster("cluster-a", 3, 100);
        final DestinationLaneId laneA = lane("a");
        final DestinationLaneId laneB = lane("b");
        admission.registerLane(spec(laneA, "cluster-a", 1, 10, 3, 80, 2, 60));
        admission.registerLane(spec(laneB, "cluster-a", 1, 10, 3, 80, 2, 60));
        admission.openReady(laneA);
        admission.openReady(laneB);

        final DestinationPhysicalAdmission.AdmissionDecision first =
                admission.tryAcquire(laneA, new byte[16], 20);
        assertTrue(first.granted());
        final DestinationPhysicalAdmission.AdmissionDecision second =
                admission.tryAcquire(laneA, new byte[16], 20);
        assertTrue(second.granted());
        final DestinationPhysicalAdmission.AdmissionDecision clusterFull =
                admission.tryAcquire(laneA, new byte[16], 20);
        assertFalse(clusterFull.granted());
        assertEquals(DestinationPhysicalAdmission.Rejection.TARGET_CLUSTER_CAPACITY, clusterFull.rejection());

        first.reservation().release();
        second.reservation().release();
        assertEquals(0, admission.workerSnapshot().activeRequests());
        assertEquals(2, admission.workerSnapshot().protectedReadyRequests());
    }

    @Test
    void laneIdentityAndReadyGateFailClosed() {
        final DestinationPhysicalAdmission admission = new DestinationPhysicalAdmission(2, 20);
        admission.registerTargetCluster("cluster-a", 2, 20);
        final DestinationLaneId lane = lane("identity");
        admission.registerLane(spec(lane, "cluster-a", 0, 0, 2, 20, 1, 20));

        assertEquals(DestinationPhysicalAdmission.Rejection.LANE_NOT_READY,
                admission.tryAcquire(lane, new byte[16], 1).rejection());
        admission.openReady(lane);
        assertEquals(DestinationPhysicalAdmission.Rejection.LANE_IDENTITY_MISMATCH,
                admission.tryAcquire(lane, new byte[15], 1).rejection());
        assertThrows(IllegalArgumentException.class, () -> admission.registerLane(
                spec(lane, "cluster-a", 0, 0, 2, 20, 1, 20)));
    }

    @Test
    void zombieChargeBlocksLaneUntilPhysicalReleaseAndExplicitClear() {
        final DestinationPhysicalAdmission admission = new DestinationPhysicalAdmission(3, 30);
        admission.registerTargetCluster("cluster-a", 3, 30);
        final DestinationLaneId lane = lane("zombie");
        admission.registerLane(spec(lane, "cluster-a", 0, 0, 3, 30, 1, 10));
        admission.openReady(lane);

        final var first = admission.tryAcquire(lane, new byte[16], 10).reservation();
        final var second = admission.tryAcquire(lane, new byte[16], 10).reservation();
        assertTrue(first.markZombie());
        assertFalse(second.markZombie());
        assertTrue(admission.laneSnapshot(lane).blocked());
        assertEquals(DestinationPhysicalAdmission.Rejection.ZOMBIE_CAPACITY,
                admission.tryAcquire(lane, new byte[16], 1).rejection());

        assertThrows(IllegalStateException.class, () -> admission.clearZombieBlock(lane));
        first.release();
        second.release();
        admission.clearZombieBlock(lane);
        final var next = admission.tryAcquire(lane, new byte[16], 10);
        assertTrue(next.granted());
        next.reservation().close();
    }

    @Test
    void readyMinimumCannotBeOpenedAfterCapacityIsConsumed() {
        final DestinationPhysicalAdmission admission = new DestinationPhysicalAdmission(2, 20);
        admission.registerTargetCluster("cluster-a", 2, 20);
        final DestinationLaneId firstLane = lane("first");
        final DestinationLaneId secondLane = lane("second");
        admission.registerLane(spec(firstLane, "cluster-a", 0, 0, 2, 20, 1, 20));
        admission.registerLane(spec(secondLane, "cluster-a", 1, 10, 2, 20, 1, 20));
        admission.openReady(firstLane);
        final var reservation = admission.tryAcquire(firstLane, new byte[16], 15).reservation();
        assertThrows(IllegalStateException.class, () -> admission.openReady(secondLane));
        reservation.release();
        admission.openReady(secondLane);
        assertTrue(admission.laneSnapshot(secondLane).ready());
    }

    private static DestinationPhysicalAdmission.LaneSpec spec(final DestinationLaneId lane,
                                                               final String cluster,
                                                               final long minimumRequests,
                                                               final long minimumBytes,
                                                               final long maxRequests,
                                                               final long maxBytes,
                                                               final long maxZombieRequests,
                                                               final long maxZombieBytes) {
        return new DestinationPhysicalAdmission.LaneSpec(lane, new byte[16], cluster, minimumRequests,
                minimumBytes, maxRequests, maxBytes, maxZombieRequests, maxZombieBytes);
    }

    private static DestinationLaneId lane(final String seed) {
        return DestinationLaneId.derive(Bytes.utf8(seed));
    }
}
