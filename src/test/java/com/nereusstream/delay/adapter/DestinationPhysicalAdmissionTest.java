package com.nereusstream.delay.adapter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DestinationLaneId;
import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;

class DestinationPhysicalAdmissionTest {
    @Test
    void protectsOtherReadyLaneMinimumsAtWorkerAndTargetCluster() {
        final DestinationPhysicalAdmission admission = new DestinationPhysicalAdmission(4, 100);
        admission.registerTargetCluster("cluster-a", 3, 100);
        final DestinationLaneId laneA = lane("a");
        final DestinationLaneId laneB = lane("b");
        admission.registerLane(spec(laneA, "cluster-a", 1, 10, 3, 80, 3, 80));
        admission.registerLane(spec(laneB, "cluster-a", 1, 10, 3, 80, 3, 80));
        admission.openReady(laneA);
        admission.openReady(laneB);

        final DestinationPhysicalAdmission.AdmissionDecision first = admission.tryAcquire(laneA, new byte[16], 20);
        assertTrue(first.granted());
        final DestinationPhysicalAdmission.AdmissionDecision second = admission.tryAcquire(laneA, new byte[16], 20);
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

        assertEquals(
                DestinationPhysicalAdmission.Rejection.LANE_NOT_READY,
                admission.tryAcquire(lane, new byte[16], 1).rejection());
        admission.openReady(lane);
        assertEquals(
                DestinationPhysicalAdmission.Rejection.LANE_IDENTITY_MISMATCH,
                admission.tryAcquire(lane, new byte[15], 1).rejection());
        assertThrows(
                IllegalArgumentException.class,
                () -> admission.registerLane(spec(lane, "cluster-a", 0, 0, 2, 20, 1, 20)));
    }

    @Test
    void zombieChargeBlocksLaneUntilPhysicalReleaseAndExplicitClear() {
        final DestinationPhysicalAdmission admission = new DestinationPhysicalAdmission(3, 30);
        admission.registerTargetCluster("cluster-a", 3, 30);
        final DestinationLaneId lane = lane("zombie");
        admission.registerLane(spec(lane, "cluster-a", 0, 0, 3, 30, 1, 10));
        admission.openReady(lane);

        final var first = admission.tryAcquire(lane, new byte[16], 10).reservation();
        assertEquals(
                DestinationPhysicalAdmission.Rejection.ZOMBIE_CAPACITY,
                admission.tryAcquire(lane, new byte[16], 10).rejection());
        assertTrue(first.markZombie());
        assertFalse(admission.laneSnapshot(lane).blocked());
        assertEquals(
                DestinationPhysicalAdmission.Rejection.ZOMBIE_CAPACITY,
                admission.tryAcquire(lane, new byte[16], 1).rejection());
        assertTrue(admission.laneSnapshot(lane).blocked());

        assertThrows(IllegalStateException.class, () -> admission.clearZombieBlock(lane));
        first.release();
        admission.clearZombieBlock(lane);
        final var next = admission.tryAcquire(lane, new byte[16], 10);
        assertTrue(next.granted());
        next.reservation().close();
    }

    @Test
    void admissionReservesZombieRequestCapacityForAllOutstandingRequests() {
        final DestinationPhysicalAdmission admission = new DestinationPhysicalAdmission(3, 30);
        admission.registerTargetCluster("cluster-a", 3, 30);
        final DestinationLaneId lane = lane("zombie-request-reserve");
        admission.registerLane(spec(lane, "cluster-a", 0, 0, 3, 30, 2, 30));
        admission.openReady(lane);

        final var first = admission.tryAcquire(lane, new byte[16], 10);
        assertTrue(first.granted());
        final var second = admission.tryAcquire(lane, new byte[16], 10);
        assertTrue(second.granted());
        assertEquals(
                DestinationPhysicalAdmission.Rejection.ZOMBIE_CAPACITY,
                admission.tryAcquire(lane, new byte[16], 1).rejection());

        first.reservation().release();
        second.reservation().release();
        final var afterRelease = admission.tryAcquire(lane, new byte[16], 10);
        assertTrue(afterRelease.granted());
        afterRelease.reservation().release();
    }

    @Test
    void admissionReservesZombieByteCapacityForAllOutstandingRequests() {
        final DestinationPhysicalAdmission admission = new DestinationPhysicalAdmission(3, 30);
        admission.registerTargetCluster("cluster-a", 3, 30);
        final DestinationLaneId lane = lane("zombie-byte-reserve");
        admission.registerLane(spec(lane, "cluster-a", 0, 0, 3, 30, 3, 15));
        admission.openReady(lane);

        final var first = admission.tryAcquire(lane, new byte[16], 10);
        assertTrue(first.granted());
        assertEquals(
                DestinationPhysicalAdmission.Rejection.ZOMBIE_CAPACITY,
                admission.tryAcquire(lane, new byte[16], 6).rejection());
        final var exact = admission.tryAcquire(lane, new byte[16], 5);
        assertTrue(exact.granted());

        first.reservation().release();
        exact.reservation().release();
        assertEquals(0, admission.workerSnapshot().activeRequests());
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
        final var reservation =
                admission.tryAcquire(firstLane, new byte[16], 15).reservation();
        assertThrows(IllegalStateException.class, () -> admission.openReady(secondLane));
        reservation.release();
        admission.openReady(secondLane);
        assertTrue(admission.laneSnapshot(secondLane).ready());
    }

    @Test
    void openingLaneCountsItsReadyMinimumExactlyOnce() {
        final DestinationPhysicalAdmission admission = new DestinationPhysicalAdmission(2, 10);
        admission.registerTargetCluster("cluster-a", 2, 10);
        final DestinationLaneId lane = lane("exact-minimum");
        admission.registerLane(spec(lane, "cluster-a", 2, 10, 2, 10, 1, 10));

        admission.openReady(lane);

        assertTrue(admission.laneSnapshot(lane).ready());
        assertEquals(2, admission.workerSnapshot().protectedReadyRequests());
        assertEquals(10, admission.workerSnapshot().protectedReadyBytes());
    }

    @Test
    void unregisterRequiresFencedIncarnationAndPhysicalQuiescence() {
        final DestinationPhysicalAdmission admission = new DestinationPhysicalAdmission(2, 20);
        admission.registerTargetCluster("cluster-a", 2, 20);
        final DestinationLaneId lane = lane("unregister");
        final byte[] firstIncarnation = new byte[16];
        firstIncarnation[0] = 1;
        admission.registerLane(spec(lane, firstIncarnation, "cluster-a", 0, 0, 2, 20, 1, 20));
        admission.openReady(lane);
        assertThrows(IllegalStateException.class, () -> admission.unregisterLane(lane, firstIncarnation));

        final var reservation = admission.tryAcquire(lane, firstIncarnation, 5).reservation();
        admission.closeReady(lane);
        assertThrows(IllegalStateException.class, () -> admission.unregisterLane(lane, firstIncarnation));
        reservation.release();
        assertThrows(IllegalArgumentException.class, () -> admission.unregisterLane(lane, new byte[16]));
        admission.unregisterLane(lane, firstIncarnation);
        assertEquals(
                DestinationPhysicalAdmission.Rejection.LANE_NOT_REGISTERED,
                admission.tryAcquire(lane, firstIncarnation, 1).rejection());

        final byte[] replacementIncarnation = new byte[16];
        replacementIncarnation[0] = 2;
        admission.registerLane(spec(lane, replacementIncarnation, "cluster-a", 0, 0, 2, 20, 1, 20));
        assertThrows(IllegalArgumentException.class, () -> admission.unregisterLane(lane, firstIncarnation));
        admission.openReady(lane);
        final var replacement = admission.tryAcquire(lane, replacementIncarnation, 5);
        assertTrue(replacement.granted());
        replacement.reservation().release();
    }

    @Test
    void zombieReleaseUnderflowDoesNotPartiallyDecrementActiveCharge() throws Exception {
        final DestinationPhysicalAdmission admission = new DestinationPhysicalAdmission(2, 20);
        admission.registerTargetCluster("cluster-a", 2, 20);
        final DestinationLaneId lane = lane("zombie-underflow");
        admission.registerLane(spec(lane, "cluster-a", 0, 0, 2, 20, 1, 20));
        admission.openReady(lane);
        final DestinationPhysicalAdmission.Reservation reservation =
                admission.tryAcquire(lane, new byte[16], 10).reservation();
        assertTrue(reservation.markZombie());

        final Field lanes = DestinationPhysicalAdmission.class.getDeclaredField("lanes");
        lanes.setAccessible(true);
        final Object laneState = ((java.util.Map<?, ?>) lanes.get(admission)).get(lane);
        final Field zombieRequests = laneState.getClass().getDeclaredField("zombieRequests");
        zombieRequests.setAccessible(true);
        zombieRequests.setLong(laneState, 0);

        assertThrows(IllegalStateException.class, reservation::release);
        assertEquals(1, admission.laneSnapshot(lane).activeRequests());
        assertEquals(0, admission.laneSnapshot(lane).zombieRequests());
        assertEquals(DestinationPhysicalAdmission.ReservationState.ZOMBIE, reservation.state());

        zombieRequests.setLong(laneState, 1);
        assertTrue(reservation.release());
        assertEquals(0, admission.laneSnapshot(lane).activeRequests());
        assertEquals(DestinationPhysicalAdmission.ReservationState.RELEASED, reservation.state());
    }

    @Test
    void targetClusterIdentityRejectsNonCanonicalText() {
        final DestinationPhysicalAdmission admission = new DestinationPhysicalAdmission(2, 20);
        final String nonCanonical = "cluster" + '\u0301';
        assertThrows(IllegalArgumentException.class, () -> admission.registerTargetCluster(nonCanonical, 2, 20));
        assertThrows(IllegalArgumentException.class, () -> admission.registerTargetCluster("cluster\0", 2, 20));
    }

    private static DestinationPhysicalAdmission.LaneSpec spec(
            final DestinationLaneId lane,
            final String cluster,
            final long minimumRequests,
            final long minimumBytes,
            final long maxRequests,
            final long maxBytes,
            final long maxZombieRequests,
            final long maxZombieBytes) {
        return spec(
                lane,
                new byte[16],
                cluster,
                minimumRequests,
                minimumBytes,
                maxRequests,
                maxBytes,
                maxZombieRequests,
                maxZombieBytes);
    }

    private static DestinationPhysicalAdmission.LaneSpec spec(
            final DestinationLaneId lane,
            final byte[] incarnation,
            final String cluster,
            final long minimumRequests,
            final long minimumBytes,
            final long maxRequests,
            final long maxBytes,
            final long maxZombieRequests,
            final long maxZombieBytes) {
        return new DestinationPhysicalAdmission.LaneSpec(
                lane,
                incarnation,
                cluster,
                minimumRequests,
                minimumBytes,
                maxRequests,
                maxBytes,
                maxZombieRequests,
                maxZombieBytes);
    }

    private static DestinationLaneId lane(final String seed) {
        return DestinationLaneId.derive(Bytes.utf8(seed));
    }
}
