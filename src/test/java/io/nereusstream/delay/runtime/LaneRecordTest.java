package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.DestinationLaneId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LaneRecordTest {
    @Test
    void controlTransitionsAreExplicitAndIrreversibleWhereRequired() {
        final LaneRecord initial = new LaneRecord(new DestinationLaneId(new byte[32]), new byte[16], 1, 0,
                AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 0);
        final LaneRecord paused = initial.pauseByAdmin();
        assertEquals(AdmissionGate.ADMIN_PAUSED, paused.admissionGate());
        final LaneRecord resumed = paused.resumeByAdmin();
        assertEquals(AdmissionGate.OPEN, resumed.admissionGate());
        final LaneRecord broken = resumed.breakOrdering();
        assertEquals(AdmissionGate.ORDERING_BROKEN, broken.admissionGate());
        final LaneRecord closed = broken.closeForNewAdmission();
        assertEquals(AdmissionGate.CLOSED, closed.admissionGate());
        final LaneRecord retired = closed.retire();
        assertEquals(AdmissionGate.RETIRED, retired.admissionGate());
        assertThrows(IllegalStateException.class, () -> broken.withGate(AdmissionGate.ADMIN_PAUSED));
        assertThrows(IllegalStateException.class, () -> broken.withGate(AdmissionGate.OPEN));
        assertThrows(IllegalStateException.class, () -> closed.withGate(AdmissionGate.OPEN));
        assertThrows(IllegalStateException.class, retired::resumeByAdmin);
        assertThrows(IllegalStateException.class, closed::breakOrdering);
    }

    @Test
    void versionCountersFailClosedBeforeLongOverflow() {
        final DestinationLaneId lane = new DestinationLaneId(new byte[32]);
        final LaneRecord runtimeExhausted = new LaneRecord(lane, new byte[16], 1, Long.MAX_VALUE,
                AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 0);
        assertThrows(IllegalStateException.class,
                () -> runtimeExhausted.withReadiness(RuntimeReadiness.BLOCKED));

        final LaneRecord controlExhausted = new LaneRecord(lane, new byte[16], Long.MAX_VALUE, 0,
                AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 0);
        assertThrows(IllegalStateException.class, controlExhausted::pauseByAdmin);
    }

    @Test
    void runtimeReadinessMustPassThroughRecoveryBeforeBecomingReadyAgain() {
        final LaneRecord initial = new LaneRecord(new DestinationLaneId(new byte[32]), new byte[16], 1, 0,
                AdmissionGate.OPEN, RuntimeReadiness.RECOVERING_EVIDENCE, 1, 0);
        final LaneRecord ready = initial.withReadiness(RuntimeReadiness.READY);
        final LaneRecord blocked = ready.withReadiness(RuntimeReadiness.BLOCKED);

        assertThrows(IllegalStateException.class,
                () -> blocked.withReadiness(RuntimeReadiness.READY));
        final LaneRecord recovering = blocked.withReadiness(RuntimeReadiness.RECOVERING_EVIDENCE);
        assertEquals(RuntimeReadiness.RECOVERING_EVIDENCE, recovering.runtimeReadiness());
        assertEquals(RuntimeReadiness.READY, recovering.withReadiness(RuntimeReadiness.READY).runtimeReadiness());
        assertEquals(recovering, recovering.withReadiness(RuntimeReadiness.RECOVERING_EVIDENCE));
    }

    @Test
    void directProjectionCannotPersistReadyLaneBehindAClosedAdmissionGate() {
        final DestinationLaneId lane = new DestinationLaneId(new byte[32]);

        assertThrows(IllegalArgumentException.class, () -> new LaneRecord(lane, new byte[16], 1, 0,
                AdmissionGate.ADMIN_PAUSED, RuntimeReadiness.READY, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new LaneRecord(lane, new byte[16], 1, 0,
                AdmissionGate.CLOSED, RuntimeReadiness.READY, 1, 0));
    }
}
