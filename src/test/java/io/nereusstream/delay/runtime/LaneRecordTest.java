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
}
