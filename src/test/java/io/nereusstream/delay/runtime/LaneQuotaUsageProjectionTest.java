package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.LaneQuotaUsageMapV1;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LaneQuotaUsageProjectionTest {
    @Test
    void tracksScheduleReservationCommitAndRetirementWithCheckedRevisions() {
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-quota-projection"));
        final byte[] incarnation = bytes(16, 3);
        LaneQuotaUsageProjection projection = LaneQuotaUsageProjection.empty()
                .addSchedule(lane, incarnation, 5, true, 1)
                .addReservation(lane, incarnation, 7, false, 2);

        var entry = projection.map().entries().get(0);
        assertEquals(1, entry.usage().activeMessages());
        assertEquals(5, entry.usage().pendingPayloadBytes());
        assertEquals(1, entry.usage().reservationMessages());
        assertEquals(7, entry.usage().reservationPayloadBytes());
        assertEquals(1, entry.usage().laneCount());
        assertEquals(projection.map(), LaneQuotaUsageMapV1.decode(projection.canonicalBytes()));

        projection = projection.commitReservation(lane, incarnation, 7, 3)
                .removeSchedule(lane, incarnation, 5, 4);
        entry = projection.map().entries().get(0);
        assertEquals(1, entry.usage().activeMessages());
        assertEquals(7, entry.usage().pendingPayloadBytes());
        assertEquals(0, entry.usage().reservationMessages());
        assertEquals(1, entry.usage().laneCount());

        projection = projection.removeSchedule(lane, incarnation, 7, 5)
                .removeLane(lane, incarnation, 6);
        assertEquals(0, projection.map().entries().size());
    }

    @Test
    void rejectsMissingOrForeignLaneAndUsageUnderflow() {
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-quota-fence"));
        final byte[] incarnation = bytes(16, 4);
        final byte[] foreignIncarnation = bytes(16, 5);
        final LaneQuotaUsageProjection projection = LaneQuotaUsageProjection.empty()
                .addSchedule(lane, incarnation, 4, true, 1);

        assertThrows(IllegalStateException.class,
                () -> projection.removeSchedule(lane, foreignIncarnation, 4, 2));
        assertThrows(IllegalStateException.class,
                () -> projection.removeSchedule(lane, incarnation, 5, 2));
        assertThrows(IllegalStateException.class,
                () -> LaneQuotaUsageProjection.empty().removeLane(lane, incarnation, 1));
        assertThrows(IllegalArgumentException.class,
                () -> projection.addSchedule(lane, incarnation, 1, false, 0));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
