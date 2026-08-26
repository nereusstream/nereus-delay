package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.LaneQuotaUsageEntry;
import com.nereusstream.delay.protocol.LaneQuotaUsageMap;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import java.util.List;
import org.junit.jupiter.api.Test;

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
        assertEquals(projection.map(), LaneQuotaUsageMap.decode(projection.canonicalBytes()));

        projection = projection.commitReservation(lane, incarnation, 7, 3).removeSchedule(lane, incarnation, 5, 4);
        entry = projection.map().entries().get(0);
        assertEquals(1, entry.usage().activeMessages());
        assertEquals(7, entry.usage().pendingPayloadBytes());
        assertEquals(0, entry.usage().reservationMessages());
        assertEquals(1, entry.usage().laneCount());

        projection = projection.removeSchedule(lane, incarnation, 7, 5).removeLane(lane, incarnation, 6);
        assertEquals(0, projection.map().entries().size());
    }

    @Test
    void rejectsMissingOrForeignLaneAndUsageUnderflow() {
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-quota-fence"));
        final byte[] incarnation = bytes(16, 4);
        final byte[] foreignIncarnation = bytes(16, 5);
        final LaneQuotaUsageProjection projection =
                LaneQuotaUsageProjection.empty().addSchedule(lane, incarnation, 4, true, 1);

        assertThrows(IllegalStateException.class, () -> projection.removeSchedule(lane, foreignIncarnation, 4, 2));
        assertThrows(IllegalStateException.class, () -> projection.removeSchedule(lane, incarnation, 5, 2));
        assertThrows(IllegalStateException.class, () -> LaneQuotaUsageProjection.empty()
                .removeLane(lane, incarnation, 1));
        assertThrows(IllegalArgumentException.class, () -> projection.addSchedule(lane, incarnation, 1, false, 0));
    }

    @Test
    void tracksClaimAndAttemptExecutionChargeWithCheckedArithmetic() {
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-quota-inflight"));
        final byte[] incarnation = bytes(16, 6);
        LaneQuotaUsageProjection projection = LaneQuotaUsageProjection.empty()
                .addSchedule(lane, incarnation, 8, true, 1)
                .addInflight(lane, incarnation, 1, 13, 1);

        var entry = projection.map().entries().get(0);
        assertEquals(1, entry.usage().inflightMessages());
        assertEquals(13, entry.usage().inflightBytes());

        projection = projection.removeInflight(lane, incarnation, 1, 13, 2);
        assertEquals(0, projection.map().entries().get(0).usage().inflightMessages());
        assertEquals(0, projection.map().entries().get(0).usage().inflightBytes());
        final LaneQuotaUsageProjection afterRelease = projection;
        assertThrows(IllegalStateException.class, () -> afterRelease.removeInflight(lane, incarnation, 1, 1, 3));
    }

    @Test
    void retirementRejectsAnyNonSlotUsageDimension() {
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-quota-retained"));
        final byte[] incarnation = bytes(16, 7);
        final PublishAdmissionBody.ChargeVector usage =
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0);
        final LaneQuotaUsageProjection projection = LaneQuotaUsageProjection.decode(
                new LaneQuotaUsageMap(List.of(new LaneQuotaUsageEntry(lane, incarnation, usage, 1))).canonicalBytes());

        assertThrows(IllegalStateException.class, () -> projection.removeLane(lane, incarnation, 2));

        final PublishAdmissionBody.ChargeVector strongUsage =
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1);
        final LaneQuotaUsageProjection strongProjection = LaneQuotaUsageProjection.decode(
                new LaneQuotaUsageMap(List.of(new LaneQuotaUsageEntry(lane, incarnation, strongUsage, 1)))
                        .canonicalBytes());
        assertEquals(
                0,
                strongProjection
                        .removeLane(lane, incarnation, 2)
                        .map()
                        .entries()
                        .size());
    }

    @Test
    void findsExactIncarnationWhenSameLaneRetainsAForeignEntry() {
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-quota-incarnations"));
        final byte[] oldIncarnation = bytes(16, 1);
        final byte[] newIncarnation = bytes(16, 2);
        final PublishAdmissionBody.ChargeVector oldUsage =
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0);
        final PublishAdmissionBody.ChargeVector newUsage =
                new PublishAdmissionBody.ChargeVector(1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0);
        LaneQuotaUsageProjection projection = LaneQuotaUsageProjection.decode(new LaneQuotaUsageMap(List.of(
                        new LaneQuotaUsageEntry(lane, oldIncarnation, oldUsage, 7),
                        new LaneQuotaUsageEntry(lane, newIncarnation, newUsage, 7)))
                .canonicalBytes());

        assertEquals(newUsage, projection.usageFor(lane, newIncarnation));
        projection = projection.addSchedule(lane, newIncarnation, 3, false, 8);
        assertEquals(oldUsage, projection.usageFor(lane, oldIncarnation));
        assertEquals(2, projection.usageFor(lane, newIncarnation).activeMessages());
        assertEquals(12, projection.usageFor(lane, newIncarnation).pendingPayloadBytes());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
