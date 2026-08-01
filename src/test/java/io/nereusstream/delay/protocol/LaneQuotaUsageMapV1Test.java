package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LaneQuotaUsageMapV1Test {
    @Test
    void roundTripsCanonicalSortedEntries() {
        final LaneQuotaUsageEntryV1 first = entry(1, 1);
        final LaneQuotaUsageEntryV1 second = entry(2, 2);
        final LaneQuotaUsageMapV1 map = new LaneQuotaUsageMapV1(List.of(first, second));

        assertEquals(map, LaneQuotaUsageMapV1.decode(map.canonicalBytes()));
        assertEquals(List.of(first, second), map.entries());
    }

    @Test
    void rejectsUnsortedDuplicateOrTamperedEntries() {
        final LaneQuotaUsageEntryV1 first = entry(1, 1);
        final LaneQuotaUsageEntryV1 second = entry(2, 2);
        assertThrows(IllegalArgumentException.class, () -> new LaneQuotaUsageMapV1(List.of(second, first)));
        assertThrows(IllegalArgumentException.class, () -> new LaneQuotaUsageMapV1(List.of(first, first)));

        final byte[] encoded = first.canonicalBytes();
        encoded[encoded.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> LaneQuotaUsageEntryV1.decode(encoded));
    }

    private static LaneQuotaUsageEntryV1 entry(final int laneSeed, final int incarnationSeed) {
        final byte[] laneBytes = new byte[32];
        laneBytes[31] = (byte) laneSeed;
        final byte[] incarnation = new byte[16];
        incarnation[15] = (byte) incarnationSeed;
        return new LaneQuotaUsageEntryV1(new DestinationLaneId(laneBytes), incarnation,
                new PublishAdmissionBody.ChargeVector(laneSeed, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        1, 0), incarnationSeed);
    }
}
