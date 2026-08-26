package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

class LaneQuotaUsageMapTest {
    @Test
    void roundTripsCanonicalSortedEntries() {
        final LaneQuotaUsageEntry first = entry(1, 1);
        final LaneQuotaUsageEntry second = entry(2, 2);
        final LaneQuotaUsageMap map = new LaneQuotaUsageMap(List.of(first, second));

        assertEquals(map, LaneQuotaUsageMap.decode(map.canonicalBytes()));
        assertEquals(List.of(first, second), map.entries());
    }

    @Test
    void usageRevisionPreservesCompleteUnsigned64BitPattern() {
        final LaneQuotaUsageEntry entry = new LaneQuotaUsageEntry(
                new DestinationLaneId(bytes(32, 7)),
                bytes(16, 8),
                new PublishAdmissionBody.ChargeVector(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0),
                Long.MIN_VALUE);

        final LaneQuotaUsageEntry decoded = LaneQuotaUsageEntry.decode(entry.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.usageRevision());
        assertEquals(entry, decoded);
    }

    @Test
    void rejectsUnsortedDuplicateOrTamperedEntries() {
        final LaneQuotaUsageEntry first = entry(1, 1);
        final LaneQuotaUsageEntry second = entry(2, 2);
        assertThrows(IllegalArgumentException.class, () -> new LaneQuotaUsageMap(List.of(second, first)));
        assertThrows(IllegalArgumentException.class, () -> new LaneQuotaUsageMap(List.of(first, first)));

        final byte[] encoded = first.canonicalBytes();
        encoded[encoded.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> LaneQuotaUsageEntry.decode(encoded));
    }

    private static LaneQuotaUsageEntry entry(final int laneSeed, final int incarnationSeed) {
        return new LaneQuotaUsageEntry(
                new DestinationLaneId(bytes(32, laneSeed)),
                bytes(16, incarnationSeed),
                new PublishAdmissionBody.ChargeVector(laneSeed, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0),
                incarnationSeed);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        value[length - 1] = (byte) seed;
        return value;
    }
}
