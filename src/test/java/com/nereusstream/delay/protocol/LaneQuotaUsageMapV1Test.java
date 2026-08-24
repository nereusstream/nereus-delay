package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

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
    void usageRevisionPreservesCompleteUnsigned64BitPattern() {
        final LaneQuotaUsageEntryV1 entry = new LaneQuotaUsageEntryV1(
                new DestinationLaneId(bytes(32, 7)),
                bytes(16, 8),
                new PublishAdmissionBody.ChargeVector(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0),
                Long.MIN_VALUE);

        final LaneQuotaUsageEntryV1 decoded = LaneQuotaUsageEntryV1.decode(entry.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.usageRevision());
        assertEquals(entry, decoded);
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
        return new LaneQuotaUsageEntryV1(
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
