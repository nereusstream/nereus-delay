package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchedulerProjectionsV1Test {
    @Test
    void closedSchedulerValuesRoundTripWithCanonicalDigests() {
        final DestinationLaneId first = lane(1);
        final DestinationLaneId second = lane(2);
        final byte[] firstIncarnation = bytes(16, 1);
        final byte[] secondIncarnation = bytes(16, 2);
        final OwnerIdentityV1 owner = new OwnerIdentityV1(Bytes.utf8("deployment"), Bytes.utf8("worker"), 7,
                bytes(32, 9));
        final SchedulerProjectionsV1.ReadyDiscoveryCursor discovery =
                new SchedulerProjectionsV1.ReadyDiscoveryCursor(bytes(7, 4), 3, 8);
        final SchedulerProjectionsV1.ActiveRing ring = new SchedulerProjectionsV1.ActiveRing(8, 12, 1, List.of(
                new SchedulerProjectionsV1.RingEntry(second, secondIncarnation, 4),
                new SchedulerProjectionsV1.RingEntry(first, firstIncarnation, 5)));
        final SchedulerProjectionsV1.DeficitMap deficits = new SchedulerProjectionsV1.DeficitMap(List.of(
                new SchedulerProjectionsV1.DeficitEntry(second, secondIncarnation, 22, 4),
                new SchedulerProjectionsV1.DeficitEntry(first, firstIncarnation, 11, 5)));
        final SchedulerProjectionsV1.Round round = new SchedulerProjectionsV1.Round(12, owner, true);
        final SchedulerProjectionsV1.LastServedMap served = new SchedulerProjectionsV1.LastServedMap(List.of(
                new SchedulerProjectionsV1.LastServedEntry(second, secondIncarnation, 9, 3),
                new SchedulerProjectionsV1.LastServedEntry(first, firstIncarnation, 10, 2)));

        assertArrayEquals(discovery.canonicalBytes(),
                SchedulerProjectionsV1.ReadyDiscoveryCursor.decode(discovery.canonicalBytes()).canonicalBytes());
        assertArrayEquals(ring.canonicalBytes(),
                SchedulerProjectionsV1.ActiveRing.decode(ring.canonicalBytes()).canonicalBytes());
        assertArrayEquals(deficits.canonicalBytes(),
                SchedulerProjectionsV1.DeficitMap.decode(deficits.canonicalBytes()).canonicalBytes());
        assertArrayEquals(round.canonicalBytes(),
                SchedulerProjectionsV1.Round.decode(round.canonicalBytes()).canonicalBytes());
        assertArrayEquals(served.canonicalBytes(),
                SchedulerProjectionsV1.LastServedMap.decode(served.canonicalBytes()).canonicalBytes());
        assertArrayEquals(owner.canonicalBytes(), OwnerIdentityV1.decode(owner.canonicalBytes()).canonicalBytes());
    }

    @Test
    void schedulerProjectionDigestAndOrderingAreClosed() {
        final SchedulerProjectionsV1.DeficitMap map = new SchedulerProjectionsV1.DeficitMap(List.of(
                new SchedulerProjectionsV1.DeficitEntry(lane(1), bytes(16, 1), 1, 1)));
        final byte[] tampered = map.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> SchedulerProjectionsV1.DeficitMap.decode(tampered));

        final SchedulerProjectionsV1.ActiveRing ring = new SchedulerProjectionsV1.ActiveRing(1, 0, 0, List.of(
                new SchedulerProjectionsV1.RingEntry(lane(1), bytes(16, 1), 1)));
        final byte[] duplicate = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.uint64(output, 2, 1);
            CanonicalProtobuf.uint64(output, 3, 0);
            CanonicalProtobuf.uint32(output, 4, 0);
            CanonicalProtobuf.bytes(output, 5, ring.entries().get(0).canonicalBytes());
            CanonicalProtobuf.bytes(output, 5, ring.entries().get(0).canonicalBytes());
            CanonicalProtobuf.bytes(output, 6, bytes(32, 0));
        });
        assertThrows(IllegalArgumentException.class, () -> SchedulerProjectionsV1.ActiveRing.decode(duplicate));
    }

    private static DestinationLaneId lane(final int value) {
        final byte[] bytes = new byte[DestinationLaneId.LENGTH];
        bytes[bytes.length - 1] = (byte) value;
        return new DestinationLaneId(bytes);
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
