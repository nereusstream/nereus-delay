package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class SchedulerProjectionsTest {
    @Test
    void closedSchedulerValuesRoundTripWithCanonicalDigests() {
        final DestinationLaneId first = lane(1);
        final DestinationLaneId second = lane(2);
        final byte[] firstIncarnation = bytes(16, 1);
        final byte[] secondIncarnation = bytes(16, 2);
        final OwnerIdentity owner = new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("worker"), 7, bytes(32, 9));
        final SchedulerProjections.ReadyDiscoveryCursor discovery =
                new SchedulerProjections.ReadyDiscoveryCursor(bytes(7, 4), 3, 8);
        final SchedulerProjections.ActiveRing ring = new SchedulerProjections.ActiveRing(
                8,
                12,
                1,
                List.of(
                        new SchedulerProjections.RingEntry(second, secondIncarnation, 4),
                        new SchedulerProjections.RingEntry(first, firstIncarnation, 5)));
        final SchedulerProjections.DeficitMap deficits = new SchedulerProjections.DeficitMap(List.of(
                new SchedulerProjections.DeficitEntry(second, secondIncarnation, 22, 4),
                new SchedulerProjections.DeficitEntry(first, firstIncarnation, 11, 5)));
        final SchedulerProjections.Round round = new SchedulerProjections.Round(12, owner, true);
        final SchedulerProjections.LastServedMap served = new SchedulerProjections.LastServedMap(List.of(
                new SchedulerProjections.LastServedEntry(second, secondIncarnation, 9, 3),
                new SchedulerProjections.LastServedEntry(first, firstIncarnation, 10, 2)));

        assertArrayEquals(
                discovery.canonicalBytes(),
                SchedulerProjections.ReadyDiscoveryCursor.decode(discovery.canonicalBytes())
                        .canonicalBytes());
        assertArrayEquals(
                ring.canonicalBytes(),
                SchedulerProjections.ActiveRing.decode(ring.canonicalBytes()).canonicalBytes());
        assertArrayEquals(
                deficits.canonicalBytes(),
                SchedulerProjections.DeficitMap.decode(deficits.canonicalBytes())
                        .canonicalBytes());
        assertArrayEquals(
                round.canonicalBytes(),
                SchedulerProjections.Round.decode(round.canonicalBytes()).canonicalBytes());
        assertArrayEquals(
                served.canonicalBytes(),
                SchedulerProjections.LastServedMap.decode(served.canonicalBytes())
                        .canonicalBytes());
        assertArrayEquals(
                owner.canonicalBytes(),
                OwnerIdentity.decode(owner.canonicalBytes()).canonicalBytes());
    }

    @Test
    void schedulerProjectionDigestAndOrderingAreClosed() {
        final SchedulerProjections.DeficitMap map = new SchedulerProjections.DeficitMap(
                List.of(new SchedulerProjections.DeficitEntry(lane(1), bytes(16, 1), 1, 1)));
        final byte[] tampered = map.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> SchedulerProjections.DeficitMap.decode(tampered));

        final SchedulerProjections.ActiveRing ring = new SchedulerProjections.ActiveRing(
                1, 0, 0, List.of(new SchedulerProjections.RingEntry(lane(1), bytes(16, 1), 1)));
        final byte[] duplicate = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.uint64(output, 2, 1);
            CanonicalProtobuf.uint64(output, 3, 0);
            CanonicalProtobuf.uint32(output, 4, 0);
            CanonicalProtobuf.bytes(output, 5, ring.entries().get(0).canonicalBytes());
            CanonicalProtobuf.bytes(output, 5, ring.entries().get(0).canonicalBytes());
            CanonicalProtobuf.bytes(output, 6, bytes(32, 0));
        });
        assertThrows(IllegalArgumentException.class, () -> SchedulerProjections.ActiveRing.decode(duplicate));
    }

    @Test
    void schedulerUint64FieldsPreserveCompleteRawBitPatterns() {
        final DestinationLaneId lane = lane(9);
        final byte[] incarnation = bytes(16, 9);
        final OwnerIdentity owner =
                new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("worker"), Long.MIN_VALUE, bytes(32, 9));
        final SchedulerProjections.ReadyDiscoveryCursor discovery =
                new SchedulerProjections.ReadyDiscoveryCursor(bytes(7, 4), Long.MIN_VALUE, -1L);
        final SchedulerProjections.RingEntry ringEntry =
                new SchedulerProjections.RingEntry(lane, incarnation, Long.MIN_VALUE);
        final SchedulerProjections.ActiveRing ring =
                new SchedulerProjections.ActiveRing(Long.MIN_VALUE, -1L, 0, List.of(ringEntry));
        final SchedulerProjections.DeficitMap deficits = new SchedulerProjections.DeficitMap(
                List.of(new SchedulerProjections.DeficitEntry(lane, incarnation, -1L, Long.MIN_VALUE)));
        final SchedulerProjections.Round round = new SchedulerProjections.Round(-1L, owner, true);
        final SchedulerProjections.LastServedMap served = new SchedulerProjections.LastServedMap(
                List.of(new SchedulerProjections.LastServedEntry(lane, incarnation, -1L, Long.MIN_VALUE)));

        assertArrayEquals(
                discovery.canonicalBytes(),
                SchedulerProjections.ReadyDiscoveryCursor.decode(discovery.canonicalBytes())
                        .canonicalBytes());
        assertArrayEquals(
                ring.canonicalBytes(),
                SchedulerProjections.ActiveRing.decode(ring.canonicalBytes()).canonicalBytes());
        assertArrayEquals(
                deficits.canonicalBytes(),
                SchedulerProjections.DeficitMap.decode(deficits.canonicalBytes())
                        .canonicalBytes());
        assertArrayEquals(
                round.canonicalBytes(),
                SchedulerProjections.Round.decode(round.canonicalBytes()).canonicalBytes());
        assertArrayEquals(
                served.canonicalBytes(),
                SchedulerProjections.LastServedMap.decode(served.canonicalBytes())
                        .canonicalBytes());
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
