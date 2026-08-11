package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadReference;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PayloadReservationTest {
    @Test
    void decodeRejectsTruncatedNumericAndPresenceFieldsAsValidationErrors() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("payload-reservation-truncation"));
        final LargeScheduleIntent intent = new LargeScheduleIntent(lane, 2_000, 5_000,
                OrderingMode.BEST_EFFORT, 7, Bytes.sha256(Bytes.utf8("payload")), 1_000, 1);
        final PayloadReservation reservation = new PayloadReservation(shardId, new byte[32],
                CommandId.random(shardId), DelayMessageId.random(shardId), Bytes.sha256(Bytes.utf8("command")),
                intent, 4_000, PayloadReservationStatus.RESERVED, 1, new byte[]{1}, null);
        final byte[] encoded = reservation.encode();

        for (int length = 0; length < encoded.length; length++) {
            final byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(IllegalArgumentException.class, () -> PayloadReservation.decode(truncated),
                    "truncated payload reservation length=" + length);
        }
        assertArrayEquals(encoded, PayloadReservation.decode(encoded).encode());
    }

    @Test
    void legacyReservationValueUpgradesToCurrentReceiptAnchorProjection() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("payload-reservation-legacy"));
        final LargeScheduleIntent intent = new LargeScheduleIntent(lane, 2_000, 5_000,
                OrderingMode.BEST_EFFORT, 7, Bytes.sha256(Bytes.utf8("payload")), 1_000, 1);
        final PayloadReservation reservation = new PayloadReservation(shardId, new byte[32],
                CommandId.random(shardId), DelayMessageId.random(shardId), Bytes.sha256(Bytes.utf8("command")),
                intent, 4_000, PayloadReservationStatus.RESERVED, 1, new byte[]{1}, null);
        final byte[] current = reservation.encode();
        final int anchorBytes = Long.BYTES + Integer.BYTES + reservation.receiptAnchorSourcePosition().length;
        final byte[] legacy = Arrays.copyOf(current, current.length - anchorBytes);
        legacy[3] = 1;

        final PayloadReservation upgraded = PayloadReservation.decode(legacy);
        assertEquals(reservation.stateVersion(), upgraded.receiptAnchorStateVersion());
        assertArrayEquals(reservation.sourcePosition(), upgraded.receiptAnchorSourcePosition());
        assertArrayEquals(current, upgraded.encode());
    }

    @Test
    void committedPayloadMustMatchPrepareLengthAndDigest() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("payload-reservation-binding"));
        final byte[] expectedPayload = Bytes.utf8("expected");
        final LargeScheduleIntent intent = new LargeScheduleIntent(lane, 2_000, 5_000,
                OrderingMode.BEST_EFFORT, expectedPayload.length, Bytes.sha256(expectedPayload), 1_000, 1);
        final PayloadReference mismatched = new PayloadReference(Bytes.sha256(Bytes.utf8("profile")),
                Bytes.utf8("container"), Bytes.utf8("object"), Bytes.utf8("version"), null,
                expectedPayload.length + 1, Bytes.sha256(Bytes.utf8("different")));

        assertThrows(IllegalArgumentException.class, () -> new PayloadReservation(shardId, new byte[32],
                CommandId.random(shardId), DelayMessageId.random(shardId), Bytes.sha256(Bytes.utf8("command")),
                intent, 4_000, PayloadReservationStatus.COMMITTED, 1, new byte[]{1}, mismatched));
    }
}
