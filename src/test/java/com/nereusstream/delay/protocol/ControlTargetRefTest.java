package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

class ControlTargetRefTest {
    @Test
    void roundTripsAllClosedTargetBranchesAndOptionalMutationIdentity() {
        final RouteIncarnation route = RouteIncarnation.random();
        final ShardId shard = new ShardId(route, 3);
        final DelayMessageId message =
                new DelayMessageId(SelfRoutingId.random(shard).bytes());
        final ProfileRef profile = new ProfileRef(Bytes.utf8("destination"), 2, bytes(32, 20), ProfileKind.DESTINATION);
        final QuotaGrantRef quota = new QuotaGrantRef(
                bytes(32, 21),
                1,
                new PublishAdmissionBody.ChargeVector(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        final List<ControlTargetRef> targets = List.of(
                new ControlTargetRef(0, ControlTargetKind.SHARD, new ShardSubject(shard), null, null),
                new ControlTargetRef(
                        1,
                        ControlTargetKind.LANE,
                        new LaneControlTarget(bytes(32, 1), bytes(16, 2), 3),
                        bytes(32, 3),
                        bytes(32, 4)),
                new ControlTargetRef(
                        2,
                        ControlTargetKind.MESSAGE,
                        new ControlMessageTarget(message, 1, 2, bytes(32, 5)),
                        null,
                        null),
                new ControlTargetRef(3, ControlTargetKind.ROUTE, route.bytes(), null, null),
                new ControlTargetRef(
                        4,
                        ControlTargetKind.PROFILE,
                        new ProfileControlTarget(profile, 2, bytes(32, 6), Long.MIN_VALUE),
                        null,
                        null),
                new ControlTargetRef(5, ControlTargetKind.QUOTA_GRANT, quota, null, null));

        for (ControlTargetRef target : targets) {
            assertEquals(target, ControlTargetRef.decode(target.canonicalBytes()), "target " + target.targetKind());
        }
        assertEquals(2L, ((ProfileControlTarget) targets.get(4).target()).expectedSecretGeneration());
        assertEquals(Long.MIN_VALUE, ((ProfileControlTarget) targets.get(4).target()).expectedBindingHeadRevision());
        assertArrayEquals(route.bytes(), targets.get(3).routeUuid());
    }

    @Test
    void rejectsDigestTamperingPartialMutationIdentityAndWrongBranchType() {
        final RouteIncarnation route = RouteIncarnation.random();
        final ControlTargetRef target =
                new ControlTargetRef(1, ControlTargetKind.ROUTE, route.bytes(), bytes(32, 1), bytes(32, 2));
        final byte[] tampered = target.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ControlTargetRef.decode(tampered));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ControlTargetRef(1, ControlTargetKind.ROUTE, route.bytes(), bytes(32, 1), null));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ControlTargetRef(1, ControlTargetKind.SHARD, route.bytes(), null, null));

        final byte[] wrongKind = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.uint32(output, 2, ControlTargetKind.ROUTE.wireValue());
            CanonicalProtobuf.bytes(output, 13, bytes(16, 2));
            CanonicalProtobuf.bytes(output, 22, bytes(32, 3));
        });
        assertThrows(IllegalArgumentException.class, () -> ControlTargetRef.decode(wrongKind));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
