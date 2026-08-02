package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControlTargetRefV1Test {
    @Test
    void roundTripsAllClosedTargetBranchesAndOptionalMutationIdentity() {
        final RouteIncarnation route = RouteIncarnation.random();
        final ShardId shard = new ShardId(route, 3);
        final DelayMessageId message = new DelayMessageId(SelfRoutingId.random(shard).bytes());
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("destination"), 2, bytes(32, 20),
                ProfileKindV1.DESTINATION);
        final QuotaGrantRefV1 quota = new QuotaGrantRefV1(bytes(32, 21), 1,
                new PublishAdmissionBody.ChargeVector(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        final List<ControlTargetRefV1> targets = List.of(
                new ControlTargetRefV1(0, ControlTargetKindV1.SHARD, new ShardSubjectV1(shard), null, null),
                new ControlTargetRefV1(1, ControlTargetKindV1.LANE,
                        new LaneControlTargetV1(bytes(32, 1), bytes(16, 2), 3), bytes(32, 3), bytes(32, 4)),
                new ControlTargetRefV1(2, ControlTargetKindV1.MESSAGE,
                        new ControlMessageTargetV1(message, 1, 2, bytes(32, 5)), null, null),
                new ControlTargetRefV1(3, ControlTargetKindV1.ROUTE, route.bytes(), null, null),
                new ControlTargetRefV1(4, ControlTargetKindV1.PROFILE, new ProfileControlTargetV1(profile, 2,
                        bytes(32, 6), 7), null, null),
                new ControlTargetRefV1(5, ControlTargetKindV1.QUOTA_GRANT, quota, null, null));

        for (ControlTargetRefV1 target : targets) {
            assertEquals(target, ControlTargetRefV1.decode(target.canonicalBytes()),
                    "target " + target.targetKind());
        }
        assertEquals(2L, ((ProfileControlTargetV1) targets.get(4).target()).expectedSecretGeneration());
        assertArrayEquals(route.bytes(), targets.get(3).routeUuid());
    }

    @Test
    void rejectsDigestTamperingPartialMutationIdentityAndWrongBranchType() {
        final RouteIncarnation route = RouteIncarnation.random();
        final ControlTargetRefV1 target = new ControlTargetRefV1(1, ControlTargetKindV1.ROUTE, route.bytes(),
                bytes(32, 1), bytes(32, 2));
        final byte[] tampered = target.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ControlTargetRefV1.decode(tampered));
        assertThrows(IllegalArgumentException.class, () -> new ControlTargetRefV1(1, ControlTargetKindV1.ROUTE,
                route.bytes(), bytes(32, 1), null));
        assertThrows(IllegalArgumentException.class, () -> new ControlTargetRefV1(1, ControlTargetKindV1.SHARD,
                route.bytes(), null, null));

        final byte[] wrongKind = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.uint32(output, 2, ControlTargetKindV1.ROUTE.wireValue());
            CanonicalProtobuf.bytes(output, 13, bytes(16, 2));
            CanonicalProtobuf.bytes(output, 22, bytes(32, 3));
        });
        assertThrows(IllegalArgumentException.class, () -> ControlTargetRefV1.decode(wrongKind));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
