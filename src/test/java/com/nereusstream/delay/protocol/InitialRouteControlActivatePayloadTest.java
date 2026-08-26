package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

class InitialRouteControlActivatePayloadTest {
    @Test
    void sortsAndRoundTripsTheInitialControlSet() {
        final ProtocolTuple command = ProtocolTuple.managedCommand();
        final ProtocolTuple system = new ProtocolTuple(1, 1, ProtocolTuple.SYSTEM_MUTATION, 1, 1);
        final ProfileRef firstProfile = new ProfileRef(
                Bytes.utf8("profile-b"), 2, Bytes.sha256(Bytes.utf8("profile-b")), ProfileKind.DESTINATION);
        final ProfileRef secondProfile = new ProfileRef(
                Bytes.utf8("profile-a"), 1, Bytes.sha256(Bytes.utf8("profile-a")), ProfileKind.DELIVERY_CAPABILITY);
        final QuotaGrantRef grant = new QuotaGrantRef(
                Bytes.sha256(Bytes.utf8("grant")),
                1,
                new PublishAdmissionBody.ChargeVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17));
        final InitialRouteControlActivatePayload payload = new InitialRouteControlActivatePayload(
                List.of(system, command),
                List.of(firstProfile, secondProfile),
                grant,
                Bytes.sha256(Bytes.utf8("initial-control-snapshot")));

        assertEquals(List.of(command, system), payload.protocolTuples());
        assertEquals(List.of(secondProfile, firstProfile), payload.profiles());
        assertEquals(payload, InitialRouteControlActivatePayload.decode(payload.canonicalBytes()));
    }

    @Test
    void applyShardControlDecodesTheFieldFourteenBranch() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final ControlRef controlRef =
                new ControlRef(Bytes.sha256(Bytes.utf8("operation")), Bytes.sha256(Bytes.utf8("request")), 1);
        final InitialRouteControlActivatePayload payload = new InitialRouteControlActivatePayload(
                List.of(ProtocolTuple.managedCommand()),
                List.of(),
                new QuotaGrantRef(
                        Bytes.sha256(Bytes.utf8("grant")),
                        1,
                        new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)),
                Bytes.sha256(Bytes.utf8("initial-control-snapshot")));
        final byte[] body = controlBody(shard, controlRef, payload);

        assertEquals(payload, ApplyShardControlBody.decode(body).initialRouteControlActivate());
    }

    @Test
    void rejectsDuplicateOrOutOfOrderRepeatedBranches() {
        final ProtocolTuple tuple = ProtocolTuple.managedCommand();
        final QuotaGrantRef grant = new QuotaGrantRef(
                Bytes.sha256(Bytes.utf8("grant")),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> new InitialRouteControlActivatePayload(
                        List.of(tuple, tuple), List.of(), grant, Bytes.sha256(Bytes.utf8("snapshot"))));

        final byte[] outOfOrder = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(
                    output,
                    2,
                    new ProfileRef(
                                    Bytes.utf8("profile"),
                                    1,
                                    Bytes.sha256(Bytes.utf8("profile")),
                                    ProfileKind.DESTINATION)
                            .canonicalBytes());
            CanonicalProtobuf.bytes(output, 1, tuple.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, grant.canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, Bytes.sha256(Bytes.utf8("snapshot")));
        });
        assertThrows(IllegalArgumentException.class, () -> InitialRouteControlActivatePayload.decode(outOfOrder));
    }

    private static byte[] controlBody(
            final ShardId shard, final ControlRef controlRef, final InitialRouteControlActivatePayload payload) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] controlPayload =
                CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 14, payload.canonicalBytes()));
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, 14);
            CanonicalProtobuf.uint64Bits(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, Bytes.sha256(Bytes.utf8("initial-control")));
            CanonicalProtobuf.bytes(output, 15, controlPayload);
        });
    }
}
