package io.nereusstream.delay.protocol;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InitialRouteControlActivatePayloadV1Test {
    @Test
    void sortsAndRoundTripsTheInitialControlSet() {
        final ProtocolTupleV1 command = ProtocolTupleV1.managedCommandV1();
        final ProtocolTupleV1 system = new ProtocolTupleV1(1, 1, ProtocolTupleV1.SYSTEM_MUTATION, 1, 1);
        final ProfileRefV1 firstProfile = new ProfileRefV1(Bytes.utf8("profile-b"), 2,
                Bytes.sha256(Bytes.utf8("profile-b-v2")), ProfileKindV1.DESTINATION);
        final ProfileRefV1 secondProfile = new ProfileRefV1(Bytes.utf8("profile-a"), 1,
                Bytes.sha256(Bytes.utf8("profile-a-v1")), ProfileKindV1.DELIVERY_CAPABILITY);
        final QuotaGrantRefV1 grant = new QuotaGrantRefV1(Bytes.sha256(Bytes.utf8("grant")), 1,
                new PublishAdmissionBody.ChargeVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                        17));
        final InitialRouteControlActivatePayloadV1 payload = new InitialRouteControlActivatePayloadV1(
                List.of(system, command), List.of(firstProfile, secondProfile), grant,
                Bytes.sha256(Bytes.utf8("initial-control-snapshot")));

        assertEquals(List.of(command, system), payload.protocolTuples());
        assertEquals(List.of(secondProfile, firstProfile), payload.profiles());
        assertEquals(payload, InitialRouteControlActivatePayloadV1.decode(payload.canonicalBytes()));
    }

    @Test
    void applyShardControlDecodesTheFieldFourteenBranch() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final ControlRef controlRef = new ControlRef(Bytes.sha256(Bytes.utf8("operation")),
                Bytes.sha256(Bytes.utf8("request")), 1);
        final InitialRouteControlActivatePayloadV1 payload = new InitialRouteControlActivatePayloadV1(
                List.of(ProtocolTupleV1.managedCommandV1()), List.of(),
                new QuotaGrantRefV1(Bytes.sha256(Bytes.utf8("grant")), 1,
                        new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0)),
                Bytes.sha256(Bytes.utf8("initial-control-snapshot")));
        final byte[] body = controlBody(shard, controlRef, payload);

        assertEquals(payload, ApplyShardControlBody.decode(body).initialRouteControlActivate());
    }

    @Test
    void rejectsDuplicateOrOutOfOrderRepeatedBranches() {
        final ProtocolTupleV1 tuple = ProtocolTupleV1.managedCommandV1();
        final QuotaGrantRefV1 grant = new QuotaGrantRefV1(Bytes.sha256(Bytes.utf8("grant")), 1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new InitialRouteControlActivatePayloadV1(List.of(tuple, tuple), List.of(), grant,
                        Bytes.sha256(Bytes.utf8("snapshot"))));

        final byte[] outOfOrder = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 2, new ProfileRefV1(Bytes.utf8("profile"), 1,
                    Bytes.sha256(Bytes.utf8("profile")), ProfileKindV1.DESTINATION).canonicalBytes());
            CanonicalProtobuf.bytes(output, 1, tuple.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, grant.canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, Bytes.sha256(Bytes.utf8("snapshot")));
        });
        assertThrows(IllegalArgumentException.class, () -> InitialRouteControlActivatePayloadV1.decode(outOfOrder));
    }

    private static byte[] controlBody(final ShardId shard, final ControlRef controlRef,
                                      final InitialRouteControlActivatePayloadV1 payload) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] controlPayload = CanonicalProtobuf.message(output ->
                CanonicalProtobuf.bytes(output, 14, payload.canonicalBytes()));
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
