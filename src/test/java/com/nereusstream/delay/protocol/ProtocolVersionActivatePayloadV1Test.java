package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ProtocolVersionActivatePayloadV1Test {
    @Test
    void roundTripsTupleAndBothAuthorityHashes() {
        final ProtocolVersionActivatePayloadV1 payload = new ProtocolVersionActivatePayloadV1(
                ProtocolTupleV1.managedCommandV1(),
                Bytes.sha256(Bytes.utf8("schema")),
                Bytes.sha256(Bytes.utf8("eligible-readers")));

        assertEquals(payload, ProtocolVersionActivatePayloadV1.decode(payload.canonicalBytes()));
        assertEquals(ProtocolTupleV1.managedCommandV1(), payload.tuple());
        assertArrayEquals(Bytes.sha256(Bytes.utf8("schema")), payload.canonicalSchemaHash());
        assertArrayEquals(Bytes.sha256(Bytes.utf8("eligible-readers")), payload.compatibleReaderSetEvidenceHash());
    }

    @Test
    void applyShardControlDecodesTheFieldOneBranch() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final ControlRef controlRef =
                new ControlRef(Bytes.sha256(Bytes.utf8("operation")), Bytes.sha256(Bytes.utf8("request")), 1);
        final ProtocolVersionActivatePayloadV1 payload = new ProtocolVersionActivatePayloadV1(
                ProtocolTupleV1.managedCommandV1(),
                Bytes.sha256(Bytes.utf8("schema")),
                Bytes.sha256(Bytes.utf8("eligible-readers")));
        final byte[] body = controlBody(shard, controlRef, payload);

        assertEquals(payload, ApplyShardControlBody.decode(body).protocolVersionActivate());
    }

    @Test
    void rejectsWrongHashLengthAndNonCanonicalFieldOrder() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ProtocolVersionActivatePayloadV1(
                        ProtocolTupleV1.managedCommandV1(), new byte[31], new byte[32]));

        final byte[] schemaHash = Bytes.sha256(Bytes.utf8("schema"));
        final byte[] readerHash = Bytes.sha256(Bytes.utf8("eligible-readers"));
        final byte[] tuple = ProtocolTupleV1.managedCommandV1().canonicalBytes();
        final byte[] outOfOrder = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 2, schemaHash);
            CanonicalProtobuf.bytes(output, 1, tuple);
            CanonicalProtobuf.bytes(output, 3, readerHash);
        });
        assertThrows(IllegalArgumentException.class, () -> ProtocolVersionActivatePayloadV1.decode(outOfOrder));
    }

    private static byte[] controlBody(
            final ShardId shard, final ControlRef controlRef, final ProtocolVersionActivatePayloadV1 payload) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] controlPayload =
                CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, payload.canonicalBytes()));
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, 1);
            CanonicalProtobuf.uint64Bits(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, Bytes.sha256(Bytes.utf8("protocol-activation")));
            CanonicalProtobuf.bytes(output, 15, controlPayload);
        });
    }
}
