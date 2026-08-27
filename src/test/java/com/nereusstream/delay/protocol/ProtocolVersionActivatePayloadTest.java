package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class ProtocolVersionActivatePayloadTest {
    @Test
    void roundTripsTupleAndBothAuthorityHashes() {
        final ProtocolVersionActivatePayload payload = new ProtocolVersionActivatePayload(
                ProtocolTuple.managedCommand(),
                Bytes.sha256(Bytes.utf8("schema")),
                Bytes.sha256(Bytes.utf8("eligible-readers")));

        assertEquals(payload, ProtocolVersionActivatePayload.decode(payload.canonicalBytes()));
        assertEquals(ProtocolTuple.managedCommand(), payload.tuple());
        assertArrayEquals(Bytes.sha256(Bytes.utf8("schema")), payload.canonicalSchemaHash());
        assertArrayEquals(Bytes.sha256(Bytes.utf8("eligible-readers")), payload.compatibleReaderSetEvidenceHash());
    }

    @Test
    void applyShardControlDecodesTheFieldOneBranch() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final ControlRef controlRef =
                new ControlRef(Bytes.sha256(Bytes.utf8("operation")), Bytes.sha256(Bytes.utf8("request")), 1);
        final ProtocolVersionActivatePayload payload = new ProtocolVersionActivatePayload(
                ProtocolTuple.managedCommand(),
                Bytes.sha256(Bytes.utf8("schema")),
                Bytes.sha256(Bytes.utf8("eligible-readers")));
        final byte[] body = controlBody(shard, controlRef, payload);

        assertEquals(payload, ApplyShardControlBody.decode(body).protocolVersionActivate());
    }

    @Test
    void rejectsWrongHashLengthAndNonCanonicalFieldOrder() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ProtocolVersionActivatePayload(ProtocolTuple.managedCommand(), new byte[31], new byte[32]));

        final byte[] schemaHash = Bytes.sha256(Bytes.utf8("schema"));
        final byte[] readerHash = Bytes.sha256(Bytes.utf8("eligible-readers"));
        final byte[] tuple = ProtocolTuple.managedCommand().canonicalBytes();
        final byte[] outOfOrder = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 2, schemaHash);
            CanonicalProtobuf.bytes(output, 1, tuple);
            CanonicalProtobuf.bytes(output, 3, readerHash);
        });
        assertThrows(IllegalArgumentException.class, () -> ProtocolVersionActivatePayload.decode(outOfOrder));
    }

    @Test
    void currentPayloadBindsTheFullArtifactSetAndManifest() {
        final ArtifactGenerationSet artifacts =
                ArtifactGenerationSet.current(9, PulsarSourceLock.digest(), Bytes.sha256(Bytes.utf8("schema-bundle")));
        final ProtocolVersionActivatePayload payload = new ProtocolVersionActivatePayload(
                artifacts.clientCommandTuple(),
                artifacts.canonicalSchemaBundleHash(),
                Bytes.sha256(Bytes.utf8("current-readers")),
                artifacts,
                Bytes.sha256(Bytes.utf8("manifest")));

        final ProtocolVersionActivatePayload decoded = ProtocolVersionActivatePayload.decode(payload.canonicalBytes());
        assertEquals(payload, decoded);
        assertTrue(decoded.isCurrentGeneration());
        assertEquals(artifacts, decoded.artifactGenerationSet());
        assertArrayEquals(payload.manifestDigest(), decoded.manifestDigest());
        assertThrows(
                IllegalArgumentException.class,
                () -> new ProtocolVersionActivatePayload(
                        new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1),
                        artifacts.canonicalSchemaBundleHash(),
                        Bytes.sha256(Bytes.utf8("current-readers")),
                        artifacts,
                        Bytes.sha256(Bytes.utf8("manifest"))));
    }

    private static byte[] controlBody(
            final ShardId shard, final ControlRef controlRef, final ProtocolVersionActivatePayload payload) {
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
