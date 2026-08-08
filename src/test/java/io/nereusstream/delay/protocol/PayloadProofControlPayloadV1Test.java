package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PayloadProofControlPayloadV1Test {
    @Test
    void trustSetControlBranchesRoundTripCanonically() {
        final PayloadProofTrustSetRefV1 trustSet = new PayloadProofTrustSetRefV1(4,
                Bytes.sha256(Bytes.utf8("trust-set")));
        final PayloadProofTrustSetActivatePayloadV1 activate =
                new PayloadProofTrustSetActivatePayloadV1(trustSet);
        assertEquals(activate, PayloadProofTrustSetActivatePayloadV1.decode(activate.canonicalBytes()));

        final ControlReasonV1 reason = new ControlReasonV1(ControlReasonKindV1.INCIDENT,
                Bytes.sha256(Bytes.utf8("ticket")), null);
        final PayloadProofIssuanceClosePayloadV1 close =
                new PayloadProofIssuanceClosePayloadV1(trustSet, 3, reason);
        assertEquals(close, PayloadProofIssuanceClosePayloadV1.decode(close.canonicalBytes()));
        assertEquals(reason, ControlReasonV1.decode(reason.canonicalBytes()));

        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final ControlRef controlRef = new ControlRef(Bytes.sha256(Bytes.utf8("operation")),
                Bytes.sha256(Bytes.utf8("request")), 1);
        final ApplyShardControlBody activateBody = ApplyShardControlBody.decode(controlBody(shard, controlRef, 12,
                CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, trustSet.canonicalBytes())),
                trustSet.semanticHash()));
        assertEquals(activate, activateBody.payloadProofTrustSetActivate());
        final ApplyShardControlBody closeBody = ApplyShardControlBody.decode(controlBody(shard, controlRef, 13,
                close.canonicalBytes(), trustSet.semanticHash()));
        assertEquals(close, closeBody.payloadProofIssuanceClose());
    }

    @Test
    void rejectsReasonOptionalOrderAndInvalidProofKeyVersion() {
        final ControlReasonV1 reason = new ControlReasonV1(ControlReasonKindV1.MAINTENANCE,
                null, Bytes.sha256(Bytes.utf8("detail")));
        final byte[] outOfOrder = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, reason.kind().wireValue());
            CanonicalProtobuf.bytes(output, 3, reason.boundedDetailHash());
            CanonicalProtobuf.bytes(output, 2, Bytes.sha256(Bytes.utf8("ticket")));
        });
        assertThrows(IllegalArgumentException.class, () -> ControlReasonV1.decode(outOfOrder));

        final PayloadProofTrustSetRefV1 trustSet = new PayloadProofTrustSetRefV1(1,
                Bytes.sha256(Bytes.utf8("trust-set")));
        assertThrows(IllegalArgumentException.class, () -> new PayloadProofIssuanceClosePayloadV1(trustSet, 0,
                reason));

        final byte[] tampered = reason.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertNotEquals(reason, ControlReasonV1.decode(tampered));
        // Optional presence is explicit; an empty present hash is not an omitted field.
        assertThrows(IllegalArgumentException.class,
                () -> new ControlReasonV1(ControlReasonKindV1.INCIDENT, new byte[0], null));
    }

    @Test
    void applyShardControlPreservesUnsignedSemanticVersionBits() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final ControlRef controlRef = new ControlRef(Bytes.sha256(Bytes.utf8("operation-high-bit")),
                Bytes.sha256(Bytes.utf8("request-high-bit")), 1);
        final byte[] body = controlBody(shard, controlRef, 12,
                CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1,
                        new PayloadProofTrustSetRefV1(Long.MIN_VALUE,
                                Bytes.sha256(Bytes.utf8("trust-set-high-bit"))).canonicalBytes())),
                Bytes.sha256(Bytes.utf8("trust-set-high-bit")), Long.MIN_VALUE);

        assertEquals(Long.MIN_VALUE, ApplyShardControlBody.decode(body).semanticVersion());
    }

    @Test
    void laneControlUsesTypedReasonAndAcknowledgementCodecs() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final ControlRef controlRef = new ControlRef(Bytes.sha256(Bytes.utf8("lane-operation")),
                Bytes.sha256(Bytes.utf8("lane-request")), 1);
        final byte[] laneTarget = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.sha256(Bytes.utf8("lane")));
            CanonicalProtobuf.bytes(output, 2, new byte[16]);
            CanonicalProtobuf.uint32(output, 3, 1);
        });
        final byte[] malformedReason = CanonicalProtobuf.message(output -> CanonicalProtobuf.uint32(output, 1, 99));
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, laneTarget);
            CanonicalProtobuf.bytes(output, 2, malformedReason);
        });
        final ApplyShardControlBody body = ApplyShardControlBody.decode(controlBody(shard, controlRef, 8,
                branch, Bytes.sha256(Bytes.utf8("lane-semantic"))));

        assertThrows(IllegalArgumentException.class, body::laneTarget);
    }

    private static byte[] controlBody(final ShardId shard, final ControlRef controlRef, final int controlKind,
                                      final byte[] branch, final byte[] semanticHash) {
        return controlBody(shard, controlRef, controlKind, branch, semanticHash, 1);
    }

    private static byte[] controlBody(final ShardId shard, final ControlRef controlRef, final int controlKind,
                                      final byte[] branch, final byte[] semanticHash,
                                      final long semanticVersion) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] payload = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, controlKind,
                branch));
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, controlKind);
            CanonicalProtobuf.uint64Bits(output, 12, semanticVersion);
            CanonicalProtobuf.bytes(output, 13, semanticHash);
            CanonicalProtobuf.bytes(output, 15, payload);
        });
    }
}
