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
}
