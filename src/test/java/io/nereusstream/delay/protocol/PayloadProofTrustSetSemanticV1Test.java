package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.security.KeyPairGenerator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PayloadProofTrustSetSemanticV1Test {
    @Test
    void canonicalTrustSetRoundTripsAndExposesMatchingReference() throws Exception {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final var first = PayloadProofVerifierKeyV1.fromPublicKey(1, generator.generateKeyPair().getPublic(),
                100, 10_000);
        final var second = PayloadProofVerifierKeyV1.fromPublicKey(2, generator.generateKeyPair().getPublic(),
                200, 20_000);
        final PayloadProofTrustSetSemanticV1 trustSet = new PayloadProofTrustSetSemanticV1(7,
                List.of(first, second));

        assertEquals(first, PayloadProofVerifierKeyV1.decode(first.canonicalBytes()));
        assertEquals(trustSet, PayloadProofTrustSetSemanticV1.decode(trustSet.canonicalBytes()));
        assertEquals(new PayloadProofTrustSetRefV1(7, trustSet.semanticHash()), trustSet.ref());
        assertEquals(first.toPublicKey(), PayloadProofVerifierKeyV1.decode(first.canonicalBytes()).toPublicKey());
    }

    @Test
    void rejectsUnsortedKeysAndSemanticHashDrift() throws Exception {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final var first = PayloadProofVerifierKeyV1.fromPublicKey(1, generator.generateKeyPair().getPublic(),
                0, 10_000);
        final var second = PayloadProofVerifierKeyV1.fromPublicKey(2, generator.generateKeyPair().getPublic(),
                0, 20_000);
        assertThrows(IllegalArgumentException.class,
                () -> new PayloadProofTrustSetSemanticV1(1, List.of(second, first)));

        final PayloadProofTrustSetSemanticV1 trustSet = new PayloadProofTrustSetSemanticV1(1,
                List.of(first, second));
        final byte[] tampered = trustSet.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> PayloadProofTrustSetSemanticV1.decode(tampered));
    }
}
