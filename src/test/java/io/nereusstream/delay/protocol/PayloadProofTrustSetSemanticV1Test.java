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
        final var second = PayloadProofVerifierKeyV1.fromPublicKey(Integer.MIN_VALUE,
                generator.generateKeyPair().getPublic(),
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

    @Test
    void preservesCompleteUnsignedTrustSetVersionBits() throws Exception {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final var key = PayloadProofVerifierKeyV1.fromPublicKey(1, generator.generateKeyPair().getPublic(), 0, 10_000);
        final PayloadProofTrustSetSemanticV1 trustSet = new PayloadProofTrustSetSemanticV1(Long.MIN_VALUE,
                List.of(key));

        final PayloadProofTrustSetSemanticV1 decoded =
                PayloadProofTrustSetSemanticV1.decode(trustSet.canonicalBytes());

        assertEquals(Long.MIN_VALUE, decoded.version());
        assertEquals(Long.MIN_VALUE, decoded.ref().version());
        assertEquals(trustSet, decoded);
        assertEquals(decoded.ref(), PayloadProofTrustSetRefV1.decode(decoded.ref().canonicalBytes()));
    }

    @Test
    void localVerifierAdapterAppliesSourceTimeValidityWindow() throws Exception {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final var keyPair = generator.generateKeyPair();
        final var key = PayloadProofVerifierKeyV1.fromPublicKey(Integer.MIN_VALUE, keyPair.getPublic(), 100, 200);
        final PayloadProofTrustSetSemanticV1 semantic = new PayloadProofTrustSetSemanticV1(9, List.of(key));
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final PayloadCommitProofV1 proof = PayloadCommitProofV1.signed(Bytes.sha256(Bytes.utf8("reservation")),
                Bytes.sha256(Bytes.utf8("scope")), shard.routeIncarnation().bytes(), shard.partition(), messageId,
                new ProfileRefV1(Bytes.utf8("object-store"), 1, Bytes.sha256(Bytes.utf8("profile")),
                        ProfileKindV1.OBJECT_STORE), 9, Integer.MIN_VALUE, Bytes.utf8("bucket"), Bytes.utf8("key"),
                Bytes.utf8("version"), null, 1, Bytes.sha256(Bytes.utf8("payload")), 200,
                keyPair.getPrivate());
        final PayloadProofTrustSet adapter = PayloadProofTrustSet.fromSemantic(semantic);

        org.junit.jupiter.api.Assertions.assertTrue(adapter.verifies(proof, 150));
        org.junit.jupiter.api.Assertions.assertFalse(adapter.verifies(proof, 99));
        org.junit.jupiter.api.Assertions.assertFalse(adapter.verifies(proof, 201));
    }
}
