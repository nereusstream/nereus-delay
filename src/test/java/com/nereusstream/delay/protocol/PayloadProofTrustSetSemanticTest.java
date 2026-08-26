package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class PayloadProofTrustSetSemanticTest {
    @Test
    void canonicalTrustSetRoundTripsAndExposesMatchingReference() throws Exception {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final var first = PayloadProofVerifierKey.fromPublicKey(
                1, generator.generateKeyPair().getPublic(), 100, 10_000);
        final var second = PayloadProofVerifierKey.fromPublicKey(
                Integer.MIN_VALUE, generator.generateKeyPair().getPublic(), 200, 20_000);
        final PayloadProofTrustSetSemantic trustSet = new PayloadProofTrustSetSemantic(7, List.of(first, second));

        assertEquals(first, PayloadProofVerifierKey.decode(first.canonicalBytes()));
        assertEquals(trustSet, PayloadProofTrustSetSemantic.decode(trustSet.canonicalBytes()));
        assertEquals(new PayloadProofTrustSetRef(7, trustSet.semanticHash()), trustSet.ref());
        assertEquals(
                first.toPublicKey(),
                PayloadProofVerifierKey.decode(first.canonicalBytes()).toPublicKey());
    }

    @Test
    void rejectsUnsortedKeysAndSemanticHashDrift() throws Exception {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final var first = PayloadProofVerifierKey.fromPublicKey(
                1, generator.generateKeyPair().getPublic(), 0, 10_000);
        final var second = PayloadProofVerifierKey.fromPublicKey(
                2, generator.generateKeyPair().getPublic(), 0, 20_000);
        assertThrows(IllegalArgumentException.class, () -> new PayloadProofTrustSetSemantic(1, List.of(second, first)));

        final PayloadProofTrustSetSemantic trustSet = new PayloadProofTrustSetSemantic(1, List.of(first, second));
        final byte[] tampered = trustSet.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> PayloadProofTrustSetSemantic.decode(tampered));
    }

    @Test
    void preservesCompleteUnsignedTrustSetVersionBits() throws Exception {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final var key = PayloadProofVerifierKey.fromPublicKey(
                1, generator.generateKeyPair().getPublic(), 0, 10_000);
        final PayloadProofTrustSetSemantic trustSet = new PayloadProofTrustSetSemantic(Long.MIN_VALUE, List.of(key));

        final PayloadProofTrustSetSemantic decoded = PayloadProofTrustSetSemantic.decode(trustSet.canonicalBytes());

        assertEquals(Long.MIN_VALUE, decoded.version());
        assertEquals(Long.MIN_VALUE, decoded.ref().version());
        assertEquals(trustSet, decoded);
        assertEquals(decoded.ref(), PayloadProofTrustSetRef.decode(decoded.ref().canonicalBytes()));
    }

    @Test
    void localVerifierAdapterAppliesSourceTimeValidityWindow() throws Exception {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final var keyPair = generator.generateKeyPair();
        final var key = PayloadProofVerifierKey.fromPublicKey(Integer.MIN_VALUE, keyPair.getPublic(), 100, 200);
        final long highBitTrustSetVersion = Long.MIN_VALUE;
        final PayloadProofTrustSetSemantic semantic =
                new PayloadProofTrustSetSemantic(highBitTrustSetVersion, List.of(key));
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final ProfileRef objectStoreProfile = new ProfileRef(
                Bytes.utf8("object-store"), 1, Bytes.sha256(Bytes.utf8("profile")), ProfileKind.OBJECT_STORE);
        final CanonicalPayloadCommitProof proof = CanonicalPayloadCommitProof.signed(
                Bytes.sha256(Bytes.utf8("reservation")),
                Bytes.sha256(Bytes.utf8("scope")),
                shard.routeIncarnation().bytes(),
                shard.partition(),
                messageId,
                objectStoreProfile,
                highBitTrustSetVersion,
                Integer.MIN_VALUE,
                Bytes.utf8("bucket"),
                Bytes.utf8("key"),
                Bytes.utf8("version"),
                null,
                1,
                Bytes.sha256(Bytes.utf8("payload")),
                200,
                keyPair.getPrivate());
        final PayloadCommitProof legacyProof = PayloadCommitProof.signed(
                highBitTrustSetVersion,
                Integer.MIN_VALUE,
                shard.routeIncarnation().bytes(),
                shard.partition(),
                messageId,
                Bytes.sha256(Bytes.utf8("reservation")),
                objectStoreProfile.semanticHash(),
                Bytes.utf8("bucket"),
                Bytes.utf8("key"),
                Bytes.utf8("version"),
                Bytes.utf8("etag"),
                1,
                Bytes.sha256(Bytes.utf8("payload")),
                200,
                keyPair.getPrivate());
        final PayloadProofTrustSet adapter = PayloadProofTrustSet.fromSemantic(semantic);

        assertEquals(highBitTrustSetVersion, proof.trustSetVersion());
        assertEquals(proof, CanonicalPayloadCommitProof.decode(proof.canonicalBytes()));
        assertEquals(legacyProof, PayloadCommitProof.decode(legacyProof.canonicalBytes()));
        org.junit.jupiter.api.Assertions.assertTrue(adapter.verifies(proof, 150));
        org.junit.jupiter.api.Assertions.assertTrue(adapter.verifies(legacyProof, 150));
        org.junit.jupiter.api.Assertions.assertFalse(adapter.verifies(proof, 99));
        org.junit.jupiter.api.Assertions.assertFalse(adapter.verifies(proof, 201));

        final byte[] malformedSignature = new byte[64];
        Arrays.fill(malformedSignature, (byte) 0xff);
        final CanonicalPayloadCommitProof malformedTyped = new CanonicalPayloadCommitProof(
                proof.reservationId(),
                proof.tenantRoutingScope(),
                proof.routeIncarnationUuid(),
                proof.partition(),
                proof.delayMessageId(),
                proof.objectStoreProfile(),
                proof.trustSetVersion(),
                proof.proofKeyVersion(),
                proof.container(),
                proof.objectKey(),
                proof.immutableObjectVersion(),
                proof.etag(),
                proof.length(),
                proof.payloadSha256(),
                proof.notAfterEpochMs(),
                proof.proofId(),
                malformedSignature);
        final PayloadCommitProof malformedLegacy = new PayloadCommitProof(
                legacyProof.trustSetVersion(),
                legacyProof.proofKeyVersion(),
                legacyProof.routeIncarnationUuid(),
                legacyProof.partition(),
                legacyProof.delayMessageId(),
                legacyProof.reservationId(),
                legacyProof.objectStoreProfileHash(),
                legacyProof.container(),
                legacyProof.objectKey(),
                legacyProof.immutableObjectVersion(),
                legacyProof.etag(),
                legacyProof.length(),
                legacyProof.payloadSha256(),
                legacyProof.notAfterEpochMs(),
                legacyProof.proofId(),
                malformedSignature);
        assertFalse(adapter.verifies(malformedTyped, 150));
        assertFalse(adapter.verifies(malformedLegacy, 150));
    }
}
