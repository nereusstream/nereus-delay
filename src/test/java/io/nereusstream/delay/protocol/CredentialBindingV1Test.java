package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CredentialBindingV1Test {
    @Test
    void signsAndRoundTripsAttestationBindingHeadAndProtection() throws Exception {
        final ProfileRefV1 profile = profile();
        final TrustedUtcIntervalEvidence verifiedAt = trustedTime();
        final KeyPair keyPair = ed25519();
        final byte[] secretReference = Bytes.utf8("provider://credential/v7");
        final CredentialEquivalenceAttestationV1 attestation = CredentialEquivalenceAttestationV1.signed(
                profile, 7, Bytes.sha256(secretReference), bytes(32, 2), bytes(32, 3), 4,
                Bytes.utf8("verifier-a"), verifiedAt, 1_500, bytes(32, 4), 9, keyPair.getPrivate());

        assertTrue(attestation.verifySignature(keyPair.getPublic()));
        attestation.requireAuthorizationScopeDigest(bytes(32, 2));
        attestation.requireCandidate(profile, 7, Bytes.sha256(secretReference));
        attestation.requireNotAfterAtMost(500);
        assertEquals(attestation, CredentialEquivalenceAttestationV1.decode(attestation.canonicalBytes()));

        final CredentialBindingV1 binding = CredentialBindingV1.create(profile, 7, secretReference, attestation);
        assertArrayEquals(Bytes.sha256(secretReference), binding.secretReferenceSha256());
        assertEquals(binding, CredentialBindingV1.decode(binding.canonicalBytes()));

        final CredentialBindingHeadV1 head = CredentialBindingHeadV1.forBinding(binding, 3);
        assertEquals(head, CredentialBindingHeadV1.decode(head.canonicalBytes()));

        final CredentialBindingProtectionV1 protection = CredentialBindingProtectionV1.forBinding(
                binding, 1_600, 1_700, 1_800, 1_900, 4);
        assertEquals(protection, CredentialBindingProtectionV1.decode(protection.canonicalBytes()));

        final CredentialUseLeaseV1 lease = new CredentialUseLeaseV1(profile,
                CredentialUseKindV1.DESTINATION_CHANNEL, bytes(32, 9), 7, binding.bindingDigest(),
                attestation.resolvedCredentialFingerprintDigest(), verifiedAt, 1_500, 4);
        lease.requireBinding(binding);
        lease.requireProtectedBy(protection);
        assertEquals(lease, CredentialUseLeaseV1.decode(lease.canonicalBytes()));
    }

    @Test
    void rejectsCandidateDigestProtocolAndCanonicalTampering() throws Exception {
        final ProfileRefV1 profile = profile();
        final byte[] reference = Bytes.utf8("provider://credential/v1");
        final CredentialEquivalenceAttestationV1 attestation = CredentialEquivalenceAttestationV1.signed(
                profile, 1, Bytes.sha256(reference), bytes(32, 5), bytes(32, 6), 1,
                Bytes.utf8("verifier"), trustedTime(), 1_100, bytes(32, 7), 1, ed25519().getPrivate());

        assertThrows(IllegalArgumentException.class, () -> CredentialBindingV1.create(profile, 2, reference,
                attestation));
        assertThrows(IllegalArgumentException.class, () -> CredentialBindingV1.create(profile, 1,
                Bytes.utf8("provider://credential/other"), attestation));
        assertThrows(IllegalArgumentException.class, () -> CredentialBindingHeadV1.create(profile, 1,
                bytes(32, 8), 0));
        assertThrows(IllegalArgumentException.class, () -> new CredentialUseLeaseV1(profile,
                CredentialUseKindV1.OBJECT_STORE_ADAPTER, bytes(32, 9), 1, bytes(32, 8), bytes(32, 9),
                trustedTime(), 1_100, 1));

        final CredentialBindingV1 binding = CredentialBindingV1.create(profile, 1, reference, attestation);
        final byte[] tampered = binding.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CredentialBindingV1.decode(tampered));

        final KeyPair keyPair = ed25519();
        final CredentialEquivalenceAttestationV1 unsigned = CredentialEquivalenceAttestationV1.signed(
                profile, 1, Bytes.sha256(reference), bytes(32, 5), bytes(32, 6), 1,
                Bytes.utf8("verifier"), trustedTime(), 1_100, bytes(32, 7), 1, keyPair.getPrivate());
        final byte[] signature = unsigned.signature();
        signature[0] ^= 1;
        final CredentialEquivalenceAttestationV1 altered = new CredentialEquivalenceAttestationV1(profile, 1,
                Bytes.sha256(reference), bytes(32, 5), bytes(32, 6), 1, Bytes.utf8("verifier"), trustedTime(),
                1_100, bytes(32, 7), unsigned.attestationDigest(), 1, signature);
        assertFalse(altered.verifySignature(keyPair.getPublic()));
    }

    private static ProfileRefV1 profile() {
        return new ProfileRefV1(Bytes.utf8("destination"), 7, bytes(32, 1), ProfileKindV1.DESTINATION);
    }

    private static TrustedUtcIntervalEvidence trustedTime() {
        return new TrustedUtcIntervalEvidence(1_000, 1_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock-a"), 1, 2, 3,
                bytes(32, 8), 0, new byte[0]);
    }

    private static KeyPair ed25519() throws Exception {
        return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
