package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CredentialBindingV1;
import io.nereusstream.delay.protocol.CredentialEquivalenceAttestationV1;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CredentialAttestationTrustSetTest {
    @Test
    void verifiesExactVerifierIdentitySignatureAndWindow() throws Exception {
        final Fixture fixture = fixture();
        final CredentialAttestationTrustSet trustSet = CredentialAttestationTrustSet.single(
                7, Bytes.utf8("credential-verifier"), 3, fixture.keyPair().getPublic(), 900, 12_000);

        assertDoesNotThrow(() -> trustSet.verify(fixture.profile(), fixture.binding()));
        assertThrows(UnsupportedOperationException.class, () -> trustSet.keys().clear());
    }

    @Test
    void rejectsUnknownVerifierAndOutOfWindowAttestation() throws Exception {
        final Fixture fixture = fixture();
        final CredentialAttestationTrustSet wrongIdentity = CredentialAttestationTrustSet.single(
                7, Bytes.utf8("other-verifier"), 3, fixture.keyPair().getPublic(), 900, 12_000);
        assertThrows(IllegalArgumentException.class,
                () -> wrongIdentity.verify(fixture.profile(), fixture.binding()));

        final CredentialAttestationTrustSet shortWindow = CredentialAttestationTrustSet.single(
                7, Bytes.utf8("credential-verifier"), 3, fixture.keyPair().getPublic(), 1_100, 12_000);
        assertThrows(IllegalArgumentException.class,
                () -> shortWindow.verify(fixture.profile(), fixture.binding()));
    }

    private static Fixture fixture() throws Exception {
        final ObjectStoreProfileSemanticV1 semantic = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3_COMPATIBLE, bytes(32, 1), bytes(32, 2), 1,
                true, true, true, true, bytes(32, 3), 1 << 20,
                ObjectStoreProfileSemanticV1.SINGLE_PUT, 1, bytes(32, 4));
        final ProfileSemanticEnvelopeV1 profile = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.OBJECT_STORE, Bytes.utf8("attestation-profile"), 1, semantic);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] reference = Bytes.utf8("secret://trust-set/v1");
        final CredentialEquivalenceAttestationV1 attestation = CredentialEquivalenceAttestationV1.signed(
                profile.ref(), 1, Bytes.sha256(reference), semantic.credentialAuthorizationScopeDigest(),
                bytes(32, 5), 7, Bytes.utf8("credential-verifier"), evidence(1_000), 10_000,
                bytes(32, 6), 3, keyPair.getPrivate());
        return new Fixture(profile, CredentialBindingV1.create(profile.ref(), 1, reference, attestation), keyPair);
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest) {
        return new TrustedUtcIntervalEvidence(earliest, earliest + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, (int) earliest), 1, 1, 1,
                bytes(32, (int) earliest + 1), 0, null);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record Fixture(ProfileSemanticEnvelopeV1 profile, CredentialBindingV1 binding, KeyPair keyPair) {
    }
}
