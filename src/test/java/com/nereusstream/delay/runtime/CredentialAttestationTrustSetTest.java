package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialEquivalenceAttestation;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.ObjectStoreProviderKind;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import org.junit.jupiter.api.Test;

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
        assertThrows(IllegalArgumentException.class, () -> wrongIdentity.verify(fixture.profile(), fixture.binding()));

        final CredentialAttestationTrustSet shortWindow = CredentialAttestationTrustSet.single(
                7, Bytes.utf8("credential-verifier"), 3, fixture.keyPair().getPublic(), 1_100, 12_000);
        assertThrows(IllegalArgumentException.class, () -> shortWindow.verify(fixture.profile(), fixture.binding()));
    }

    private static Fixture fixture() throws Exception {
        final ObjectStoreProfileSemantic semantic = new ObjectStoreProfileSemantic(
                ObjectStoreProviderKind.S3_COMPATIBLE,
                bytes(32, 1),
                bytes(32, 2),
                1,
                true,
                true,
                true,
                true,
                bytes(32, 3),
                1 << 20,
                ObjectStoreProfileSemantic.SINGLE_PUT,
                1,
                bytes(32, 4));
        final ProfileSemanticEnvelope profile =
                new ProfileSemanticEnvelope(ProfileKind.OBJECT_STORE, Bytes.utf8("attestation-profile"), 1, semantic);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] reference = Bytes.utf8("secret://trust-set/initial");
        final CredentialEquivalenceAttestation attestation = CredentialEquivalenceAttestation.signed(
                profile.ref(),
                1,
                Bytes.sha256(reference),
                semantic.credentialAuthorizationScopeDigest(),
                bytes(32, 5),
                7,
                Bytes.utf8("credential-verifier"),
                evidence(1_000),
                10_000,
                bytes(32, 6),
                3,
                keyPair.getPrivate());
        return new Fixture(profile, CredentialBinding.create(profile.ref(), 1, reference, attestation), keyPair);
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest) {
        return new TrustedUtcIntervalEvidence(
                earliest,
                earliest + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(8, (int) earliest),
                1,
                1,
                1,
                bytes(32, (int) earliest + 1),
                0,
                null);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record Fixture(ProfileSemanticEnvelope profile, CredentialBinding binding, KeyPair keyPair) {}
}
