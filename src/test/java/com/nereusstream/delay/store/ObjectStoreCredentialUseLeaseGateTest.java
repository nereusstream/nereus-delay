package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialBindingProtection;
import com.nereusstream.delay.protocol.CredentialEquivalenceAttestation;
import com.nereusstream.delay.protocol.CredentialUseKind;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.ObjectStoreProviderKind;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class ObjectStoreCredentialUseLeaseGateTest {
    @Test
    void acceptsExactProtectedFingerprintAndCurrentLeaseBeforeProviderCall() throws Exception {
        final Fixture fixture = fixture(3_000);
        final ObjectStoreCredentialUseLeaseGate gate = fixture.gate(3_000, fixture.fingerprint());

        assertDoesNotThrow(gate::requireBeforeProviderCall);
        assertEquals(fixture.profile().ref(), gate.profile());
        assertEquals(fixture.lease(), gate.lease());
    }

    @Test
    void rejectsExpiredLeaseBeforeProviderCall() throws Exception {
        final Fixture fixture = fixture(8_000);
        final ObjectStoreCredentialUseLeaseGate gate = fixture.gate(8_000, fixture.fingerprint());

        assertThrows(IllegalStateException.class, gate::requireBeforeProviderCall);
    }

    @Test
    void rejectsLoadedCredentialFingerprintDriftAtConstruction() throws Exception {
        final Fixture fixture = fixture(3_000);

        assertThrows(IllegalArgumentException.class, () -> fixture.gate(3_000, bytes(32, 99)));
    }

    @Test
    void rejectsProtectionThatDoesNotCoverTheLease() throws Exception {
        final Fixture fixture = fixture(3_000);
        final CredentialBindingProtection shortProtection =
                CredentialBindingProtection.forBinding(fixture.binding(), 0, 7_000, 0, 0, 2);

        assertThrows(
                IllegalArgumentException.class,
                () -> new ObjectStoreCredentialUseLeaseGate(
                        fixture.profile(),
                        fixture.binding(),
                        shortProtection,
                        fixture.lease(),
                        fixture.fingerprint(),
                        clock(3_000),
                        7_000,
                        10_000));
    }

    private static Fixture fixture(final long now) throws Exception {
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
                new ProfileSemanticEnvelope(ProfileKind.OBJECT_STORE, Bytes.utf8("object-store"), 1, semantic);
        final byte[] secretReference = Bytes.utf8("secret-reference");
        final byte[] fingerprint = bytes(32, 5);
        final CredentialEquivalenceAttestation attestation = CredentialEquivalenceAttestation.signed(
                profile.ref(),
                1,
                Bytes.sha256(secretReference),
                semantic.credentialAuthorizationScopeDigest(),
                fingerprint,
                1,
                Bytes.utf8("verifier"),
                evidence(1_000, 1_001),
                10_000,
                bytes(32, 6),
                1,
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());
        final CredentialBinding binding = CredentialBinding.create(profile.ref(), 1, secretReference, attestation);
        final CredentialBindingProtection protection =
                CredentialBindingProtection.forBinding(binding, 0, 9_000, 0, 0, 2);
        final CredentialUseLease lease = new CredentialUseLease(
                profile.ref(),
                CredentialUseKind.OBJECT_STORE_ADAPTER,
                bytes(32, 7),
                1,
                binding.bindingDigest(),
                fingerprint,
                evidence(2_000, 2_001),
                8_000,
                protection.protectionRevision());
        return new Fixture(profile, binding, protection, lease, fingerprint);
    }

    private static Clock clock(final long now) {
        return Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC);
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest, final long latest) {
        return new TrustedUtcIntervalEvidence(
                earliest,
                latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(8, (int) earliest),
                1,
                1,
                1,
                bytes(32, (int) latest),
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

    private record Fixture(
            ProfileSemanticEnvelope profile,
            CredentialBinding binding,
            CredentialBindingProtection protection,
            CredentialUseLease lease,
            byte[] fingerprint) {
        private ObjectStoreCredentialUseLeaseGate gate(final long currentTime, final byte[] loadedFingerprint) {
            return new ObjectStoreCredentialUseLeaseGate(
                    profile, binding, protection, lease, loadedFingerprint, clock(currentTime), 7_000, 10_000);
        }

        @Override
        public byte[] fingerprint() {
            return Bytes.copy(fingerprint);
        }
    }
}
