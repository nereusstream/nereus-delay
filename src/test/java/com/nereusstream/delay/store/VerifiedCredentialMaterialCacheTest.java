package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialEquivalenceAttestation;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.ObjectStoreProviderKind;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;

class VerifiedCredentialMaterialCacheTest {
    private static final String ACCESS_KEY = "cache-access";
    private static final String SECRET_KEY = "cache-secret";

    @Test
    void resolvesOnlyTheExactVerifiedBindingAndSupportsRemoval() throws Exception {
        final Fixture fixture = fixture();
        final VerifiedCredentialMaterialCache cache = new VerifiedCredentialMaterialCache(fixture.trustSet());
        final OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial material =
                material(fixture.fingerprint());

        cache.install(fixture.profile(), fixture.binding(), material);

        final OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial resolved =
                cache.resolve(fixture.profile(), fixture.binding());
        assertNotNull(resolved);
        assertEquals(ACCESS_KEY, resolved.accessKeyId());
        assertEquals(SECRET_KEY, resolved.secretAccessKey());
        assertArrayEquals(fixture.fingerprint(), resolved.resolvedCredentialFingerprintDigest());
        assertEquals(1, cache.size());
        assertNull(cache.resolve(
                fixture.profile(),
                binding(fixture.profile(), 2, "secret://cache/current", bytes(32, 7), fixture.keyPair())));

        cache.remove(fixture.profile(), fixture.binding());
        assertEquals(0, cache.size());
    }

    @Test
    void rejectsUntrustedOrFingerprintDriftAndKeepsPreviousSnapshotOnFailedReplace() throws Exception {
        final Fixture fixture = fixture();
        final VerifiedCredentialMaterialCache cache = new VerifiedCredentialMaterialCache(fixture.trustSet());
        cache.install(fixture.profile(), fixture.binding(), material(fixture.fingerprint()));

        assertThrows(
                IllegalArgumentException.class,
                () -> cache.install(fixture.profile(), fixture.binding(), material(bytes(32, 99))));

        final KeyPair foreignKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final CredentialBinding foreignBinding =
                binding(fixture.profile(), 2, "secret://cache/foreign", fixture.fingerprint(), foreignKey);
        assertThrows(
                IllegalArgumentException.class,
                () -> cache.install(fixture.profile(), foreignBinding, material(fixture.fingerprint())));

        final CredentialBinding nextBinding =
                binding(fixture.profile(), 2, "secret://cache/current", bytes(32, 7), fixture.keyPair());
        assertThrows(
                IllegalArgumentException.class,
                () -> cache.replaceAll(List.of(new VerifiedCredentialMaterialCache.Entry(
                        fixture.profile(), nextBinding, material(bytes(32, 8))))));
        assertEquals(1, cache.size());
        assertNotNull(cache.resolve(fixture.profile(), fixture.binding()));
    }

    private static OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial material(
            final byte[] fingerprint) {
        return new OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial(
                ACCESS_KEY, SECRET_KEY, null, fingerprint);
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
                new ProfileSemanticEnvelope(ProfileKind.OBJECT_STORE, Bytes.utf8("credential-cache"), 1, semantic);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] fingerprint = bytes(32, 5);
        final CredentialBinding binding = binding(profile, 1, "secret://cache/initial", fingerprint, keyPair);
        return new Fixture(
                profile,
                binding,
                fingerprint,
                keyPair,
                CredentialAttestationTrustSet.single(
                        1, Bytes.utf8("cache-verifier"), 1, keyPair.getPublic(), 0, 20_000));
    }

    private static CredentialBinding binding(
            final ProfileSemanticEnvelope profile,
            final long generation,
            final String reference,
            final byte[] fingerprint,
            final KeyPair keyPair) {
        final ObjectStoreProfileSemantic semantic = (ObjectStoreProfileSemantic) profile.body();
        final byte[] referenceBytes = Bytes.utf8(reference);
        final CredentialEquivalenceAttestation attestation = CredentialEquivalenceAttestation.signed(
                profile.ref(),
                generation,
                Bytes.sha256(referenceBytes),
                semantic.credentialAuthorizationScopeDigest(),
                fingerprint,
                1,
                Bytes.utf8("cache-verifier"),
                evidence(1_000),
                10_000,
                bytes(32, 6),
                1,
                keyPair.getPrivate());
        return CredentialBinding.create(profile.ref(), generation, referenceBytes, attestation);
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

    private record Fixture(
            ProfileSemanticEnvelope profile,
            CredentialBinding binding,
            byte[] fingerprint,
            KeyPair keyPair,
            CredentialAttestationTrustSet trustSet) {
        @Override
        public byte[] fingerprint() {
            return Bytes.copy(fingerprint);
        }
    }
}
