package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialEquivalenceAttestation;
import com.nereusstream.delay.protocol.CredentialUseKind;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.ObjectStoreProviderKind;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.RotateEquivalentSecretRequest;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Opt-in real Oxia coverage for the single-record credential Profile authority. */
@Tag("real-service")
class OxiaRealProfileCatalogSmokeTest {
    @Test
    void profileHeadProtectionLeaseAndRotationReopenAgainstRealService() throws Exception {
        final String endpoint = endpoint();
        final String prefix = "nereus-delay-real-profile/" + UUID.randomUUID();
        final Fixture fixture = fixture();

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = connect(endpoint, prefix + "/client")) {
            final OxiaSyncProfileCatalogBackend backend = new OxiaSyncProfileCatalogBackend(
                    client, prefix + "/catalog", 5_000, 10_000, trustSet(fixture.keyPair()));
            assertEquals(
                    1, backend.publish(fixture.profile(), fixture.binding()).headRevision());
            final CredentialUseLease lease = backend.issueCredentialUseLease(
                    fixture.profile().ref(),
                    CredentialUseKind.OBJECT_STORE_ADAPTER,
                    id32(30),
                    1,
                    fixture.binding().bindingDigest(),
                    fixture.fingerprint(),
                    evidence(2_000),
                    6_000,
                    1);
            assertEquals(6_000, lease.validUntilEpochMs());
            assertEquals(
                    6_000,
                    backend.resolveProtection(fixture.profile().ref(), 1).objectStoreLeaseProtectionUntilEpochMs());

            final OxiaSyncProfileCatalogBackend reopened = new OxiaSyncProfileCatalogBackend(
                    client, prefix + "/catalog", 5_000, 10_000, trustSet(fixture.keyPair()));
            assertEquals(fixture.profile(), reopened.resolve(fixture.profile().ref()));
            final CredentialUseLease shorterLease = reopened.issueCredentialUseLease(
                    fixture.profile().ref(),
                    CredentialUseKind.OBJECT_STORE_ADAPTER,
                    id32(30),
                    1,
                    fixture.binding().bindingDigest(),
                    fixture.fingerprint(),
                    evidence(2_000),
                    5_000,
                    1);
            assertEquals(5_000, shorterLease.validUntilEpochMs());
            assertEquals(lease.protectionRevision(), shorterLease.protectionRevision());

            final CredentialBinding nextBinding =
                    binding(fixture.profile(), 2, Bytes.utf8("secret://real-object/current"), fixture.keyPair());
            final RotateEquivalentSecretRequest rotation = new RotateEquivalentSecretRequest(
                    fixture.profile().ref(),
                    1,
                    2,
                    Bytes.utf8("secret://real-object/current"),
                    Bytes.sha256(Bytes.utf8("secret://real-object/current")),
                    nextBinding.equivalenceAttestation(),
                    fixture.binding().bindingDigest(),
                    1);
            assertEquals(2, reopened.rotate(rotation).headRevision());
            assertEquals(2, reopened.resolveHead(fixture.profile().ref()).secretGeneration());
        }
    }

    private static String endpoint() {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(), "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        return endpoint;
    }

    private static OxiaSyncOwnerLeaseBackend.ClientHandle connect(final String endpoint, final String identifier)
            throws Exception {
        return OxiaSyncOwnerLeaseBackend.connect(
                endpoint, "default", identifier, Duration.ofSeconds(15), "real-profile-smoke");
    }

    private static Fixture fixture() throws Exception {
        final ObjectStoreProfileSemantic semantic = new ObjectStoreProfileSemantic(
                ObjectStoreProviderKind.S3_COMPATIBLE,
                id32(1),
                id32(2),
                1,
                true,
                true,
                true,
                true,
                id32(3),
                1 << 20,
                ObjectStoreProfileSemantic.SINGLE_PUT,
                1,
                id32(4));
        final ProfileSemanticEnvelope profile =
                new ProfileSemanticEnvelope(ProfileKind.OBJECT_STORE, Bytes.utf8("real-object-store"), 1, semantic);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] reference = Bytes.utf8("secret://real-object/initial");
        final byte[] fingerprint = id32(5);
        final CredentialEquivalenceAttestation attestation = CredentialEquivalenceAttestation.signed(
                profile.ref(),
                1,
                Bytes.sha256(reference),
                semantic.credentialAuthorizationScopeDigest(),
                fingerprint,
                1,
                Bytes.utf8("real-verifier"),
                evidence(1_000),
                10_000,
                id32(6),
                1,
                keyPair.getPrivate());
        return new Fixture(
                profile, CredentialBinding.create(profile.ref(), 1, reference, attestation), fingerprint, keyPair);
    }

    private static CredentialBinding binding(
            final ProfileSemanticEnvelope profile,
            final long generation,
            final byte[] reference,
            final KeyPair keyPair) {
        final ObjectStoreProfileSemantic semantic = (ObjectStoreProfileSemantic) profile.body();
        final CredentialEquivalenceAttestation attestation = CredentialEquivalenceAttestation.signed(
                profile.ref(),
                generation,
                Bytes.sha256(reference),
                semantic.credentialAuthorizationScopeDigest(),
                id32(5 + (int) generation),
                1,
                Bytes.utf8("real-verifier"),
                evidence(1_000),
                10_000,
                id32(7 + (int) generation),
                1,
                keyPair.getPrivate());
        return CredentialBinding.create(profile.ref(), generation, reference, attestation);
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest) {
        return new TrustedUtcIntervalEvidence(
                earliest,
                earliest + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                id32((int) earliest),
                1,
                1,
                1,
                id32((int) earliest + 1),
                0,
                null);
    }

    private static CredentialAttestationTrustSet trustSet(final KeyPair keyPair) {
        return CredentialAttestationTrustSet.single(1, Bytes.utf8("real-verifier"), 1, keyPair.getPublic(), 0, 20_000);
    }

    private static byte[] id32(final int seed) {
        final byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record Fixture(
            ProfileSemanticEnvelope profile, CredentialBinding binding, byte[] fingerprint, KeyPair keyPair) {
        @Override
        public byte[] fingerprint() {
            return Bytes.copy(fingerprint);
        }
    }
}
