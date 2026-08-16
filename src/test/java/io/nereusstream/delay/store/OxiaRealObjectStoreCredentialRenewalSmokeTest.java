package io.nereusstream.delay.store;

import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CredentialBindingV1;
import io.nereusstream.delay.protocol.CredentialEquivalenceAttestationV1;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.RotateEquivalentSecretRequestV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import io.nereusstream.delay.runtime.OxiaSyncProfileCatalogBackend;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Opt-in real Oxia coverage for renewable Object Store credential authority. */
@Tag("real-service")
class OxiaRealObjectStoreCredentialRenewalSmokeTest {
    private static final long MAX_LEASE_TTL_MS = 5_000;
    private static final long MAX_ATTESTATION_AGE_MS = 60_000;
    private static final long RENEW_BEFORE_MS = 2_000;
    private static final CheckpointManifestLimits LIMITS = new CheckpointManifestLimits(
            10, 1 << 20, 1 << 20, 1_024, 1 << 20, 10, 1_024);

    @Test
    void renewsRealOxiaLeaseAndFencesTheLiveAdapterAtHeadRotation() throws Exception {
        final String oxiaEndpoint = required("NEREUS_DELAY_OXIA_ENDPOINT");
        final URI objectStoreEndpoint = URI.create(required("NEREUS_DELAY_MINIO_ENDPOINT"));
        final String region = valueOrDefault("NEREUS_DELAY_MINIO_REGION", "us-east-1");
        final String bucket = required("NEREUS_DELAY_MINIO_BUCKET");
        final String accessKey = required("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String secretKey = required("NEREUS_DELAY_MINIO_SECRET_KEY");
        final long now = System.currentTimeMillis();
        final Fixture fixture = fixture(objectStoreEndpoint, region, bucket, accessKey, now);
        final MutableClock clock = new MutableClock(now);
        final String prefix = "nereus-delay-real-object-store-renewal/" + UUID.randomUUID();

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = OxiaSyncOwnerLeaseBackend.connect(
                oxiaEndpoint, "default", "real-object-store-renewal-" + UUID.randomUUID(),
                Duration.ofSeconds(15), prefix + "/client")) {
            final OxiaSyncProfileCatalogBackend authority = new OxiaSyncProfileCatalogBackend(
                    client, prefix + "/catalog", MAX_LEASE_TTL_MS, MAX_ATTESTATION_AGE_MS,
                    CredentialAttestationTrustSet.single(1, fixture.verifierId(), 1,
                            fixture.keyPair().getPublic(), 0, now + 120_000));
            assertEquals(1, authority.publish(fixture.profile(), fixture.binding()).headRevision());

            final OxiaObjectStoreCredentialLeaseActivator activator =
                    new OxiaObjectStoreCredentialLeaseActivator(authority,
                            (profile, binding) -> {
                                if (!fixture.profile().equals(profile) || !fixture.binding().equals(binding)) {
                                    throw new IllegalStateException("renewal resolver received a different binding");
                                }
                                return new OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial(
                                        accessKey, secretKey, null, fixture.fingerprint());
                            }, MAX_LEASE_TTL_MS, MAX_ATTESTATION_AGE_MS);
            final OxiaObjectStoreCredentialLeaseActivator.ActivationRequest request =
                    new OxiaObjectStoreCredentialLeaseActivator.ActivationRequest(
                            fixture.profile().ref(), objectStoreEndpoint, region, bucket, bytes(32, 30),
                            evidence(now), now + 3_000, LIMITS, HttpClient.newHttpClient(), clock,
                            Duration.ofSeconds(5));
            final RenewableS3CompatibleCheckpointObjectStoreAdapter adapter = activator
                    .activateRenewableS3Compatible(request, RENEW_BEFORE_MS, () -> evidence(clock.millis()));

            assertEquals(now + 3_000, adapter.lease().validUntilEpochMs());
            clock.setMillis(now + 1_500);
            adapter.renewIfNeeded();
            assertEquals(now + 6_500, adapter.lease().validUntilEpochMs());
            assertEquals(now + 6_500,
                    authority.resolveProtection(fixture.profile().ref(), 1)
                            .objectStoreLeaseProtectionUntilEpochMs());

            final CredentialBindingV1 rotated = binding(fixture.profile(), 2, now, fixture.keyPair());
            final RotateEquivalentSecretRequestV1 rotation = new RotateEquivalentSecretRequestV1(
                    fixture.profile().ref(), 1, 2, Bytes.utf8("secret://real-minio/v2"),
                    Bytes.sha256(Bytes.utf8("secret://real-minio/v2")), rotated.equivalenceAttestation(),
                    fixture.binding().bindingDigest(), 1);
            assertEquals(2, authority.rotate(rotation).headRevision());

            clock.setMillis(now + 5_000);
            assertThrows(IllegalStateException.class, adapter::renewIfNeeded);
            System.out.println("Oxia + MinIO Object Store credential renewal E2E passed: real Profile Head/protection CAS renewed the exact lease and fenced the live adapter at secret rotation");
        }
    }

    private static Fixture fixture(final URI endpoint, final String region, final String bucket,
                                   final String accessKey, final long now) throws Exception {
        final ObjectStoreProfileSemanticV1 semantic = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3_COMPATIBLE,
                S3CompatibleCheckpointObjectStoreAdapter.endpointConfigDigest(endpoint, region, bucket),
                S3CompatibleCheckpointObjectStoreAdapter.credentialAuthorizationScopeDigest(
                        accessKey, region, bucket),
                1, true, true, true, true, bytes(32, 1), 1 << 20,
                ObjectStoreProfileSemanticV1.SINGLE_PUT, 1, bytes(32, 2));
        final ProfileSemanticEnvelopeV1 profile = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.OBJECT_STORE, Bytes.utf8("real-renewable-object-store"), 1, semantic);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] reference = Bytes.utf8("secret://real-minio/v1");
        final byte[] fingerprint = bytes(32, 5);
        final CredentialEquivalenceAttestationV1 attestation = CredentialEquivalenceAttestationV1.signed(
                profile.ref(), 1, Bytes.sha256(reference), semantic.credentialAuthorizationScopeDigest(),
                fingerprint, 1, Bytes.utf8("real-minio-verifier"), evidence(now - 1_000), now + 30_000,
                bytes(32, 6), 1, keyPair.getPrivate());
        return new Fixture(profile, CredentialBindingV1.create(profile.ref(), 1, reference, attestation),
                fingerprint, keyPair, Bytes.utf8("real-minio-verifier"));
    }

    private static CredentialBindingV1 binding(final ProfileSemanticEnvelopeV1 profile, final long generation,
                                               final long now, final KeyPair keyPair) {
        final ObjectStoreProfileSemanticV1 semantic = (ObjectStoreProfileSemanticV1) profile.body();
        final byte[] reference = Bytes.utf8("secret://real-minio/v2");
        final CredentialEquivalenceAttestationV1 attestation = CredentialEquivalenceAttestationV1.signed(
                profile.ref(), generation, Bytes.sha256(reference), semantic.credentialAuthorizationScopeDigest(),
                bytes(32, 8), 1, Bytes.utf8("real-minio-verifier"), evidence(now - 1_000), now + 30_000,
                bytes(32, 9), 1, keyPair.getPrivate());
        return CredentialBindingV1.create(profile.ref(), generation, reference, attestation);
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest) {
        return new TrustedUtcIntervalEvidence(earliest, earliest + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 14), 1, 1, 1,
                bytes(32, 15), 0, null);
    }

    private static String required(final String name) {
        final String value = System.getenv(name);
        Assumptions.assumeTrue(value != null && !value.isBlank(), name + " is not configured");
        return value;
    }

    private static String valueOrDefault(final String name, final String defaultValue) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record Fixture(ProfileSemanticEnvelopeV1 profile, CredentialBindingV1 binding, byte[] fingerprint,
                           KeyPair keyPair, byte[] verifierId) {
        @Override
        public byte[] fingerprint() {
            return Bytes.copy(fingerprint);
        }

        @Override
        public byte[] verifierId() {
            return Bytes.copy(verifierId);
        }
    }

    private static final class MutableClock extends Clock {
        private final AtomicLong millis;

        private MutableClock(final long initialMillis) {
            millis = new AtomicLong(initialMillis);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(final java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public long millis() {
            return millis.get();
        }

        private void setMillis(final long value) {
            millis.set(value);
        }
    }
}
