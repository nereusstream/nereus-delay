package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CredentialBindingHeadV1;
import io.nereusstream.delay.protocol.CredentialBindingProtectionV1;
import io.nereusstream.delay.protocol.CredentialBindingV1;
import io.nereusstream.delay.protocol.CredentialEquivalenceAttestationV1;
import io.nereusstream.delay.protocol.CredentialUseKindV1;
import io.nereusstream.delay.protocol.CredentialUseLeaseV1;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.store.CheckpointManifestLimits;
import io.nereusstream.delay.store.OxiaObjectStoreCredentialLeaseActivator;
import io.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapter;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OxiaObjectStoreCredentialLeaseActivatorTest {
    private static final URI ENDPOINT = URI.create("http://127.0.0.1:12345/api");
    private static final String REGION = "us-east-1";
    private static final String BUCKET = "checkpoint-bucket";
    private static final String ACCESS_KEY = "activation-access";
    private static final String SECRET_KEY = "activation-secret";
    private static final CheckpointManifestLimits LIMITS = new CheckpointManifestLimits(
            10, 1 << 20, 1 << 20, 1024, 1 << 20, 10, 1024);
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(3_000), ZoneOffset.UTC);

    @Test
    void activatesAdapterOnlyFromExactHeadBindingMaterialAndProtectedLease() throws Exception {
        final Fixture fixture = fixture();
        final FakeAuthority authority = new FakeAuthority(fixture);
        final AtomicInteger resolverCalls = new AtomicInteger();
        final OxiaObjectStoreCredentialLeaseActivator activator = new OxiaObjectStoreCredentialLeaseActivator(
                authority, (profile, binding) -> {
                    resolverCalls.incrementAndGet();
                    assertEquals(fixture.profile(), profile);
                    assertEquals(fixture.binding(), binding);
                    return new OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial(
                            ACCESS_KEY, SECRET_KEY, null, fixture.fingerprint());
                }, 5_000, 10_000);

        final S3CompatibleCheckpointObjectStoreAdapter adapter = activator.activateS3Compatible(
                request(fixture.profile().ref(), 6_000));

        assertEquals(1, resolverCalls.get());
        assertEquals(1, authority.issueCalls);
        assertEquals(S3CompatibleCheckpointObjectStoreAdapter.class, adapter.getClass());
    }

    @Test
    void rejectsResolverFingerprintDriftBeforeLeaseIssuance() throws Exception {
        final Fixture fixture = fixture();
        final FakeAuthority authority = new FakeAuthority(fixture);
        final OxiaObjectStoreCredentialLeaseActivator activator = new OxiaObjectStoreCredentialLeaseActivator(
                authority, (profile, binding) -> new OxiaObjectStoreCredentialLeaseActivator
                        .ObjectStoreCredentialMaterial(ACCESS_KEY, SECRET_KEY, null, bytes(32, 99)),
                5_000, 10_000);

        assertThrows(IllegalStateException.class, () -> activator.activateS3Compatible(
                request(fixture.profile().ref(), 6_000)));
        assertEquals(0, authority.issueCalls);
    }

    @Test
    void rejectsAuthorityLeaseThatIsNotProtectedByTheRereadProjection() throws Exception {
        final Fixture fixture = fixture();
        final FakeAuthority authority = new FakeAuthority(fixture);
        authority.lease = new CredentialUseLeaseV1(fixture.profile().ref(),
                CredentialUseKindV1.OBJECT_STORE_ADAPTER, bytes(32, 30), 1, fixture.binding().bindingDigest(),
                fixture.fingerprint(), evidence(2_000), 6_000, 1);
        final OxiaObjectStoreCredentialLeaseActivator activator = new OxiaObjectStoreCredentialLeaseActivator(
                authority, (profile, binding) -> new OxiaObjectStoreCredentialLeaseActivator
                        .ObjectStoreCredentialMaterial(ACCESS_KEY, SECRET_KEY, null, fixture.fingerprint()),
                5_000, 10_000);

        assertThrows(IllegalArgumentException.class, () -> activator.activateS3Compatible(
                request(fixture.profile().ref(), 6_000)));
    }

    private static OxiaObjectStoreCredentialLeaseActivator.ActivationRequest request(
            final ProfileRefV1 profile, final long validUntil) {
        return new OxiaObjectStoreCredentialLeaseActivator.ActivationRequest(profile, ENDPOINT, REGION, BUCKET,
                bytes(32, 30), evidence(2_000), validUntil, LIMITS, HttpClient.newHttpClient(), CLOCK,
                Duration.ofSeconds(5));
    }

    private static Fixture fixture() throws Exception {
        final ObjectStoreProfileSemanticV1 semantic = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3_COMPATIBLE,
                S3CompatibleCheckpointObjectStoreAdapter.endpointConfigDigest(ENDPOINT, REGION, BUCKET),
                S3CompatibleCheckpointObjectStoreAdapter.credentialAuthorizationScopeDigest(
                        ACCESS_KEY, REGION, BUCKET),
                1, true, true, true, true, bytes(32, 1), 1 << 20,
                ObjectStoreProfileSemanticV1.SINGLE_PUT, 1, bytes(32, 2));
        final ProfileSemanticEnvelopeV1 profile = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.OBJECT_STORE, Bytes.utf8("activation-object-store"), 1, semantic);
        final byte[] reference = Bytes.utf8("secret://activation/v1");
        final byte[] fingerprint = bytes(32, 5);
        final CredentialEquivalenceAttestationV1 attestation = CredentialEquivalenceAttestationV1.signed(
                profile.ref(), 1, Bytes.sha256(reference), semantic.credentialAuthorizationScopeDigest(),
                fingerprint, 1, Bytes.utf8("activation-verifier"), evidence(1_000), 10_000, bytes(32, 6), 1,
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());
        final CredentialBindingV1 binding = CredentialBindingV1.create(profile.ref(), 1, reference, attestation);
        final CredentialBindingHeadV1 head = CredentialBindingHeadV1.forBinding(binding, 1);
        final CredentialBindingProtectionV1 protection = CredentialBindingProtectionV1.forBinding(
                binding, 0, 6_000, 0, 0, 2);
        final CredentialUseLeaseV1 lease = new CredentialUseLeaseV1(profile.ref(),
                CredentialUseKindV1.OBJECT_STORE_ADAPTER, bytes(32, 30), 1, binding.bindingDigest(), fingerprint,
                evidence(2_000), 6_000, 2);
        return new Fixture(profile, binding, head, protection, lease, fingerprint);
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

    private record Fixture(ProfileSemanticEnvelopeV1 profile, CredentialBindingV1 binding,
                           CredentialBindingHeadV1 head, CredentialBindingProtectionV1 protection,
                           CredentialUseLeaseV1 lease, byte[] fingerprint) {
        @Override
        public byte[] fingerprint() {
            return Bytes.copy(fingerprint);
        }
    }

    private static final class FakeAuthority implements CredentialProfileAuthority {
        private final Fixture fixture;
        private CredentialUseLeaseV1 lease;
        private int issueCalls;

        private FakeAuthority(final Fixture fixture) {
            this.fixture = fixture;
            this.lease = fixture.lease();
        }

        @Override
        public ProfileSemanticEnvelopeV1 resolve(final ProfileRefV1 reference) {
            return fixture.profile().ref().equals(reference) ? fixture.profile() : null;
        }

        @Override
        public CredentialBindingV1 resolveBinding(final ProfileRefV1 profile, final long generation) {
            return fixture.profile().ref().equals(profile) && generation == 1 ? fixture.binding() : null;
        }

        @Override
        public CredentialBindingHeadV1 resolveHead(final ProfileRefV1 profile) {
            return fixture.profile().ref().equals(profile) ? fixture.head() : null;
        }

        @Override
        public CredentialBindingProtectionV1 resolveProtection(final ProfileRefV1 profile,
                                                               final long generation) {
            return fixture.profile().ref().equals(profile) && generation == 1 ? fixture.protection() : null;
        }

        @Override
        public CredentialUseLeaseV1 issueCredentialUseLease(final ProfileRefV1 profile,
                                                             final CredentialUseKindV1 kind,
                                                             final byte[] holderScopeDigest,
                                                             final long expectedSecretGeneration,
                                                             final byte[] expectedBindingDigest,
                                                             final byte[] resolvedFingerprint,
                                                             final TrustedUtcIntervalEvidence issuedAt,
                                                             final long validUntilEpochMs,
                                                             final long expectedHeadRevision) {
            issueCalls++;
            return lease;
        }
    }
}
