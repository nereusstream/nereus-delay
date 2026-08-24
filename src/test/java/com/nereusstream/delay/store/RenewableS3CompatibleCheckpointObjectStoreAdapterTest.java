package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CredentialBindingHeadV1;
import com.nereusstream.delay.protocol.CredentialBindingProtectionV1;
import com.nereusstream.delay.protocol.CredentialBindingV1;
import com.nereusstream.delay.protocol.CredentialEquivalenceAttestationV1;
import com.nereusstream.delay.protocol.CredentialUseKindV1;
import com.nereusstream.delay.protocol.CredentialUseLeaseV1;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import com.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import com.nereusstream.delay.protocol.ProfileKindV1;
import com.nereusstream.delay.protocol.ProfileRefV1;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialProfileAuthority;
import java.net.URI;
import java.net.http.HttpClient;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class RenewableS3CompatibleCheckpointObjectStoreAdapterTest {
    private static final URI ENDPOINT = URI.create("http://127.0.0.1:12345/api");
    private static final String REGION = "us-east-1";
    private static final String BUCKET = "checkpoint-bucket";
    private static final String ACCESS_KEY = "renew-access";
    private static final String SECRET_KEY = "renew-secret";
    private static final long MAX_TTL_MS = 5_000;
    private static final long MAX_ATTESTATION_AGE_MS = 10_000;
    private static final CheckpointManifestLimits LIMITS =
            new CheckpointManifestLimits(10, 1 << 20, 1 << 20, 1024, 1 << 20, 10, 1024);

    @Test
    void renewsOnlyInsideWindowAndReplacesTheLocalGate() throws Exception {
        final Fixture fixture = fixture();
        final MutableClock clock = new MutableClock(3_000);
        final FakeAuthority authority = new FakeAuthority(fixture);
        final RenewableS3CompatibleCheckpointObjectStoreAdapter adapter = activate(authority, fixture, clock);

        adapter.renewIfNeeded();
        assertEquals(1, authority.issueCalls);

        clock.setMillis(4_500);
        adapter.renewIfNeeded();
        assertEquals(2, authority.issueCalls);
        assertEquals(9_500, adapter.lease().validUntilEpochMs());
        assertEquals(3, adapter.lease().protectionRevision());
    }

    @Test
    void refusesToRenewAcrossAHeadRotationBeforeProviderIo() throws Exception {
        final Fixture fixture = fixture();
        final MutableClock clock = new MutableClock(4_500);
        final FakeAuthority authority = new FakeAuthority(fixture);
        final RenewableS3CompatibleCheckpointObjectStoreAdapter adapter = activate(authority, fixture, clock);
        authority.rotate(fixture.profile());

        assertThrows(IllegalStateException.class, adapter::renewIfNeeded);
        assertEquals(1, authority.issueCalls);
    }

    private static RenewableS3CompatibleCheckpointObjectStoreAdapter activate(
            final FakeAuthority authority, final Fixture fixture, final MutableClock clock) {
        final OxiaObjectStoreCredentialLeaseActivator activator = new OxiaObjectStoreCredentialLeaseActivator(
                authority,
                (profile, binding) -> new OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial(
                        ACCESS_KEY, SECRET_KEY, null, fixture.fingerprint()),
                MAX_TTL_MS,
                MAX_ATTESTATION_AGE_MS);
        return activator.activateRenewableS3Compatible(
                new OxiaObjectStoreCredentialLeaseActivator.ActivationRequest(
                        fixture.profile().ref(),
                        ENDPOINT,
                        REGION,
                        BUCKET,
                        bytes(32, 30),
                        evidence(2_000),
                        6_000,
                        LIMITS,
                        HttpClient.newHttpClient(),
                        clock,
                        Duration.ofSeconds(5)),
                2_000,
                () -> evidence(clock.millis()));
    }

    private static Fixture fixture() throws Exception {
        final ObjectStoreProfileSemanticV1 semantic = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3_COMPATIBLE,
                S3CompatibleCheckpointObjectStoreAdapter.endpointConfigDigest(ENDPOINT, REGION, BUCKET),
                S3CompatibleCheckpointObjectStoreAdapter.credentialAuthorizationScopeDigest(ACCESS_KEY, REGION, BUCKET),
                1,
                true,
                true,
                true,
                true,
                bytes(32, 1),
                1 << 20,
                ObjectStoreProfileSemanticV1.SINGLE_PUT,
                1,
                bytes(32, 2));
        final ProfileSemanticEnvelopeV1 profile = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.OBJECT_STORE, Bytes.utf8("renew-object-store"), 1, semantic);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] reference = Bytes.utf8("secret://renew/v1");
        final byte[] fingerprint = bytes(32, 5);
        final CredentialEquivalenceAttestationV1 attestation = CredentialEquivalenceAttestationV1.signed(
                profile.ref(),
                1,
                Bytes.sha256(reference),
                semantic.credentialAuthorizationScopeDigest(),
                fingerprint,
                1,
                Bytes.utf8("renew-verifier"),
                evidence(1_000),
                10_000,
                bytes(32, 6),
                1,
                keyPair.getPrivate());
        final CredentialBindingV1 binding = CredentialBindingV1.create(profile.ref(), 1, reference, attestation);
        final CredentialBindingProtectionV1 protection =
                CredentialBindingProtectionV1.forBinding(binding, 0, 6_000, 0, 0, 2);
        final CredentialUseLeaseV1 lease = new CredentialUseLeaseV1(
                profile.ref(),
                CredentialUseKindV1.OBJECT_STORE_ADAPTER,
                bytes(32, 30),
                1,
                binding.bindingDigest(),
                fingerprint,
                evidence(2_000),
                6_000,
                2);
        return new Fixture(profile, binding, protection, lease, fingerprint, keyPair);
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
            ProfileSemanticEnvelopeV1 profile,
            CredentialBindingV1 binding,
            CredentialBindingProtectionV1 protection,
            CredentialUseLeaseV1 lease,
            byte[] fingerprint,
            KeyPair keyPair) {
        @Override
        public byte[] fingerprint() {
            return Bytes.copy(fingerprint);
        }
    }

    private static final class FakeAuthority implements CredentialProfileAuthority {
        private final Fixture fixture;
        private CredentialBindingV1 currentBinding;
        private CredentialBindingHeadV1 currentHead;
        private CredentialBindingProtectionV1 protection;
        private CredentialUseLeaseV1 lease;
        private int issueCalls;

        private FakeAuthority(final Fixture fixture) {
            this.fixture = fixture;
            this.currentBinding = fixture.binding();
            this.currentHead = CredentialBindingHeadV1.forBinding(fixture.binding(), 1);
            this.protection = fixture.protection();
            this.lease = fixture.lease();
        }

        @Override
        public ProfileSemanticEnvelopeV1 resolve(final ProfileRefV1 reference) {
            return fixture.profile().ref().equals(reference) ? fixture.profile() : null;
        }

        @Override
        public CredentialBindingV1 resolveBinding(final ProfileRefV1 profile, final long generation) {
            return fixture.profile().ref().equals(profile) && currentBinding.secretGeneration() == generation
                    ? currentBinding
                    : null;
        }

        @Override
        public CredentialBindingHeadV1 resolveHead(final ProfileRefV1 profile) {
            return fixture.profile().ref().equals(profile) ? currentHead : null;
        }

        @Override
        public CredentialBindingProtectionV1 resolveProtection(final ProfileRefV1 profile, final long generation) {
            return fixture.profile().ref().equals(profile) && protection.secretGeneration() == generation
                    ? protection
                    : null;
        }

        @Override
        public CredentialUseLeaseV1 issueCredentialUseLease(
                final ProfileRefV1 profile,
                final CredentialUseKindV1 kind,
                final byte[] holderScopeDigest,
                final long expectedSecretGeneration,
                final byte[] expectedBindingDigest,
                final byte[] resolvedCredentialFingerprintDigest,
                final TrustedUtcIntervalEvidence issuedAt,
                final long validUntilEpochMs,
                final long expectedHeadRevision) {
            issueCalls++;
            assertEquals(currentBinding.secretGeneration(), expectedSecretGeneration);
            assertEquals(currentHead.headRevision(), expectedHeadRevision);
            assertArrayEquals(currentBinding.bindingDigest(), expectedBindingDigest);
            assertArrayEquals(fixture.fingerprint(), resolvedCredentialFingerprintDigest);
            final long currentUntil = protection.objectStoreLeaseProtectionUntilEpochMs();
            final long nextUntil = Math.max(currentUntil, validUntilEpochMs);
            final long nextRevision =
                    nextUntil == currentUntil ? protection.protectionRevision() : protection.protectionRevision() + 1;
            protection = CredentialBindingProtectionV1.forBinding(currentBinding, 0, nextUntil, 0, 0, nextRevision);
            lease = new CredentialUseLeaseV1(
                    profile,
                    kind,
                    holderScopeDigest,
                    expectedSecretGeneration,
                    currentBinding.bindingDigest(),
                    resolvedCredentialFingerprintDigest,
                    issuedAt,
                    validUntilEpochMs,
                    nextRevision);
            return lease;
        }

        private void rotate(final ProfileSemanticEnvelopeV1 profile) throws Exception {
            final byte[] reference = Bytes.utf8("secret://renew/v2");
            final ObjectStoreProfileSemanticV1 semantic = (ObjectStoreProfileSemanticV1) profile.body();
            final CredentialEquivalenceAttestationV1 attestation = CredentialEquivalenceAttestationV1.signed(
                    profile.ref(),
                    2,
                    Bytes.sha256(reference),
                    semantic.credentialAuthorizationScopeDigest(),
                    bytes(32, 8),
                    1,
                    Bytes.utf8("renew-verifier"),
                    evidence(1_000),
                    10_000,
                    bytes(32, 9),
                    1,
                    fixture.keyPair().getPrivate());
            currentBinding = CredentialBindingV1.create(profile.ref(), 2, reference, attestation);
            currentHead = CredentialBindingHeadV1.forBinding(currentBinding, 2);
        }
    }

    private static final class MutableClock extends Clock {
        private final AtomicLong millis;

        private MutableClock(final long initialMillis) {
            this.millis = new AtomicLong(initialMillis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(final ZoneId zone) {
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
