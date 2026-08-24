package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CredentialBindingV1;
import com.nereusstream.delay.protocol.CredentialEquivalenceAttestationV1;
import com.nereusstream.delay.protocol.CredentialUseKindV1;
import com.nereusstream.delay.protocol.CredentialUseLeaseV1;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import com.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import com.nereusstream.delay.protocol.ProfileKindV1;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import com.nereusstream.delay.protocol.RotateEquivalentSecretRequestV1;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class OxiaSyncProfileCatalogBackendTest {
    private static final long MAX_LEASE_TTL_MS = 5_000;
    private static final long MAX_ATTESTATION_AGE_MS = 10_000;

    @Test
    void publishesReopensRotatesAndIssuesProtectionBoundLease() throws Exception {
        final FakeRecordClient records = new FakeRecordClient();
        final Fixture fixture = fixture();
        final OxiaSyncProfileCatalogBackend backend = backend(records, "delay/profile", fixture.keyPair());

        assertEquals(1, backend.publish(fixture.profile(), fixture.binding()).headRevision());
        final CredentialUseLeaseV1 lease = backend.issueCredentialUseLease(
                fixture.profile().ref(),
                CredentialUseKindV1.OBJECT_STORE_ADAPTER,
                bytes(32, 30),
                1,
                fixture.binding().bindingDigest(),
                fixture.fingerprint(),
                evidence(2_000),
                6_000,
                1);
        assertEquals(6_000, lease.validUntilEpochMs());
        assertEquals(2, backend.resolveProtection(fixture.profile().ref(), 1).protectionRevision());
        assertEquals(fixture.binding(), backend.resolveBinding(fixture.profile().ref(), 1));

        final OxiaSyncProfileCatalogBackend reopened = backend(records, "delay/profile", fixture.keyPair());
        assertEquals(fixture.profile(), reopened.resolve(fixture.profile().ref()));
        final CredentialUseLeaseV1 shorterLease = reopened.issueCredentialUseLease(
                fixture.profile().ref(),
                CredentialUseKindV1.OBJECT_STORE_ADAPTER,
                bytes(32, 30),
                1,
                fixture.binding().bindingDigest(),
                fixture.fingerprint(),
                evidence(2_000),
                5_000,
                1);
        assertEquals(5_000, shorterLease.validUntilEpochMs());
        assertEquals(2, shorterLease.protectionRevision());

        final CredentialBindingV1 nextBinding =
                binding(fixture.profile(), 2, Bytes.utf8("secret://object/v2"), fixture.keyPair());
        final RotateEquivalentSecretRequestV1 rotation = new RotateEquivalentSecretRequestV1(
                fixture.profile().ref(),
                1,
                2,
                Bytes.utf8("secret://object/v2"),
                Bytes.sha256(Bytes.utf8("secret://object/v2")),
                nextBinding.equivalenceAttestation(),
                fixture.binding().bindingDigest(),
                1);
        assertEquals(2, reopened.rotate(rotation).headRevision());
        assertEquals(2, reopened.resolveHead(fixture.profile().ref()).secretGeneration());
        assertEquals(2, reopened.rotate(rotation).headRevision());
    }

    @Test
    void responseLossIsAcceptedOnlyAfterExactReread() throws Exception {
        final FakeRecordClient records = new FakeRecordClient();
        final Fixture fixture = fixture();
        final OxiaSyncProfileCatalogBackend backend = backend(records, "delay/profile-loss", fixture.keyPair());

        records.failNextPutAfterCommit = true;
        assertEquals(1, backend.publish(fixture.profile(), fixture.binding()).headRevision());
        assertEquals(fixture.profile(), backend.resolve(fixture.profile().ref()));

        records.putRaw(profileKey("delay/profile-loss", fixture.profile()), new byte[] {0x08, 0x02});
        assertThrows(
                IllegalStateException.class,
                () -> backend.resolve(fixture.profile().ref()));
    }

    @Test
    void sessionFenceRejectsACommittedPublicationAfterTheMarkerChanges() throws Exception {
        final FakeRecordClient records = new FakeRecordClient();
        final Fixture fixture = fixture();
        final AtomicBoolean sessionAlive = new AtomicBoolean(true);
        final OxiaSyncProfileCatalogBackend backend = new OxiaSyncProfileCatalogBackend(
                records,
                "delay/profile-fenced",
                MAX_LEASE_TTL_MS,
                MAX_ATTESTATION_AGE_MS,
                CredentialAttestationTrustSet.single(
                        1, Bytes.utf8("verifier"), 1, fixture.keyPair().getPublic(), 0, 20_000),
                () -> {
                    if (!sessionAlive.get()) {
                        throw new IllegalStateException("simulated Oxia session fence");
                    }
                });
        records.afterPut = () -> sessionAlive.set(false);

        assertThrows(IllegalStateException.class, () -> backend.publish(fixture.profile(), fixture.binding()));

        final OxiaSyncProfileCatalogBackend reopened = backend(records, "delay/profile-fenced", fixture.keyPair());
        assertEquals(fixture.profile(), reopened.resolve(fixture.profile().ref()));
    }

    @Test
    void rejectsHeadCasDriftAndProfileSemanticCollision() throws Exception {
        final FakeRecordClient records = new FakeRecordClient();
        final Fixture fixture = fixture();
        final OxiaSyncProfileCatalogBackend backend = backend(records, "delay/profile-fence", fixture.keyPair());
        backend.publish(fixture.profile(), fixture.binding());

        final CredentialBindingV1 nextBinding =
                binding(fixture.profile(), 2, Bytes.utf8("secret://object/v2"), fixture.keyPair());
        final RotateEquivalentSecretRequestV1 wrongHead = new RotateEquivalentSecretRequestV1(
                fixture.profile().ref(),
                1,
                2,
                Bytes.utf8("secret://object/v2"),
                Bytes.sha256(Bytes.utf8("secret://object/v2")),
                nextBinding.equivalenceAttestation(),
                bytes(32, 99),
                1);
        assertThrows(IllegalStateException.class, () -> backend.rotate(wrongHead));
        assertEquals(1, backend.resolveHead(fixture.profile().ref()).secretGeneration());

        final ProfileSemanticEnvelopeV1 conflicting = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.OBJECT_STORE,
                Bytes.utf8("object-store"),
                1,
                new ObjectStoreProfileSemanticV1(
                        ObjectStoreProviderKindV1.S3_COMPATIBLE,
                        bytes(32, 1),
                        bytes(32, 2),
                        1,
                        true,
                        true,
                        true,
                        true,
                        bytes(32, 44),
                        1 << 20,
                        ObjectStoreProfileSemanticV1.SINGLE_PUT,
                        1,
                        bytes(32, 4)));
        assertThrows(IllegalStateException.class, () -> backend.resolve(conflicting.ref()));
    }

    private static OxiaSyncProfileCatalogBackend backend(
            final FakeRecordClient records, final String prefix, final KeyPair keyPair) {
        return new OxiaSyncProfileCatalogBackend(
                records,
                prefix,
                MAX_LEASE_TTL_MS,
                MAX_ATTESTATION_AGE_MS,
                CredentialAttestationTrustSet.single(1, Bytes.utf8("verifier"), 1, keyPair.getPublic(), 0, 20_000));
    }

    private static Fixture fixture() throws Exception {
        final ObjectStoreProfileSemanticV1 semantic = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3_COMPATIBLE,
                bytes(32, 1),
                bytes(32, 2),
                1,
                true,
                true,
                true,
                true,
                bytes(32, 3),
                1 << 20,
                ObjectStoreProfileSemanticV1.SINGLE_PUT,
                1,
                bytes(32, 4));
        final ProfileSemanticEnvelopeV1 profile =
                new ProfileSemanticEnvelopeV1(ProfileKindV1.OBJECT_STORE, Bytes.utf8("object-store"), 1, semantic);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] reference = Bytes.utf8("secret://object/v1");
        final byte[] fingerprint = bytes(32, 5);
        final CredentialEquivalenceAttestationV1 attestation = CredentialEquivalenceAttestationV1.signed(
                profile.ref(),
                1,
                Bytes.sha256(reference),
                semantic.credentialAuthorizationScopeDigest(),
                fingerprint,
                1,
                Bytes.utf8("verifier"),
                evidence(1_000),
                10_000,
                bytes(32, 6),
                1,
                keyPair.getPrivate());
        return new Fixture(
                profile, CredentialBindingV1.create(profile.ref(), 1, reference, attestation), fingerprint, keyPair);
    }

    private static CredentialBindingV1 binding(
            final ProfileSemanticEnvelopeV1 profile,
            final long generation,
            final byte[] reference,
            final KeyPair keyPair) {
        final ObjectStoreProfileSemanticV1 semantic = (ObjectStoreProfileSemanticV1) profile.body();
        final CredentialEquivalenceAttestationV1 attestation = CredentialEquivalenceAttestationV1.signed(
                profile.ref(),
                generation,
                Bytes.sha256(reference),
                semantic.credentialAuthorizationScopeDigest(),
                bytes(32, 5 + (int) generation),
                1,
                Bytes.utf8("verifier"),
                evidence(1_000),
                10_000,
                bytes(32, 7 + (int) generation),
                1,
                keyPair.getPrivate());
        return CredentialBindingV1.create(profile.ref(), generation, reference, attestation);
    }

    private static String profileKey(final String prefix, final ProfileSemanticEnvelopeV1 profile) {
        return prefix + "/profile/" + profile.profileKind().wireValue() + "/"
                + Bytes.hex(profile.ref().profileId()) + "/"
                + Long.toUnsignedString(profile.ref().version())
                + "/profile";
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
            ProfileSemanticEnvelopeV1 profile, CredentialBindingV1 binding, byte[] fingerprint, KeyPair keyPair) {
        @Override
        public byte[] fingerprint() {
            return Bytes.copy(fingerprint);
        }
    }

    private static final class FakeRecordClient implements OxiaSyncProfileCatalogBackend.RecordClient {
        private final Map<String, GetResult> records = new HashMap<>();
        private long nextVersion = 1;
        private boolean failNextPutAfterCommit;
        private Runnable afterPut = () -> {};

        @Override
        public GetResult get(final String key) {
            return records.get(key);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            final GetResult current = records.get(key);
            final OptionVersionId condition = options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .findFirst()
                    .orElse(null);
            if (condition != null && condition.versionId() == OptionVersionId.KEY_NOT_EXISTS && current != null) {
                throw new KeyAlreadyExistsException(key);
            }
            if (condition != null
                    && condition.versionId() != OptionVersionId.KEY_NOT_EXISTS
                    && (current == null || current.version().versionId() != condition.versionId())) {
                throw new UnexpectedVersionIdException(
                        key,
                        current == null
                                ? OptionVersionId.KEY_NOT_EXISTS
                                : current.version().versionId());
            }
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            records.put(key, new GetResult(key, Bytes.copy(value), version));
            afterPut.run();
            if (failNextPutAfterCommit) {
                failNextPutAfterCommit = false;
                throw new IllegalStateException("simulated response loss");
            }
            return new PutResult(key, version);
        }

        private void putRaw(final String key, final byte[] value) {
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            records.put(key, new GetResult(key, Bytes.copy(value), version));
        }
    }
}
