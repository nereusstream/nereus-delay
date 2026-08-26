package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlReason;
import com.nereusstream.delay.protocol.ControlReasonKind;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialEquivalenceAttestation;
import com.nereusstream.delay.protocol.DeprecateDestinationProfileRequest;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.PublishDestinationProfileRequest;
import com.nereusstream.delay.protocol.RotateEquivalentSecretRequest;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TargetPartitionPolicy;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ProfileCatalogTest {
    @Test
    void publishesExactProfileBindingHeadAndProtectionAtomically() throws Exception {
        final ProfileSemanticEnvelope profile = destinationEnvelope(1, 2);
        final byte[] reference = Bytes.utf8("provider://credential/initial");
        final CredentialBinding binding = binding(profile, 1, reference, ed25519());
        final InMemoryProfileCatalog catalog = new InMemoryProfileCatalog();

        catalog.publish(new PublishDestinationProfileRequest(profile, binding));

        assertEquals(profile, catalog.resolve(profile.ref()));
        assertEquals(binding, catalog.resolveBinding(profile.ref(), 1));
        assertEquals(1, catalog.resolveHead(profile.ref()).secretGeneration());
        assertEquals(1, catalog.resolveHead(profile.ref()).headRevision());
        assertEquals(binding, catalog.resolveBinding(profile.ref(), 1));
        assertEquals(1, catalog.resolveProtection(profile.ref(), 1).protectionRevision());
        assertEquals(1, catalog.size());
        assertEquals(1, catalog.bindingCount());

        catalog.publish(new PublishDestinationProfileRequest(profile, binding));
        assertEquals(1, catalog.bindingCount());
    }

    @Test
    void rotatesWithCheckedHeadCasAndMakesRetryIdempotent() throws Exception {
        final ProfileSemanticEnvelope profile = destinationEnvelope(2, 3);
        final KeyPair keyPair = ed25519();
        final byte[] first = Bytes.utf8("provider://credential/initial");
        final CredentialBinding initial = binding(profile, 1, first, keyPair);
        final InMemoryProfileCatalog catalog = new InMemoryProfileCatalog();
        catalog.publish(profile, initial);
        final byte[] next = Bytes.utf8("provider://credential/current");
        final CredentialEquivalenceAttestation attestation = attestation(profile, 2, next, keyPair);
        final RotateEquivalentSecretRequest request = new RotateEquivalentSecretRequest(
                profile.ref(), 1, 2, next, Bytes.sha256(next), attestation, initial.bindingDigest(), 1);

        final var head = catalog.rotate(request);
        assertEquals(2, head.secretGeneration());
        assertEquals(2, head.headRevision());
        assertEquals(request.newBinding(), catalog.resolveBinding(profile.ref(), 2));
        assertEquals(head, catalog.rotate(request));
        assertEquals(2, catalog.bindingCount());
        assertThrows(
                IllegalStateException.class,
                () -> catalog.rotate(new RotateEquivalentSecretRequest(
                        profile.ref(),
                        1,
                        2,
                        next,
                        Bytes.sha256(next),
                        attestation,
                        Bytes.sha256(Bytes.utf8("wrong-head")),
                        1)));
    }

    @Test
    void keepsDeprecationSeparateFromSourceOrderedFirstBindingMarkers() throws Exception {
        final ProfileSemanticEnvelope profile = destinationEnvelope(3, 4);
        final CredentialBinding binding = binding(profile, 1, Bytes.utf8("provider://credential/initial"), ed25519());
        final InMemoryProfileCatalog catalog = new InMemoryProfileCatalog();
        catalog.publish(profile, binding);
        final ControlReason reason =
                new ControlReason(ControlReasonKind.OPERATOR_REQUEST, Bytes.sha256(Bytes.utf8("ticket")), null);
        catalog.deprecate(new DeprecateDestinationProfileRequest(profile.ref(), reason));
        catalog.deprecate(new DeprecateDestinationProfileRequest(profile.ref(), reason));
        assertTrue(catalog.isDeprecated(profile.ref()));
        assertEquals(reason, catalog.deprecationReason(profile.ref()));
        assertEquals(profile, catalog.resolve(profile.ref()));
        assertThrows(
                IllegalStateException.class,
                () -> catalog.deprecate(new DeprecateDestinationProfileRequest(
                        profile.ref(), new ControlReason(ControlReasonKind.INCIDENT, null, null))));
    }

    @Test
    void rejectsReferenceCollisionsAndNeverResolvesUnknownProfiles() throws Exception {
        final ProfileSemanticEnvelope first = destinationEnvelope(4, 5);
        final CredentialBinding binding = binding(first, 1, Bytes.utf8("provider://credential/initial"), ed25519());
        final InMemoryProfileCatalog catalog = new InMemoryProfileCatalog();
        catalog.publish(first, binding);
        final ProfileSemanticEnvelope conflicting = destinationEnvelope(4, 6);
        assertThrows(
                IllegalStateException.class,
                () -> catalog.publish(
                        conflicting, bindingFor(conflicting, 1, Bytes.utf8("provider://credential/initial"))));
        assertNull(catalog.resolve(new ProfileRef(Bytes.utf8("missing"), 1, bytes(32, 8), ProfileKind.DESTINATION)));
        assertNull(catalog.resolveBinding(first.ref(), 2));
        assertFalse(
                catalog.isDeprecated(new ProfileRef(Bytes.utf8("missing"), 1, bytes(32, 8), ProfileKind.DESTINATION)));
    }

    private static CredentialBinding binding(
            final ProfileSemanticEnvelope profile,
            final long generation,
            final byte[] reference,
            final KeyPair keyPair) {
        return CredentialBinding.create(
                profile.ref(), generation, reference, attestation(profile, generation, reference, keyPair));
    }

    private static CredentialBinding bindingFor(
            final ProfileSemanticEnvelope profile, final long generation, final byte[] reference) throws Exception {
        return binding(profile, generation, reference, ed25519());
    }

    private static CredentialEquivalenceAttestation attestation(
            final ProfileSemanticEnvelope profile,
            final long generation,
            final byte[] reference,
            final KeyPair keyPair) {
        return CredentialEquivalenceAttestation.signed(
                profile.ref(),
                generation,
                Bytes.sha256(reference),
                ((DestinationProfileSemantic) profile.body()).credentialAuthorizationScopeDigest(),
                bytes(32, 11),
                1,
                Bytes.utf8("verifier"),
                trustedTime(),
                1_500,
                bytes(32, 12),
                1,
                keyPair.getPrivate());
    }

    private static ProfileSemanticEnvelope destinationEnvelope(final int version, final int seed) {
        final ProfileRef capability =
                new ProfileRef(Bytes.utf8("capability"), 1, bytes(32, 1), ProfileKind.DELIVERY_CAPABILITY);
        final DestinationProfileSemantic body = new DestinationProfileSemantic(
                AdapterKind.KAFKA,
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", UUID.randomUUID())),
                2,
                TargetPartitionPolicy.EXPLICIT_OR_HASH,
                TargetPartitionHashInput.ORDERING_KEY,
                List.of(0),
                capability,
                1,
                0,
                0,
                bytes(32, seed),
                1_000,
                128,
                512,
                1,
                Bytes.utf8("destination"),
                0,
                0,
                1,
                bytes(32, seed + 1));
        return new ProfileSemanticEnvelope(ProfileKind.DESTINATION, Bytes.utf8("destination"), version, body);
    }

    private static TrustedUtcIntervalEvidence trustedTime() {
        return new TrustedUtcIntervalEvidence(
                1_000,
                1_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock"),
                1,
                2,
                3,
                bytes(32, 13),
                0,
                new byte[0]);
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
