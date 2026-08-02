package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ControlReasonKindV1;
import io.nereusstream.delay.protocol.ControlReasonV1;
import io.nereusstream.delay.protocol.CredentialBindingV1;
import io.nereusstream.delay.protocol.CredentialEquivalenceAttestationV1;
import io.nereusstream.delay.protocol.DestinationProfileSemanticV1;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.PublishDestinationProfileRequestV1;
import io.nereusstream.delay.protocol.RotateEquivalentSecretRequestV1;
import io.nereusstream.delay.protocol.DeprecateDestinationProfileRequestV1;
import io.nereusstream.delay.protocol.TargetPartitionHashInputV1;
import io.nereusstream.delay.protocol.TargetPartitionPolicyV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProfileCatalogTest {
    @Test
    void publishesExactProfileBindingHeadAndProtectionAtomically() throws Exception {
        final ProfileSemanticEnvelopeV1 profile = destinationEnvelope(1, 2);
        final byte[] reference = Bytes.utf8("provider://credential/v1");
        final CredentialBindingV1 binding = binding(profile, 1, reference, ed25519());
        final InMemoryProfileCatalog catalog = new InMemoryProfileCatalog();

        catalog.publish(new PublishDestinationProfileRequestV1(profile, binding));

        assertEquals(profile, catalog.resolve(profile.ref()));
        assertEquals(binding, catalog.resolveBinding(profile.ref(), 1));
        assertEquals(1, catalog.resolveHead(profile.ref()).secretGeneration());
        assertEquals(1, catalog.resolveHead(profile.ref()).headRevision());
        assertEquals(binding, catalog.resolveBinding(profile.ref(), 1));
        assertEquals(1, catalog.resolveProtection(profile.ref(), 1).protectionRevision());
        assertEquals(1, catalog.size());
        assertEquals(1, catalog.bindingCount());

        catalog.publish(new PublishDestinationProfileRequestV1(profile, binding));
        assertEquals(1, catalog.bindingCount());
    }

    @Test
    void rotatesWithCheckedHeadCasAndMakesRetryIdempotent() throws Exception {
        final ProfileSemanticEnvelopeV1 profile = destinationEnvelope(2, 3);
        final KeyPair keyPair = ed25519();
        final byte[] first = Bytes.utf8("provider://credential/v1");
        final CredentialBindingV1 initial = binding(profile, 1, first, keyPair);
        final InMemoryProfileCatalog catalog = new InMemoryProfileCatalog();
        catalog.publish(profile, initial);
        final byte[] next = Bytes.utf8("provider://credential/v2");
        final CredentialEquivalenceAttestationV1 attestation = attestation(profile, 2, next, keyPair);
        final RotateEquivalentSecretRequestV1 request = new RotateEquivalentSecretRequestV1(profile.ref(), 1, 2,
                next, Bytes.sha256(next), attestation, initial.bindingDigest(), 1);

        final var head = catalog.rotate(request);
        assertEquals(2, head.secretGeneration());
        assertEquals(2, head.headRevision());
        assertEquals(request.newBinding(), catalog.resolveBinding(profile.ref(), 2));
        assertEquals(head, catalog.rotate(request));
        assertEquals(2, catalog.bindingCount());
        assertThrows(IllegalStateException.class, () -> catalog.rotate(
                new RotateEquivalentSecretRequestV1(profile.ref(), 1, 2, next, Bytes.sha256(next), attestation,
                        Bytes.sha256(Bytes.utf8("wrong-head")), 1)));
    }

    @Test
    void keepsDeprecationSeparateFromSourceOrderedFirstBindingMarkers() throws Exception {
        final ProfileSemanticEnvelopeV1 profile = destinationEnvelope(3, 4);
        final CredentialBindingV1 binding = binding(profile, 1, Bytes.utf8("provider://credential/v1"),
                ed25519());
        final InMemoryProfileCatalog catalog = new InMemoryProfileCatalog();
        catalog.publish(profile, binding);
        final ControlReasonV1 reason = new ControlReasonV1(ControlReasonKindV1.OPERATOR_REQUEST,
                Bytes.sha256(Bytes.utf8("ticket")), null);
        catalog.deprecate(new DeprecateDestinationProfileRequestV1(profile.ref(), reason));
        catalog.deprecate(new DeprecateDestinationProfileRequestV1(profile.ref(), reason));
        assertTrue(catalog.isDeprecated(profile.ref()));
        assertEquals(reason, catalog.deprecationReason(profile.ref()));
        assertEquals(profile, catalog.resolve(profile.ref()));
        assertThrows(IllegalStateException.class, () -> catalog.deprecate(
                new DeprecateDestinationProfileRequestV1(profile.ref(),
                        new ControlReasonV1(ControlReasonKindV1.INCIDENT, null, null))));
    }

    @Test
    void rejectsReferenceCollisionsAndNeverResolvesUnknownProfiles() throws Exception {
        final ProfileSemanticEnvelopeV1 first = destinationEnvelope(4, 5);
        final CredentialBindingV1 binding = binding(first, 1, Bytes.utf8("provider://credential/v1"),
                ed25519());
        final InMemoryProfileCatalog catalog = new InMemoryProfileCatalog();
        catalog.publish(first, binding);
        final ProfileSemanticEnvelopeV1 conflicting = destinationEnvelope(4, 6);
        assertThrows(IllegalStateException.class, () -> catalog.publish(conflicting,
                bindingFor(conflicting, 1, Bytes.utf8("provider://credential/v1"))));
        assertNull(catalog.resolve(new ProfileRefV1(Bytes.utf8("missing"), 1, bytes(32, 8),
                ProfileKindV1.DESTINATION)));
        assertNull(catalog.resolveBinding(first.ref(), 2));
        assertFalse(catalog.isDeprecated(new ProfileRefV1(Bytes.utf8("missing"), 1, bytes(32, 8),
                ProfileKindV1.DESTINATION)));
    }

    private static CredentialBindingV1 binding(final ProfileSemanticEnvelopeV1 profile, final long generation,
                                               final byte[] reference, final KeyPair keyPair) {
        return CredentialBindingV1.create(profile.ref(), generation, reference,
                attestation(profile, generation, reference, keyPair));
    }

    private static CredentialBindingV1 bindingFor(final ProfileSemanticEnvelopeV1 profile, final long generation,
                                                  final byte[] reference) throws Exception {
        return binding(profile, generation, reference, ed25519());
    }

    private static CredentialEquivalenceAttestationV1 attestation(final ProfileSemanticEnvelopeV1 profile,
                                                                  final long generation,
                                                                  final byte[] reference,
                                                                  final KeyPair keyPair) {
        return CredentialEquivalenceAttestationV1.signed(profile.ref(), generation, Bytes.sha256(reference),
                ((DestinationProfileSemanticV1) profile.body()).credentialAuthorizationScopeDigest(),
                bytes(32, 11), 1, Bytes.utf8("verifier"), trustedTime(), 1_500,
                bytes(32, 12), 1, keyPair.getPrivate());
    }

    private static ProfileSemanticEnvelopeV1 destinationEnvelope(final int version, final int seed) {
        final ProfileRefV1 capability = new ProfileRefV1(Bytes.utf8("capability"), 1, bytes(32, 1),
                ProfileKindV1.DELIVERY_CAPABILITY);
        final DestinationProfileSemanticV1 body = new DestinationProfileSemanticV1(
                AdapterKindV1.KAFKA,
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster", UUID.randomUUID())),
                2, TargetPartitionPolicyV1.EXPLICIT_OR_HASH, TargetPartitionHashInputV1.ORDERING_KEY,
                List.of(0), capability, 1, 0, 0, bytes(32, seed), 1_000, 128, 512, 1,
                Bytes.utf8("destination"), 0, 0, 1, bytes(32, seed + 1));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.DESTINATION, Bytes.utf8("destination"), version, body);
    }

    private static TrustedUtcIntervalEvidence trustedTime() {
        return new TrustedUtcIntervalEvidence(1_000, 1_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 2, 3,
                bytes(32, 13), 0, new byte[0]);
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
