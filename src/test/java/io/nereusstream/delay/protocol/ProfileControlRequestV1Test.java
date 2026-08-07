package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProfileControlRequestV1Test {
    @Test
    void roundTripsPublishDeprecateAndRotateRequests() throws Exception {
        final ProfileSemanticEnvelopeV1 profile = destinationEnvelope();
        final ProfileRefV1 reference = profile.ref();
        final byte[] firstSecret = Bytes.utf8("provider://credential/v1");
        final KeyPair keyPair = ed25519();
        final CredentialEquivalenceAttestationV1 firstAttestation = attestation(reference, 1, firstSecret,
                keyPair);
        final CredentialBindingV1 firstBinding = CredentialBindingV1.create(reference, 1, firstSecret,
                firstAttestation);

        final PublishDestinationProfileRequestV1 publish = new PublishDestinationProfileRequestV1(profile,
                firstBinding);
        assertEquals(publish, PublishDestinationProfileRequestV1.decode(publish.canonicalBytes()));

        final DeprecateDestinationProfileRequestV1 deprecate = new DeprecateDestinationProfileRequestV1(reference,
                new ControlReasonV1(ControlReasonKindV1.OPERATOR_REQUEST, bytes(32, 20), null));
        assertEquals(deprecate, DeprecateDestinationProfileRequestV1.decode(deprecate.canonicalBytes()));

        final byte[] nextSecret = Bytes.utf8("provider://credential/v2");
        final CredentialEquivalenceAttestationV1 nextAttestation = attestation(reference, 2, nextSecret, keyPair);
        final RotateEquivalentSecretRequestV1 rotate = new RotateEquivalentSecretRequestV1(reference, 1, 2,
                nextSecret, Bytes.sha256(nextSecret), nextAttestation, firstBinding.bindingDigest(), 4);
        assertEquals(rotate, RotateEquivalentSecretRequestV1.decode(rotate.canonicalBytes()));
        assertEquals(2, rotate.newBinding().secretGeneration());
    }

    @Test
    void rejectsWrongProfileBindingGenerationRotationAndTampering() throws Exception {
        final ProfileSemanticEnvelopeV1 profile = destinationEnvelope();
        final ProfileRefV1 reference = profile.ref();
        final KeyPair keyPair = ed25519();
        final byte[] secret = Bytes.utf8("provider://credential/v1");
        final CredentialEquivalenceAttestationV1 attestation = attestation(reference, 1, secret, keyPair);
        final CredentialBindingV1 binding = CredentialBindingV1.create(reference, 1, secret, attestation);

        assertThrows(IllegalArgumentException.class, () -> new PublishDestinationProfileRequestV1(profile,
                new CredentialBindingV1(reference, 2, secret, Bytes.sha256(secret), attestation,
                        1, Bytes.sha256(Bytes.utf8("wrong")))));
        assertThrows(IllegalArgumentException.class, () -> new RotateEquivalentSecretRequestV1(reference, 1, 3,
                Bytes.utf8("provider://credential/v3"), Bytes.sha256(Bytes.utf8("provider://credential/v3")),
                attestation(reference, 2, Bytes.utf8("provider://credential/v3"), keyPair), binding.bindingDigest(), 1));

        final RotateEquivalentSecretRequestV1 rotate = new RotateEquivalentSecretRequestV1(reference, 1, 2,
                Bytes.utf8("provider://credential/v2"), Bytes.sha256(Bytes.utf8("provider://credential/v2")),
                attestation(reference, 2, Bytes.utf8("provider://credential/v2"), keyPair), binding.bindingDigest(), 1);
        final byte[] tampered = rotate.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> RotateEquivalentSecretRequestV1.decode(tampered));
    }

    @Test
    void preservesHighBitSecretGenerationsAcrossRotation() throws Exception {
        final ProfileRefV1 reference = destinationEnvelope().ref();
        final KeyPair keyPair = ed25519();
        final long expectedGeneration = Long.MIN_VALUE;
        final long newGeneration = expectedGeneration + 1;
        final byte[] nextSecret = Bytes.utf8("provider://credential/high-bit-v2");
        final CredentialEquivalenceAttestationV1 nextAttestation = attestation(reference, newGeneration,
                nextSecret, keyPair);

        final RotateEquivalentSecretRequestV1 rotate = new RotateEquivalentSecretRequestV1(reference,
                expectedGeneration, newGeneration, nextSecret, Bytes.sha256(nextSecret), nextAttestation,
                bytes(32, 21), 4);

        final RotateEquivalentSecretRequestV1 decoded = RotateEquivalentSecretRequestV1.decode(rotate.canonicalBytes());
        assertEquals(expectedGeneration, decoded.expectedSecretGeneration());
        assertEquals(newGeneration, decoded.newSecretGeneration());
        assertEquals(newGeneration, decoded.newBinding().secretGeneration());
    }

    private static ProfileSemanticEnvelopeV1 destinationEnvelope() {
        final ProfileRefV1 capability = new ProfileRefV1(Bytes.utf8("capability"), 1, bytes(32, 1),
                ProfileKindV1.DELIVERY_CAPABILITY);
        final DestinationProfileSemanticV1 body = new DestinationProfileSemanticV1(
                AdapterKindV1.KAFKA,
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster", UUID.randomUUID())),
                2, TargetPartitionPolicyV1.EXPLICIT_OR_HASH, TargetPartitionHashInputV1.ORDERING_KEY,
                List.of(0), capability, 1, 0, 0, bytes(32, 2), 1_000, 128, 512, 1,
                Bytes.utf8("destination"), 0, 0, 1, bytes(32, 3));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.DESTINATION, Bytes.utf8("destination"), 1, body);
    }

    private static CredentialEquivalenceAttestationV1 attestation(final ProfileRefV1 profile,
                                                                  final long generation,
                                                                  final byte[] secretReference,
                                                                  final KeyPair keyPair) {
        return CredentialEquivalenceAttestationV1.signed(profile, generation, Bytes.sha256(secretReference),
                bytes(32, 2), bytes(32, 4), 1, Bytes.utf8("verifier"), trustedTime(), 1_500, bytes(32, 5), 1,
                keyPair.getPrivate());
    }

    private static TrustedUtcIntervalEvidence trustedTime() {
        return new TrustedUtcIntervalEvidence(1_000, 1_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 2, 3,
                bytes(32, 6), 0, new byte[0]);
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
