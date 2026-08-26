package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ProfileControlRequestTest {
    @Test
    void roundTripsPublishDeprecateAndRotateRequests() throws Exception {
        final ProfileSemanticEnvelope profile = destinationEnvelope();
        final ProfileRef reference = profile.ref();
        final byte[] firstSecret = Bytes.utf8("provider://credential/initial");
        final KeyPair keyPair = ed25519();
        final CredentialEquivalenceAttestation firstAttestation = attestation(reference, 1, firstSecret, keyPair);
        final CredentialBinding firstBinding = CredentialBinding.create(reference, 1, firstSecret, firstAttestation);

        final PublishDestinationProfileRequest publish = new PublishDestinationProfileRequest(profile, firstBinding);
        assertEquals(publish, PublishDestinationProfileRequest.decode(publish.canonicalBytes()));

        final DeprecateDestinationProfileRequest deprecate = new DeprecateDestinationProfileRequest(
                reference, new ControlReason(ControlReasonKind.OPERATOR_REQUEST, bytes(32, 20), null));
        assertEquals(deprecate, DeprecateDestinationProfileRequest.decode(deprecate.canonicalBytes()));

        final byte[] nextSecret = Bytes.utf8("provider://credential/current");
        final CredentialEquivalenceAttestation nextAttestation = attestation(reference, 2, nextSecret, keyPair);
        final RotateEquivalentSecretRequest rotate = new RotateEquivalentSecretRequest(
                reference,
                1,
                2,
                nextSecret,
                Bytes.sha256(nextSecret),
                nextAttestation,
                firstBinding.bindingDigest(),
                Long.MIN_VALUE);
        assertEquals(rotate, RotateEquivalentSecretRequest.decode(rotate.canonicalBytes()));
        assertEquals(Long.MIN_VALUE, rotate.expectedBindingHeadRevision());
        assertEquals(2, rotate.newBinding().secretGeneration());
    }

    @Test
    void rejectsWrongProfileBindingGenerationRotationAndTampering() throws Exception {
        final ProfileSemanticEnvelope profile = destinationEnvelope();
        final ProfileRef reference = profile.ref();
        final KeyPair keyPair = ed25519();
        final byte[] secret = Bytes.utf8("provider://credential/initial");
        final CredentialEquivalenceAttestation attestation = attestation(reference, 1, secret, keyPair);
        final CredentialBinding binding = CredentialBinding.create(reference, 1, secret, attestation);

        assertThrows(
                IllegalArgumentException.class,
                () -> new PublishDestinationProfileRequest(
                        profile,
                        new CredentialBinding(
                                reference,
                                2,
                                secret,
                                Bytes.sha256(secret),
                                attestation,
                                1,
                                Bytes.sha256(Bytes.utf8("wrong")))));
        assertThrows(
                IllegalArgumentException.class,
                () -> new RotateEquivalentSecretRequest(
                        reference,
                        1,
                        3,
                        Bytes.utf8("provider://credential/v3"),
                        Bytes.sha256(Bytes.utf8("provider://credential/v3")),
                        attestation(reference, 2, Bytes.utf8("provider://credential/v3"), keyPair),
                        binding.bindingDigest(),
                        1));

        final RotateEquivalentSecretRequest rotate = new RotateEquivalentSecretRequest(
                reference,
                1,
                2,
                Bytes.utf8("provider://credential/current"),
                Bytes.sha256(Bytes.utf8("provider://credential/current")),
                attestation(reference, 2, Bytes.utf8("provider://credential/current"), keyPair),
                binding.bindingDigest(),
                1);
        final byte[] tampered = rotate.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> RotateEquivalentSecretRequest.decode(tampered));
    }

    @Test
    void preservesHighBitSecretGenerationsAcrossRotation() throws Exception {
        final ProfileRef reference = destinationEnvelope().ref();
        final KeyPair keyPair = ed25519();
        final long expectedGeneration = Long.MIN_VALUE;
        final long newGeneration = expectedGeneration + 1;
        final byte[] nextSecret = Bytes.utf8("provider://credential/high-bit");
        final CredentialEquivalenceAttestation nextAttestation =
                attestation(reference, newGeneration, nextSecret, keyPair);

        final RotateEquivalentSecretRequest rotate = new RotateEquivalentSecretRequest(
                reference,
                expectedGeneration,
                newGeneration,
                nextSecret,
                Bytes.sha256(nextSecret),
                nextAttestation,
                bytes(32, 21),
                4);

        final RotateEquivalentSecretRequest decoded = RotateEquivalentSecretRequest.decode(rotate.canonicalBytes());
        assertEquals(expectedGeneration, decoded.expectedSecretGeneration());
        assertEquals(newGeneration, decoded.newSecretGeneration());
        assertEquals(newGeneration, decoded.newBinding().secretGeneration());
    }

    private static ProfileSemanticEnvelope destinationEnvelope() {
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
                bytes(32, 2),
                1_000,
                128,
                512,
                1,
                Bytes.utf8("destination"),
                0,
                0,
                1,
                bytes(32, 3));
        return new ProfileSemanticEnvelope(ProfileKind.DESTINATION, Bytes.utf8("destination"), 1, body);
    }

    private static CredentialEquivalenceAttestation attestation(
            final ProfileRef profile, final long generation, final byte[] secretReference, final KeyPair keyPair) {
        return CredentialEquivalenceAttestation.signed(
                profile,
                generation,
                Bytes.sha256(secretReference),
                bytes(32, 2),
                bytes(32, 4),
                1,
                Bytes.utf8("verifier"),
                trustedTime(),
                1_500,
                bytes(32, 5),
                1,
                keyPair.getPrivate());
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
                bytes(32, 6),
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
