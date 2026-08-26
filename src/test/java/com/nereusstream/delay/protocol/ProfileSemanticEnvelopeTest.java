package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ProfileSemanticEnvelopeTest {
    @Test
    void roundTripsAllProfileBodyBranchesAndBindsSemanticHash() {
        final ProfileRef capabilityRef =
                new ProfileRef(Bytes.utf8("capability"), 2, bytes(32, 1), ProfileKind.DELIVERY_CAPABILITY);
        final DestinationProfileSemantic destination = new DestinationProfileSemantic(
                AdapterKind.KAFKA,
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", UUID.randomUUID())),
                4,
                TargetPartitionPolicy.EXPLICIT_OR_HASH,
                TargetPartitionHashInput.ORDERING_KEY,
                List.of(1, 3),
                capabilityRef,
                0x03,
                0,
                0,
                bytes(32, 2),
                1_000,
                128,
                512,
                8,
                Bytes.utf8("safe-destination"),
                10_000,
                20_000,
                1,
                bytes(32, 3));
        final ProfileSemanticEnvelope destinationEnvelope =
                new ProfileSemanticEnvelope(ProfileKind.DESTINATION, Bytes.utf8("destination"), 7, destination);
        assertEquals(destinationEnvelope, ProfileSemanticEnvelope.decode(destinationEnvelope.canonicalBytes()));
        assertEquals(
                destinationEnvelope.ref(),
                ProfileRef.decode(destinationEnvelope.ref().canonicalBytes()));
        assertArrayEquals(
                destinationEnvelope.semanticHash(), destinationEnvelope.ref().semanticHash());

        final DeliveryCapabilitySemantic capability = new DeliveryCapabilitySemantic(
                AdapterKind.KAFKA,
                OutcomeCapability.AT_LEAST_ONCE,
                TimingCapability.ORDINARY_MANAGED,
                null,
                0,
                0,
                0,
                0,
                bytes(32, 4),
                bytes(32, 5),
                0,
                0);
        final ProfileSemanticEnvelope capabilityEnvelope =
                new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("capability"), 2, capability);
        assertEquals(capabilityEnvelope, ProfileSemanticEnvelope.decode(capabilityEnvelope.canonicalBytes()));

        final ObjectStoreProfileSemantic objectStore = new ObjectStoreProfileSemantic(
                ObjectStoreProviderKind.S3,
                bytes(32, 6),
                bytes(32, 7),
                1,
                true,
                true,
                true,
                true,
                bytes(32, 8),
                10_000,
                0x03,
                1,
                bytes(32, 9));
        final ProfileSemanticEnvelope objectStoreEnvelope =
                new ProfileSemanticEnvelope(ProfileKind.OBJECT_STORE, Bytes.utf8("objects"), 1, objectStore);
        assertEquals(objectStoreEnvelope, ProfileSemanticEnvelope.decode(objectStoreEnvelope.canonicalBytes()));

        final EvidenceVerifierProfileSemantic verifier =
                new EvidenceVerifierProfileSemantic(1, 3, bytes(32, 10), bytes(32, 11), 100, 200, bytes(32, 12));
        final ProfileSemanticEnvelope verifierEnvelope =
                new ProfileSemanticEnvelope(ProfileKind.EVIDENCE_VERIFIER, Bytes.utf8("verifier"), 1, verifier);
        assertEquals(verifierEnvelope, ProfileSemanticEnvelope.decode(verifierEnvelope.canonicalBytes()));
    }

    @Test
    void preservesUnsignedTargetPartitionFields() {
        final ProfileRef capabilityRef =
                new ProfileRef(Bytes.utf8("unsigned-capability"), 1, bytes(32, 20), ProfileKind.DELIVERY_CAPABILITY);
        final DestinationProfileSemantic destination = new DestinationProfileSemantic(
                AdapterKind.KAFKA,
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", UUID.randomUUID())),
                -1,
                TargetPartitionPolicy.EXPLICIT_OR_HASH,
                TargetPartitionHashInput.ORDERING_KEY,
                List.of(Integer.MIN_VALUE, -2),
                capabilityRef,
                1,
                0,
                0,
                bytes(32, 21),
                1,
                1,
                1,
                1,
                Bytes.utf8("unsigned-destination"),
                0,
                0,
                1,
                bytes(32, 22));

        assertEquals(destination, DestinationProfileSemantic.decode(destination.canonicalBytes()));
        assertEquals(0xffff_ffffL, Integer.toUnsignedLong(destination.targetPartitionCount()));
        assertEquals(List.of(Integer.MIN_VALUE, -2), destination.allowedExplicitPartitions());
    }

    @Test
    void preservesCompleteUnsignedProfileVersionBits() {
        final DeliveryCapabilitySemantic capability = new DeliveryCapabilitySemantic(
                AdapterKind.KAFKA,
                OutcomeCapability.AT_LEAST_ONCE,
                TimingCapability.ORDINARY_MANAGED,
                null,
                0,
                0,
                0,
                0,
                bytes(32, 23),
                bytes(32, 24),
                0,
                0);
        final ProfileSemanticEnvelope envelope = new ProfileSemanticEnvelope(
                ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("unsigned-profile"), Long.MIN_VALUE, capability);

        final ProfileSemanticEnvelope decoded = ProfileSemanticEnvelope.decode(envelope.canonicalBytes());

        assertEquals(Long.MIN_VALUE, decoded.version());
        assertEquals(Long.MIN_VALUE, decoded.ref().version());
        assertEquals(envelope, decoded);
        assertEquals(decoded.ref(), ProfileRef.decode(decoded.ref().canonicalBytes()));
    }

    @Test
    void rejectsProfileBodyAndPartitionSafetyViolations() {
        final ProfileRef capabilityRef =
                new ProfileRef(Bytes.utf8("capability"), 1, bytes(32, 1), ProfileKind.DELIVERY_CAPABILITY);
        final BrokerResourceIdentity target =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", UUID.randomUUID()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DestinationProfileSemantic(
                        AdapterKind.KAFKA,
                        target,
                        4,
                        TargetPartitionPolicy.EXPLICIT_ONLY,
                        TargetPartitionHashInput.ORDERING_KEY,
                        List.of(),
                        capabilityRef,
                        1,
                        0,
                        0,
                        bytes(32, 2),
                        1,
                        1,
                        1,
                        1,
                        Bytes.utf8("alias"),
                        0,
                        0,
                        1,
                        bytes(32, 3)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DestinationProfileSemantic(
                        AdapterKind.KAFKA,
                        target,
                        4,
                        TargetPartitionPolicy.HASH_ONLY,
                        TargetPartitionHashInput.ORDERING_KEY,
                        List.of(1),
                        capabilityRef,
                        1,
                        0,
                        0,
                        bytes(32, 2),
                        1,
                        1,
                        1,
                        1,
                        Bytes.utf8("alias"),
                        0,
                        0,
                        1,
                        bytes(32, 3)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DestinationProfileSemantic(
                        AdapterKind.KAFKA,
                        target,
                        4,
                        TargetPartitionPolicy.HASH_ONLY,
                        TargetPartitionHashInput.ORDERING_KEY,
                        List.of(),
                        capabilityRef,
                        1,
                        1,
                        0,
                        bytes(32, 2),
                        1,
                        1,
                        1,
                        1,
                        Bytes.utf8("alias"),
                        0,
                        0,
                        1,
                        bytes(32, 3)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ObjectStoreProfileSemantic(
                        ObjectStoreProviderKind.S3,
                        bytes(32, 1),
                        bytes(32, 2),
                        1,
                        false,
                        true,
                        true,
                        true,
                        bytes(32, 3),
                        1,
                        1,
                        1,
                        bytes(32, 4)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ProfileSemanticEnvelope(
                        ProfileKind.DESTINATION,
                        Bytes.utf8("destination"),
                        1,
                        new DeliveryCapabilitySemantic(
                                AdapterKind.KAFKA,
                                OutcomeCapability.AT_LEAST_ONCE,
                                TimingCapability.ORDINARY_MANAGED,
                                null,
                                0,
                                0,
                                0,
                                0,
                                bytes(32, 5),
                                bytes(32, 6),
                                0,
                                0)));
    }

    @Test
    void rejectsTamperedEnvelopeHashAndUnknownBranch() {
        final DeliveryCapabilitySemantic capability = new DeliveryCapabilitySemantic(
                AdapterKind.KAFKA,
                OutcomeCapability.AT_LEAST_ONCE,
                TimingCapability.ORDINARY_MANAGED,
                null,
                0,
                0,
                0,
                0,
                bytes(32, 1),
                bytes(32, 2),
                0,
                0);
        final ProfileSemanticEnvelope envelope =
                new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("capability"), 1, capability);
        final byte[] tampered = envelope.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ProfileSemanticEnvelope.decode(tampered));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
