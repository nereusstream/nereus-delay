package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProfileSemanticEnvelopeV1Test {
    @Test
    void roundTripsAllProfileBodyBranchesAndBindsSemanticHash() {
        final ProfileRefV1 capabilityRef = new ProfileRefV1(Bytes.utf8("capability"), 2,
                bytes(32, 1), ProfileKindV1.DELIVERY_CAPABILITY);
        final DestinationProfileSemanticV1 destination = new DestinationProfileSemanticV1(
                AdapterKindV1.KAFKA,
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster", UUID.randomUUID())),
                4, TargetPartitionPolicyV1.EXPLICIT_OR_HASH, TargetPartitionHashInputV1.ORDERING_KEY,
                List.of(1, 3), capabilityRef, 0x03, 0, 0, bytes(32, 2), 1_000, 128, 512, 8,
                Bytes.utf8("safe-destination"), 10_000, 20_000, 1, bytes(32, 3));
        final ProfileSemanticEnvelopeV1 destinationEnvelope = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.DESTINATION, Bytes.utf8("destination"), 7, destination);
        assertEquals(destinationEnvelope, ProfileSemanticEnvelopeV1.decode(destinationEnvelope.canonicalBytes()));
        assertEquals(destinationEnvelope.ref(), ProfileRefV1.decode(destinationEnvelope.ref().canonicalBytes()));
        assertArrayEquals(destinationEnvelope.semanticHash(), destinationEnvelope.ref().semanticHash());

        final DeliveryCapabilitySemanticV1 capability = new DeliveryCapabilitySemanticV1(
                AdapterKindV1.KAFKA, OutcomeCapabilityV1.AT_LEAST_ONCE, TimingCapabilityV1.ORDINARY_MANAGED,
                null, 0, 0, 0, 0, bytes(32, 4), bytes(32, 5), 0, 0);
        final ProfileSemanticEnvelopeV1 capabilityEnvelope = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.DELIVERY_CAPABILITY, Bytes.utf8("capability"), 2, capability);
        assertEquals(capabilityEnvelope, ProfileSemanticEnvelopeV1.decode(capabilityEnvelope.canonicalBytes()));

        final ObjectStoreProfileSemanticV1 objectStore = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3, bytes(32, 6), bytes(32, 7), 1,
                true, true, true, true, bytes(32, 8), 10_000, 0x03, 1, bytes(32, 9));
        final ProfileSemanticEnvelopeV1 objectStoreEnvelope = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.OBJECT_STORE, Bytes.utf8("objects"), 1, objectStore);
        assertEquals(objectStoreEnvelope, ProfileSemanticEnvelopeV1.decode(objectStoreEnvelope.canonicalBytes()));

        final EvidenceVerifierProfileSemanticV1 verifier = new EvidenceVerifierProfileSemanticV1(
                1, 3, bytes(32, 10), bytes(32, 11), 100, 200, bytes(32, 12));
        final ProfileSemanticEnvelopeV1 verifierEnvelope = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.EVIDENCE_VERIFIER, Bytes.utf8("verifier"), 1, verifier);
        assertEquals(verifierEnvelope, ProfileSemanticEnvelopeV1.decode(verifierEnvelope.canonicalBytes()));
    }

    @Test
    void rejectsProfileBodyAndPartitionSafetyViolations() {
        final ProfileRefV1 capabilityRef = new ProfileRefV1(Bytes.utf8("capability"), 1,
                bytes(32, 1), ProfileKindV1.DELIVERY_CAPABILITY);
        final BrokerResourceIdentityV1 target = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("cluster", UUID.randomUUID()));
        assertThrows(IllegalArgumentException.class, () -> new DestinationProfileSemanticV1(
                AdapterKindV1.KAFKA, target, 4, TargetPartitionPolicyV1.EXPLICIT_ONLY,
                TargetPartitionHashInputV1.ORDERING_KEY, List.of(), capabilityRef, 1, 0, 0,
                bytes(32, 2), 1, 1, 1, 1, Bytes.utf8("alias"), 0, 0, 1, bytes(32, 3)));
        assertThrows(IllegalArgumentException.class, () -> new DestinationProfileSemanticV1(
                AdapterKindV1.KAFKA, target, 4, TargetPartitionPolicyV1.HASH_ONLY,
                TargetPartitionHashInputV1.ORDERING_KEY, List.of(1), capabilityRef, 1, 0, 0,
                bytes(32, 2), 1, 1, 1, 1, Bytes.utf8("alias"), 0, 0, 1, bytes(32, 3)));
        assertThrows(IllegalArgumentException.class, () -> new DestinationProfileSemanticV1(
                AdapterKindV1.KAFKA, target, 4, TargetPartitionPolicyV1.HASH_ONLY,
                TargetPartitionHashInputV1.ORDERING_KEY, List.of(), capabilityRef, 1, 1, 0,
                bytes(32, 2), 1, 1, 1, 1, Bytes.utf8("alias"), 0, 0, 1, bytes(32, 3)));
        assertThrows(IllegalArgumentException.class, () -> new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3, bytes(32, 1), bytes(32, 2), 1,
                false, true, true, true, bytes(32, 3), 1, 1, 1, bytes(32, 4)));
        assertThrows(IllegalArgumentException.class, () -> new ProfileSemanticEnvelopeV1(
                ProfileKindV1.DESTINATION, Bytes.utf8("destination"), 1,
                new DeliveryCapabilitySemanticV1(AdapterKindV1.KAFKA, OutcomeCapabilityV1.AT_LEAST_ONCE,
                        TimingCapabilityV1.ORDINARY_MANAGED, null, 0, 0, 0, 0,
                        bytes(32, 5), bytes(32, 6), 0, 0)));
    }

    @Test
    void rejectsTamperedEnvelopeHashAndUnknownBranch() {
        final DeliveryCapabilitySemanticV1 capability = new DeliveryCapabilitySemanticV1(
                AdapterKindV1.KAFKA, OutcomeCapabilityV1.AT_LEAST_ONCE, TimingCapabilityV1.ORDINARY_MANAGED,
                null, 0, 0, 0, 0, bytes(32, 1), bytes(32, 2), 0, 0);
        final ProfileSemanticEnvelopeV1 envelope = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.DELIVERY_CAPABILITY, Bytes.utf8("capability"), 1, capability);
        final byte[] tampered = envelope.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ProfileSemanticEnvelopeV1.decode(tampered));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
