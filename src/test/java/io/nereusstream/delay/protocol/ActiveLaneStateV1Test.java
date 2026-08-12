package io.nereusstream.delay.protocol;

import io.nereusstream.delay.runtime.AdmissionGate;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ActiveLaneStateV1Test {
    @Test
    void roundTripsBlockedStateWithCheckedTupleAndCharge() {
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 1);
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, 2);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final ActiveLaneStateV1 state = new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 1), AdmissionGate.OPEN,
                RuntimeReadiness.BLOCKED, LaneRuntimeBlockReasonV1.CAPABILITY, 3, 4, destination, capability,
                tuple, 5, charge(), 100L, 140L, LaneCircuitStateV1.CLOSED, 0, 2, 130, 140, null, null, null);

        final ActiveLaneStateV1 decoded = ActiveLaneStateV1.decode(state.canonicalBytes());
        assertEquals(state, decoded);
        assertArrayEquals(state.stateDigest(), decoded.stateDigest());
        assertArrayEquals(Bytes.sha256(tuple), decoded.canonicalLaneTupleSha256());
    }

    @Test
    void readyRequiresCertificateAndRejectsReadyKeyDigestTampering() {
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 3);
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, 4);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 2), AdmissionGate.OPEN, RuntimeReadiness.READY,
                null, 1, 1, destination, capability, tuple, 1, charge(), null, null,
                LaneCircuitStateV1.CLOSED, 0, 0, 0, 0, null, null, null));

        final ShardId certificateShard = new ShardId(RouteIncarnation.random(), 0);
        final byte[] certificate = PublishAdmissionBodyTest.Fixture.createForSourceWithLane(certificateShard,
                DelayMessageId.random(certificateShard), bytes(16, 3), Bytes.utf8("timeline"), 1, 1, 0,
                Bytes.sha256(Bytes.utf8("obligations")), Bytes.sha256(Bytes.utf8("semantic")),
                DestinationLaneId.derive(tuple).bytes()).body();
        final byte[] validCertificate = PublishAdmissionBody.decode(certificate).readyCertificate().canonicalBytes();
        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 2), AdmissionGate.OPEN, RuntimeReadiness.READY,
                null, 1, 1, destination, capability, tuple, 1, charge(), null, null,
                LaneCircuitStateV1.CLOSED, 0, 0, 0, 0, Bytes.utf8("ready"), validCertificate, null));
        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 2), AdmissionGate.OPEN, RuntimeReadiness.READY,
                null, 1, 1, destination, capability, tuple, 1, charge(), 100L, 200L,
                LaneCircuitStateV1.CLOSED, 0, 0, 0, 0,
                readyKey(DestinationLaneId.derive(tuple), 200, 1), validCertificate, null));
        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 3), AdmissionGate.ADMIN_PAUSED,
                RuntimeReadiness.READY, null, 1, 1, destination, capability, tuple, 1, charge(), 100L, 200L,
                LaneCircuitStateV1.CLOSED, 0, 0, 0, 0,
                readyKey(DestinationLaneId.derive(tuple), 200, 1), validCertificate, null));
        final ActiveLaneStateV1 state = new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 3), AdmissionGate.OPEN, RuntimeReadiness.READY,
                null, 1, 1, destination, capability, tuple, 1, charge(), 100L, 200L,
                LaneCircuitStateV1.CLOSED, 0, 0, 0, 0,
                readyKey(DestinationLaneId.derive(tuple), 200, 1), validCertificate, null);
        final ActiveLaneStateV1 projected = state.withLocalProjection(
                AdmissionGate.OPEN, RuntimeReadiness.READY, null, 1, 2, 1, charge(), 150L, 250L,
                readyKey(state.laneId(), 250, 2));
        assertEquals(150L, projected.earliestActionAtEpochMs());
        assertEquals(250L, projected.nextEligibleAtEpochMs());
        final byte[] tampered = state.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ActiveLaneStateV1.decode(tampered));
    }

    @Test
    void readyCertificateMayRemainAfterTheCurrentReadyHeadIsConsumed() {
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 17);
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, 18);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final DestinationLaneId laneId = DestinationLaneId.derive(tuple);
        final byte[] laneIncarnation = bytes(16, 20);
        final ShardId certificateShard = new ShardId(RouteIncarnation.random(), 0);
        final byte[] certificate = PublishAdmissionBody.decode(
                PublishAdmissionBodyTest.Fixture.createForSourceWithLane(certificateShard,
                        DelayMessageId.random(certificateShard), laneIncarnation, Bytes.utf8("timeline"), 1, 1, 0,
                        Bytes.sha256(Bytes.utf8("obligations")), Bytes.sha256(Bytes.utf8("semantic")),
                        laneId.bytes()).body()).readyCertificate().canonicalBytes();
        final ActiveLaneStateV1 state = new ActiveLaneStateV1(
                laneId, laneIncarnation, AdmissionGate.OPEN, RuntimeReadiness.READY, null, 1, 1,
                destination, capability, tuple, 1, charge(), 100L, 200L, LaneCircuitStateV1.CLOSED,
                0, 0, 0, 0, readyKey(laneId, 200, 1), certificate, null);

        final ActiveLaneStateV1 afterClaim = state.withLocalProjection(
                AdmissionGate.OPEN, RuntimeReadiness.READY, null, 1, 2, 1, charge(), null, null, null);

        assertEquals(RuntimeReadiness.READY, afterClaim.runtimeReadiness());
        assertNull(afterClaim.earliestActionAtEpochMs());
        assertNull(afterClaim.nextEligibleAtEpochMs());
        assertNull(afterClaim.encodedReadyKey());
        assertArrayEquals(certificate, afterClaim.readyCertificate());
        assertEquals(afterClaim, ActiveLaneStateV1.decode(afterClaim.canonicalBytes()));
    }

    @Test
    void rejectsTupleIdentityAndCircuitInvariantViolations() {
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 5);
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, 6);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                new DestinationLaneId(bytes(32, 7)), bytes(16, 4), AdmissionGate.OPEN,
                RuntimeReadiness.RECOVERING_EVIDENCE, null, 1, 1, destination, capability, tuple, 1, charge(),
                null, null, LaneCircuitStateV1.CLOSED, 0, 0, 0, 0, null, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 5), AdmissionGate.OPEN,
                RuntimeReadiness.RECOVERING_EVIDENCE, null, 1, 1, destination, capability, tuple, 1, charge(),
                null, null, LaneCircuitStateV1.OPEN, 0, 0, 0, 0, null, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 5), AdmissionGate.OPEN,
                RuntimeReadiness.BLOCKED, LaneRuntimeBlockReasonV1.CAPABILITY, 1, 1, destination, capability,
                tuple, 1, charge(), 100L, 200L, LaneCircuitStateV1.OPEN, 300, 0, 0, 0, null, null, null));
    }

    @Test
    void readyKeyMustBeTheExactLaneVersionAndEligibilityProjection() {
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 14);
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, 15);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final DestinationLaneId laneId = DestinationLaneId.derive(tuple);
        final ShardId certificateShard = new ShardId(RouteIncarnation.random(), 0);
        final byte[] certificate = PublishAdmissionBody.decode(
                PublishAdmissionBodyTest.Fixture.createForSourceWithLane(certificateShard,
                        DelayMessageId.random(certificateShard), bytes(16, 16), Bytes.utf8("timeline"), 1, 1, 0,
                        Bytes.sha256(Bytes.utf8("obligations")), Bytes.sha256(Bytes.utf8("semantic")), laneId.bytes())
                        .body()).readyCertificate().canonicalBytes();

        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                laneId, bytes(16, 16), AdmissionGate.OPEN, RuntimeReadiness.READY, null, 1, 7,
                destination, capability, tuple, 1, charge(), 100L, 200L, LaneCircuitStateV1.CLOSED, 0, 0, 0, 0,
                readyKey(laneId, 199, 7), certificate, null));
        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                laneId, bytes(16, 16), AdmissionGate.OPEN, RuntimeReadiness.READY, null, 1, 7,
                destination, capability, tuple, 1, charge(), 100L, 200L, LaneCircuitStateV1.CLOSED, 0, 0, 0, 0,
                readyKey(DestinationLaneId.derive(Bytes.utf8("other-lane")), 200, 7), certificate, null));
    }

    @Test
    void preservesUnsignedLaneVersionWeightAndFailureBits() {
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 7);
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, 8);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final ActiveLaneStateV1 state = new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 6), AdmissionGate.OPEN,
                RuntimeReadiness.BLOCKED, LaneRuntimeBlockReasonV1.CAPABILITY,
                Long.MIN_VALUE, -1L, destination, capability, tuple, Long.MIN_VALUE, charge(),
                null, null, LaneCircuitStateV1.CLOSED, 0, -1L, 0, 0, null, null, null);

        final ActiveLaneStateV1 decoded = ActiveLaneStateV1.decode(state.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.laneControlVersion());
        assertEquals(-1L, decoded.laneVersion());
        assertEquals(Long.MIN_VALUE, decoded.schedulerWeight());
        assertEquals(-1L, decoded.consecutiveFailures());
        assertArrayEquals(state.canonicalBytes(), decoded.canonicalBytes());
    }

    @Test
    void rejectsProfileProjectionDriftInTypedState() {
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 9);
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, 10);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final ProfileRefV1 driftedDestination = new ProfileRefV1(bytes(8, 11), destination.version(),
                destination.semanticHash(), ProfileKindV1.DESTINATION);

        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 7), AdmissionGate.OPEN,
                RuntimeReadiness.BLOCKED, LaneRuntimeBlockReasonV1.CAPABILITY, 1, 1, driftedDestination,
                capability, tuple, 1, charge(), null, null, LaneCircuitStateV1.CLOSED, 0, 0, 0, 0,
                null, null, null));
    }

    @Test
    void rejectsMalformedCanonicalTupleInTypedState() {
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 12);
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, 13);
        final byte[] validTuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final byte[] tupleWithTrailingBytes = Bytes.concat(validTuple, new byte[]{0});

        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                DestinationLaneId.derive(tupleWithTrailingBytes), bytes(16, 8), AdmissionGate.OPEN,
                RuntimeReadiness.BLOCKED, LaneRuntimeBlockReasonV1.CAPABILITY, 1, 1, destination, capability,
                tupleWithTrailingBytes, 1, charge(), null, null, LaneCircuitStateV1.CLOSED, 0, 0, 0, 0,
                null, null, null));
    }

    private static ProfileRefV1 profile(final ProfileKindV1 kind, final int seed) {
        return new ProfileRefV1(bytes(8, seed), 1, bytes(32, seed + 10), kind);
    }

    private static PublishAdmissionBody.ChargeVector charge() {
        return new PublishAdmissionBody.ChargeVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) (seed + i);
        }
        return value;
    }

    private static byte[] readyKey(final DestinationLaneId laneId, final long nextEligibleAtEpochMs,
                                   final long laneVersion) {
        return Bytes.concat(new byte[]{3, 1}, Bytes.u64be(nextEligibleAtEpochMs), laneId.bytes(),
                Bytes.u64beBits(laneVersion));
    }
}
