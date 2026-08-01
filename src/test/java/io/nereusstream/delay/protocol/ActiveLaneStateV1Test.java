package io.nereusstream.delay.protocol;

import io.nereusstream.delay.runtime.AdmissionGate;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ActiveLaneStateV1Test {
    @Test
    void roundTripsBlockedStateWithCheckedTupleAndCharge() {
        final byte[] tuple = Bytes.utf8("canonical-lane-tuple");
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 1);
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, 2);
        final ActiveLaneStateV1 state = new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 1), AdmissionGate.OPEN,
                RuntimeReadiness.BLOCKED, LaneRuntimeBlockReasonV1.CAPABILITY, 3, 4, destination, capability,
                tuple, 5, charge(), 100L, 120L, LaneCircuitStateV1.CLOSED, 0, 2, 130, 140, null, null, null);

        final ActiveLaneStateV1 decoded = ActiveLaneStateV1.decode(state.canonicalBytes());
        assertEquals(state, decoded);
        assertArrayEquals(state.stateDigest(), decoded.stateDigest());
        assertArrayEquals(Bytes.sha256(tuple), decoded.canonicalLaneTupleSha256());
    }

    @Test
    void readyRequiresCertificateAndRejectsReadyKeyDigestTampering() {
        final byte[] tuple = Bytes.utf8("ready-lane-tuple");
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 3);
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, 4);
        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 2), AdmissionGate.OPEN, RuntimeReadiness.READY,
                null, 1, 1, destination, capability, tuple, 1, charge(), null, null,
                LaneCircuitStateV1.CLOSED, 0, 0, 0, 0, null, null, null));

        final ShardId certificateShard = new ShardId(RouteIncarnation.random(), 0);
        final byte[] certificate = PublishAdmissionBodyTest.Fixture.create(certificateShard)
                .body();
        final byte[] validCertificate = PublishAdmissionBody.decode(certificate).readyCertificate().canonicalBytes();
        final ActiveLaneStateV1 state = new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 3), AdmissionGate.OPEN, RuntimeReadiness.READY,
                null, 1, 1, destination, capability, tuple, 1, charge(), null, 200L,
                LaneCircuitStateV1.OPEN, 300, 0, 0, 0, Bytes.utf8("ready"), validCertificate, null);
        final byte[] tampered = state.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ActiveLaneStateV1.decode(tampered));
    }

    @Test
    void rejectsTupleIdentityAndCircuitInvariantViolations() {
        final byte[] tuple = Bytes.utf8("tuple");
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 5);
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, 6);
        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                new DestinationLaneId(bytes(32, 7)), bytes(16, 4), AdmissionGate.OPEN,
                RuntimeReadiness.RECOVERING_EVIDENCE, null, 1, 1, destination, capability, tuple, 1, charge(),
                null, null, LaneCircuitStateV1.CLOSED, 0, 0, 0, 0, null, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ActiveLaneStateV1(
                DestinationLaneId.derive(tuple), bytes(16, 5), AdmissionGate.OPEN,
                RuntimeReadiness.RECOVERING_EVIDENCE, null, 1, 1, destination, capability, tuple, 1, charge(),
                null, null, LaneCircuitStateV1.OPEN, 0, 0, 0, 0, null, null, null));
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
}
