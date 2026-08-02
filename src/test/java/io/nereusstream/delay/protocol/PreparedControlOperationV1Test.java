package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreparedControlOperationV1Test {
    @Test
    void preparesSignsAndRoundTripsCanonicalEnvelope() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlReasonV1 reason = new ControlReasonV1(ControlReasonKindV1.MAINTENANCE,
                bytes(32, 1), null);
        final ControlOperationRequestV1 request = ControlOperationRequestV1.stopNewSchedules(reason);
        final ControlTargetRefV1 target = new ControlTargetRefV1(0, ControlTargetKindV1.ROUTE,
                bytes(16, 2), null, null);
        final ControlTargetRefV1 shardTarget = new ControlTargetRefV1(1, ControlTargetKindV1.SHARD,
                new ShardSubjectV1(new ShardId(new RouteIncarnation(bytes(16, 3)), 0)), bytes(32, 4), bytes(32, 5));
        final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(bytes(32, 3),
                request.kind(), new ControlAuthorV1(bytes(32, 4), bytes(32, 5), bytes(32, 6)), request,
                List.of(target, shardTarget), 7, 1_000, 2, keyPair.getPrivate());

        assertTrue(prepared.verifySignature(keyPair.getPublic()));
        assertFalse(prepared.verifySignature(KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic()));
        assertEquals(prepared, PreparedControlOperationV1.decode(prepared.canonicalBytes()));
        assertEquals(7, prepared.controlQueryPolicyVersion());
        assertEquals(2, prepared.signingKeyVersion());
    }

    @Test
    void rejectsHashSignatureAndTargetOrderingTampering() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlOperationRequestV1 request = ControlOperationRequestV1.stopNewSchedules(
                new ControlReasonV1(ControlReasonKindV1.INCIDENT, null, null));
        final ControlTargetRefV1 first = new ControlTargetRefV1(0, ControlTargetKindV1.ROUTE, bytes(16, 1), null, null);
        final ControlTargetRefV1 second = new ControlTargetRefV1(1, ControlTargetKindV1.SHARD,
                new ShardSubjectV1(new ShardId(new RouteIncarnation(bytes(16, 2)), 0)), bytes(32, 11), bytes(32, 12));
        assertThrows(IllegalArgumentException.class, () -> PreparedControlOperationV1.prepare(bytes(32, 3),
                request.kind(), new ControlAuthorV1(bytes(32, 4), bytes(32, 5), bytes(32, 6)), request,
                List.of(second, first), 1, 2, 1, keyPair.getPrivate()));

        final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(bytes(32, 7),
                request.kind(), new ControlAuthorV1(bytes(32, 8), bytes(32, 9), bytes(32, 10)), request,
                List.of(first, second), 1, 2, 1, keyPair.getPrivate());
        final byte[] tampered = prepared.canonicalBytes();
        tampered[4] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> PreparedControlOperationV1.decode(tampered));
    }

    @Test
    void enforcesOperationSpecificTargetPresenceAndMutationIdentity() {
        final ControlReasonV1 reason = new ControlReasonV1(ControlReasonKindV1.OPERATOR_REQUEST, null, null);
        final ControlOperationRequestV1 pause = ControlOperationRequestV1.pauseDestinationLane(reason);
        final ControlTargetRefV1 laneWithoutMutation = new ControlTargetRefV1(0, ControlTargetKindV1.LANE,
                new LaneControlTargetV1(bytes(32, 1), bytes(16, 2), 1), null, null);
        assertThrows(IllegalArgumentException.class, () -> PreparedControlOperationV1.validateTargetPresence(
                pause.kind(), pause, List.of(laneWithoutMutation)));

        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(bytes(16, 3), 1, bytes(32, 4));
        final ControlOperationRequestV1 replay = ControlOperationRequestV1.replayDeadLetter(
                new ReplayDeadLetterRequestV1(10, 20, retryPolicy, false, AcknowledgementSetV1.empty()));
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, 5)), 1);
        final ShardSubjectV1 shard = new ShardSubjectV1(shardId);
        final ControlTargetRefV1 wrongReplayTarget = new ControlTargetRefV1(0, ControlTargetKindV1.SHARD, shard,
                bytes(32, 6), bytes(32, 7));
        assertThrows(IllegalArgumentException.class, () -> PreparedControlOperationV1.validateTargetPresence(
                replay.kind(), replay, List.of(wrongReplayTarget)));

        final DelayMessageId message = new DelayMessageId(SelfRoutingId.random(shardId).bytes());
        final ControlTargetRefV1 validReplayTarget = new ControlTargetRefV1(0, ControlTargetKindV1.MESSAGE,
                new ControlMessageTargetV1(message, 0, 1, null), bytes(32, 8), bytes(32, 9));
        PreparedControlOperationV1.validateTargetPresence(replay.kind(), replay, List.of(validReplayTarget));

        final ControlOperationRequestV1 force = ControlOperationRequestV1.forceCheckpoint(
                new ForceCheckpointRequestV1(reason));
        final ControlTargetRefV1 forceTargetWithMutation = new ControlTargetRefV1(0, ControlTargetKindV1.SHARD,
                shard, bytes(32, 10), bytes(32, 11));
        assertThrows(IllegalArgumentException.class, () -> PreparedControlOperationV1.validateTargetPresence(
                force.kind(), force, List.of(forceTargetWithMutation)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
