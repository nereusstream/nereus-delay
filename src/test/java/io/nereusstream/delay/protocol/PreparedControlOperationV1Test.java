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
    void initialCurrentOperationProjectsEveryPreparedTarget() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlOperationRequestV1 request = ControlOperationRequestV1.forceCheckpoint(
                new ForceCheckpointRequestV1(new ControlReasonV1(ControlReasonKindV1.MAINTENANCE, null, null)));
        final ControlTargetRefV1 first = new ControlTargetRefV1(0, ControlTargetKindV1.SHARD,
                new ShardSubjectV1(new ShardId(new RouteIncarnation(bytes(16, 1)), 0)), null, null);
        final ControlTargetRefV1 second = new ControlTargetRefV1(1, ControlTargetKindV1.SHARD,
                new ShardSubjectV1(new ShardId(new RouteIncarnation(bytes(16, 2)), 1)), null, null);
        final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(bytes(32, 3),
                request.kind(), new ControlAuthorV1(bytes(32, 4), bytes(32, 5), bytes(32, 6)), request,
                List.of(first, second), 1, 2, 1, keyPair.getPrivate());
        final CurrentControlOperationV1 current = prepared.initialCurrentOperation();
        assertEquals(ControlOperationStateV1.PENDING, current.state());
        assertEquals(1, current.operationRevision());
        assertEquals(2, current.targetStates().size());
        assertEquals(TargetMarkerStateV1.PENDING, current.targetStates().get(0).markerState());
        assertEquals(TargetMarkerStateV1.PENDING, current.targetStates().get(1).markerState());
    }

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
                List.of(target, shardTarget), Long.MIN_VALUE, 1_000, 2, keyPair.getPrivate());

        assertTrue(prepared.verifySignature(keyPair.getPublic()));
        assertFalse(prepared.verifySignature(KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic()));
        assertEquals(prepared, PreparedControlOperationV1.decode(prepared.canonicalBytes()));
        assertEquals(Long.MIN_VALUE, prepared.controlQueryPolicyVersion());
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

    @Test
    void bindsPreparedReplayTargetToCompletedSystemMutation() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(bytes(16, 21), 1, bytes(32, 22));
        final ControlOperationRequestV1 request = ControlOperationRequestV1.replayDeadLetter(
                new ReplayDeadLetterRequestV1(100, 200, retryPolicy, false, AcknowledgementSetV1.empty()));
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, 23)), 4);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final byte[] operationId = bytes(32, 24);
        final byte[] requestHash = PreparedControlOperationV1.requestHash(request.kind(), request);
        final ControlRef controlRef = new ControlRef(operationId, requestHash, 0);
        final long retryUntil = 9_000;
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubjectV1(shardId).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.REPLAY_DEAD_LETTER.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntil);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, 0);
            CanonicalProtobuf.uint64(output, 13, 7);
            CanonicalProtobuf.int64(output, 14, 100);
            CanonicalProtobuf.int64(output, 15, 200);
            CanonicalProtobuf.bytes(output, 16, retryPolicy.canonicalBytes());
            CanonicalProtobuf.uint32(output, 17, 0);
        });
        final byte[] mutationHash = SystemMutation.computeMutationHash(shardId,
                SystemMutationType.REPLAY_DEAD_LETTER, retryUntil, body);
        final byte[] mutationId = SystemMutation.computeSystemMutationId(shardId,
                SystemMutationType.REPLAY_DEAD_LETTER,
                controlRef.logicalOperationIdentity(SystemMutationType.REPLAY_DEAD_LETTER), mutationHash);
        final ControlTargetRefV1 target = new ControlTargetRefV1(0, ControlTargetKindV1.MESSAGE,
                new ControlMessageTargetV1(messageId, 0, 7, null), mutationId, mutationHash);
        final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(operationId, request.kind(),
                new ControlAuthorV1(bytes(32, 25), bytes(32, 26), bytes(32, 27)), request, List.of(target),
                1, 2, 1, keyPair.getPrivate());
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.REPLAY_DEAD_LETTER,
                retryUntil, controlRef.logicalOperationIdentity(SystemMutationType.REPLAY_DEAD_LETTER), body,
                AuthorIdentity.control(bytes(32, 28), bytes(32, 29), bytes(32, 30)).canonicalBytes(), 1,
                keyPair.getPrivate());

        prepared.validateTargetMutation(target, mutation);
        final byte[] alteredBody = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubjectV1(shardId).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.REPLAY_DEAD_LETTER.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntil);
            CanonicalProtobuf.bytes(output, 10,
                    new ControlRef(operationId, bytes(32, 31), 0).canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, 0);
            CanonicalProtobuf.uint64(output, 13, 7);
            CanonicalProtobuf.int64(output, 14, 100);
            CanonicalProtobuf.int64(output, 15, 200);
            CanonicalProtobuf.bytes(output, 16, retryPolicy.canonicalBytes());
            CanonicalProtobuf.uint32(output, 17, 0);
        });
        final SystemMutation altered = SystemMutation.signed(shardId, SystemMutationType.REPLAY_DEAD_LETTER,
                retryUntil, new ControlRef(operationId, bytes(32, 31), 0)
                        .logicalOperationIdentity(SystemMutationType.REPLAY_DEAD_LETTER), alteredBody,
                AuthorIdentity.control(bytes(32, 28), bytes(32, 29), bytes(32, 30)).canonicalBytes(), 1,
                keyPair.getPrivate());
        assertThrows(IllegalArgumentException.class, () -> prepared.validateTargetMutation(target, altered));
    }

    @Test
    void bindsPreparedLaneTargetToApplyShardControlBody() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlReasonV1 reason = new ControlReasonV1(ControlReasonKindV1.OPERATOR_REQUEST, null, null);
        final ControlOperationRequestV1 request = ControlOperationRequestV1.pauseDestinationLane(reason);
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, 32)), 2);
        final LaneControlTargetV1 lane = new LaneControlTargetV1(bytes(32, 33), bytes(16, 34), 5);
        final byte[] operationId = bytes(32, 35);
        final byte[] requestHash = PreparedControlOperationV1.requestHash(request.kind(), request);
        final ControlRef controlRef = new ControlRef(operationId, requestHash, 0);
        final byte[] payloadBranch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, lane.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, reason.canonicalBytes());
        });
        final byte[] payload = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 8, payloadBranch));
        final long retryUntil = 9_000;
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubjectV1(shardId).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntil);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, 8);
            CanonicalProtobuf.uint64(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, bytes(32, 36));
            CanonicalProtobuf.bytes(output, 15, payload);
        });
        final byte[] mutationHash = SystemMutation.computeMutationHash(shardId,
                SystemMutationType.APPLY_SHARD_CONTROL, retryUntil, body);
        final byte[] mutationId = SystemMutation.computeSystemMutationId(shardId,
                SystemMutationType.APPLY_SHARD_CONTROL, controlRef.logicalOperationIdentity(8), mutationHash);
        final ControlTargetRefV1 target = new ControlTargetRefV1(0, ControlTargetKindV1.LANE, lane,
                mutationId, mutationHash);
        final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(operationId, request.kind(),
                new ControlAuthorV1(bytes(32, 37), bytes(32, 38), bytes(32, 39)), request, List.of(target),
                1, 2, 1, keyPair.getPrivate());
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.APPLY_SHARD_CONTROL,
                retryUntil, controlRef.logicalOperationIdentity(8), body,
                AuthorIdentity.control(bytes(32, 40), bytes(32, 41), bytes(32, 42)).canonicalBytes(), 1,
                keyPair.getPrivate());

        prepared.validateTargetMutation(target, mutation);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
