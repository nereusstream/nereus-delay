package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;

class PreparedControlOperationTest {
    @Test
    void initialCurrentOperationProjectsEveryPreparedTarget() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlOperationRequest request = ControlOperationRequest.forceCheckpoint(
                new ForceCheckpointRequest(new ControlReason(ControlReasonKind.MAINTENANCE, null, null)));
        final ControlTargetRef first = new ControlTargetRef(
                0,
                ControlTargetKind.SHARD,
                new ShardSubject(new ShardId(new RouteIncarnation(bytes(16, 1)), 0)),
                null,
                null);
        final ControlTargetRef second = new ControlTargetRef(
                1,
                ControlTargetKind.SHARD,
                new ShardSubject(new ShardId(new RouteIncarnation(bytes(16, 2)), 1)),
                null,
                null);
        final PreparedControlOperation prepared = PreparedControlOperation.prepare(
                bytes(32, 3),
                request.kind(),
                new ControlAuthor(bytes(32, 4), bytes(32, 5), bytes(32, 6)),
                request,
                List.of(first, second),
                1,
                2,
                1,
                keyPair.getPrivate());
        final CurrentControlOperation current = prepared.initialCurrentOperation();
        assertEquals(ControlOperationState.PENDING, current.state());
        assertEquals(1, current.operationRevision());
        assertEquals(2, current.targetStates().size());
        assertEquals(TargetMarkerState.PENDING, current.targetStates().get(0).markerState());
        assertEquals(TargetMarkerState.PENDING, current.targetStates().get(1).markerState());
    }

    @Test
    void preparesSignsAndRoundTripsCanonicalEnvelope() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlReason reason = new ControlReason(ControlReasonKind.MAINTENANCE, bytes(32, 1), null);
        final ControlOperationRequest request = ControlOperationRequest.stopNewSchedules(reason);
        final ControlTargetRef target = new ControlTargetRef(0, ControlTargetKind.ROUTE, bytes(16, 2), null, null);
        final ControlTargetRef shardTarget = new ControlTargetRef(
                1,
                ControlTargetKind.SHARD,
                new ShardSubject(new ShardId(new RouteIncarnation(bytes(16, 3)), 0)),
                bytes(32, 4),
                bytes(32, 5));
        final PreparedControlOperation prepared = PreparedControlOperation.prepare(
                bytes(32, 3),
                request.kind(),
                new ControlAuthor(bytes(32, 4), bytes(32, 5), bytes(32, 6)),
                request,
                List.of(target, shardTarget),
                Long.MIN_VALUE,
                1_000,
                2,
                keyPair.getPrivate());

        assertTrue(prepared.verifySignature(keyPair.getPublic()));
        assertFalse(prepared.verifySignature(
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic()));
        assertEquals(prepared, PreparedControlOperation.decode(prepared.canonicalBytes()));
        assertEquals(Long.MIN_VALUE, prepared.controlQueryPolicyVersion());
        assertEquals(2, prepared.signingKeyVersion());
    }

    @Test
    void rejectsHashSignatureAndTargetOrderingTampering() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlOperationRequest request =
                ControlOperationRequest.stopNewSchedules(new ControlReason(ControlReasonKind.INCIDENT, null, null));
        final ControlTargetRef first = new ControlTargetRef(0, ControlTargetKind.ROUTE, bytes(16, 1), null, null);
        final ControlTargetRef second = new ControlTargetRef(
                1,
                ControlTargetKind.SHARD,
                new ShardSubject(new ShardId(new RouteIncarnation(bytes(16, 2)), 0)),
                bytes(32, 11),
                bytes(32, 12));
        assertThrows(
                IllegalArgumentException.class,
                () -> PreparedControlOperation.prepare(
                        bytes(32, 3),
                        request.kind(),
                        new ControlAuthor(bytes(32, 4), bytes(32, 5), bytes(32, 6)),
                        request,
                        List.of(second, first),
                        1,
                        2,
                        1,
                        keyPair.getPrivate()));

        final PreparedControlOperation prepared = PreparedControlOperation.prepare(
                bytes(32, 7),
                request.kind(),
                new ControlAuthor(bytes(32, 8), bytes(32, 9), bytes(32, 10)),
                request,
                List.of(first, second),
                1,
                2,
                1,
                keyPair.getPrivate());
        final byte[] tampered = prepared.canonicalBytes();
        tampered[4] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> PreparedControlOperation.decode(tampered));
    }

    @Test
    void enforcesOperationSpecificTargetPresenceAndMutationIdentity() {
        final ControlReason reason = new ControlReason(ControlReasonKind.OPERATOR_REQUEST, null, null);
        final ControlOperationRequest pause = ControlOperationRequest.pauseDestinationLane(reason);
        final ControlTargetRef laneWithoutMutation = new ControlTargetRef(
                0, ControlTargetKind.LANE, new LaneControlTarget(bytes(32, 1), bytes(16, 2), 1), null, null);
        assertThrows(
                IllegalArgumentException.class,
                () -> PreparedControlOperation.validateTargetPresence(
                        pause.kind(), pause, List.of(laneWithoutMutation)));

        final RetryPolicyRef retryPolicy = new RetryPolicyRef(bytes(16, 3), 1, bytes(32, 4));
        final ControlOperationRequest replay = ControlOperationRequest.replayDeadLetter(
                new ReplayDeadLetterRequest(10, 20, retryPolicy, false, AcknowledgementSet.empty()));
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, 5)), 1);
        final ShardSubject shard = new ShardSubject(shardId);
        final ControlTargetRef wrongReplayTarget =
                new ControlTargetRef(0, ControlTargetKind.SHARD, shard, bytes(32, 6), bytes(32, 7));
        assertThrows(
                IllegalArgumentException.class,
                () -> PreparedControlOperation.validateTargetPresence(
                        replay.kind(), replay, List.of(wrongReplayTarget)));

        final DelayMessageId message =
                new DelayMessageId(SelfRoutingId.random(shardId).bytes());
        final ControlTargetRef validReplayTarget = new ControlTargetRef(
                0,
                ControlTargetKind.MESSAGE,
                new ControlMessageTarget(message, 0, 1, null),
                bytes(32, 8),
                bytes(32, 9));
        PreparedControlOperation.validateTargetPresence(replay.kind(), replay, List.of(validReplayTarget));

        final ControlOperationRequest force =
                ControlOperationRequest.forceCheckpoint(new ForceCheckpointRequest(reason));
        final ControlTargetRef forceTargetWithMutation =
                new ControlTargetRef(0, ControlTargetKind.SHARD, shard, bytes(32, 10), bytes(32, 11));
        assertThrows(
                IllegalArgumentException.class,
                () -> PreparedControlOperation.validateTargetPresence(
                        force.kind(), force, List.of(forceTargetWithMutation)));
    }

    @Test
    void bindsPreparedReplayTargetToCompletedSystemMutation() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final RetryPolicyRef retryPolicy = new RetryPolicyRef(bytes(16, 21), 1, bytes(32, 22));
        final ControlOperationRequest request = ControlOperationRequest.replayDeadLetter(
                new ReplayDeadLetterRequest(100, 200, retryPolicy, false, AcknowledgementSet.empty()));
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, 23)), 4);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final byte[] operationId = bytes(32, 24);
        final byte[] requestHash = PreparedControlOperation.requestHash(request.kind(), request);
        final ControlRef controlRef = new ControlRef(operationId, requestHash, 0);
        final long retryUntil = 9_000;
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubject(shardId).canonicalBytes());
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
        final byte[] mutationHash =
                SystemMutation.computeMutationHash(shardId, SystemMutationType.REPLAY_DEAD_LETTER, retryUntil, body);
        final byte[] mutationId = SystemMutation.computeSystemMutationId(
                shardId,
                SystemMutationType.REPLAY_DEAD_LETTER,
                controlRef.logicalOperationIdentity(SystemMutationType.REPLAY_DEAD_LETTER),
                mutationHash);
        final ControlTargetRef target = new ControlTargetRef(
                0,
                ControlTargetKind.MESSAGE,
                new ControlMessageTarget(messageId, 0, 7, null),
                mutationId,
                mutationHash);
        final PreparedControlOperation prepared = PreparedControlOperation.prepare(
                operationId,
                request.kind(),
                new ControlAuthor(bytes(32, 25), bytes(32, 26), bytes(32, 27)),
                request,
                List.of(target),
                1,
                2,
                1,
                keyPair.getPrivate());
        final SystemMutation mutation = SystemMutation.signed(
                shardId,
                SystemMutationType.REPLAY_DEAD_LETTER,
                retryUntil,
                controlRef.logicalOperationIdentity(SystemMutationType.REPLAY_DEAD_LETTER),
                body,
                AuthorIdentity.control(bytes(32, 28), bytes(32, 29), bytes(32, 30))
                        .canonicalBytes(),
                1,
                keyPair.getPrivate());

        prepared.validateTargetMutation(target, mutation);
        final byte[] alteredBody = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubject(shardId).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.REPLAY_DEAD_LETTER.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntil);
            CanonicalProtobuf.bytes(output, 10, new ControlRef(operationId, bytes(32, 31), 0).canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, 0);
            CanonicalProtobuf.uint64(output, 13, 7);
            CanonicalProtobuf.int64(output, 14, 100);
            CanonicalProtobuf.int64(output, 15, 200);
            CanonicalProtobuf.bytes(output, 16, retryPolicy.canonicalBytes());
            CanonicalProtobuf.uint32(output, 17, 0);
        });
        final SystemMutation altered = SystemMutation.signed(
                shardId,
                SystemMutationType.REPLAY_DEAD_LETTER,
                retryUntil,
                new ControlRef(operationId, bytes(32, 31), 0)
                        .logicalOperationIdentity(SystemMutationType.REPLAY_DEAD_LETTER),
                alteredBody,
                AuthorIdentity.control(bytes(32, 28), bytes(32, 29), bytes(32, 30))
                        .canonicalBytes(),
                1,
                keyPair.getPrivate());
        assertThrows(IllegalArgumentException.class, () -> prepared.validateTargetMutation(target, altered));
    }

    @Test
    void bindsPreparedLaneTargetToApplyShardControlBody() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlReason reason = new ControlReason(ControlReasonKind.OPERATOR_REQUEST, null, null);
        final ControlOperationRequest request = ControlOperationRequest.pauseDestinationLane(reason);
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, 32)), 2);
        final LaneControlTarget lane = new LaneControlTarget(bytes(32, 33), bytes(16, 34), 5);
        final byte[] operationId = bytes(32, 35);
        final byte[] requestHash = PreparedControlOperation.requestHash(request.kind(), request);
        final ControlRef controlRef = new ControlRef(operationId, requestHash, 0);
        final byte[] payloadBranch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, lane.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, reason.canonicalBytes());
        });
        final byte[] payload = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 8, payloadBranch));
        final long retryUntil = 9_000;
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubject(shardId).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntil);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, 8);
            CanonicalProtobuf.uint64(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, bytes(32, 36));
            CanonicalProtobuf.bytes(output, 15, payload);
        });
        final byte[] mutationHash =
                SystemMutation.computeMutationHash(shardId, SystemMutationType.APPLY_SHARD_CONTROL, retryUntil, body);
        final byte[] mutationId = SystemMutation.computeSystemMutationId(
                shardId, SystemMutationType.APPLY_SHARD_CONTROL, controlRef.logicalOperationIdentity(8), mutationHash);
        final ControlTargetRef target = new ControlTargetRef(0, ControlTargetKind.LANE, lane, mutationId, mutationHash);
        final PreparedControlOperation prepared = PreparedControlOperation.prepare(
                operationId,
                request.kind(),
                new ControlAuthor(bytes(32, 37), bytes(32, 38), bytes(32, 39)),
                request,
                List.of(target),
                1,
                2,
                1,
                keyPair.getPrivate());
        final SystemMutation mutation = SystemMutation.signed(
                shardId,
                SystemMutationType.APPLY_SHARD_CONTROL,
                retryUntil,
                controlRef.logicalOperationIdentity(8),
                body,
                AuthorIdentity.control(bytes(32, 40), bytes(32, 41), bytes(32, 42))
                        .canonicalBytes(),
                1,
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
