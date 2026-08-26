package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertThrows;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;

class ControlSystemMutationFactoryTest {
    @Test
    void signsAndBindsPreparedLaneMutation() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlReason reason = new ControlReason(ControlReasonKind.OPERATOR_REQUEST, null, null);
        final ControlOperationRequest request = ControlOperationRequest.pauseDestinationLane(reason);
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, 1)), 2);
        final LaneControlTarget lane = new LaneControlTarget(bytes(32, 2), bytes(16, 3), 5);
        final byte[] operationId = bytes(32, 4);
        final ControlRef controlRef =
                new ControlRef(operationId, PreparedControlOperation.requestHash(request.kind(), request), 0);
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
            CanonicalProtobuf.bytes(output, 13, bytes(32, 5));
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
                new ControlAuthor(bytes(32, 6), bytes(32, 7), bytes(32, 8)),
                request,
                List.of(target),
                1,
                2,
                1,
                keyPair.getPrivate());

        ControlSystemMutationFactory.sign(
                prepared,
                target,
                shardId,
                retryUntil,
                body,
                AuthorIdentity.control(bytes(32, 9), bytes(32, 10), bytes(32, 11))
                        .canonicalBytes(),
                1,
                keyPair.getPrivate());
    }

    @Test
    void rejectsBodyDriftAfterTargetIdentityWasPrepared() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlReason reason = new ControlReason(ControlReasonKind.OPERATOR_REQUEST, null, null);
        final ControlOperationRequest request = ControlOperationRequest.pauseDestinationLane(reason);
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, 12)), 2);
        final LaneControlTarget lane = new LaneControlTarget(bytes(32, 13), bytes(16, 14), 5);
        final byte[] operationId = bytes(32, 15);
        final ControlRef controlRef =
                new ControlRef(operationId, PreparedControlOperation.requestHash(request.kind(), request), 0);
        final byte[] payloadBranch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, lane.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, reason.canonicalBytes());
        });
        final byte[] payload = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 8, payloadBranch));
        final byte[] body = body(shardId, controlRef, payload, 9_001, bytes(32, 16));
        final byte[] mutationHash =
                SystemMutation.computeMutationHash(shardId, SystemMutationType.APPLY_SHARD_CONTROL, 9_001, body);
        final byte[] mutationId = SystemMutation.computeSystemMutationId(
                shardId, SystemMutationType.APPLY_SHARD_CONTROL, controlRef.logicalOperationIdentity(8), mutationHash);
        final ControlTargetRef target = new ControlTargetRef(0, ControlTargetKind.LANE, lane, mutationId, mutationHash);
        final PreparedControlOperation prepared = PreparedControlOperation.prepare(
                operationId,
                request.kind(),
                new ControlAuthor(bytes(32, 17), bytes(32, 18), bytes(32, 19)),
                request,
                List.of(target),
                1,
                2,
                1,
                keyPair.getPrivate());
        final byte[] changed = body(shardId, controlRef, payload, 9_002, bytes(32, 20));
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlSystemMutationFactory.sign(
                        prepared,
                        target,
                        shardId,
                        9_002,
                        changed,
                        AuthorIdentity.control(bytes(32, 21), bytes(32, 22), bytes(32, 23))
                                .canonicalBytes(),
                        1,
                        keyPair.getPrivate()));
    }

    private static byte[] body(
            final ShardId shardId,
            final ControlRef controlRef,
            final byte[] payload,
            final long retryUntil,
            final byte[] semanticHash) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubject(shardId).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntil);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, 8);
            CanonicalProtobuf.uint64(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, semanticHash);
            CanonicalProtobuf.bytes(output, 15, payload);
        });
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
