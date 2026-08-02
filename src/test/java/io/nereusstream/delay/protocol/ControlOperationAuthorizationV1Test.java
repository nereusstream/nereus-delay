package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class ControlOperationAuthorizationV1Test {
    @Test
    void bindsActorRoleScopeAndTargetCoverageBeforeRegistration() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] actor = bytes(32, 1);
        final byte[] scope = bytes(32, 2);
        final ControlRoleSetV1 roles = ControlRoleSetV1.of(ControlRoleV1.TENANT_POLICY_ADMINISTRATOR);
        final ControlOperationRequestV1 request = ControlOperationRequestV1.pauseDestinationLane(
                new ControlReasonV1(ControlReasonKindV1.OPERATOR_REQUEST, null, null));
        final ControlTargetRefV1 target = new ControlTargetRefV1(0, ControlTargetKindV1.LANE,
                new LaneControlTargetV1(bytes(32, 3), bytes(16, 4), 1), bytes(32, 5), bytes(32, 6));
        final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(bytes(32, 7), request.kind(),
                new ControlAuthorV1(actor, roles.digest(), scope), request, List.of(target), 1, 2, 1,
                keyPair.getPrivate());
        final ControlAuthorizationContextV1 context = new ControlAuthorizationContextV1(actor, roles, scope);

        assertDoesNotThrow(() -> ControlOperationAuthorizationV1.authorize(prepared, context, ignored -> true));
        assertThrows(IllegalArgumentException.class,
                () -> ControlOperationAuthorizationV1.authorize(prepared, context, ignored -> false));
        assertThrows(IllegalArgumentException.class, () -> ControlOperationAuthorizationV1.authorize(prepared,
                new ControlAuthorizationContextV1(actor,
                        ControlRoleSetV1.of(ControlRoleV1.DEAD_LETTER_OPERATOR), scope), ignored -> true));
    }

    @Test
    void enforcesPlatformAndDualRoleControlMatrices() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] actor = bytes(32, 10);
        final byte[] scope = bytes(32, 11);
        final ControlOperationRequestV1 force = ControlOperationRequestV1.forceCheckpoint(
                new ForceCheckpointRequestV1(new ControlReasonV1(ControlReasonKindV1.MAINTENANCE, null, null)));
        final ControlTargetRefV1 shard = new ControlTargetRefV1(0, ControlTargetKindV1.SHARD,
                new ShardSubjectV1(new ShardId(new RouteIncarnation(bytes(16, 12)), 0)), null, null);
        final ControlRoleSetV1 platform = ControlRoleSetV1.of(ControlRoleV1.PLATFORM_OPERATOR);
        final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(bytes(32, 13), force.kind(),
                new ControlAuthorV1(actor, platform.digest(), scope), force, List.of(shard), 1, 2, 1,
                keyPair.getPrivate());
        assertDoesNotThrow(() -> ControlOperationAuthorizationV1.authorize(prepared,
                new ControlAuthorizationContextV1(actor, platform, scope), ignored -> true));

        final ControlOperationRequestV1 resolve = ControlOperationRequestV1.resolveUncertain(
                new ResolveUncertainRequestV1(UncertainResolutionKindV1.RETRY_ALLOW_POSSIBLE_DUPLICATE, null,
                        true, false, new AcknowledgementSetV1(List.of(new AcknowledgementV1(
                                AcknowledgementKindV1.POSSIBLE_DUPLICATE, bytes(32, 14), bytes(32, 15))))));
        final ControlTargetRefV1 message = new ControlTargetRefV1(0, ControlTargetKindV1.MESSAGE,
                new ControlMessageTargetV1(DelayMessageId.random(shard.shard().shardId()), 0, 1, bytes(32, 16)),
                bytes(32, 17), bytes(32, 18));
        final ControlRoleSetV1 operatorAndAdmin = ControlRoleSetV1.of(ControlRoleV1.DEAD_LETTER_OPERATOR,
                ControlRoleV1.TENANT_POLICY_ADMINISTRATOR);
        final PreparedControlOperationV1 resolvePrepared = PreparedControlOperationV1.prepare(bytes(32, 19),
                resolve.kind(), new ControlAuthorV1(actor, operatorAndAdmin.digest(), scope), resolve,
                List.of(message), 1, 2, 1, keyPair.getPrivate());
        assertDoesNotThrow(() -> ControlOperationAuthorizationV1.authorize(resolvePrepared,
                new ControlAuthorizationContextV1(actor, operatorAndAdmin, scope), ignored -> true));
        assertThrows(IllegalArgumentException.class, () -> ControlOperationAuthorizationV1.authorize(resolvePrepared,
                new ControlAuthorizationContextV1(actor,
                        ControlRoleSetV1.of(ControlRoleV1.DEAD_LETTER_OPERATOR), scope), ignored -> true));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
