package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;

class ControlOperationAuthorizationTest {
    @Test
    void bindsActorRoleScopeAndTargetCoverageBeforeRegistration() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] actor = bytes(32, 1);
        final byte[] scope = bytes(32, 2);
        final ControlRoleSet roles = ControlRoleSet.of(ControlRole.TENANT_POLICY_ADMINISTRATOR);
        final ControlOperationRequest request = ControlOperationRequest.pauseDestinationLane(
                new ControlReason(ControlReasonKind.OPERATOR_REQUEST, null, null));
        final ControlTargetRef target = new ControlTargetRef(
                0,
                ControlTargetKind.LANE,
                new LaneControlTarget(bytes(32, 3), bytes(16, 4), 1),
                bytes(32, 5),
                bytes(32, 6));
        final PreparedControlOperation prepared = PreparedControlOperation.prepare(
                bytes(32, 7),
                request.kind(),
                new ControlAuthor(actor, roles.digest(), scope),
                request,
                List.of(target),
                1,
                2,
                1,
                keyPair.getPrivate());
        final ControlAuthorizationContext context = new ControlAuthorizationContext(actor, roles, scope);

        assertDoesNotThrow(() -> ControlOperationAuthorization.authorize(prepared, context, ignored -> true));
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlOperationAuthorization.authorize(prepared, context, ignored -> false));
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlOperationAuthorization.authorize(
                        prepared,
                        new ControlAuthorizationContext(
                                actor, ControlRoleSet.of(ControlRole.DEAD_LETTER_OPERATOR), scope),
                        ignored -> true));
    }

    @Test
    void enforcesPlatformAndDualRoleControlMatrices() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] actor = bytes(32, 10);
        final byte[] scope = bytes(32, 11);
        final ControlOperationRequest force = ControlOperationRequest.forceCheckpoint(
                new ForceCheckpointRequest(new ControlReason(ControlReasonKind.MAINTENANCE, null, null)));
        final ControlTargetRef shard = new ControlTargetRef(
                0,
                ControlTargetKind.SHARD,
                new ShardSubject(new ShardId(new RouteIncarnation(bytes(16, 12)), 0)),
                null,
                null);
        final ControlRoleSet platform = ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR);
        final PreparedControlOperation prepared = PreparedControlOperation.prepare(
                bytes(32, 13),
                force.kind(),
                new ControlAuthor(actor, platform.digest(), scope),
                force,
                List.of(shard),
                1,
                2,
                1,
                keyPair.getPrivate());
        assertDoesNotThrow(() -> ControlOperationAuthorization.authorize(
                prepared, new ControlAuthorizationContext(actor, platform, scope), ignored -> true));

        final ControlOperationRequest resolve = ControlOperationRequest.resolveUncertain(new ResolveUncertainRequest(
                UncertainResolutionKind.RETRY_ALLOW_POSSIBLE_DUPLICATE,
                null,
                true,
                false,
                new AcknowledgementSet(List.of(
                        new Acknowledgement(AcknowledgementKind.POSSIBLE_DUPLICATE, bytes(32, 14), bytes(32, 15))))));
        final ControlTargetRef message = new ControlTargetRef(
                0,
                ControlTargetKind.MESSAGE,
                new ControlMessageTarget(DelayMessageId.random(shard.shard().shardId()), 0, 1, bytes(32, 16)),
                bytes(32, 17),
                bytes(32, 18));
        final ControlRoleSet operatorAndAdmin =
                ControlRoleSet.of(ControlRole.DEAD_LETTER_OPERATOR, ControlRole.TENANT_POLICY_ADMINISTRATOR);
        final PreparedControlOperation resolvePrepared = PreparedControlOperation.prepare(
                bytes(32, 19),
                resolve.kind(),
                new ControlAuthor(actor, operatorAndAdmin.digest(), scope),
                resolve,
                List.of(message),
                1,
                2,
                1,
                keyPair.getPrivate());
        assertDoesNotThrow(() -> ControlOperationAuthorization.authorize(
                resolvePrepared, new ControlAuthorizationContext(actor, operatorAndAdmin, scope), ignored -> true));
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlOperationAuthorization.authorize(
                        resolvePrepared,
                        new ControlAuthorizationContext(
                                actor, ControlRoleSet.of(ControlRole.DEAD_LETTER_OPERATOR), scope),
                        ignored -> true));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
