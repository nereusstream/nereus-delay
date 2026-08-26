package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ControlResultCodecTest {
    @Test
    void roundTripsAllClosedControlResultBranches() {
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, 1)), 4);
        final ShardSubject shard = new ShardSubject(shardId);
        final DestinationLaneId laneId = new DestinationLaneId(bytes(32, 2));
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final ProfileRef destination = profile(ProfileKind.DESTINATION, 3);
        final QuotaGrantRef quota = new QuotaGrantRef(bytes(32, 4), 2, charge());

        final LaneControlResult lane =
                new LaneControlResult(laneId, bytes(16, 5), 6, LaneAdmissionGate.OPEN, 7, StableCode.OK);
        final ShardControlResult shardResult =
                new ShardControlResult(shard, ShardLifecycleState.ACTIVE_FOR_COMMANDS, Long.MIN_VALUE, StableCode.OK);
        final CheckpointControlResult checkpoint = new CheckpointControlResult(shard, bytes(16, 6), bytes(32, 7), 9);
        final ProfileControlResult profile =
                new ProfileControlResult(destination, ProfileAcceptance.ACTIVE_FOR_FIRST_BINDING, Long.MIN_VALUE);
        final QuotaControlResult quotaResult = new QuotaControlResult(quota, bytes(32, 8));
        final MessageControlResult message = new MessageControlResult(
                messageId,
                1,
                2,
                MessageGenerationState.UNCERTAIN,
                StableCode.ENQUEUE_RESULT_UNCERTAIN,
                new PublicEvidenceRef(
                        PublishEvidenceKind.OPERATOR_ATTESTATION, bytes(32, 9), EvidenceVerificationStatus.UNRESOLVED));
        final RouteControlResult route = new RouteControlResult(bytes(16, 10), RouteLifecycle.CONTROL_ONLY, 11);
        final SecretRotationResult rotation = new SecretRotationResult(
                destination, Long.MIN_VALUE, bytes(32, 11), bytes(32, 12), Long.MIN_VALUE, bytes(32, 13));

        assertEquals(lane, LaneControlResult.decode(lane.canonicalBytes()));
        assertEquals(shardResult, ShardControlResult.decode(shardResult.canonicalBytes()));
        assertEquals(checkpoint, CheckpointControlResult.decode(checkpoint.canonicalBytes()));
        assertEquals(profile, ProfileControlResult.decode(profile.canonicalBytes()));
        assertEquals(quotaResult, QuotaControlResult.decode(quotaResult.canonicalBytes()));
        assertEquals(message, MessageControlResult.decode(message.canonicalBytes()));
        assertEquals(route, RouteControlResult.decode(route.canonicalBytes()));
        assertEquals(rotation, SecretRotationResult.decode(rotation.canonicalBytes()));
        assertEquals(
                Long.MIN_VALUE,
                SecretRotationResult.decode(rotation.canonicalBytes()).bindingHeadRevision());
    }

    @Test
    void enforcesOptionalBranchPresenceAndTypedProfileRules() {
        final ProfileRef capability = profile(ProfileKind.DELIVERY_CAPABILITY, 20);
        assertThrows(
                IllegalArgumentException.class,
                () -> new ProfileControlResult(capability, ProfileAcceptance.ABSENT, 1L));
        final ProfileRef destination = profile(ProfileKind.DESTINATION, 21);
        assertThrows(
                IllegalArgumentException.class,
                () -> new ProfileControlResult(destination, ProfileAcceptance.ABSENT, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SecretRotationResult(capability, 1, bytes(32, 1), bytes(32, 2), 1, bytes(32, 3)));

        final ShardControlResult noOwner = new ShardControlResult(
                new ShardSubject(new RouteIncarnation(bytes(16, 30)), 1),
                ShardLifecycleState.RESTORING,
                null,
                StableCode.SHARD_TRANSITIONING);
        assertEquals(noOwner, ShardControlResult.decode(noOwner.canonicalBytes()));
    }

    private static ProfileRef profile(final ProfileKind kind, final int seed) {
        return new ProfileRef(bytes(8, seed), 1, bytes(32, seed + 20), kind);
    }

    private static PublishAdmissionBody.ChargeVector charge() {
        return new PublishAdmissionBody.ChargeVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
