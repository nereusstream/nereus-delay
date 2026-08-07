package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControlResultCodecTest {
    @Test
    void roundTripsAllClosedControlResultBranches() {
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, 1)), 4);
        final ShardSubjectV1 shard = new ShardSubjectV1(shardId);
        final DestinationLaneId laneId = new DestinationLaneId(bytes(32, 2));
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 3);
        final QuotaGrantRefV1 quota = new QuotaGrantRefV1(bytes(32, 4), 2, charge());

        final LaneControlResultV1 lane = new LaneControlResultV1(laneId, bytes(16, 5), 6,
                LaneAdmissionGateV1.OPEN, 7, StableCode.OK);
        final ShardControlResultV1 shardResult = new ShardControlResultV1(shard, ShardLifecycleStateV1.ACTIVE_FOR_COMMANDS,
                Long.MIN_VALUE, StableCode.OK);
        final CheckpointControlResultV1 checkpoint = new CheckpointControlResultV1(shard, bytes(16, 6),
                bytes(32, 7), 9);
        final ProfileControlResultV1 profile = new ProfileControlResultV1(destination,
                ProfileAcceptanceV1.ACTIVE_FOR_FIRST_BINDING, 10L);
        final QuotaControlResultV1 quotaResult = new QuotaControlResultV1(quota, bytes(32, 8));
        final MessageControlResultV1 message = new MessageControlResultV1(messageId, 1, 2,
                MessageGenerationStateV1.UNCERTAIN, StableCode.ENQUEUE_RESULT_UNCERTAIN,
                new PublicEvidenceRefV1(PublishEvidenceKindV1.OPERATOR_ATTESTATION, bytes(32, 9),
                        EvidenceVerificationStatusV1.UNRESOLVED));
        final RouteControlResultV1 route = new RouteControlResultV1(bytes(16, 10), RouteLifecycleV1.CONTROL_ONLY, 11);
        final SecretRotationResultV1 rotation = new SecretRotationResultV1(destination, 12, bytes(32, 11),
                bytes(32, 12), 13, bytes(32, 13));

        assertEquals(lane, LaneControlResultV1.decode(lane.canonicalBytes()));
        assertEquals(shardResult, ShardControlResultV1.decode(shardResult.canonicalBytes()));
        assertEquals(checkpoint, CheckpointControlResultV1.decode(checkpoint.canonicalBytes()));
        assertEquals(profile, ProfileControlResultV1.decode(profile.canonicalBytes()));
        assertEquals(quotaResult, QuotaControlResultV1.decode(quotaResult.canonicalBytes()));
        assertEquals(message, MessageControlResultV1.decode(message.canonicalBytes()));
        assertEquals(route, RouteControlResultV1.decode(route.canonicalBytes()));
        assertEquals(rotation, SecretRotationResultV1.decode(rotation.canonicalBytes()));
    }

    @Test
    void enforcesOptionalBranchPresenceAndTypedProfileRules() {
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, 20);
        assertThrows(IllegalArgumentException.class, () -> new ProfileControlResultV1(capability,
                ProfileAcceptanceV1.ABSENT, 1L));
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, 21);
        assertThrows(IllegalArgumentException.class, () -> new ProfileControlResultV1(destination,
                ProfileAcceptanceV1.ABSENT, null));
        assertThrows(IllegalArgumentException.class, () -> new SecretRotationResultV1(capability, 1,
                bytes(32, 1), bytes(32, 2), 1, bytes(32, 3)));

        final ShardControlResultV1 noOwner = new ShardControlResultV1(
                new ShardSubjectV1(new RouteIncarnation(bytes(16, 30)), 1), ShardLifecycleStateV1.RESTORING,
                null, StableCode.SHARD_TRANSITIONING);
        assertEquals(noOwner, ShardControlResultV1.decode(noOwner.canonicalBytes()));
    }

    private static ProfileRefV1 profile(final ProfileKindV1 kind, final int seed) {
        return new ProfileRefV1(bytes(8, seed), 1, bytes(32, seed + 20), kind);
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
