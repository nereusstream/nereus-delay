package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProfileBindingControlStateTest {
    @Test
    void profileActivationAndCloseRoundTripWithSourceAcceptance() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final ProfileRefV1 profile = profile(1);
        final ControlReasonV1 reason = new ControlReasonV1(ControlReasonKindV1.POLICY_CHANGE,
                Bytes.sha256(Bytes.utf8("ticket")), null);
        final ProfileNewBindingClosePayloadV1 close = new ProfileNewBindingClosePayloadV1(profile, reason);
        ProfileBindingControlState state = ProfileBindingControlState.empty()
                .activate(profile, position(shard, 10, 100));
        assertEquals(ProfileAcceptanceV1.ACTIVE_FOR_FIRST_BINDING,
                state.firstBindingAcceptance(profile, position(shard, 10, 100)));
        state = state.close(close, position(shard, 20, 200));
        assertEquals(ProfileAcceptanceV1.CLOSED_FOR_FIRST_BINDING,
                state.firstBindingAcceptance(profile, position(shard, 21, 201)));
        assertEquals(state, ProfileBindingControlState.decode(state.canonicalBytes()));
    }

    @Test
    void profileMarkersRejectReactivationAndOutOfOrderClose() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final ProfileRefV1 profile = profile(2);
        final ProfileBindingControlState state = ProfileBindingControlState.empty()
                .activate(profile, position(shard, 10, 100));
        assertEquals(state, state.activate(profile, position(shard, 10, 100)));
        assertThrows(IllegalArgumentException.class,
                () -> state.activate(profile, new KafkaSourcePosition(shard, "cluster-a",
                        UUID.fromString("00000000-0000-0000-0000-000000000008"), 10, 7, 101)));
        assertThrows(IllegalArgumentException.class,
                () -> state.activate(profile, position(shard, 11, 101)));
        final ProfileNewBindingClosePayloadV1 close = new ProfileNewBindingClosePayloadV1(profile,
                new ControlReasonV1(ControlReasonKindV1.INCIDENT, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> state.close(close, position(shard, 9, 99)));
    }

    @Test
    void profilePayloadBranchesDecodeCanonically() {
        final ProfileRefV1 profile = profile(3);
        final ProfileBindingActivatePayloadV1 activate = new ProfileBindingActivatePayloadV1(profile);
        assertEquals(activate, ProfileBindingActivatePayloadV1.decode(activate.canonicalBytes()));
        final ProfileNewBindingClosePayloadV1 close = new ProfileNewBindingClosePayloadV1(profile,
                new ControlReasonV1(ControlReasonKindV1.MAINTENANCE, null, Bytes.sha256(Bytes.utf8("detail"))));
        assertEquals(close, ProfileNewBindingClosePayloadV1.decode(close.canonicalBytes()));
    }

    private static ProfileRefV1 profile(final int value) {
        return new ProfileRefV1(Bytes.utf8("destination-" + value), value,
                Bytes.sha256(Bytes.utf8("profile-hash-" + value)), ProfileKindV1.DESTINATION);
    }

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long timestamp) {
        return new KafkaSourcePosition(shard, "cluster-a",
                UUID.fromString("00000000-0000-0000-0000-000000000008"), offset, null, timestamp);
    }
}
