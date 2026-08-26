package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ProfileBindingControlStateTest {
    @Test
    void profileActivationAndCloseRoundTripWithSourceAcceptance() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final ProfileRef profile = profile(1);
        final ControlReason reason =
                new ControlReason(ControlReasonKind.POLICY_CHANGE, Bytes.sha256(Bytes.utf8("ticket")), null);
        final ProfileNewBindingClosePayload close = new ProfileNewBindingClosePayload(profile, reason);
        ProfileBindingControlState state =
                ProfileBindingControlState.empty().activate(profile, position(shard, 10, 100));
        assertEquals(
                ProfileAcceptance.ACTIVE_FOR_FIRST_BINDING,
                state.firstBindingAcceptance(profile, position(shard, 10, 100)));
        state = state.close(close, position(shard, 20, 200));
        assertEquals(
                ProfileAcceptance.CLOSED_FOR_FIRST_BINDING,
                state.firstBindingAcceptance(profile, position(shard, 21, 201)));
        assertEquals(state, ProfileBindingControlState.decode(state.canonicalBytes()));
    }

    @Test
    void profileMarkersRejectReactivationAndOutOfOrderClose() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final ProfileRef profile = profile(2);
        final ProfileBindingControlState state =
                ProfileBindingControlState.empty().activate(profile, position(shard, 10, 100));
        assertEquals(state, state.activate(profile, position(shard, 10, 100)));
        assertThrows(
                IllegalArgumentException.class,
                () -> state.activate(
                        profile,
                        new KafkaSourcePosition(
                                shard,
                                "cluster-a",
                                UUID.fromString("00000000-0000-0000-0000-000000000008"),
                                10,
                                7,
                                101)));
        assertThrows(IllegalArgumentException.class, () -> state.activate(profile, position(shard, 11, 101)));
        final ProfileNewBindingClosePayload close =
                new ProfileNewBindingClosePayload(profile, new ControlReason(ControlReasonKind.INCIDENT, null, null));
        assertThrows(IllegalArgumentException.class, () -> state.close(close, position(shard, 9, 99)));
    }

    @Test
    void profilePayloadBranchesDecodeCanonically() {
        final ProfileRef profile = profile(3);
        final ProfileBindingActivatePayload activate = new ProfileBindingActivatePayload(profile);
        assertEquals(activate, ProfileBindingActivatePayload.decode(activate.canonicalBytes()));
        final ProfileNewBindingClosePayload close = new ProfileNewBindingClosePayload(
                profile, new ControlReason(ControlReasonKind.MAINTENANCE, null, Bytes.sha256(Bytes.utf8("detail"))));
        assertEquals(close, ProfileNewBindingClosePayload.decode(close.canonicalBytes()));
    }

    private static ProfileRef profile(final int value) {
        return new ProfileRef(
                Bytes.utf8("destination-" + value),
                value,
                Bytes.sha256(Bytes.utf8("profile-hash-" + value)),
                ProfileKind.DESTINATION);
    }

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long timestamp) {
        return new KafkaSourcePosition(
                shard, "cluster-a", UUID.fromString("00000000-0000-0000-0000-000000000008"), offset, null, timestamp);
    }
}
