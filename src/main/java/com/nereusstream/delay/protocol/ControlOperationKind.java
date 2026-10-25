package com.nereusstream.delay.protocol;

/** Registry §6.3 closed Control Operation request branches. */
public enum ControlOperationKind {
    STOP_NEW_SCHEDULES(1),
    PAUSE_DESTINATION_LANE(2),
    RESUME_DESTINATION_LANE(3),
    CLOSE_DESTINATION_LANE(4),
    BREAK_ORDERING_DOMAIN(5),
    DRAIN_SHARD(6),
    FENCE_SHARD_FOR_MAINTENANCE(7),
    FORCE_CHECKPOINT(8),
    GET_CHECKPOINT_CATALOG(9),
    REPLAY_DEAD_LETTER(10),
    RESOLVE_UNCERTAIN(11),
    PUBLISH_DESTINATION_PROFILE_VERSION(12),
    DEPRECATE_DESTINATION_PROFILE_VERSION(13),
    PUBLISH_QUOTA_GRANT(14),
    ROTATE_EQUIVALENT_SECRET_REFERENCE(15),
    GRANT_TARGET_MEMBERSHIP(16),
    CLOSE_TARGET_MEMBERSHIP(17),
    PUBLISH_TARGET_QUOTA_GRANT(18),
    CLOSE_TARGET(19),
    INSTALL_TARGET_NATIVE_PUBLISHER_PERMISSION(20),
    CLOSE_TARGET_NATIVE_PUBLISHER_PERMISSION(21),
    ACTIVATE_TARGET_NATIVE_POLICY(22),
    APPROVE_TARGET_NATIVE_MEMBER(23),
    CLOSE_TARGET_NATIVE_MEMBER(24),
    CREATE_TARGET_CHANNEL(25),
    RENEW_TARGET_CHANNEL(26),
    STOP_TARGET_CHANNEL_NEW_WORK(27),
    ACTIVATE_TARGET_PROTOCOL(28),
    ENABLE_TARGET_SLO_CERTIFICATION(29),
    STOP_TARGET_SLO_CERTIFICATION(30),
    REGISTER_TARGET_SLO_WRITER_EPOCH(31),
    CLOSE_TARGET_SLO_WRITER_EPOCH(32);

    private final int wireValue;

    ControlOperationKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlOperationKind fromWire(final long value) {
        for (ControlOperationKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ControlOperationKind: " + value);
    }
}
