package com.nereusstream.delay.protocol;

/** Closed SLO objective registry. */
public enum SloObjectiveName {
    COMMAND_QUEUED_LATENCY(1),
    COMMAND_APPLIED_LATENCY(2),
    DUE_ADMISSION_LAG(3),
    NATIVE_HANDOFF_ACK_LAG(4),
    QUERY_LATENCY(5),
    OWNERSHIP_FAILOVER_RTO(6),
    LOCAL_DISK_LOSS_RTO(7),
    CHECKPOINT_AGE(8),
    SOURCE_RETENTION_TIME_MARGIN(9),
    SOURCE_RETENTION_BYTE_MARGIN(10),
    POSSIBLE_DUPLICATE_WINDOW(11),
    HEALTHY_LANE_DISCOVERY_AGE(12),
    HEALTHY_LANE_SERVICE_GAP(13),
    LANE_RECOVERY_READY_RTO(14);

    private final int wireValue;

    SloObjectiveName(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    /** Returns the direction fixed by this closed objective branch. */
    public SloThresholdDirection requiredDirection() {
        return switch (this) {
            case SOURCE_RETENTION_TIME_MARGIN, SOURCE_RETENTION_BYTE_MARGIN -> SloThresholdDirection.AT_LEAST;
            default -> SloThresholdDirection.AT_MOST;
        };
    }

    /** Returns the measurement unit fixed by this closed objective branch. */
    public SloThresholdUnit requiredUnit() {
        return this == SOURCE_RETENTION_BYTE_MARGIN ? SloThresholdUnit.BYTES : SloThresholdUnit.MILLISECONDS;
    }

    public static SloObjectiveName fromWire(final long value) {
        for (SloObjectiveName objective : values()) {
            if (objective.wireValue == value) {
                return objective;
            }
        }
        throw new IllegalArgumentException("unknown SloObjectiveName: " + value);
    }
}
