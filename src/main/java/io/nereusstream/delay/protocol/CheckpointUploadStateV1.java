package io.nereusstream.delay.protocol;

/** Closed checkpoint upload intent states from Registry §13. */
public enum CheckpointUploadStateV1 {
    PENDING_UPLOAD(1),
    PUBLISHED(2),
    REAPING(3);

    private final int wireValue;

    CheckpointUploadStateV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static CheckpointUploadStateV1 fromWire(final long value) {
        for (CheckpointUploadStateV1 state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown CheckpointUploadStateV1: " + value);
    }
}
