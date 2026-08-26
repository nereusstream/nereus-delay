package com.nereusstream.delay.protocol;

/** Closed checkpoint upload intent states from Registry §13. */
public enum CheckpointUploadState {
    PENDING_UPLOAD(1),
    PUBLISHED(2),
    REAPING(3);

    private final int wireValue;

    CheckpointUploadState(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static CheckpointUploadState fromWire(final long value) {
        for (CheckpointUploadState state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown CheckpointUploadState: " + value);
    }
}
