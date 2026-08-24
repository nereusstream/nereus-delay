package com.nereusstream.delay.protocol;

public enum CommandType {
    SCHEDULE(1),
    PREPARE_LARGE_SCHEDULE(2),
    COMMIT_LARGE_SCHEDULE(3),
    CANCEL(4),
    RESCHEDULE(5);

    private final int wireValue;

    CommandType(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }
}
