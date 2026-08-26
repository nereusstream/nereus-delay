package com.nereusstream.delay.runtime;

/** Registry {@code CurrentSendWorkKind} values. */
public enum CurrentSendWorkKind {
    NONE(1),
    TIMELINE(2),
    CLAIMED(3),
    PUBLISHING(4);

    private final int wireValue;

    CurrentSendWorkKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static CurrentSendWorkKind fromWire(final long value) {
        for (CurrentSendWorkKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown current send work kind: " + value);
    }
}
