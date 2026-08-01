package io.nereusstream.delay.runtime;

public enum MessageStatus {
    SCHEDULED(1),
    CANCELED(2),
    SUPERSEDED(3),
    PUBLISHING(4),
    PUBLISHED(5),
    UNCERTAIN(6),
    EXPIRED(7),
    DEAD_LETTER(8),
    /** Local current-generation projection for a reversible inflight Claim. */
    CLAIMED(9);

    private final int wireValue;

    MessageStatus(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static MessageStatus fromWire(final int value) {
        for (MessageStatus status : values()) {
            if (status.wireValue == value) {
                return status;
            }
        }
        throw new IllegalArgumentException("unknown message status: " + value);
    }
}
