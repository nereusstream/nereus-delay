package com.nereusstream.delay.protocol;

/** Closed public Message generation states. */
public enum MessageGenerationState {
    SCHEDULED(1),
    CLAIMED(2),
    PUBLISHING(3),
    RETRY_WAIT(4),
    UNCERTAIN(5),
    PUBLISHED(6),
    HANDED_OFF(7),
    CANCELED(8),
    EXPIRED(9),
    DEAD_LETTER(10),
    SUPERSEDED(11);

    private final int wireValue;

    MessageGenerationState(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static MessageGenerationState fromWire(final long value) {
        for (MessageGenerationState state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown MessageGenerationState: " + value);
    }

    public boolean active() {
        return switch (this) {
            case SCHEDULED, CLAIMED, PUBLISHING, RETRY_WAIT, UNCERTAIN -> true;
            default -> false;
        };
    }

    public boolean terminal() {
        return switch (this) {
            case PUBLISHED, HANDED_OFF, CANCELED, EXPIRED, DEAD_LETTER, SUPERSEDED -> true;
            default -> false;
        };
    }
}
