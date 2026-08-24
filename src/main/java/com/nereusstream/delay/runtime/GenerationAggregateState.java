package com.nereusstream.delay.runtime;

/** Exact registry {@code MessageGenerationStateV1} values used by the runtime index. */
public enum GenerationAggregateState {
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

    GenerationAggregateState(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static GenerationAggregateState fromWire(final long value) {
        for (GenerationAggregateState state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown generation aggregate state: " + value);
    }

    public static GenerationAggregateState fromMessageStatus(final MessageStatus status) {
        return switch (status) {
            case SCHEDULED -> SCHEDULED;
            case CLAIMED -> CLAIMED;
            case PUBLISHING -> PUBLISHING;
            case UNCERTAIN -> UNCERTAIN;
            case PUBLISHED -> PUBLISHED;
            case HANDED_OFF -> HANDED_OFF;
            case CANCELED -> CANCELED;
            case EXPIRED -> EXPIRED;
            case DEAD_LETTER -> DEAD_LETTER;
            case SUPERSEDED -> SUPERSEDED;
        };
    }
}
