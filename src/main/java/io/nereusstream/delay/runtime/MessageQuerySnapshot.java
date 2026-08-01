package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.StableCode;

import java.util.Objects;

/**
 * Bounded local projection of one current Message generation.
 *
 * <p>This is intentionally not the wire-level V1 {@code MessageQueryResponseV1}:
 * it contains no tenant authorization result, receipt branch, safe destination
 * binding, evidence reference or retention decision.</p>
 */
public record MessageQuerySnapshot(
        DelayMessageId messageId,
        int generation,
        long stateVersion,
        GenerationAggregateState state,
        long deliverAtEpochMs,
        long expireAtEpochMs,
        PayloadAvailability payloadAvailability,
        boolean possibleDestinationDuplicate,
        StableCode terminalCode) {
    public MessageQuerySnapshot {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(payloadAvailability, "payloadAvailability");
        if (generation < 0 || stateVersion < 0 || deliverAtEpochMs < 0
                || expireAtEpochMs < deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid message query snapshot");
        }
        final boolean terminal = switch (state) {
            case PUBLISHED, HANDED_OFF, CANCELED, EXPIRED, DEAD_LETTER, SUPERSEDED -> true;
            default -> false;
        };
        if (terminal != (terminalCode != null)) {
            throw new IllegalArgumentException("terminal code presence does not match message state");
        }
    }

    public boolean terminal() {
        return terminalCode != null;
    }
}
