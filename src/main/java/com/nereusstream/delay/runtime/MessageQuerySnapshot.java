package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DlqExportState;
import com.nereusstream.delay.protocol.StableCode;
import java.util.Objects;

/**
 * Bounded local projection of one current Message generation.
 *
 * <p>This is intentionally not the wire-level {@code MessageQueryResponse}:
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
        StableCode terminalCode,
        DlqExportState dlqExportState) {
    /** Compatibility constructor for callers that do not expose DLQ state. */
    public MessageQuerySnapshot(
            final DelayMessageId messageId,
            final int generation,
            final long stateVersion,
            final GenerationAggregateState state,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final PayloadAvailability payloadAvailability,
            final boolean possibleDestinationDuplicate,
            final StableCode terminalCode) {
        this(
                messageId,
                generation,
                stateVersion,
                state,
                deliverAtEpochMs,
                expireAtEpochMs,
                payloadAvailability,
                possibleDestinationDuplicate,
                terminalCode,
                DlqExportState.NOT_CONFIGURED);
    }

    public MessageQuerySnapshot {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(payloadAvailability, "payloadAvailability");
        Objects.requireNonNull(dlqExportState, "dlqExportState");
        if (stateVersion < 0 || deliverAtEpochMs < 0 || expireAtEpochMs < deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid message query snapshot");
        }
        final boolean terminal =
                switch (state) {
                    case PUBLISHED, HANDED_OFF, CANCELED, EXPIRED, DEAD_LETTER, SUPERSEDED -> true;
                    default -> false;
                };
        if (terminal != (terminalCode != null)) {
            throw new IllegalArgumentException("terminal code presence does not match message state");
        }
        if (state != GenerationAggregateState.DEAD_LETTER && dlqExportState != DlqExportState.NOT_CONFIGURED) {
            throw new IllegalArgumentException("only a dead-letter generation may have a DLQ export state");
        }
    }

    public boolean terminal() {
        return terminalCode != null;
    }
}
