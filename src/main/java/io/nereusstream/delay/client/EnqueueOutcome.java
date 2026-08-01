package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.PreparedCommand;

import java.util.Objects;

/** Closed three-state ingress result. */
public record EnqueueOutcome(
        EnqueueStatus status,
        PreparedCommand preparedCommand,
        CommandQueuedReceipt receipt,
        int stableCode) {
    public EnqueueOutcome {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(preparedCommand, "preparedCommand");
        if (status == EnqueueStatus.QUEUED && receipt == null) {
            throw new IllegalArgumentException("QUEUED requires a receipt");
        }
        if (status != EnqueueStatus.QUEUED && receipt != null) {
            throw new IllegalArgumentException("non-queued outcome cannot carry a receipt");
        }
    }

    public static EnqueueOutcome queued(final PreparedCommand command, final CommandQueuedReceipt receipt) {
        return new EnqueueOutcome(EnqueueStatus.QUEUED, command, receipt, 0);
    }

    public static EnqueueOutcome definitelyNotQueued(final PreparedCommand command, final int stableCode) {
        return new EnqueueOutcome(EnqueueStatus.DEFINITELY_NOT_QUEUED, command, null, stableCode);
    }

    public static EnqueueOutcome uncertain(final PreparedCommand command, final int stableCode) {
        return new EnqueueOutcome(EnqueueStatus.ENQUEUE_UNCERTAIN, command, null, stableCode);
    }
}

