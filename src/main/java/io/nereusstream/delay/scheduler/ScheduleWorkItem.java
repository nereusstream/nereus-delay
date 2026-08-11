package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;

import java.util.Objects;

/** Bounded scheduler snapshot; a visit never waits for a Broker future. */
public record ScheduleWorkItem(
        DestinationLaneId laneId,
        DelayMessageId messageId,
        int generation,
        long eligibleAtEpochMs,
        long accountedBytes) {
    public ScheduleWorkItem {
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(messageId, "messageId");
        if (eligibleAtEpochMs < 0 || accountedBytes <= 0) {
            throw new IllegalArgumentException("invalid scheduler work item");
        }
    }
}
