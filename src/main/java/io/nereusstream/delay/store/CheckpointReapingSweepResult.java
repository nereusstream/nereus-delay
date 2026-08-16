package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;

import java.util.Objects;

/** Exact REAPING intent and provider sweep receipt returned by the bounded coordinator. */
public record CheckpointReapingSweepResult(
        CheckpointUploadIntentV1 reapingIntent,
        CheckpointPrefixSweepResult prefixSweep) {
    public CheckpointReapingSweepResult {
        Objects.requireNonNull(reapingIntent, "reapingIntent");
        if (reapingIntent.state() != CheckpointUploadStateV1.REAPING) {
            throw new IllegalArgumentException("checkpoint reaping result requires a REAPING intent");
        }
        Objects.requireNonNull(prefixSweep, "prefixSweep");
    }
}
