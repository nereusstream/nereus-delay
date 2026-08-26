package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import java.util.Objects;

/** Exact REAPING intent and provider sweep receipt returned by the bounded coordinator. */
public record CheckpointReapingSweepResult(
        CheckpointUploadIntent reapingIntent, CheckpointPrefixSweepResult prefixSweep) {
    public CheckpointReapingSweepResult {
        Objects.requireNonNull(reapingIntent, "reapingIntent");
        if (reapingIntent.state() != CheckpointUploadState.REAPING) {
            throw new IllegalArgumentException("checkpoint reaping result requires a REAPING intent");
        }
        Objects.requireNonNull(prefixSweep, "prefixSweep");
    }
}
