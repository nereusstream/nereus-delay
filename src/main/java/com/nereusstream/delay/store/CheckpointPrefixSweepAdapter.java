package com.nereusstream.delay.store;

/** Provider boundary for an externally authorized, exact bounded checkpoint-prefix sweep. */
@FunctionalInterface
public interface CheckpointPrefixSweepAdapter {
    CheckpointPrefixSweepResult sweep(CheckpointPrefixSweepRequest request);
}
