package io.nereusstream.delay.store;

/** Provider boundary for deleting one exact, already-authorized checkpoint resource. */
@FunctionalInterface
public interface CheckpointDeleteAdapter {
    CheckpointDeleteResult delete(CheckpointDeleteRequest request);
}
