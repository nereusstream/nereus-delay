package com.nereusstream.delay.transport;

/** One-shot, process-local capability authorizing a transport handoff. */
public interface TransportOwnershipPermit extends AutoCloseable {
    PhysicalEnqueueAttemptId physicalAttemptId();

    /** Transfers AVAILABLE to LIBRARY_OWNED exactly once. */
    boolean tryTransferToLibraryOwnership();

    TransportOwnershipState state();

    @Override
    void close();
}
