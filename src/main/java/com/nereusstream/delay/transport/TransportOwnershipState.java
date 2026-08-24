package com.nereusstream.delay.transport;

/** Monotonic ownership state for one physical transport attempt. */
public enum TransportOwnershipState {
    AVAILABLE,
    LIBRARY_OWNED,
    INVALID
}
