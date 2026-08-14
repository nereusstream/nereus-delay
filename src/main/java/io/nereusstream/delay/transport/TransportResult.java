package io.nereusstream.delay.transport;

/** Closed result union at the transport SPI boundary. */
public interface TransportResult {
    /** Physical attempt binding supplied by the guarded transport bridge. */
    PhysicalEnqueueAttemptId physicalAttemptId();
}
