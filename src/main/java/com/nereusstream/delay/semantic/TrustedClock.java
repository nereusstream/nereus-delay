package com.nereusstream.delay.semantic;

/** Trusted UTC source used by zero-I/O preparation and UUIDv7 freshness gates. */
@FunctionalInterface
public interface TrustedClock {
    long nowEpochMs();
}
