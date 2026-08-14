package io.nereusstream.delay.route;

/** Closed local Route-cache health projection. */
public enum RouteCacheHealth {
    HEALTHY,
    WATCH_GAP,
    SIGNATURE_INVALID,
    QUARANTINED,
    UNAVAILABLE,
    CLOSED
}
