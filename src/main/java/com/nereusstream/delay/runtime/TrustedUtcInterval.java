package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.Objects;

/**
 * The current conservative UTC interval exposed by {@link TrustedUtcClock}.
 * A caller may use the interval for timing decisions only when
 * {@link #qualified()} is true.
 */
public record TrustedUtcInterval(
        long earliestEpochMs, long latestEpochMs, boolean qualified, TrustedUtcIntervalEvidence evidence) {
    public TrustedUtcInterval {
        if (earliestEpochMs < 0 || latestEpochMs < earliestEpochMs) {
            throw new IllegalArgumentException("invalid trusted UTC interval projection");
        }
        Objects.requireNonNull(evidence, "evidence");
    }

    public long widthMs() {
        return latestEpochMs - earliestEpochMs;
    }

    /** Returns true only when this qualified interval proves that the due boundary has passed. */
    public boolean provesDue(final long actionAtEpochMs) {
        return qualified && actionAtEpochMs >= 0 && earliestEpochMs >= actionAtEpochMs;
    }

    /** Returns true only when this qualified interval proves admission is still before expiry. */
    public boolean provesBeforeExpiry(final long expireAtEpochMs) {
        return qualified && expireAtEpochMs >= 0 && latestEpochMs < expireAtEpochMs;
    }

    /** Applies the strict due and pre-expiry gates together. */
    public boolean allowsAdmission(final long actionAtEpochMs, final long expireAtEpochMs) {
        return provesDue(actionAtEpochMs) && provesBeforeExpiry(expireAtEpochMs);
    }
}
