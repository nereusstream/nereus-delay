package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.SourcePosition;
import java.util.Objects;

/** Immutable policy for the full command-result retention boundary. */
public record CommandResultRetentionPolicy(long policyVersion, long fullCommandResultRetentionMs) {
    public CommandResultRetentionPolicy {
        if (policyVersion <= 0 || fullCommandResultRetentionMs <= 0) {
            throw new IllegalArgumentException("command result retention policy must be positive and bounded");
        }
    }

    /**
     * Computes the V1 full-result boundary from the first result Source
     * Position. Overflow is a configuration/integrity failure, never a
     * wrapped or saturated retention deadline.
     */
    public long retainUntil(final SourcePosition firstSourcePosition) {
        Objects.requireNonNull(firstSourcePosition, "firstSourcePosition");
        try {
            return Math.addExact(firstSourcePosition.brokerPersistenceTimeEpochMs(), fullCommandResultRetentionMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("full result retention boundary overflows epoch milliseconds", overflow);
        }
    }
}
