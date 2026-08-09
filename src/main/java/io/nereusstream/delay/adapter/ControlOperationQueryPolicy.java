package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

import java.util.Objects;

/** Immutable policy for a Control Operation receipt query boundary. */
public record ControlOperationQueryPolicy(long policyVersion, long queryWindowMs) {
    public ControlOperationQueryPolicy {
        if (policyVersion <= 0 || queryWindowMs < 0) {
            throw new IllegalArgumentException("control operation query policy must be positive and bounded");
        }
    }

    /**
     * Computes the only V1 control receipt boundary permitted by this policy.
     * A wrapped timestamp would make an otherwise valid receipt immediately
     * expire, so overflow is an integrity/configuration failure.
     */
    public long queryUntil(final TrustedUtcIntervalEvidence registeredAt) {
        Objects.requireNonNull(registeredAt, "registeredAt");
        try {
            return Math.addExact(registeredAt.latestEpochMs(), queryWindowMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("control operation query boundary overflows epoch milliseconds",
                    overflow);
        }
    }
}
