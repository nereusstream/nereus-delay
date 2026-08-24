package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.Objects;

/**
 * Local Worker clock guard for the V1 Trusted UTC Interval contract.
 *
 * <p>The guard consumes approved interval samples and advances them with a
 * caller-supplied monotonic reading.  It never reads {@code currentTimeMillis}
 * itself, so a caller cannot accidentally turn an unqualified wall clock into
 * a due or pre-expiry decision.  The external time source/signature authority
 * remains outside this class.</p>
 */
public final class TrustedUtcClock {
    private static final long NANOS_PER_MILLISECOND = 1_000_000L;

    private final Config config;
    private TrustedUtcIntervalEvidence sample;
    private boolean sampleHealthy;
    private boolean hasHealthySince;
    private long healthySinceMonotonicNs;
    private boolean qualified;

    public TrustedUtcClock(final Config config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * Installs one approved sample and returns its projected interval.  A
     * wide, regressing, stale or step-inconsistent sample closes the gate and
     * starts a fresh stabilization window instead of throwing a business
     * result or making an unsafe timing decision.
     */
    public synchronized TrustedUtcInterval observe(final TrustedUtcIntervalEvidence evidence) {
        Objects.requireNonNull(evidence, "evidence");
        boolean healthy = intervalWithinBound(evidence);
        if (healthy && sample != null && sampleHealthy) {
            healthy = sampleIsFreshAt(evidence.monotonicAnchorNs())
                    && monotonicAndWallProgressIsConsistent(sample, evidence);
        }
        sample = evidence;
        sampleHealthy = healthy;
        if (!healthy) {
            qualified = false;
            hasHealthySince = false;
        } else {
            if (!hasHealthySince) {
                healthySinceMonotonicNs = evidence.monotonicAnchorNs();
                hasHealthySince = true;
            }
            qualified = config.stabilizationWindowMs() == 0
                    || elapsedMilliseconds(evidence.monotonicAnchorNs(), healthySinceMonotonicNs)
                            >= config.stabilizationWindowMs();
        }
        return current(evidence.monotonicAnchorNs());
    }

    /** Returns the conservative interval projected at a monotonic reading. */
    public synchronized TrustedUtcInterval current(final long monotonicNowNs) {
        if (sample == null) {
            return nullInterval();
        }
        final long elapsedNs = elapsedNanoseconds(monotonicNowNs, sample.monotonicAnchorNs());
        if (elapsedNs < 0) {
            return projected(sample, false, 0);
        }
        final long elapsedMs = elapsedNs / NANOS_PER_MILLISECOND;
        final long earliest = subtractFloor(
                addSaturating(sample.earliestEpochMs(), elapsedMs), config.maxWallMonotonicDivergenceMs());
        final long latest =
                addSaturating(addSaturating(sample.latestEpochMs(), elapsedMs), config.maxWallMonotonicDivergenceMs());
        final boolean fresh = elapsedMs <= config.maxSampleAgeMs();
        final boolean widthWithinBound = latest - earliest <= config.maxUncertaintyMs();
        return new TrustedUtcInterval(
                earliest, latest, qualified && sampleHealthy && fresh && widthWithinBound, sample);
    }

    public synchronized boolean isQualified(final long monotonicNowNs) {
        return current(monotonicNowNs).qualified();
    }

    public Config config() {
        return config;
    }

    private boolean intervalWithinBound(final TrustedUtcIntervalEvidence evidence) {
        final long rawWidth = evidence.latestEpochMs() - evidence.earliestEpochMs();
        final long divergenceWidth;
        try {
            divergenceWidth = Math.multiplyExact(config.maxWallMonotonicDivergenceMs(), 2L);
            return Math.addExact(rawWidth, divergenceWidth) <= config.maxUncertaintyMs();
        } catch (ArithmeticException overflow) {
            return false;
        }
    }

    private boolean sampleIsFreshAt(final long monotonicNowNs) {
        final long elapsedNs = elapsedNanoseconds(monotonicNowNs, sample.monotonicAnchorNs());
        return elapsedNs >= 0 && elapsedNs / NANOS_PER_MILLISECOND <= config.maxSampleAgeMs();
    }

    private boolean monotonicAndWallProgressIsConsistent(
            final TrustedUtcIntervalEvidence prior, final TrustedUtcIntervalEvidence next) {
        final long monotonicDeltaNs = elapsedNanoseconds(next.monotonicAnchorNs(), prior.monotonicAnchorNs());
        if (monotonicDeltaNs < 0) {
            return false;
        }
        if (monotonicDeltaNs == 0) {
            return prior.equals(next);
        }
        final long priorMidpoint = midpoint(prior.earliestEpochMs(), prior.latestEpochMs());
        final long nextMidpoint = midpoint(next.earliestEpochMs(), next.latestEpochMs());
        final long wallDelta;
        try {
            wallDelta = Math.subtractExact(nextMidpoint, priorMidpoint);
        } catch (ArithmeticException overflow) {
            return false;
        }
        final long monotonicDeltaMs = monotonicDeltaNs / NANOS_PER_MILLISECOND;
        final long divergence;
        try {
            divergence = Math.subtractExact(wallDelta, monotonicDeltaMs);
        } catch (ArithmeticException overflow) {
            return false;
        }
        return divergence <= config.maxWallMonotonicDivergenceMs()
                && divergence >= -config.maxWallMonotonicDivergenceMs();
    }

    private TrustedUtcInterval projected(
            final TrustedUtcIntervalEvidence evidence, final boolean isQualified, final long elapsedMs) {
        final long earliest = subtractFloor(
                addSaturating(evidence.earliestEpochMs(), elapsedMs), config.maxWallMonotonicDivergenceMs());
        final long latest = addSaturating(
                addSaturating(evidence.latestEpochMs(), elapsedMs), config.maxWallMonotonicDivergenceMs());
        return new TrustedUtcInterval(earliest, latest, isQualified, evidence);
    }

    private TrustedUtcInterval nullInterval() {
        final TrustedUtcIntervalEvidence empty = new TrustedUtcIntervalEvidence(
                0,
                0,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                new byte[] {1},
                0,
                0,
                0,
                new byte[TrustedUtcIntervalEvidence.HASH_LENGTH],
                0,
                new byte[0]);
        return new TrustedUtcInterval(0, 0, false, empty);
    }

    private static long elapsedNanoseconds(final long current, final long prior) {
        try {
            return Math.subtractExact(current, prior);
        } catch (ArithmeticException overflow) {
            return -1;
        }
    }

    private static long elapsedMilliseconds(final long current, final long prior) {
        final long elapsedNs = elapsedNanoseconds(current, prior);
        return elapsedNs < 0 ? 0 : elapsedNs / NANOS_PER_MILLISECOND;
    }

    private static long midpoint(final long lower, final long upper) {
        return lower + (upper - lower) / 2;
    }

    private static long addSaturating(final long value, final long amount) {
        return value > Long.MAX_VALUE - amount ? Long.MAX_VALUE : value + amount;
    }

    private static long subtractFloor(final long value, final long amount) {
        return value <= amount ? 0 : value - amount;
    }

    /** Required Worker clock safety bounds; values are deliberately not defaulted by V1. */
    public record Config(
            long maxUncertaintyMs, long maxSampleAgeMs, long maxWallMonotonicDivergenceMs, long stabilizationWindowMs) {
        public Config {
            if (maxUncertaintyMs < 0
                    || maxSampleAgeMs <= 0
                    || maxWallMonotonicDivergenceMs < 0
                    || stabilizationWindowMs < 0) {
                throw new IllegalArgumentException("invalid trusted UTC clock bounds");
            }
            try {
                if (Math.multiplyExact(maxWallMonotonicDivergenceMs, 2L) > maxUncertaintyMs) {
                    throw new IllegalArgumentException("trusted UTC uncertainty must cover wall/monotonic divergence");
                }
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("trusted UTC divergence bound overflows", overflow);
            }
        }
    }
}
