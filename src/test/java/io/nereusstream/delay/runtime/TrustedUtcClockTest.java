package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TrustedUtcClockTest {
    @Test
    void requiresHealthyStabilizationBeforeOpeningTimingGate() {
        final TrustedUtcClock clock = new TrustedUtcClock(new TrustedUtcClock.Config(20, 1_000, 0, 100));

        assertFalse(clock.observe(evidence(1_000, 1_005, 0)).qualified());
        final TrustedUtcInterval qualified = clock.observe(evidence(1_100, 1_105, 100_000_000));

        assertTrue(qualified.qualified());
        assertTrue(qualified.provesDue(1_100));
        assertTrue(qualified.provesBeforeExpiry(1_106));
        assertFalse(qualified.provesBeforeExpiry(1_105));
    }

    @Test
    void wideSampleAndWallClockStepCloseGateUntilFreshHealthySample() {
        final TrustedUtcClock clock = new TrustedUtcClock(new TrustedUtcClock.Config(10, 1_000, 5, 0));

        assertFalse(clock.observe(evidence(1_000, 1_020, 0)).qualified());
        assertTrue(clock.observe(evidence(1_000, 1_000, 0)).qualified());
        assertFalse(clock.observe(evidence(5_000, 5_000, 1_000_000_000)).qualified());
        assertTrue(clock.observe(evidence(6_000, 6_000, 2_000_000_000)).qualified());
    }

    @Test
    void staleOrRegressingMonotonicReadCannotProveTimingBoundary() {
        final TrustedUtcClock clock = new TrustedUtcClock(new TrustedUtcClock.Config(20, 50, 0, 0));
        clock.observe(evidence(1_000, 1_000, 1_000_000_000));

        assertFalse(clock.current(900_000_000).qualified());
        assertFalse(clock.current(1_051_000_000).qualified());
    }

    @Test
    void projectedIntervalWidensConservativelyWithConfiguredDivergence() {
        final TrustedUtcClock clock = new TrustedUtcClock(new TrustedUtcClock.Config(20, 1_000, 5, 0));
        clock.observe(evidence(1_000, 1_000, 0));

        final TrustedUtcInterval current = clock.current(100_000_000);

        assertTrue(current.qualified());
        assertTrue(current.earliestEpochMs() <= 1_100);
        assertTrue(current.latestEpochMs() >= 1_100);
        assertTrue(current.allowsAdmission(current.earliestEpochMs(), current.latestEpochMs() + 1));
    }

    @Test
    void uncertaintyBudgetIncludesBothDivergenceSides() {
        assertThrows(IllegalArgumentException.class,
                () -> new TrustedUtcClock.Config(9, 1_000, 5, 0));

        final TrustedUtcClock clock = new TrustedUtcClock(new TrustedUtcClock.Config(10, 1_000, 5, 0));
        assertFalse(clock.observe(evidence(1_000, 1_001, 0)).qualified());
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest, final long latest,
                                                       final long monotonicAnchorNs) {
        return new TrustedUtcIntervalEvidence(earliest, latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, new byte[]{1}, 1, 1,
                monotonicAnchorNs, new byte[TrustedUtcIntervalEvidence.HASH_LENGTH], 0, new byte[0]);
    }
}
