package io.nereusstream.delay.store;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObjectStoreProviderOwnershipTrackerTest {
    @Test
    void fenceRequiresTheCompleteOperationToCloseBeforeLocalQuiescence() {
        final MutableClock clock = new MutableClock(1_000);
        final ObjectStoreProviderOwnershipTracker tracker =
                new ObjectStoreProviderOwnershipTracker(clock, 100);
        final ObjectStoreProviderOwnershipTracker.Operation operation = tracker.begin();

        assertEquals(1, tracker.observe().activeOperationCount());
        tracker.beginQuiescence();
        assertFalse(tracker.observe().locallyQuiescent());
        assertThrows(IllegalStateException.class, tracker::requireLocallyQuiescent);

        operation.complete();
        final ObjectStoreProviderOwnershipTracker.Observation observation = tracker.requireLocallyQuiescent();
        assertFalse(observation.acceptingNewOperations());
        assertEquals(0, observation.activeOperationCount());
        assertEquals(1_000, observation.lastOperationClosedAtEpochMs());
        assertEquals(32, observation.observationDigest().length);
        assertThrows(IllegalStateException.class, tracker::begin);
    }

    @Test
    void uncertainCloseRetainsTheConfiguredProviderHorizon() {
        final MutableClock clock = new MutableClock(2_000);
        final ObjectStoreProviderOwnershipTracker tracker =
                new ObjectStoreProviderOwnershipTracker(clock, 100);
        final ObjectStoreProviderOwnershipTracker.Operation operation = tracker.begin();

        clock.setMillis(2_010);
        operation.uncertain();
        tracker.beginQuiescence();
        assertEquals(2_110, tracker.observe().uncertainUntilEpochMs());
        assertThrows(IllegalStateException.class, tracker::requireLocallyQuiescent);

        clock.setMillis(2_109);
        assertThrows(IllegalStateException.class, tracker::requireLocallyQuiescent);
        clock.setMillis(2_110);
        assertTrue(tracker.requireLocallyQuiescent().locallyQuiescent());
    }

    @Test
    void operationCloseIsOneShotAndObservationDigestBindsState() {
        final MutableClock clock = new MutableClock(3_000);
        final ObjectStoreProviderOwnershipTracker tracker =
                new ObjectStoreProviderOwnershipTracker(clock, 10);
        final ObjectStoreProviderOwnershipTracker.Operation operation = tracker.begin();
        final byte[] activeDigest = tracker.observe().observationDigest();

        operation.complete();
        assertFalse(Arrays.equals(activeDigest, tracker.observe().observationDigest()));
        assertThrows(IllegalStateException.class, operation::complete);
    }

    private static final class MutableClock extends Clock {
        private long millis;

        private MutableClock(final long millis) {
            this.millis = millis;
        }

        private void setMillis(final long millis) {
            this.millis = millis;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(final ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long millis() {
            return millis;
        }
    }
}
