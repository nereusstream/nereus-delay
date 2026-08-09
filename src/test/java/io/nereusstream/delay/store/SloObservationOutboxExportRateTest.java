package io.nereusstream.delay.store;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SloObservationOutboxExportRateTest {
    @Test
    void rejectsUsageAboveRecordOrByteBudgetUntilWindowResets() {
        final AtomicLong now = new AtomicLong(10);
        final SloObservationOutboxExportRate rate = new SloObservationOutboxExportRate(
                new SloObservationOutboxExportRate.Limits(2, 100), now::get);

        assertTrue(rate.tryAcquire(1, 60));
        assertFalse(rate.tryAcquire(2, 1));
        assertFalse(rate.tryAcquire(1, 41));
        now.set(1_000_000_010L);
        assertTrue(rate.tryAcquire(2, 100));
    }

    @Test
    void rejectsARegressingMonotonicClock() {
        final AtomicLong now = new AtomicLong(100);
        final SloObservationOutboxExportRate rate = new SloObservationOutboxExportRate(
                new SloObservationOutboxExportRate.Limits(1, 100), now::get);
        assertTrue(rate.tryAcquire(1, 1));
        now.set(99);
        assertThrows(IllegalStateException.class, () -> rate.tryAcquire(1, 1));
    }
}
