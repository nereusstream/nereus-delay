package io.nereusstream.delay.adapter;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CloseGuardTest {
    @Test
    void failedCloseRemainsFencedButCanBeRetried() {
        final CloseGuard guard = new CloseGuard();
        final AtomicInteger attempts = new AtomicInteger();

        assertThrows(IllegalStateException.class, () -> guard.close(() -> {
            if (attempts.getAndIncrement() == 0) {
                throw new IllegalStateException("close failed");
            }
        }));
        assertTrue(guard.isClosed());

        guard.close(attempts::incrementAndGet);
        guard.close(attempts::incrementAndGet);
        assertEquals(2, attempts.get());
    }
}
