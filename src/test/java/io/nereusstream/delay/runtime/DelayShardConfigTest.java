package io.nereusstream.delay.runtime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DelayShardConfigTest {
    @Test
    void legacyConstructorsCarryCompatibilityBrokerTimingBounds() {
        final DelayShardConfig config = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 3, 1, 2_000, 4_000);

        assertEquals(0, config.maxIngressBrokerTimestampDivergenceMs());
        assertEquals(20_000, config.maximumAdmissionMutationEnqueueAgeMs());
    }

    @Test
    void rejectsNegativeBrokerTimingBounds() {
        assertThrows(IllegalArgumentException.class, () -> new DelayShardConfig(
                10_000, 1, 20_000, 10, 100, 4, 3, 100, 10_000,
                3, 1, 2_000, 4_000, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> new DelayShardConfig(
                10_000, 1, 20_000, 10, 100, 4, 3, 100, 10_000,
                3, 1, 2_000, 4_000, 0, -1));
    }
}
