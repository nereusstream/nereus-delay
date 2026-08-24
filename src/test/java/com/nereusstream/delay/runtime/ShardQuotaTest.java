package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ShardQuotaTest {
    @Test
    void singleChargeOperationsRejectNegativeBytes() {
        final ShardQuota quota = new ShardQuota(2, 10, 2, 10, 1, 1);

        assertThrows(IllegalArgumentException.class, () -> quota.addSchedule(-1, false));
        assertThrows(IllegalArgumentException.class, () -> quota.removeSchedule(-1));
        assertThrows(IllegalArgumentException.class, () -> quota.addReservation(-1, false));
        assertThrows(IllegalArgumentException.class, () -> quota.removeReservation(-1));
        assertThrows(IllegalArgumentException.class, () -> quota.commitReservation(-1));
    }
}
