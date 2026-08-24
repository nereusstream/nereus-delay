package com.nereusstream.delay.adapter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;

class ControlOperationQueryPolicyTest {
    @Test
    void derivesBoundaryFromTrustedRegistrationUpperBound() {
        final TrustedUtcIntervalEvidence registeredAt = evidence(2_000);
        assertEquals(7_000, new ControlOperationQueryPolicy(9, 5_000).queryUntil(registeredAt));
    }

    @Test
    void rejectsBoundaryOverflowInsteadOfWrapping() {
        assertThrows(IllegalArgumentException.class, () -> new ControlOperationQueryPolicy(9, 1)
                .queryUntil(evidence(Long.MAX_VALUE)));
    }

    private static TrustedUtcIntervalEvidence evidence(final long latestEpochMs) {
        return new TrustedUtcIntervalEvidence(
                latestEpochMs,
                latestEpochMs,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("control-policy-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("control-policy-evidence")),
                0,
                null);
    }
}
