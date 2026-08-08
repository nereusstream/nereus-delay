package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PublishAttemptLedgerTest {
    @Test
    void decodeRejectsTruncatedNumericSuffixesAsValidationErrors() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final PublishAttemptLedger ledger = PublishAttemptLedger.publishing(
                DelayMessageId.random(shardId), 0, Bytes.sha256(Bytes.utf8("publish-attempt")),
                Bytes.sha256(Bytes.utf8("claim")), Long.MIN_VALUE, 1,
                DestinationLaneId.derive(Bytes.utf8("publish-attempt-lane")), new byte[16],
                new byte[]{1}, new byte[16], Bytes.sha256(Bytes.utf8("prepared")), new byte[]{2}, new byte[]{3});
        final byte[] encoded = ledger.encode();

        for (int length = 0; length < encoded.length; length++) {
            final byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(IllegalArgumentException.class, () -> PublishAttemptLedger.decode(truncated),
                    "truncated publish attempt ledger length=" + length);
        }
        assertEquals(ledger, PublishAttemptLedger.decode(encoded));
    }

    @Test
    void v2LedgerRoundTripsAnIndependentRetryWindowAndKeepsV1Compatibility() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final PublishAttemptLedger ledger = PublishAttemptLedger.publishingWithRetryWindow(
                DelayMessageId.random(shardId), 0, Bytes.sha256(Bytes.utf8("publish-attempt-v2")),
                Bytes.sha256(Bytes.utf8("claim-v2")), 1, 1,
                DestinationLaneId.derive(Bytes.utf8("publish-attempt-lane-v2")), new byte[16], new byte[]{1},
                new byte[16], Bytes.sha256(Bytes.utf8("prepared-v2")), canonicalAdmissionBytes(), 2_001, 5_000,
                new byte[]{3});

        assertTrue(ledger.hasRetryWindow());
        assertEquals(2_001, ledger.firstAttemptAtEpochMs());
        assertEquals(5_000, ledger.retryDeadlineEpochMs());
        assertEquals(ledger, PublishAttemptLedger.decode(ledger.encode()));
        assertEquals(2, java.nio.ByteBuffer.wrap(ledger.encode()).getInt());

        final PublishAttemptLedger legacy = PublishAttemptLedger.publishing(
                ledger.delayMessageId(), 0, Bytes.sha256(Bytes.utf8("publish-attempt-v1")),
                Bytes.sha256(Bytes.utf8("claim-v1")), 1, 1, ledger.laneId(), new byte[16], new byte[]{1},
                new byte[16], Bytes.sha256(Bytes.utf8("prepared-v1")), canonicalAdmissionBytes(), new byte[]{4});
        assertFalse(legacy.hasRetryWindow());
        assertEquals(legacy, PublishAttemptLedger.decode(legacy.encode()));
        assertEquals(1, java.nio.ByteBuffer.wrap(legacy.encode()).getInt());
        assertThrows(IllegalStateException.class, legacy::firstAttemptAtEpochMs);
    }

    private static byte[] canonicalAdmissionBytes() {
        return Bytes.utf8("canonical-admission-placeholder");
    }
}
