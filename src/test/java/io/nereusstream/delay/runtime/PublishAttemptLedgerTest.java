package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
