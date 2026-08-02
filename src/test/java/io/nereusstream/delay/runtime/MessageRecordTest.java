package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.OrderingMode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageRecordTest {
    @Test
    void decodeRejectsEveryCanonicalPrefixTruncationAsValidationError() {
        final MessageRecord record = new MessageRecord(MessageStatus.SCHEDULED, 0, 1,
                2_000, 5_000, DestinationLaneId.derive(Bytes.utf8("message-record-truncation")),
                OrderingMode.BEST_EFFORT, Bytes.utf8("payload"), Bytes.utf8("source-position"));
        final byte[] encoded = record.encode();

        for (int length = 0; length < encoded.length; length++) {
            final byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(IllegalArgumentException.class, () -> MessageRecord.decode(truncated),
                    "truncated message record length=" + length);
        }
    }
}
