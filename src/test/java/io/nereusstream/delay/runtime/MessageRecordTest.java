package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.OrderingMode;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @Test
    void scalarMessageRecordUsesLegacyVersionUntilTypedRuntimeReplacesIt() {
        final MessageRecord legacy = new MessageRecord(MessageStatus.SCHEDULED, 0, 1,
                2_000, 5_000, DestinationLaneId.derive(Bytes.utf8("message-record-legacy")),
                OrderingMode.BEST_EFFORT, Bytes.utf8("payload"), Bytes.utf8("source-position"));
        assertEquals(3, ByteBuffer.wrap(legacy.encode()).getInt());
        assertEquals(legacy, MessageRecord.decode(legacy.encode()));
        assertEquals(3, ByteBuffer.wrap(MessageRecord.decode(legacy.encode()).encode()).getInt());

        final MessageRecord typed = legacy.withRuntimeIndex(GenerationRuntimeIndex.timeline(
                GenerationAggregateState.SCHEDULED,
                TimelineWorkRef.initial(io.nereusstream.delay.store.KeyCodec.timelineDue(legacy.laneId(),
                        legacy.deliverAtEpochMs(), Bytes.concat(new byte[]{1}, new byte[8]),
                        io.nereusstream.delay.protocol.DelayMessageId.random(
                                new io.nereusstream.delay.protocol.ShardId(
                                        new io.nereusstream.delay.protocol.RouteIncarnation(new byte[16]), 0)),
                        legacy.generation()), legacy.deliverAtEpochMs(), legacy.stateVersion()),
                legacy.runtimeIndex().runtimeRevision()));
        assertEquals(4, ByteBuffer.wrap(typed.encode()).getInt());
        assertEquals(typed, MessageRecord.decode(typed.encode()));
    }
}
