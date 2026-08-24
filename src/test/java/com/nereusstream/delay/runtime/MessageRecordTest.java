package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MessageRecordTest {
    @Test
    void decodeRejectsEveryCanonicalPrefixTruncationAsValidationError() {
        final MessageRecord record = new MessageRecord(
                MessageStatus.SCHEDULED,
                0,
                1,
                2_000,
                5_000,
                DestinationLaneId.derive(Bytes.utf8("message-record-truncation")),
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("payload"),
                sourcePosition());
        final byte[] encoded = record.encode();

        for (int length = 0; length < encoded.length; length++) {
            final byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> MessageRecord.decode(truncated),
                    "truncated message record length=" + length);
        }
    }

    @Test
    void scalarMessageRecordUsesLegacyVersionUntilTypedRuntimeReplacesIt() {
        final MessageRecord legacy = new MessageRecord(
                MessageStatus.SCHEDULED,
                0,
                1,
                2_000,
                5_000,
                DestinationLaneId.derive(Bytes.utf8("message-record-legacy")),
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("payload"),
                sourcePosition());
        assertEquals(3, ByteBuffer.wrap(legacy.encode()).getInt());
        assertEquals(legacy, MessageRecord.decode(legacy.encode()));
        assertEquals(
                3,
                ByteBuffer.wrap(MessageRecord.decode(legacy.encode()).encode()).getInt());

        final MessageRecord typed = legacy.withRuntimeIndex(GenerationRuntimeIndex.timeline(
                GenerationAggregateState.SCHEDULED,
                TimelineWorkRef.initial(
                        com.nereusstream.delay.store.KeyCodec.timelineDue(
                                legacy.laneId(),
                                legacy.deliverAtEpochMs(),
                                Bytes.concat(new byte[] {1}, new byte[8]),
                                com.nereusstream.delay.protocol.DelayMessageId.random(
                                        new com.nereusstream.delay.protocol.ShardId(
                                                new com.nereusstream.delay.protocol.RouteIncarnation(new byte[16]), 0)),
                                legacy.generation()),
                        legacy.deliverAtEpochMs(),
                        legacy.stateVersion()),
                legacy.runtimeIndex().runtimeRevision()));
        assertEquals(4, ByteBuffer.wrap(typed.encode()).getInt());
        assertEquals(typed, MessageRecord.decode(typed.encode()));
    }

    @Test
    void scalarMessageRecordPreservesHighBitGenerationBits() {
        final int highBit = (int) 0x8000_0000L;
        final MessageRecord record = new MessageRecord(
                MessageStatus.SCHEDULED,
                highBit,
                1,
                2_000,
                5_000,
                DestinationLaneId.derive(Bytes.utf8("message-record-high-bit")),
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("payload"),
                sourcePosition());

        final MessageRecord decoded = MessageRecord.decode(record.encode());
        assertEquals(highBit, decoded.generation());
        assertEquals(0x8000_0000L, Integer.toUnsignedLong(decoded.generation()));
    }

    @Test
    void typedRuntimeCannotDisagreeWithMessageStatus() {
        final MessageRecord base = new MessageRecord(
                MessageStatus.SCHEDULED,
                0,
                1,
                2_000,
                5_000,
                DestinationLaneId.derive(Bytes.utf8("message-record-status-fence")),
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("payload"),
                sourcePosition());
        final GenerationRuntimeIndex terminal =
                GenerationRuntimeIndex.none(GenerationAggregateState.PUBLISHED, java.util.List.of(), 1, 0, false, 2);
        assertThrows(IllegalArgumentException.class, () -> base.withRuntimeIndex(terminal));
    }

    @Test
    void handedOffStatusUsesTheRegisteredTerminalAggregateProjection() {
        final MessageRecord base = new MessageRecord(
                MessageStatus.HANDED_OFF,
                0,
                1,
                2_000,
                5_000,
                DestinationLaneId.derive(Bytes.utf8("message-record-handed-off")),
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("payload"),
                sourcePosition());
        final MessageRecord handedOff = base.withRuntimeIndex(
                GenerationRuntimeIndex.none(GenerationAggregateState.HANDED_OFF, java.util.List.of(), 1, 0, false, 2));
        final MessageRecord decoded = MessageRecord.decode(handedOff.encode());

        assertEquals(MessageStatus.HANDED_OFF, decoded.status());
        assertEquals(GenerationAggregateState.HANDED_OFF, decoded.runtimeIndex().aggregateState());
        assertEquals(handedOff, decoded);
    }

    @Test
    void sourcePositionMustBeCanonicalBeforeMessageValueConstruction() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new MessageRecord(
                        MessageStatus.SCHEDULED,
                        0,
                        1,
                        2_000,
                        5_000,
                        DestinationLaneId.derive(Bytes.utf8("message-record-source-fence")),
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("payload"),
                        Bytes.utf8("not-a-source-position")));
    }

    private static byte[] sourcePosition() {
        return new KafkaSourcePosition(
                        new ShardId(RouteIncarnation.random(), 0), "test", UUID.randomUUID(), 0, null, 1_000)
                .canonicalBytes();
    }
}
