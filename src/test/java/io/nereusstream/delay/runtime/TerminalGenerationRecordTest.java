package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.store.KeyCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TerminalGenerationRecordTest {
    @Test
    void roundTripsCanonicalOpenObligationSummary() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("terminal-attempt"));
        final AttemptObligationRef obligation = new AttemptObligationRef(attemptId, 0,
                AttemptLedgerState.PUBLISHING, KeyCodec.inflight((byte) 2, 42, attemptId));
        final TerminalGenerationRecord record = new TerminalGenerationRecord(messageId, 0,
                MessageStatus.PUBLISHED, StableCode.OK, 4, sourcePosition(shardId), true, List.of(obligation));

        final TerminalGenerationRecord decoded = TerminalGenerationRecord.decode(record.encode());

        assertEquals(messageId, decoded.messageId());
        assertEquals(MessageStatus.PUBLISHED, decoded.status());
        assertEquals(StableCode.OK, decoded.terminalCode());
        assertEquals(4, decoded.stateVersion());
        assertEquals(true, decoded.possibleDestinationDuplicate());
        assertArrayEquals(record.appliedSourcePosition(), decoded.appliedSourcePosition());
        assertEquals(List.of(obligation), decoded.openObligations());
    }

    @Test
    void readsLegacyV1SummaryAsEmptyObligationSet() {
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(RouteIncarnation.random(), 1));
        final byte[] sourcePosition = sourcePosition(messageId.routingId().shardId());
        final byte[] legacy = Bytes.concat(Bytes.u32be(1), messageId.bytes(), Bytes.u32be(0),
                new byte[]{(byte) MessageStatus.DEAD_LETTER.wireValue()}, Bytes.u32be(StableCode.OK.wireValue()),
                Bytes.u64be(2), new byte[]{0}, Bytes.lp32(sourcePosition));

        final TerminalGenerationRecord decoded = TerminalGenerationRecord.decode(legacy);

        assertEquals(messageId, decoded.messageId());
        assertEquals(List.of(), decoded.openObligations());
        assertArrayEquals(sourcePosition, decoded.appliedSourcePosition());
    }

    @Test
    void decodeRejectsEveryCanonicalPrefixTruncationAsValidationError() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 2);
        final TerminalGenerationRecord record = new TerminalGenerationRecord(
                DelayMessageId.random(shardId), 0,
                MessageStatus.DEAD_LETTER, StableCode.ALREADY_DEAD_LETTERED, 2,
                sourcePosition(shardId), false);
        final byte[] encoded = record.encode();
        for (int length = 0; length < encoded.length; length++) {
            final byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(IllegalArgumentException.class, () -> TerminalGenerationRecord.decode(truncated),
                    "truncated terminal generation length=" + length);
        }
        assertEquals(record.messageId(), TerminalGenerationRecord.decode(encoded).messageId());
    }

    @Test
    void sourcePositionMustBeCanonicalBeforeTerminalValueConstruction() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 3);
        assertThrows(IllegalArgumentException.class, () -> new TerminalGenerationRecord(
                DelayMessageId.random(shardId), 0, MessageStatus.DEAD_LETTER,
                StableCode.ALREADY_DEAD_LETTERED, 1, Bytes.utf8("not-a-source-position"), false));
    }

    private static byte[] sourcePosition(final ShardId shardId) {
        return new KafkaSourcePosition(shardId, "test", UUID.randomUUID(), 0, null, 1_000).canonicalBytes();
    }
}
