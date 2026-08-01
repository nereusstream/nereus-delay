package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.store.KeyCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TerminalGenerationRecordTest {
    @Test
    void roundTripsCanonicalOpenObligationSummary() {
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(RouteIncarnation.random(), 0));
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("terminal-attempt"));
        final AttemptObligationRef obligation = new AttemptObligationRef(attemptId, 0,
                AttemptLedgerState.PUBLISHING, KeyCodec.inflight((byte) 2, 42, attemptId));
        final TerminalGenerationRecord record = new TerminalGenerationRecord(messageId, 0,
                MessageStatus.PUBLISHED, StableCode.OK, 4, new byte[]{1, 2, 3}, true, List.of(obligation));

        final TerminalGenerationRecord decoded = TerminalGenerationRecord.decode(record.encode());

        assertEquals(messageId, decoded.messageId());
        assertEquals(MessageStatus.PUBLISHED, decoded.status());
        assertEquals(StableCode.OK, decoded.terminalCode());
        assertEquals(4, decoded.stateVersion());
        assertEquals(true, decoded.possibleDestinationDuplicate());
        assertArrayEquals(new byte[]{1, 2, 3}, decoded.appliedSourcePosition());
        assertEquals(List.of(obligation), decoded.openObligations());
    }

    @Test
    void readsLegacyV1SummaryAsEmptyObligationSet() {
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(RouteIncarnation.random(), 1));
        final byte[] sourcePosition = new byte[]{9, 8};
        final byte[] legacy = Bytes.concat(Bytes.u32be(1), messageId.bytes(), Bytes.u32be(0),
                new byte[]{(byte) MessageStatus.DEAD_LETTER.wireValue()}, Bytes.u32be(StableCode.OK.wireValue()),
                Bytes.u64be(2), new byte[]{0}, Bytes.lp32(sourcePosition));

        final TerminalGenerationRecord decoded = TerminalGenerationRecord.decode(legacy);

        assertEquals(messageId, decoded.messageId());
        assertEquals(List.of(), decoded.openObligations());
        assertArrayEquals(sourcePosition, decoded.appliedSourcePosition());
    }
}
