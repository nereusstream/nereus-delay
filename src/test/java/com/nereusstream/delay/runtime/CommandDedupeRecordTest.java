package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CommandDedupeRecordTest {
    @Test
    void legacyDedupeValuesDecodeAsManagedAndNewWritesCarryTheTuple() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final CommandResult result = result(shard);
        final byte[] hash = Bytes.sha256(Bytes.utf8("legacy-command"));
        final byte[] resultBytes = result.encode();
        final byte[] legacy = Bytes.concat(Bytes.u32be(1), hash, Bytes.u32be(resultBytes.length), resultBytes);

        final CommandDedupeRecord decodedLegacy = CommandDedupeRecord.decode(legacy);
        assertEquals(ProtocolTuple.managedCommand(), decodedLegacy.protocolTuple());
        assertArrayEquals(hash, decodedLegacy.commandHash());
        assertEquals(result, decodedLegacy.result());
        assertFalse(Arrays.equals(legacy, decodedLegacy.encode()));

        final ProtocolTuple nextTuple = new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 2);
        final CommandDedupeRecord next = new CommandDedupeRecord(nextTuple, hash, result);
        final CommandDedupeRecord decodedNext = CommandDedupeRecord.decode(next.encode());
        assertEquals(nextTuple, decodedNext.protocolTuple());
        assertArrayEquals(hash, decodedNext.commandHash());
        assertEquals(result, decodedNext.result());
    }

    @Test
    void dedupeDecodeRejectsUnsupportedTupleBranchAndVersion() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final CommandResult result = result(shard);
        final byte[] hash = Bytes.sha256(Bytes.utf8("invalid-command"));
        final byte[] resultBytes = result.encode();
        final byte[] systemTuple = new ProtocolTuple(1, 1, ProtocolTuple.SYSTEM_MUTATION, 1, 1).canonicalBytes();
        final byte[] systemRecord = Bytes.concat(
                Bytes.u32be(2),
                Bytes.u32be(systemTuple.length),
                systemTuple,
                hash,
                Bytes.u32be(resultBytes.length),
                resultBytes);

        assertThrows(IllegalArgumentException.class, () -> CommandDedupeRecord.decode(systemRecord));
        assertThrows(IllegalArgumentException.class, () -> CommandDedupeRecord.decode(Bytes.u32be(99)));
    }

    private static CommandResult result(final ShardId shard) {
        return new CommandResult(
                ApplyStatus.APPLIED,
                com.nereusstream.delay.protocol.StableCode.SCHEDULED,
                0,
                1,
                MessageStatus.SCHEDULED,
                new KafkaSourcePosition(
                                shard, "cluster-a", UUID.nameUUIDFromBytes(Bytes.utf8("dedupe-topic")), 0, 1, 1_000)
                        .canonicalBytes());
    }
}
