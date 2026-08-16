package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommandDedupeRecordTest {
    @Test
    void legacyDedupeValuesDecodeAsManagedV1AndNewWritesCarryTheTuple() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final CommandResult result = result(shard);
        final byte[] hash = Bytes.sha256(Bytes.utf8("legacy-command"));
        final byte[] resultBytes = result.encode();
        final byte[] legacy = Bytes.concat(Bytes.u32be(1), hash, Bytes.u32be(resultBytes.length), resultBytes);

        final CommandDedupeRecord decodedLegacy = CommandDedupeRecord.decode(legacy);
        assertEquals(ProtocolTupleV1.managedCommandV1(), decodedLegacy.protocolTuple());
        assertArrayEquals(hash, decodedLegacy.commandHash());
        assertEquals(result, decodedLegacy.result());
        assertFalse(Arrays.equals(legacy, decodedLegacy.encode()));

        final ProtocolTupleV1 nextTuple = new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 2);
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
        final byte[] systemTuple = new ProtocolTupleV1(1, 1, ProtocolTupleV1.SYSTEM_MUTATION, 1, 1)
                .canonicalBytes();
        final byte[] systemRecord = Bytes.concat(Bytes.u32be(2), Bytes.u32be(systemTuple.length), systemTuple,
                hash, Bytes.u32be(resultBytes.length), resultBytes);

        assertThrows(IllegalArgumentException.class, () -> CommandDedupeRecord.decode(systemRecord));
        assertThrows(IllegalArgumentException.class,
                () -> CommandDedupeRecord.decode(Bytes.u32be(99)));
    }

    private static CommandResult result(final ShardId shard) {
        return new CommandResult(ApplyStatus.APPLIED, io.nereusstream.delay.protocol.StableCode.SCHEDULED, 0, 1,
                MessageStatus.SCHEDULED,
                new KafkaSourcePosition(shard, "cluster-a", UUID.nameUUIDFromBytes(Bytes.utf8("dedupe-topic")),
                        0, 1, 1_000).canonicalBytes());
    }
}
