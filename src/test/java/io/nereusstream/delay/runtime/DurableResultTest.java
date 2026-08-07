package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutationType;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DurableResultTest {
    @Test
    void commandResultRequiresCanonicalSourcePosition() {
        final byte[] source = sourcePosition().canonicalBytes();

        assertDoesNotThrow(() -> new CommandResult(ApplyStatus.REJECTED, StableCode.INVALID_COMMAND,
                -1, 0, null, source));
        assertThrows(IllegalArgumentException.class,
                () -> new CommandResult(ApplyStatus.REJECTED, StableCode.INVALID_COMMAND,
                        -1, 0, null, new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> new CommandResult(ApplyStatus.REJECTED, StableCode.INVALID_COMMAND,
                        -1, 0, null, Bytes.concat(source, new byte[]{0})));
    }

    @Test
    void systemMutationResultRequiresCanonicalSourcePosition() {
        final byte[] mutationId = Bytes.sha256(Bytes.utf8("durable-result-mutation-id"));
        final byte[] mutationHash = Bytes.sha256(Bytes.utf8("durable-result-mutation-hash"));
        final byte[] author = Bytes.utf8("durable-result-author");
        final byte[] source = sourcePosition().canonicalBytes();

        assertDoesNotThrow(() -> new SystemMutationResult(mutationId, mutationHash,
                SystemMutationType.TIME_FENCE, 10_000, author, ApplyStatus.REJECTED,
                StableCode.INVALID_COMMAND, source));
        assertThrows(IllegalArgumentException.class,
                () -> new SystemMutationResult(mutationId, mutationHash, SystemMutationType.TIME_FENCE,
                        10_000, author, ApplyStatus.REJECTED, StableCode.INVALID_COMMAND, new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> new SystemMutationResult(mutationId, mutationHash, SystemMutationType.TIME_FENCE,
                        10_000, author, ApplyStatus.REJECTED, StableCode.INVALID_COMMAND,
                        Bytes.concat(source, new byte[]{0})));
    }

    private static KafkaSourcePosition sourcePosition() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        return new KafkaSourcePosition(shardId, "durable-result-cluster", UUID.randomUUID(),
                3, null, 1_000);
    }
}
