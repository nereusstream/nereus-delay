package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import org.junit.jupiter.api.Test;

import java.security.KeyPairGenerator;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void appliedCommandResultPreservesMaxUint32GenerationAndProjectsIt() {
        final byte[] source = sourcePosition().canonicalBytes();
        final CommandResult result = new CommandResult(ApplyStatus.APPLIED, StableCode.OK,
                -1, 1, MessageStatus.SCHEDULED, source);

        assertTrue(result.hasGeneration());
        assertEquals(result, CommandResult.decode(result.encode()));
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

    @Test
    void systemMutationResultFactoryBindsSourcePositionToMutationShard() throws Exception {
        final ShardId mutationShard = new ShardId(RouteIncarnation.random(), 8);
        final ShardId foreignShard = new ShardId(RouteIncarnation.random(), 9);
        final byte[] proofId = Bytes.sha256(Bytes.utf8("durable-result-fence-proof"));
        final byte[] body = timeFenceBody(mutationShard, proofId);
        final var keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final SystemMutation mutation = SystemMutation.signed(mutationShard, SystemMutationType.TIME_FENCE,
                10_000, proofId, body,
                AuthorIdentity.fence(Bytes.utf8("durable-result-fence"), 1).canonicalBytes(), 1,
                keyPair.getPrivate());

        assertDoesNotThrow(() -> SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                sourcePosition(mutationShard).canonicalBytes()));
        assertThrows(IllegalArgumentException.class,
                () -> SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                        sourcePosition(foreignShard).canonicalBytes()));
    }

    private static KafkaSourcePosition sourcePosition() {
        return sourcePosition(new ShardId(RouteIncarnation.random(), 7));
    }

    private static KafkaSourcePosition sourcePosition(final ShardId shardId) {
        return new KafkaSourcePosition(shardId, "durable-result-cluster", UUID.randomUUID(),
                3, null, 1_000);
    }

    private static byte[] timeFenceBody(final ShardId shard, final byte[] proofId) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] proof = CanonicalProtobuf.message(output -> CanonicalProtobuf.uint32(output, 1, 1));
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.TIME_FENCE.wireValue());
            CanonicalProtobuf.int64(output, 3, 10_000);
            CanonicalProtobuf.int64(output, 10, 1_000);
            CanonicalProtobuf.uint32(output, 11, 1);
            CanonicalProtobuf.bytes(output, 12, proofId);
            CanonicalProtobuf.bytes(output, 13, proof);
        });
    }
}
