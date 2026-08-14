package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.KafkaProduceRequest;
import io.nereusstream.delay.adapter.KafkaProduceResult;
import io.nereusstream.delay.adapter.PinnedKafkaCommandIngress;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.CommandType;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GuardedTransportOwnershipTest {
    @Test
    void permitTransfersExactlyOnceImmediatelyBeforeProduce() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final UUID topic = UUID.randomUUID();
        final PreparedCommand command = PreparedCommand.create(shard, CommandId.random(shard),
                DelayMessageId.random(shard), CommandType.SCHEDULE, 600, Bytes.utf8("command-body"));
        final KafkaProduceRequest request = new KafkaProduceRequest("cluster", "topic", topic, 3, command.commandId(),
                Bytes.utf8("frame"));
        final AtomicInteger sends = new AtomicInteger();
        final KafkaCommandTransportKey key = new KafkaCommandTransportKey("cluster", "topic", topic, 3,
                new CredentialBindingKey(1, digest(1), digest(2)));
        final GuardedKafkaCommandTransport transport = new GuardedKafkaCommandTransport(key, value -> {
            sends.incrementAndGet();
            return CompletableFuture.completedFuture(KafkaProduceResult.persisted("cluster", topic, 3, 9,
                    1, 100, Bytes.utf8("response")));
        });
        final LocalTransportOwnershipPermit permit = new LocalTransportOwnershipPermit(
                PhysicalEnqueueAttemptId.require(bytes(16, 7)));

        final KafkaProduceResult result = (KafkaProduceResult) transport.send(request, permit)
                .toCompletableFuture().join();
        transport.send(request, permit).toCompletableFuture().join();

        assertEquals(1, sends.get());
        assertEquals(permit.physicalAttemptId(), result.physicalAttemptId());
        assertEquals(TransportOwnershipState.LIBRARY_OWNED, permit.state());
        assertNotEquals(TransportOwnershipState.AVAILABLE, permit.state());
    }

    @Test
    void closedPermitAndMismatchedRequestCannotReachProducer() {
        final UUID topic = UUID.randomUUID();
        final KafkaCommandTransportKey key = new KafkaCommandTransportKey("cluster", "topic", topic, 0,
                new CredentialBindingKey(1, digest(3), digest(4)));
        final AtomicInteger sends = new AtomicInteger();
        final GuardedKafkaCommandTransport transport = new GuardedKafkaCommandTransport(key, value -> {
            sends.incrementAndGet();
            return CompletableFuture.completedFuture(KafkaProduceResult.unknown(0x0203, Bytes.utf8("unknown")));
        });
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.create(shard, CommandId.random(shard),
                DelayMessageId.random(shard), CommandType.SCHEDULE, 600, Bytes.utf8("body"));
        final KafkaProduceRequest wrongTopic = new KafkaProduceRequest("cluster", "topic", UUID.randomUUID(), 0,
                command.commandId(), Bytes.utf8("frame"));
        final LocalTransportOwnershipPermit closed = new LocalTransportOwnershipPermit(
                PhysicalEnqueueAttemptId.require(bytes(16, 8)));
        closed.close();
        transport.send(wrongTopic, closed).toCompletableFuture().join();

        assertEquals(0, sends.get());
        assertEquals(TransportOwnershipState.INVALID, closed.state());
    }

    @Test
    void resultWithDifferentPhysicalAttemptCannotCrossTheGuardedBridge() {
        final UUID topic = UUID.randomUUID();
        final KafkaCommandTransportKey key = new KafkaCommandTransportKey("cluster", "topic", topic, 0,
                new CredentialBindingKey(1, digest(5), digest(6)));
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.create(shard, CommandId.random(shard),
                DelayMessageId.random(shard), CommandType.SCHEDULE, 600, Bytes.utf8("body"));
        final KafkaProduceRequest request = new KafkaProduceRequest("cluster", "topic", topic, 0, command.commandId(),
                Bytes.utf8("frame"));
        final PhysicalEnqueueAttemptId permitId = PhysicalEnqueueAttemptId.require(bytes(16, 20));
        final GuardedKafkaCommandTransport transport = new GuardedKafkaCommandTransport(key, value ->
                CompletableFuture.completedFuture(KafkaProduceResult.unknown(
                        PhysicalEnqueueAttemptId.require(bytes(16, 21)), 0x0203, Bytes.utf8("unknown"))));
        final LocalTransportOwnershipPermit permit = new LocalTransportOwnershipPermit(permitId);

        assertThrows(java.util.concurrent.CompletionException.class,
                () -> transport.send(request, permit).toCompletableFuture().join());
        assertEquals(TransportOwnershipState.LIBRARY_OWNED, permit.state());
    }

    private static Digest32 digest(final int seed) {
        return new Digest32(bytes(32, seed));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
