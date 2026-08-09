package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommandResultRetentionPolicyTest {
    @Test
    void derivesBoundaryFromBrokerPersistenceTime() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 1,
                null, 2_000);
        assertEquals(7_000, new CommandResultRetentionPolicy(4, 5_000).retainUntil(source));
    }

    @Test
    void rejectsBoundaryOverflowInsteadOfWrapping() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 1,
                null, Long.MAX_VALUE);
        assertThrows(IllegalArgumentException.class,
                () -> new CommandResultRetentionPolicy(4, 1).retainUntil(source));
    }
}
