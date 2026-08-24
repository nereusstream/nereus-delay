package com.nereusstream.delay.adapter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CommandResultRetentionPolicyTest {
    @Test
    void derivesBoundaryFromBrokerPersistenceTime() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 1, null, 2_000);
        assertEquals(7_000, new CommandResultRetentionPolicy(4, 5_000).retainUntil(source));
    }

    @Test
    void rejectsBoundaryOverflowInsteadOfWrapping() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final KafkaSourcePosition source =
                new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 1, null, Long.MAX_VALUE);
        assertThrows(IllegalArgumentException.class, () -> new CommandResultRetentionPolicy(4, 1).retainUntil(source));
    }
}
