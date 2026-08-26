package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CheckpointCatalogResultTest {
    @Test
    void roundTripsShardCatalogWithSortedSummaries() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final UUID topic = UUID.randomUUID();
        final CheckpointSummary first = summary(shard, topic, 2, 1, false, 1);
        final CheckpointSummary floor = summary(shard, topic, 3, 2, true, 2);
        final CheckpointCatalogResult result = new CheckpointCatalogResult(
                new ShardSubject(shard),
                bytes(16, 3),
                floor.checkpointId(),
                floor.manifestSha256(),
                3,
                List.of(first, floor));

        assertEquals(result, CheckpointCatalogResult.decode(result.canonicalBytes()));
        assertEquals(List.of(first, floor), result.summaries());
    }

    @Test
    void catalogVersionsPreserveCompleteUnsigned64BitPatterns() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final UUID topic = UUID.randomUUID();
        final CheckpointSummary first = summary(shard, topic, 2, 1, false, 1);
        final CheckpointSummary floor = summary(shard, topic, 3, Long.MIN_VALUE, true, 2);
        final CheckpointCatalogResult result = new CheckpointCatalogResult(
                new ShardSubject(shard),
                bytes(16, 3),
                floor.checkpointId(),
                floor.manifestSha256(),
                Long.MIN_VALUE,
                List.of(first, floor));

        final CheckpointCatalogResult decoded = CheckpointCatalogResult.decode(result.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.catalogGeneration());
        assertEquals(1, decoded.summaries().get(0).catalogGeneration());
        assertEquals(Long.MIN_VALUE, decoded.summaries().get(1).catalogGeneration());
        assertEquals(result, decoded);
    }

    @Test
    void rejectsDuplicateFloorAndCrossShardOrTamperedValues() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final UUID topic = UUID.randomUUID();
        final CheckpointSummary first = summary(shard, topic, 2, 1, true, 1);
        final CheckpointSummary second = summary(shard, topic, 3, 2, true, 2);
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointCatalogResult(
                        new ShardSubject(shard),
                        bytes(16, 3),
                        first.checkpointId(),
                        first.manifestSha256(),
                        3,
                        List.of(first, second)));

        final ShardId otherShard = new ShardId(RouteIncarnation.random(), 2);
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointCatalogResult(
                        new ShardSubject(shard),
                        bytes(16, 3),
                        first.checkpointId(),
                        first.manifestSha256(),
                        3,
                        List.of(summary(otherShard, topic, 2, 1, false, 3))));

        final CheckpointCatalogResult result = new CheckpointCatalogResult(
                new ShardSubject(shard), bytes(16, 3), first.checkpointId(), first.manifestSha256(), 2, List.of(first));
        final byte[] malformed = Bytes.concat(result.canonicalBytes(), new byte[] {0x08, 0x01});
        assertThrows(IllegalArgumentException.class, () -> CheckpointCatalogResult.decode(malformed));
    }

    private static CheckpointSummary summary(
            final ShardId shard,
            final UUID topic,
            final long offset,
            final long generation,
            final boolean floor,
            final int seed) {
        return new CheckpointSummary(
                bytes(16, seed),
                bytes(32, seed + 10),
                new KafkaSourcePosition(shard, "cluster-a", topic, offset, null, 1_000 + offset),
                generation,
                floor);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
