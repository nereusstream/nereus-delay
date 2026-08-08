package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CheckpointCatalogResultV1Test {
    @Test
    void roundTripsShardCatalogWithSortedSummaries() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final UUID topic = UUID.randomUUID();
        final CheckpointSummaryV1 first = summary(shard, topic, 2, 1, false, 1);
        final CheckpointSummaryV1 floor = summary(shard, topic, 3, 2, true, 2);
        final CheckpointCatalogResultV1 result = new CheckpointCatalogResultV1(new ShardSubjectV1(shard),
                bytes(16, 3), floor.checkpointId(), floor.manifestSha256(), 3, List.of(first, floor));

        assertEquals(result, CheckpointCatalogResultV1.decode(result.canonicalBytes()));
        assertEquals(List.of(first, floor), result.summaries());
    }

    @Test
    void catalogVersionsPreserveCompleteUnsigned64BitPatterns() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final UUID topic = UUID.randomUUID();
        final CheckpointSummaryV1 first = summary(shard, topic, 2, 1, false, 1);
        final CheckpointSummaryV1 floor = summary(shard, topic, 3, Long.MIN_VALUE, true, 2);
        final CheckpointCatalogResultV1 result = new CheckpointCatalogResultV1(new ShardSubjectV1(shard),
                bytes(16, 3), floor.checkpointId(), floor.manifestSha256(), Long.MIN_VALUE,
                List.of(first, floor));

        final CheckpointCatalogResultV1 decoded = CheckpointCatalogResultV1.decode(result.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.catalogGeneration());
        assertEquals(1, decoded.summaries().get(0).catalogGeneration());
        assertEquals(Long.MIN_VALUE, decoded.summaries().get(1).catalogGeneration());
        assertEquals(result, decoded);
    }

    @Test
    void rejectsDuplicateFloorAndCrossShardOrTamperedValues() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final UUID topic = UUID.randomUUID();
        final CheckpointSummaryV1 first = summary(shard, topic, 2, 1, true, 1);
        final CheckpointSummaryV1 second = summary(shard, topic, 3, 2, true, 2);
        assertThrows(IllegalArgumentException.class, () -> new CheckpointCatalogResultV1(new ShardSubjectV1(shard),
                bytes(16, 3), first.checkpointId(), first.manifestSha256(), 3, List.of(first, second)));

        final ShardId otherShard = new ShardId(RouteIncarnation.random(), 2);
        assertThrows(IllegalArgumentException.class, () -> new CheckpointCatalogResultV1(new ShardSubjectV1(shard),
                bytes(16, 3), first.checkpointId(), first.manifestSha256(), 3,
                List.of(summary(otherShard, topic, 2, 1, false, 3))));

        final CheckpointCatalogResultV1 result = new CheckpointCatalogResultV1(new ShardSubjectV1(shard),
                bytes(16, 3), first.checkpointId(), first.manifestSha256(), 2, List.of(first));
        final byte[] malformed = Bytes.concat(result.canonicalBytes(), new byte[]{0x08, 0x01});
        assertThrows(IllegalArgumentException.class, () -> CheckpointCatalogResultV1.decode(malformed));
    }

    private static CheckpointSummaryV1 summary(final ShardId shard, final UUID topic, final long offset,
                                               final long generation, final boolean floor, final int seed) {
        return new CheckpointSummaryV1(bytes(16, seed), bytes(32, seed + 10),
                new KafkaSourcePosition(shard, "cluster-a", topic, offset, null, 1_000 + offset), generation, floor);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
