package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HeadIndexUpdaterTest {
    @TempDir
    Path tempDir;

    @Test
    void exactOverlayReadsAtMostDeletedKeysPlusOneAtEveryBacklogSize() {
        for (int count : new int[] {1_000, 10_000, 100_000}) {
            final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("backlog-" + count));
            final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
            try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                    ShardStore store = ShardStore.open(config, shard, resources)) {
                final byte[] prefix = new byte[] {99, 1};
                store.write(batch -> {
                    for (int index = 0; index < count; index++) {
                        batch.put(ColumnFamily.TIMELINE, key(index), new byte[] {1});
                    }
                });
                final var first = HeadIndexUpdater.firstStored(store, prefix, List.of());
                assertArrayEquals(key(0), first.entry().key());
                assertEquals(1, first.keysRead());
                final var successor = HeadIndexUpdater.firstStored(store, prefix, List.of(key(0)));
                assertArrayEquals(key(1), successor.entry().key());
                assertEquals(2, successor.keysRead());
                final var nonHead = HeadIndexUpdater.firstStored(store, prefix, List.of(key(count - 1)));
                assertArrayEquals(key(0), nonHead.entry().key());
                assertEquals(1, nonHead.keysRead());
                final List<byte[]> removed = new ArrayList<>();
                for (int index = 0; index < 64; index++) {
                    removed.add(key(index));
                }
                final var batchOverlay = HeadIndexUpdater.firstStored(store, prefix, removed);
                assertArrayEquals(key(64), batchOverlay.entry().key());
                assertEquals(65, batchOverlay.keysRead());
                System.out.printf("HEAD_COMPONENT N=%d first=1 deletedHead=2 nonHead=1 deleted64=65%n", count);
            }
        }
    }

    @Test
    void emptyAndDeletedOnlyPrefixesDoNotReadTheAdjacentNamespace() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("empty"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, new ShardId(RouteIncarnation.random(), 4), resources)) {
            assertNull(HeadIndexUpdater.firstStored(store, new byte[] {99, 1}, List.of())
                    .entry());
            store.write(batch -> {
                batch.put(ColumnFamily.TIMELINE, key(0), new byte[] {1});
                batch.put(ColumnFamily.TIMELINE, new byte[] {99, 2, 0}, new byte[] {2});
            });
            final var result = HeadIndexUpdater.firstStored(store, new byte[] {99, 1}, List.of(key(0)));
            assertNull(result.entry());
            assertEquals(1, result.keysRead());
            assertThrows(
                    IllegalArgumentException.class,
                    () -> HeadIndexUpdater.firstStored(store, new byte[] {99, 1}, List.of(new byte[] {99, 2, 0})));
        }
    }

    @Test
    void exhaustedHeadReadNeverReturnsEmptyOrAnUnvalidatedCandidate() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("incomplete"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, new ShardId(RouteIncarnation.random(), 5), resources)) {
            store.write(batch -> {
                batch.put(ColumnFamily.TIMELINE, key(0), new byte[] {1});
                batch.put(ColumnFamily.TIMELINE, key(1), new byte[] {2});
                batch.put(ColumnFamily.ID, new byte[] {99}, new byte[] {3});
            });
            final byte[] prefix = new byte[] {99, 1};
            final BoundedReadBudget beforeSeek = new BoundedReadBudget(100, 1, () -> 0);
            beforeSeek.tryCharge(100, 0);
            final var exhausted = HeadIndexUpdater.read(store, prefix, List.of(), beforeSeek, entry -> entry);
            assertEquals(HeadReadResult.Kind.INCOMPLETE, exhausted.kind());
            assertNull(exhausted.candidate());
            assertEquals(0, exhausted.keysRead());

            final BoundedReadBudget afterDeletion = new BoundedReadBudget(1, 100, 1_000, () -> 0);
            final var noSuccessor =
                    HeadIndexUpdater.read(store, prefix, List.of(key(0)), afterDeletion, entry -> entry);
            assertEquals(HeadReadResult.Kind.INCOMPLETE, noSuccessor.kind());
            assertEquals(1, noSuccessor.keysRead());
            assertNull(noSuccessor.candidate());

            final BoundedReadBudget beforeDependency = new BoundedReadBudget(1, 100, 1_000, () -> 0);
            final var noMessage = store.readWithBudget(
                            beforeDependency,
                            () -> HeadIndexUpdater.read(
                                    store,
                                    prefix,
                                    List.of(),
                                    beforeDependency,
                                    entry -> store.get(ColumnFamily.ID, new byte[] {99})))
                    .value();
            assertEquals(HeadReadResult.Kind.INCOMPLETE, noMessage.kind());
            assertEquals(BoundedReadBudget.Exhaustion.RECORDS, noMessage.reason());
            assertNull(noMessage.candidate());
            assertEquals(1, beforeDependency.actualRecords());

            final BoundedReadBudget complete = new BoundedReadBudget(2, 100, 1_000, () -> 0);
            final var found = store.readWithBudget(
                            complete,
                            () -> HeadIndexUpdater.read(
                                    store,
                                    prefix,
                                    List.of(),
                                    complete,
                                    entry -> store.get(ColumnFamily.ID, new byte[] {99})))
                    .value();
            assertEquals(HeadReadResult.Kind.FOUND, found.kind());
            assertArrayEquals(new byte[] {3}, found.candidate());
        }
    }

    private static byte[] key(final int value) {
        return Bytes.concat(new byte[] {99, 1}, Bytes.u32be(value));
    }
}
