package com.nereusstream.delay.runtime;

import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Ordered stored-head lookup with an exact, bounded read-your-writes deletion overlay. */
final class HeadIndexUpdater {
    private HeadIndexUpdater() {}

    static StoredHead firstStored(final ShardStore store, final byte[] prefix, final List<byte[]> removedKeys) {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(prefix, "prefix");
        if (prefix.length == 0 || removedKeys.size() > 64) {
            throw new IllegalArgumentException("head overlay requires a prefix and at most 64 removed keys");
        }
        final List<byte[]> exactKeys = new ArrayList<>();
        for (byte[] key : removedKeys) {
            if (key.length <= prefix.length || !Arrays.equals(prefix, Arrays.copyOf(key, prefix.length))) {
                throw new IllegalArgumentException("removed head key is outside the exact candidate prefix");
            }
            exactKeys.add(key.clone());
        }
        final List<ShardStore.KeyValue> found = new ArrayList<>(1);
        final int visited = store.visit(
                ColumnFamily.TIMELINE,
                prefix,
                upperBound(prefix),
                exactKeys.size() + 1,
                new BoundedReadBudget(Long.MAX_VALUE, Long.MAX_VALUE, () -> 0),
                (entry, ignored) -> {
                    for (byte[] removed : exactKeys) {
                        if (Arrays.equals(removed, entry.key())) {
                            return true;
                        }
                    }
                    found.add(entry);
                    return false;
                });
        return new StoredHead(found.isEmpty() ? null : found.get(0), visited);
    }

    private static byte[] upperBound(final byte[] prefix) {
        final byte[] upper = prefix.clone();
        for (int index = upper.length - 1; index >= 0; index--) {
            if ((upper[index] & 0xff) != 0xff) {
                upper[index]++;
                return Arrays.copyOf(upper, index + 1);
            }
        }
        return null;
    }

    record StoredHead(ShardStore.KeyValue entry, int keysRead) {}
}
