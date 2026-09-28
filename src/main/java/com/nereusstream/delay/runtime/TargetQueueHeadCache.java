package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.TargetPartitionId;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;

/** Bounded, process-local cache of one source Store's validated Target queue summaries. */
public final class TargetQueueHeadCache {
    public record Lookup(boolean hit, Optional<TargetQueueSnapshotReader.Entry> entry) {
        public Lookup {
            Objects.requireNonNull(entry, "entry");
            if (!hit && entry.isPresent()) {
                throw new IllegalArgumentException("cache miss cannot contain a Target entry");
            }
        }
    }

    private record Key(TargetPartitionId target, int maximumDomains) {}

    private record Value(Optional<TargetQueueSnapshotReader.Entry> entry) {
        private Value {
            Objects.requireNonNull(entry, "entry");
        }
    }

    private final LinkedHashMap<Key, Value> entries = new LinkedHashMap<>(16, 0.75f, true);
    private int maximumEntries;

    /** Enables this cache at the activated per-Store Target bound. */
    public synchronized void configure(final int maximumEntries) {
        if (maximumEntries <= 0) {
            throw new IllegalArgumentException("Target head cache requires a finite positive entry bound");
        }
        this.maximumEntries = maximumEntries;
        trim();
    }

    public synchronized Lookup lookup(final TargetPartitionId target, final int maximumDomains) {
        final var exactKey = new Key(Objects.requireNonNull(target, "target"), maximumDomains);
        if (maximumEntries == 0) {
            return new Lookup(false, Optional.empty());
        }
        final Value cached = entries.get(exactKey);
        return cached == null ? new Lookup(false, Optional.empty()) : new Lookup(true, cached.entry());
    }

    public synchronized void cache(
            final TargetPartitionId target,
            final int maximumDomains,
            final Optional<TargetQueueSnapshotReader.Entry> entry) {
        if (maximumEntries == 0) {
            return;
        }
        entries.put(
                new Key(Objects.requireNonNull(target, "target"), maximumDomains),
                new Value(Objects.requireNonNull(entry, "entry")));
        trim();
    }

    /** Called only after a successful batch changes the persisted queue or its physical identity. */
    public synchronized void invalidate(final TargetPartitionId target) {
        final var exactTarget = Objects.requireNonNull(target, "target");
        entries.keySet().removeIf(key -> key.target().equals(exactTarget));
    }

    private void trim() {
        while (entries.size() > maximumEntries) {
            entries.remove(entries.keySet().iterator().next());
        }
    }
}
