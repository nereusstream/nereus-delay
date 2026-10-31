package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ShardId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Bounded same-process Source observations on a caller-owned executor; an uncleaned read retains its exact slot. */
public final class TargetSourceBacklogCache {
    public enum Status {
        LOADING,
        CLEANUP_PENDING,
        READY,
        FAILED,
        STOPPING
    }

    public record State(Status status, Throwable failure, boolean cleanupComplete) {}

    private final int maximumShards;
    private final long maximumAgeNanos;
    private final ThreadPoolExecutor executor;
    private final LongSupplier clock;
    private final Map<ShardId, Entry> entries = new HashMap<>();
    private boolean stopping;
    private Object host;

    public synchronized void bindHost(final Object selectedHost) {
        final var selected = Objects.requireNonNull(selectedHost, "selectedHost");
        if (host != null && host != selected) {
            throw new IllegalArgumentException("Source observer cache is already owned by another Host");
        }
        host = selected;
    }

    public TargetSourceBacklogCache(
            final int maximumShards,
            final long maximumAgeNanos,
            final ThreadPoolExecutor executor,
            final LongSupplier clock) {
        if (maximumShards <= 0
                || maximumShards == Integer.MAX_VALUE
                || maximumAgeNanos <= 0
                || maximumAgeNanos == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Source backlog cache requires finite ownership/freshness limits");
        }
        this.maximumShards = maximumShards;
        this.maximumAgeNanos = maximumAgeNanos;
        this.executor = Objects.requireNonNull(executor, "executor");
        if (executor.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) executor.getQueue().size() + executor.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || executor.getRejectedExecutionHandler().getClass() != ThreadPoolExecutor.AbortPolicy.class) {
            throw new IllegalArgumentException("Source observer executor needs finite threads/queue and AbortPolicy");
        }
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public boolean refresh(
            final TargetWorkerShardRuntime worker,
            final LongSupplier ownerClock,
            final Function<TargetSourceBacklog.Cut, TargetSourceBacklogRead> factory) {
        final var actual = Objects.requireNonNull(worker, "worker");
        return refresh(actual.shardId(), () -> actual.captureSourceBacklogCut(ownerClock), factory);
    }

    /** The supplier must capture the actual Worker cut; factories construct a handle without starting SDK I/O. */
    boolean refresh(
            final ShardId shard,
            final Supplier<TargetSourceBacklog.Cut> capture,
            final Function<TargetSourceBacklog.Cut, TargetSourceBacklogRead> factory) {
        Objects.requireNonNull(shard, "shard");
        Objects.requireNonNull(capture, "capture");
        Objects.requireNonNull(factory, "factory");
        final Entry selected;
        synchronized (this) {
            if (stopping) {
                throw new IllegalStateException("Source backlog cache is stopping");
            }
            final var previous = entries.get(shard);
            if (previous != null && previous.withdrawn) {
                throw new IllegalStateException("Source observer Shard withdrawn; settle/remove before replacement");
            }
            if (previous != null && !previous.settled) {
                return false;
            }
            if (previous == null && entries.size() >= maximumShards) {
                throw new IllegalStateException("Source backlog cache Shard ownership bound reached");
            }
            selected = new Entry();
            entries.put(shard, selected);
        }
        try {
            executor.execute(() -> prepareAndRead(shard, selected, capture, factory));
            return true;
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                selected.failure = failure;
                selected.settled = true;
            }
            throw failure;
        }
    }

    private void prepareAndRead(
            final ShardId shard,
            final Entry selected,
            final Supplier<TargetSourceBacklog.Cut> capture,
            final Function<TargetSourceBacklog.Cut, TargetSourceBacklogRead> factory) {
        try {
            synchronized (this) {
                if (stopping || selected.withdrawn) {
                    throw new IllegalStateException("Source observer withdrawn before cut/provider preparation");
                }
            }
            final var cut = Objects.requireNonNull(capture.get(), "actual Source cut");
            if (!cut.applied().shardId().equals(shard)) {
                throw new IllegalArgumentException("Source observation captured another Shard");
            }
            final var operation = Objects.requireNonNull(factory.apply(cut), "Source backlog read operation");
            synchronized (this) {
                selected.operation = operation;
            }
            read(selected, cut);
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                selected.failure = failure;
            }
            if (selected.operation == null) {
                synchronized (this) {
                    selected.settled = true;
                }
            } else {
                close(selected, false);
            }
        }
    }

    /** No SDK scan or queue wait on the submission path; current cut and freshness are still mandatory. */
    public TargetSourceBacklog.Snapshot get(final ShardId shard) {
        final TargetSourceBacklog.Snapshot snapshot;
        synchronized (this) {
            final var entry = entries.get(Objects.requireNonNull(shard, "shard"));
            if (stopping
                    || entry == null
                    || entry.withdrawn
                    || entry.published == null
                    || !entry.settled
                    || entry.failure != null) {
                throw new IllegalStateException("Source backlog observation is unavailable or not cleaned up");
            }
            snapshot = entry.published;
        }
        snapshot.cut().requireCurrent();
        final long age = clock.getAsLong() - snapshot.observedAtNanos();
        if (age < 0 || age >= maximumAgeNanos) {
            throw new IllegalStateException("Source backlog cached observation expired");
        }
        return snapshot;
    }

    public synchronized State state(final ShardId shard) {
        final var entry = Objects.requireNonNull(entries.get(shard), "Source observer entry");
        final var cleanupFailure = entry.operation == null ? null : entry.operation.cleanupFailure();
        final var failure = entry.failure == null ? cleanupFailure : entry.failure;
        final var status = stopping || entry.withdrawn
                ? Status.STOPPING
                : entry.settled
                        ? failure == null ? Status.READY : Status.FAILED
                        : entry.readEnded ? Status.CLEANUP_PENDING : Status.LOADING;
        return new State(status, failure, entry.settled);
    }

    public synchronized Optional<TargetSourceBacklogRead> pendingOperation(final ShardId shard) {
        final var entry = entries.get(shard);
        return entry == null || entry.settled ? Optional.empty() : Optional.ofNullable(entry.operation);
    }

    public void retryCleanup(final ShardId shard) {
        final Entry entry;
        synchronized (this) {
            entry = Objects.requireNonNull(entries.get(shard), "Source observer entry");
        }
        queueClose(entry, true);
    }

    /** Stops publication/new reads; caller retains the executor and all pending operation/physical charges. */
    public void stop() {
        final List<Entry> pending;
        synchronized (this) {
            stopping = true;
            entries.values().forEach(entry -> entry.published = null);
            pending = entries.values().stream()
                    .filter(entry -> !entry.settled && entry.operation != null)
                    .toList();
        }
        for (var entry : pending) {
            queueClose(entry, false);
        }
    }

    public synchronized boolean cleanupComplete() {
        return stopping && entries.values().stream().allMatch(entry -> entry.settled);
    }

    public synchronized boolean cleanupComplete(final ShardId shard) {
        final var entry = entries.get(shard);
        return entry == null || entry.settled;
    }

    public void withdraw(final ShardId shard) {
        final Entry entry;
        synchronized (this) {
            entry = entries.get(Objects.requireNonNull(shard, "shard"));
            if (entry == null) {
                return;
            }
            entry.withdrawn = true;
            entry.published = null;
        }
        queueClose(entry, false);
    }

    public void requireBounds(final int shards, final long ageNanos) {
        if (maximumShards != shards || maximumAgeNanos != ageNanos) {
            throw new IllegalArgumentException("Source observer cache differs from the configured Worker bounds");
        }
    }

    public synchronized void remove(final ShardId shard) {
        final var entry = entries.get(shard);
        if (entry != null && !entry.settled) {
            throw new IllegalStateException("cannot forget an uncleaned Source observation operation");
        }
        entries.remove(shard);
    }

    private void read(final Entry entry, final TargetSourceBacklog.Cut cut) {
        try {
            cut.requireCurrent();
            synchronized (this) {
                if (stopping || entry.withdrawn) {
                    throw new IllegalStateException("Source observation stopped before SDK read");
                }
            }
            final var candidate = Objects.requireNonNull(entry.operation.read(), "Source observation result");
            if (candidate.cut() != cut) {
                throw new IllegalStateException("Source observation returned another cut");
            }
            cut.requireCurrent();
            synchronized (this) {
                entry.candidate = candidate;
            }
        } catch (Throwable failure) {
            synchronized (this) {
                entry.failure = failure;
            }
        } finally {
            synchronized (this) {
                entry.readEnded = true;
            }
            close(entry, false);
        }
    }

    private void queueClose(final Entry entry, final boolean retry) {
        synchronized (this) {
            if (entry.settled || entry.operation == null || entry.closeQueued || (!retry && entry.closeRequested)) {
                return;
            }
            entry.closeQueued = true;
        }
        try {
            executor.execute(() -> {
                try {
                    close(entry, retry);
                } finally {
                    synchronized (TargetSourceBacklogCache.this) {
                        entry.closeQueued = false;
                    }
                }
            });
        } catch (RuntimeException | Error rejected) {
            synchronized (this) {
                entry.closeQueued = false;
            }
            throw rejected;
        }
    }

    private void close(final Entry entry, final boolean retry) {
        synchronized (this) {
            if (entry.settled || (!retry && entry.closeRequested)) {
                return;
            }
            entry.closeRequested = true;
        }
        try {
            final var cleanup = retry ? entry.operation.retryClose() : entry.operation.requestClose();
            cleanup.whenComplete((ignored, failure) -> {
                try {
                    executor.execute(() -> publishAfterCleanup(entry, failure));
                } catch (RuntimeException rejected) {
                    synchronized (TargetSourceBacklogCache.this) {
                        entry.failure = rejected;
                        entry.settled = entry.operation.cleanupComplete();
                    }
                }
            });
        } catch (Throwable failure) {
            synchronized (this) {
                entry.failure = failure;
                entry.closeRequested = false;
            }
        }
    }

    private void publishAfterCleanup(final Entry entry, final Throwable cleanupFailure) {
        if (!entry.operation.cleanupComplete()) {
            return;
        }
        final TargetSourceBacklog.Snapshot candidate;
        synchronized (this) {
            if (cleanupFailure != null) {
                entry.failure = cleanupFailure;
            }
            candidate = stopping || entry.withdrawn || entry.failure != null ? null : entry.candidate;
        }
        Throwable changed = null;
        if (candidate != null) {
            try {
                candidate.cut().requireCurrent();
            } catch (RuntimeException failure) {
                changed = failure;
            }
        }
        synchronized (this) {
            entry.settled = true;
            if (changed != null) {
                entry.failure = changed;
            }
            if (!stopping && !entry.withdrawn && entry.failure == null) {
                entry.published = candidate;
            }
        }
    }

    private static final class Entry {
        private TargetSourceBacklogRead operation;
        private TargetSourceBacklog.Snapshot candidate;
        private TargetSourceBacklog.Snapshot published;
        private Throwable failure;
        private boolean readEnded;
        private boolean settled;
        private boolean withdrawn;
        private boolean closeQueued;
        private boolean closeRequested;
    }
}
