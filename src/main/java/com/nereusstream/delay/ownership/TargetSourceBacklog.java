package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.SourcePosition;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Bounded observation of actual encoded Source frames after an active Store frontier; no quota/retention authority. */
public final class TargetSourceBacklog {
    private TargetSourceBacklog() {}

    public record Limits(int readRecords, long readBytes, int maximumFrameBytes, long elapsedNanos) {
        public Limits {
            if (readRecords < 2 || readRecords == Integer.MAX_VALUE || readBytes <= 0 || readBytes == Long.MAX_VALUE
                    || maximumFrameBytes <= 0 || maximumFrameBytes == Integer.MAX_VALUE
                    || readBytes < maximumFrameBytes || elapsedNanos <= 0 || elapsedNanos == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Source backlog reads must be finite and include an anchor/frame");
            }
        }
    }

    /** Captured by the actual Target source runtime, not from a consumer cursor or an ingress receipt. */
    public static final class Cut {
        private final SourcePosition applied;
        private final Runnable current;

        Cut(final SourcePosition applied, final Runnable current) {
            this.applied = Objects.requireNonNull(applied, "applied");
            this.current = Objects.requireNonNull(current, "current");
            requireCurrent();
        }

        public SourcePosition applied() { return applied; }

        public void requireCurrent() { current.run(); }
    }

    /** Counts frames, not offsets or SDK pending requests. A snapshot is useful only while its cut is current. */
    public record Snapshot(Cut cut, SourcePosition tail, long records, long bytes,
            Long oldestBrokerPersistenceTimeEpochMs, long observedAtNanos) {
        public Snapshot {
            Objects.requireNonNull(cut, "cut");
            Objects.requireNonNull(tail, "tail");
            if (tail.compareTo(cut.applied()) < 0 || records < 0 || bytes < 0
                    || ((records == 0) != (bytes == 0))
                    || ((records == 0) != (oldestBrokerPersistenceTimeEpochMs == null))
                    || (oldestBrokerPersistenceTimeEpochMs != null && oldestBrokerPersistenceTimeEpochMs < 0)) {
                throw new IllegalArgumentException("Source backlog snapshot has inconsistent bounds");
            }
        }
    }

    /** SDK adapters must prove the inclusive anchor and the complete prefix through their captured native tail. */
    public static final class Collector {
        private final Cut cut;
        private final Limits limits;
        private final LongSupplier clock;
        private final Runnable protection;
        private final long started;
        private SourcePosition last;
        private int reads;
        private long readBytes;
        private long records;
        private long bytes;
        private Long oldest;
        private boolean complete;

        public Collector(final Cut cut, final Limits limits, final LongSupplier clock, final Runnable protection) {
            this.cut = Objects.requireNonNull(cut, "cut");
            this.limits = Objects.requireNonNull(limits, "limits");
            this.clock = Objects.requireNonNull(clock, "clock");
            this.protection = Objects.requireNonNull(protection, "protection");
            started = clock.getAsLong();
            requireCurrent();
        }

        public long remainingNanos() {
            requireCurrent();
            return limits.elapsedNanos() - (clock.getAsLong() - started);
        }

        public void requireCurrent() {
            final long elapsed = clock.getAsLong() - started;
            if (complete || elapsed < 0 || elapsed >= limits.elapsedNanos()) {
                throw new IllegalStateException("Source backlog observation is complete or its deadline expired");
            }
            cut.requireCurrent();
            protection.run();
            cut.requireCurrent();
        }

        public void add(final SourcePosition source, final int frameBytes) {
            requireCurrent();
            final var position = Objects.requireNonNull(source, "source");
            if (reads >= limits.readRecords() || frameBytes <= 0 || frameBytes > limits.maximumFrameBytes()
                    || readBytes > limits.readBytes() - frameBytes) {
                throw new IllegalStateException("Source backlog prefix exceeds its finite read/frame budget");
            }
            if (last == null) {
                if (!java.util.Arrays.equals(cut.applied().canonicalBytes(), position.canonicalBytes())) {
                    throw new IllegalStateException("Source backlog lacks the exact retained applied anchor");
                }
            } else if (position.compareTo(last) <= 0) {
                throw new IllegalStateException("Source backlog records are not strictly ordered on the same log");
            }
            reads++;
            readBytes += frameBytes;
            if (last != null) {
                records++;
                bytes += frameBytes;
                final long persisted = position.brokerPersistenceTimeEpochMs();
                oldest = oldest == null ? persisted : Math.min(oldest, persisted);
            }
            last = position;
        }

        public Snapshot finish(final SourcePosition nativeTail) {
            requireCurrent();
            if (last == null || !java.util.Arrays.equals(last.canonicalBytes(),
                    Objects.requireNonNull(nativeTail, "nativeTail").canonicalBytes())) {
                throw new IllegalStateException("Source backlog has not read its complete captured tail");
            }
            final var snapshot = new Snapshot(cut, last, records, bytes, oldest, clock.getAsLong());
            complete = true;
            return snapshot;
        }
    }
}
