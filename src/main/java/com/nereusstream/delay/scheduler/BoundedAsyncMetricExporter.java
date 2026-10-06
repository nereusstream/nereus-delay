package com.nereusstream.delay.scheduler;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded process-local metrics export queue.
 *
 * <p>The queue is bounded by both event count and canonical payload bytes. A
 * single daemon thread invokes the supplied exporter outside the queue lock;
 * exporter failures and admission drops are retained as gap counters and
 * never escape to the scheduler or message state machine. Queue admission
 * holds the lock only while updating the bounded in-memory queue. This is
 * ordinary process telemetry, not durable certification evidence.</p>
 */
public final class BoundedAsyncMetricExporter implements AutoCloseable {
    private static final int EVENT_PAYLOAD_BYTES = 1 + Long.BYTES;
    private static final AtomicLong THREAD_IDS = new AtomicLong();

    public enum Instrument {
        COUNTER,
        HISTOGRAM
    }

    /** Fixed-cardinality scheduler signals; values are aggregated at Worker scope. */
    public enum Metric {
        TARGET_DRR_TURN_VISITS(1, Instrument.HISTOGRAM, "nereus.target.drr.turn.visits"),
        TARGET_DRR_TURN_SCHEDULING_BYTES(2, Instrument.HISTOGRAM, "nereus.target.drr.turn.scheduling.bytes"),
        TARGET_DRR_TURN_DURATION_NANOS(3, Instrument.HISTOGRAM, "nereus.target.drr.turn.duration"),
        TARGET_DRR_CLAIM_TURNS(4, Instrument.COUNTER, "nereus.target.drr.claim.turns"),
        TARGET_DRR_STOP_NORMAL(5, Instrument.COUNTER, "nereus.target.drr.stop.normal"),
        TARGET_DRR_STOP_READ_INCOMPLETE(6, Instrument.COUNTER, "nereus.target.drr.stop.read_incomplete"),
        TARGET_DRR_STOP_CREDIT_WAIT(7, Instrument.COUNTER, "nereus.target.drr.stop.credit_wait"),
        TARGET_DRR_STOP_BUDGET_WAIT(8, Instrument.COUNTER, "nereus.target.drr.stop.budget_wait"),
        TARGET_DRR_TARGET_SERVICE_INTERVAL_NANOS(
                9, Instrument.HISTOGRAM, "nereus.target.drr.target.service.interval.nanos"),
        TARGET_DRR_TURN_QUEUE_REFRESH_CALLS(10, Instrument.HISTOGRAM, "nereus.target.drr.turn.queue_refresh.calls"),
        TARGET_DRR_TURN_HEAD_PROBE_CALLS(11, Instrument.HISTOGRAM, "nereus.target.drr.turn.head_probe.calls"),
        TARGET_INVENTORY_SCAN_PAGES(12, Instrument.HISTOGRAM, "nereus.target.inventory.scan.pages"),
        TARGET_INVENTORY_SCAN_BUDGET_RECORDS(13, Instrument.HISTOGRAM, "nereus.target.inventory.scan.budget.records"),
        TARGET_INVENTORY_SCAN_BUDGET_BYTES(14, Instrument.HISTOGRAM, "nereus.target.inventory.scan.budget.bytes"),
        TARGET_INVENTORY_REBUILD_DURATION_NANOS(15, Instrument.HISTOGRAM, "nereus.target.inventory.rebuild.duration"),
        TARGET_INVENTORY_ACTIVE_MESSAGE_DEPTH(
                16, Instrument.HISTOGRAM, "nereus.target.inventory.active.message.depth"),
        TARGET_INVENTORY_ACTIVE_MESSAGE_DEPTH_UNAVAILABLE(
                17, Instrument.COUNTER, "nereus.target.inventory.active.message.depth.unavailable"),
        TARGET_PHYSICAL_PUBLISH_STAGE_DURATION_NANOS(
                18, Instrument.HISTOGRAM, "nereus.target.physical.publish.stage.duration"),
        TARGET_PHYSICAL_PUBLISH_STAGE_UNAVAILABLE(
                19, Instrument.COUNTER, "nereus.target.physical.publish.stage.unavailable");

        private final int wireValue;
        private final Instrument instrument;
        private final String name;

        Metric(final int wireValue, final Instrument instrument, final String name) {
            this.wireValue = wireValue;
            this.instrument = instrument;
            this.name = name;
        }

        public int wireValue() {
            return wireValue;
        }

        public Instrument instrument() {
            return instrument;
        }

        public String instrumentName() {
            return name;
        }
    }

    public record MetricEvent(Metric metric, long value) {
        public MetricEvent {
            Objects.requireNonNull(metric, "metric");
            if (value < 0 || (metric.instrument() == Instrument.COUNTER && value == 0)) {
                throw new IllegalArgumentException("metric value is invalid for its instrument");
            }
        }

        /** One wire tag and one signed 64-bit value; record limits also bound object overhead. */
        public int canonicalPayloadBytes() {
            return EVENT_PAYLOAD_BYTES;
        }
    }

    public record Limits(int maximumPendingRecords, long maximumPendingBytes) {
        public Limits {
            if (maximumPendingRecords <= 0 || maximumPendingBytes < EVENT_PAYLOAD_BYTES) {
                throw new IllegalArgumentException("metric queue requires finite record and byte bounds");
            }
        }
    }

    @FunctionalInterface
    public interface Exporter {
        void export(MetricEvent event) throws Exception;
    }

    public enum OfferResult {
        ACCEPTED,
        DROPPED_FULL,
        DROPPED_CLOSED
    }

    public record Snapshot(
            long accepted,
            long exported,
            long droppedFull,
            long rejectedAfterClose,
            long exportFailures,
            long droppedAtWorkerStop,
            int pendingRecords,
            long pendingBytes,
            boolean closed,
            boolean workerTerminated,
            Throwable firstExportFailure) {
        public long gapEvents() {
            return Math.addExact(
                    Math.addExact(droppedFull, Math.addExact(rejectedAfterClose, exportFailures)),
                    droppedAtWorkerStop);
        }
    }

    private final Limits limits;
    private final Exporter exporter;
    private final ArrayDeque<MetricEvent> pending = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Condition terminated = lock.newCondition();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong exported = new AtomicLong();
    private final AtomicLong droppedFull = new AtomicLong();
    private final AtomicLong rejectedAfterClose = new AtomicLong();
    private final AtomicLong exportFailures = new AtomicLong();
    private final AtomicLong droppedAtWorkerStop = new AtomicLong();
    private final AtomicReference<Throwable> firstExportFailure = new AtomicReference<>();
    private final Thread worker;
    private int pendingRecords;
    private long pendingBytes;
    private boolean accepting = true;
    private boolean workerTerminated;

    public BoundedAsyncMetricExporter(final Limits limits, final Exporter exporter) {
        this.limits = Objects.requireNonNull(limits, "limits");
        this.exporter = Objects.requireNonNull(exporter, "exporter");
        worker = new Thread(this::runWorker, "nereus-delay-metric-exporter-" + THREAD_IDS.incrementAndGet());
        worker.setDaemon(true);
        worker.start();
    }

    /**
     * Offers one event without waiting for exporter work. Every rejected event
     * contributes to {@link Snapshot#gapEvents()}.
     */
    public OfferResult record(final MetricEvent event) {
        final MetricEvent exact = Objects.requireNonNull(event, "event");
        lock.lock();
        try {
            if (!accepting) {
                rejectedAfterClose.incrementAndGet();
                return OfferResult.DROPPED_CLOSED;
            }
            final int eventBytes = exact.canonicalPayloadBytes();
            if (pendingRecords >= limits.maximumPendingRecords()
                    || pendingBytes > limits.maximumPendingBytes() - eventBytes) {
                droppedFull.incrementAndGet();
                return OfferResult.DROPPED_FULL;
            }
            pending.addLast(exact);
            pendingRecords++;
            pendingBytes += eventBytes;
            accepted.incrementAndGet();
            changed.signal();
            return OfferResult.ACCEPTED;
        } finally {
            lock.unlock();
        }
    }

    /** Stops accepting new metrics; the worker drains already accepted events before exiting. */
    @Override
    public void close() {
        stopAccepting();
    }

    /** Stops admission while allowing the caller to await a drain before closing its scope. */
    public void stopAccepting() {
        lock.lock();
        try {
            accepting = false;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Waits at most the supplied duration for the best-effort queue to drain. */
    public boolean awaitTermination(final Duration timeout) {
        final Duration exact = Objects.requireNonNull(timeout, "timeout");
        if (exact.isNegative()) {
            throw new IllegalArgumentException("metric exporter wait cannot be negative");
        }
        final long timeoutNanos;
        try {
            timeoutNanos = exact.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("metric exporter wait exceeds nanoseconds", overflow);
        }
        long remaining = timeoutNanos;
        lock.lock();
        try {
            while (!workerTerminated && remaining > 0) {
                try {
                    remaining = terminated.awaitNanos(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return workerTerminated;
        } finally {
            lock.unlock();
        }
    }

    public Snapshot snapshot() {
        lock.lock();
        try {
            return new Snapshot(
                    accepted.get(),
                    exported.get(),
                    droppedFull.get(),
                    rejectedAfterClose.get(),
                    exportFailures.get(),
                    droppedAtWorkerStop.get(),
                    pendingRecords,
                    pendingBytes,
                    !accepting,
                    workerTerminated,
                    firstExportFailure.get());
        } finally {
            lock.unlock();
        }
    }

    private void runWorker() {
        boolean stoppedUnexpectedly = false;
        try {
            while (true) {
                final MetricEvent event;
                lock.lock();
                try {
                    while (pending.isEmpty() && accepting) {
                        changed.await();
                    }
                    if (pending.isEmpty()) {
                        return;
                    }
                    event = pending.removeFirst();
                } finally {
                    lock.unlock();
                }
                try {
                    exporter.export(event);
                    exported.incrementAndGet();
                } catch (Exception failure) {
                    exportFailures.incrementAndGet();
                    firstExportFailure.compareAndSet(null, failure);
                } catch (Error fatal) {
                    exportFailures.incrementAndGet();
                    firstExportFailure.compareAndSet(null, fatal);
                    throw fatal;
                } finally {
                    release(event);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            stoppedUnexpectedly = true;
        } catch (RuntimeException | Error fatal) {
            firstExportFailure.compareAndSet(null, fatal);
            stoppedUnexpectedly = true;
            throw fatal;
        } finally {
            lock.lock();
            try {
                accepting = false;
                if (stoppedUnexpectedly) {
                    while (!pending.isEmpty()) {
                        final MetricEvent abandoned = pending.removeFirst();
                        pendingRecords--;
                        pendingBytes -= abandoned.canonicalPayloadBytes();
                        droppedAtWorkerStop.incrementAndGet();
                    }
                }
                workerTerminated = true;
                terminated.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    private void release(final MetricEvent event) {
        lock.lock();
        try {
            pendingRecords--;
            pendingBytes -= event.canonicalPayloadBytes();
        } finally {
            lock.unlock();
        }
    }
}
