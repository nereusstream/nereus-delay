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
    /** Metric payload plus shared context reference, generation and fixed-size cumulative gap/status fields. */
    public static final int OBSERVED_EVENT_PAYLOAD_BYTES = EVENT_PAYLOAD_BYTES + 16 + 8 * Long.BYTES + 3;
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

    public record MetricEvent(Metric metric, long value, ObservationStatus observation) {
        public MetricEvent(Metric metric, long value) { this(metric, value, null); }

        public MetricEvent {
            Objects.requireNonNull(metric, "metric");
            if (value < 0 || (metric.instrument() == Instrument.COUNTER && value == 0)) {
                throw new IllegalArgumentException("metric value is invalid for its instrument");
            }
        }

        /** Source boundaries are shared, separately charged context; they are never copied per metric. */
        public int canonicalPayloadBytes() {
            return observation == null ? EVENT_PAYLOAD_BYTES : OBSERVED_EVENT_PAYLOAD_BYTES;
        }
    }

    /** Known losses in this instance only. Prior-process loss and population coverage remain UNKNOWN in context. */
    public record ExportGap(long droppedFull, long rejectedAfterClose, long exportFailures,
            long droppedAtWorkerStop, long droppedBeforeContext) {
        public long knownLostEvents() {
            return Math.addExact(Math.addExact(droppedFull, rejectedAfterClose),
                    Math.addExact(Math.addExact(exportFailures, droppedAtWorkerStop), droppedBeforeContext));
        }
    }

    public record ObservationStatus(OrdinaryObservationContext context, ExportGap gap,
            long statusExportFailures, boolean accepting, boolean queueDrained) {
        public ObservationStatus {
            Objects.requireNonNull(context, "ordinary observation context");
            Objects.requireNonNull(gap, "known current-instance gaps");
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

    /** Explicit capability: old functional exporters cannot silently discard coverage notifications. */
    public interface ObservationExporter extends Exporter {
        default boolean supportsObservationStatus() { return false; }
        void exportObservationStatus(ObservationStatus status) throws Exception;
    }

    public static ObservationExporter requireObservationExporter(Exporter exporter) {
        if (!(Objects.requireNonNull(exporter, "exporter") instanceof ObservationExporter qualified)
                || !qualified.supportsObservationStatus()) {
            throw new IllegalArgumentException("configured ordinary observation requires an explicit context/status consumer");
        }
        return qualified;
    }

    public enum OfferResult {
        ACCEPTED,
        DROPPED_FULL,
        DROPPED_CLOSED,
        DROPPED_BEFORE_CONTEXT
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
            Throwable firstExportFailure,
            long droppedBeforeContext,
            ObservationStatus observationStatus) {
        public Snapshot(long accepted, long exported, long droppedFull, long rejectedAfterClose,
                long exportFailures, long droppedAtWorkerStop, int pendingRecords, long pendingBytes,
                boolean closed, boolean workerTerminated, Throwable firstExportFailure) {
            this(accepted, exported, droppedFull, rejectedAfterClose, exportFailures, droppedAtWorkerStop,
                    pendingRecords, pendingBytes, closed, workerTerminated, firstExportFailure, 0, null);
        }

        public long gapEvents() {
            return Math.addExact(
                    Math.addExact(droppedFull, Math.addExact(rejectedAfterClose, exportFailures)),
                    Math.addExact(droppedAtWorkerStop, droppedBeforeContext));
        }
    }

    private final Limits limits;
    private final Exporter exporter;
    private final OrdinaryObservationConfiguration observationConfiguration;
    private final ObservationExporter observationExporter;
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
    private long droppedBeforeContext;
    private long statusExportFailures;
    private OrdinaryObservationContext observationContext;
    private long statusRevision;
    private boolean statusDirty;
    private int statusAttempts;
    private boolean statusRetryPending;
    private long lastStatusAttemptNanos;
    private final AtomicReference<Throwable> firstExportFailure = new AtomicReference<>();
    private final Thread worker;
    private int pendingRecords;
    private long pendingBytes;
    private boolean accepting = true;
    private boolean workerTerminated;

    public BoundedAsyncMetricExporter(final Limits limits, final Exporter exporter) {
        this(limits, exporter, null);
    }

    public BoundedAsyncMetricExporter(final Limits limits, final Exporter exporter,
            final OrdinaryObservationConfiguration observationConfiguration) {
        this.limits = Objects.requireNonNull(limits, "limits");
        this.exporter = Objects.requireNonNull(exporter, "exporter");
        this.observationConfiguration = observationConfiguration;
        observationExporter = observationConfiguration == null ? null : requireObservationExporter(exporter);
        if (observationConfiguration != null && limits.maximumPendingBytes() < OBSERVED_EVENT_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("metric queue cannot hold a configured observation event");
        }
        worker = new Thread(this::runWorker, "nereus-delay-metric-exporter-" + THREAD_IDS.incrementAndGet());
        worker.setDaemon(true);
        worker.start();
    }

    /** Installs exactly once before normal observation admission; no live mode-switch protocol is introduced. */
    public void installObservationContext(OrdinaryObservationContext context) {
        final var exact = Objects.requireNonNull(context, "actual ordinary observation context");
        lock.lock();
        try {
            if (!accepting || observationConfiguration == null || observationContext != null
                    || !observationConfiguration.equals(exact.configuration()) || !limits.equals(exact.queue())) {
                throw new IllegalStateException("ordinary observation context is absent, changed or already installed");
            }
            observationContext = exact;
            markStatusChanged();
        } finally {
            lock.unlock();
        }
    }

    public void requireObservationContext() {
        lock.lock();
        try {
            if (observationConfiguration != null && observationContext == null) {
                throw new IllegalStateException("configured observation has no actual Source boundary");
            }
        } finally { lock.unlock(); }
    }

    /**
     * Offers one event without waiting for exporter work. Every rejected event
     * contributes to {@link Snapshot#gapEvents()}.
     */
    public OfferResult record(final MetricEvent event) {
        final MetricEvent requested = Objects.requireNonNull(event, "event");
        lock.lock();
        try {
            if (!accepting) {
                rejectedAfterClose.incrementAndGet();
                markStatusChanged();
                return OfferResult.DROPPED_CLOSED;
            }
            if (observationConfiguration != null && observationContext == null) {
                droppedBeforeContext++;
                return OfferResult.DROPPED_BEFORE_CONTEXT;
            }
            final int eventBytes = observationContext == null ? requested.canonicalPayloadBytes()
                    : OBSERVED_EVENT_PAYLOAD_BYTES;
            if (pendingRecords >= limits.maximumPendingRecords()
                    || pendingBytes > limits.maximumPendingBytes() - eventBytes) {
                droppedFull.incrementAndGet();
                markStatusChanged();
                return OfferResult.DROPPED_FULL;
            }
            final MetricEvent exact = observationContext == null ? requested
                    : new MetricEvent(requested.metric(), requested.value(), observationStatus());
            pending.addLast(exact);
            pendingRecords++;
            pendingBytes += eventBytes;
            accepted.incrementAndGet();
            // A later observation supplies another finite retry opportunity after a status outage.
            if (observationConfiguration != null && statusDirty
                    && statusAttempts >= observationConfiguration.maximumStatusAttempts()) statusAttempts = 0;
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
            if (accepting) {
                accepting = false;
                markStatusChanged();
                changed.signalAll();
            }
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
                    firstExportFailure.get(),
                    droppedBeforeContext,
                    observationContext == null ? null : observationStatus());
        } finally {
            lock.unlock();
        }
    }

    /** Proves that the original export thread returned, including its final cleanup. Never waits on itself. */
    public boolean terminationComplete() {
        lock.lock();
        try {
            return !accepting && workerTerminated && !worker.isAlive();
        } finally {
            lock.unlock();
        }
    }

    private void runWorker() {
        boolean stoppedUnexpectedly = false;
        boolean preferStatus = true;
        try {
            while (true) {
                MetricEvent event = null;
                ObservationStatus status = null;
                long revision = 0;
                lock.lock();
                try {
                    while (true) {
                        final boolean retryable = observationContext != null && statusDirty
                                && statusAttempts < observationConfiguration.maximumStatusAttempts();
                        final long retryWait = retryable && statusRetryPending
                                ? observationConfiguration.statusRetryNanos() - (System.nanoTime() - lastStatusAttemptNanos) : 0;
                        if (retryable && retryWait <= 0 && (preferStatus || pending.isEmpty())) {
                            status = observationStatus();
                            revision = statusRevision;
                            statusAttempts++;
                            lastStatusAttemptNanos = System.nanoTime();
                            preferStatus = false;
                            break;
                        }
                        if (!pending.isEmpty()) {
                            final var queued = pending.removeFirst();
                            event = observationContext == null ? queued
                                    : new MetricEvent(queued.metric(), queued.value(), observationStatus());
                            preferStatus = true;
                            break;
                        }
                        if (retryable) {
                            if (retryWait > 0) changed.awaitNanos(retryWait);
                        } else if (!accepting) {
                            return;
                        } else {
                            changed.await();
                        }
                    }
                } finally {
                    lock.unlock();
                }
                if (status != null) {
                    exportStatus(status, revision);
                    continue;
                }
                try {
                    exporter.export(event);
                    exported.incrementAndGet();
                } catch (Exception failure) {
                    exportFailures.incrementAndGet();
                    firstExportFailure.compareAndSet(null, failure);
                    eventFailed();
                } catch (Error fatal) {
                    exportFailures.incrementAndGet();
                    firstExportFailure.compareAndSet(null, fatal);
                    eventFailed();
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
                    markStatusChanged();
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
            if (pendingRecords == 0 && !accepting) markStatusChanged();
        } finally {
            lock.unlock();
        }
    }

    /** Coalesced control state bypasses the full metric queue and remains retained after finite retry exhaustion. */
    private void markStatusChanged() {
        if (observationContext == null) return;
        statusRevision++;
        if (!statusDirty || statusAttempts >= observationConfiguration.maximumStatusAttempts()) statusAttempts = 0;
        statusDirty = true;
        changed.signal();
    }

    private ObservationStatus observationStatus() {
        return new ObservationStatus(observationContext,
                new ExportGap(droppedFull.get(), rejectedAfterClose.get(), exportFailures.get(),
                        droppedAtWorkerStop.get(), droppedBeforeContext),
                statusExportFailures, accepting, !accepting && pendingRecords == 0);
    }

    private void eventFailed() {
        lock.lock();
        try { markStatusChanged(); }
        finally { lock.unlock(); }
    }

    private void exportStatus(ObservationStatus status, long revision) {
        try {
            observationExporter.exportObservationStatus(status);
            lock.lock();
            try {
                if (revision == statusRevision) statusDirty = false;
                statusAttempts = 0;
                statusRetryPending = false;
            } finally { lock.unlock(); }
        } catch (Exception failure) {
            statusFailed(failure);
        } catch (Error fatal) {
            statusFailed(fatal);
            throw fatal;
        }
    }

    private void statusFailed(Throwable failure) {
        firstExportFailure.compareAndSet(null, failure);
        lock.lock();
        try {
            statusExportFailures++;
            statusRetryPending = true;
        } finally { lock.unlock(); }
    }
}
