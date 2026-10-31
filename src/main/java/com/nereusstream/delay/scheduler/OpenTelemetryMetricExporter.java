package com.nereusstream.delay.scheduler;

import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.ObservationExporter;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.ObservationStatus;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.Instrument;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.Metric;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.MetricEvent;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * Records fixed-cardinality process events into a caller-configured OpenTelemetry Meter.
 *
 * <p>Wrap this exporter in {@link BoundedAsyncMetricExporter} before passing process events from a scheduler. No
 * metric attributes are attached, so Target IDs, Shard IDs and other request-specific values cannot grow cardinality.
 */
public final class OpenTelemetryMetricExporter implements ObservationExporter {
    @FunctionalInterface
    public interface ObservationStatusSink {
        /** Receives context and cumulative gaps on the existing export thread, with no metric attributes. */
        void accept(ObservationStatus status);
    }

    private final Map<Metric, LongCounter> counters;
    private final Map<Metric, LongHistogram> histograms;
    private final ObservationStatusSink statusSink;
    private final LongCounter[] gapCounters;
    private final long[] recordedGaps = new long[6];
    private final LongCounter priorProcessUnknown;
    private java.util.UUID recordedInstance;
    private ObservationStatus lastStatus;

    public OpenTelemetryMetricExporter(final Meter meter) {
        this(meter, null);
    }

    /** A Meter alone has no channel for exact Source boundary/generation or unknown prior-process coverage. */
    public OpenTelemetryMetricExporter(final Meter meter, final ObservationStatusSink statusSink) {
        final Meter exactMeter = Objects.requireNonNull(meter, "meter");
        this.statusSink = statusSink;
        final Map<Metric, LongCounter> counterInstruments = new EnumMap<>(Metric.class);
        final Map<Metric, LongHistogram> histogramInstruments = new EnumMap<>(Metric.class);
        for (Metric metric : Metric.values()) {
            if (metric.instrument() == Instrument.COUNTER) {
                counterInstruments.put(
                        metric,
                        exactMeter.counterBuilder(metric.instrumentName())
                                .setUnit(unit(metric))
                                .build());
            } else {
                histogramInstruments.put(
                        metric,
                        exactMeter.histogramBuilder(metric.instrumentName())
                                .ofLongs()
                                .setUnit(unit(metric))
                                .build());
            }
        }
        counters = Map.copyOf(counterInstruments);
        histograms = Map.copyOf(histogramInstruments);
        final String prefix = "nereus.target.observation.";
        final String[] signals = {"dropped_full", "rejected_after_close", "export_failures",
                "dropped_at_worker_stop", "dropped_before_context", "status_export_failures"};
        gapCounters = new LongCounter[signals.length];
        for (int index = 0; index < signals.length; index++) {
            gapCounters[index] = exactMeter.counterBuilder(prefix + signals[index]).setUnit("1").build();
        }
        priorProcessUnknown = exactMeter.counterBuilder(prefix + "prior_process_coverage_unknown").setUnit("1").build();
    }

    @Override public boolean supportsObservationStatus() { return statusSink != null; }

    @Override
    public void export(final MetricEvent event) {
        final MetricEvent exact = Objects.requireNonNull(event, "event");
        if (exact.observation() != null) exportObservationStatus(exact.observation());
        if (exact.metric().instrument() == Instrument.COUNTER) {
            counters.get(exact.metric()).add(exact.value());
        } else {
            histograms.get(exact.metric()).record(exact.value());
        }
    }

    @Override
    public synchronized void exportObservationStatus(final ObservationStatus status) {
        final var exact = Objects.requireNonNull(status, "ordinary observation status");
        if (statusSink == null) throw new IllegalStateException("OpenTelemetry ordinary context/status consumer is absent");
        if (exact.equals(lastStatus)) return;
        // Exact identities/cuts go only to the explicit sink. No UUID, Target, Shard or generation metric labels.
        statusSink.accept(exact);
        if (!exact.context().processInstance().equals(recordedInstance)) {
            priorProcessUnknown.add(1);
            java.util.Arrays.fill(recordedGaps, 0);
            recordedInstance = exact.context().processInstance();
        }
        final var gap = exact.gap();
        final long[] cumulative = {gap.droppedFull(), gap.rejectedAfterClose(), gap.exportFailures(),
                gap.droppedAtWorkerStop(), gap.droppedBeforeContext(), exact.statusExportFailures()};
        for (int index = 0; index < cumulative.length; index++) {
            final long delta = Math.subtractExact(cumulative[index], recordedGaps[index]);
            if (delta < 0) throw new IllegalArgumentException("ordinary gap counters regressed within the same process instance");
            if (delta != 0) gapCounters[index].add(delta);
            recordedGaps[index] = cumulative[index];
        }
        lastStatus = exact;
    }

    private static String unit(final Metric metric) {
        return switch (metric) {
            case TARGET_DRR_TURN_SCHEDULING_BYTES -> "By";
            case TARGET_DRR_TURN_DURATION_NANOS,
                    TARGET_DRR_TARGET_SERVICE_INTERVAL_NANOS,
                    TARGET_INVENTORY_REBUILD_DURATION_NANOS,
                    TARGET_PHYSICAL_PUBLISH_STAGE_DURATION_NANOS -> "ns";
            case TARGET_INVENTORY_SCAN_BUDGET_BYTES -> "By";
            case TARGET_DRR_TURN_VISITS,
                    TARGET_DRR_TURN_QUEUE_REFRESH_CALLS,
                    TARGET_DRR_TURN_HEAD_PROBE_CALLS,
                    TARGET_INVENTORY_SCAN_PAGES,
                    TARGET_INVENTORY_SCAN_BUDGET_RECORDS,
                    TARGET_INVENTORY_ACTIVE_MESSAGE_DEPTH,
                    TARGET_INVENTORY_ACTIVE_MESSAGE_DEPTH_UNAVAILABLE,
                    TARGET_PHYSICAL_PUBLISH_STAGE_UNAVAILABLE,
                    TARGET_DRR_CLAIM_TURNS,
                    TARGET_DRR_STOP_NORMAL,
                    TARGET_DRR_STOP_READ_INCOMPLETE,
                    TARGET_DRR_STOP_CREDIT_WAIT,
                    TARGET_DRR_STOP_BUDGET_WAIT -> "1";
        };
    }
}
