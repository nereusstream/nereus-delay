package com.nereusstream.delay.scheduler;

import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.Exporter;
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
public final class OpenTelemetryMetricExporter implements Exporter {
    private final Map<Metric, LongCounter> counters;
    private final Map<Metric, LongHistogram> histograms;

    public OpenTelemetryMetricExporter(final Meter meter) {
        final Meter exactMeter = Objects.requireNonNull(meter, "meter");
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
    }

    @Override
    public void export(final MetricEvent event) {
        final MetricEvent exact = Objects.requireNonNull(event, "event");
        if (exact.metric().instrument() == Instrument.COUNTER) {
            counters.get(exact.metric()).add(exact.value());
        } else {
            histograms.get(exact.metric()).record(exact.value());
        }
    }

    private static String unit(final Metric metric) {
        return switch (metric) {
            case TARGET_DRR_TURN_SCHEDULING_BYTES -> "By";
            case TARGET_DRR_TURN_DURATION_NANOS,
                    TARGET_DRR_TARGET_SERVICE_INTERVAL_NANOS,
                    TARGET_INVENTORY_REBUILD_DURATION_NANOS -> "ns";
            case TARGET_INVENTORY_SCAN_BUDGET_BYTES -> "By";
            case TARGET_DRR_TURN_VISITS,
                    TARGET_DRR_TURN_QUEUE_REFRESH_CALLS,
                    TARGET_DRR_TURN_HEAD_PROBE_CALLS,
                    TARGET_INVENTORY_SCAN_PAGES,
                    TARGET_INVENTORY_SCAN_BUDGET_RECORDS,
                    TARGET_DRR_CLAIM_TURNS,
                    TARGET_DRR_STOP_NORMAL,
                    TARGET_DRR_STOP_READ_INCOMPLETE,
                    TARGET_DRR_STOP_CREDIT_WAIT,
                    TARGET_DRR_STOP_BUDGET_WAIT -> "1";
        };
    }
}
