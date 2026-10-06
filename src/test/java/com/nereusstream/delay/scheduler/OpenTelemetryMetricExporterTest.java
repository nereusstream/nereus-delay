package com.nereusstream.delay.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.Limits;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.Metric;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.MetricEvent;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class OpenTelemetryMetricExporterTest {
    @Test
    void boundedEventsReachFixedOpenTelemetryInstrumentsWithoutAttributes() {
        final InMemoryMetricReader reader = InMemoryMetricReader.create();
        try (var provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
                var metrics = new BoundedAsyncMetricExporter(
                        new Limits(14, 126), new OpenTelemetryMetricExporter(provider.get("nereus-delay")))) {
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_DRR_CLAIM_TURNS, 3)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_DRR_TURN_VISITS, 4)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_DRR_TURN_SCHEDULING_BYTES, 128)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_DRR_TARGET_SERVICE_INTERVAL_NANOS, 17)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_DRR_TURN_QUEUE_REFRESH_CALLS, 2)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_DRR_TURN_HEAD_PROBE_CALLS, 1)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_INVENTORY_SCAN_PAGES, 5)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_INVENTORY_SCAN_BUDGET_RECORDS, 9)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_INVENTORY_SCAN_BUDGET_BYTES, 256)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_INVENTORY_REBUILD_DURATION_NANOS, 21)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_INVENTORY_ACTIVE_MESSAGE_DEPTH, 17)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_INVENTORY_ACTIVE_MESSAGE_DEPTH_UNAVAILABLE, 2)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_PHYSICAL_PUBLISH_STAGE_DURATION_NANOS, 37)));
            assertEquals(
                    BoundedAsyncMetricExporter.OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_PHYSICAL_PUBLISH_STAGE_UNAVAILABLE, 1)));
            metrics.stopAccepting();
            assertTrue(metrics.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(14, metrics.snapshot().exported());

            final Map<String, MetricData> observed = reader.collectAllMetrics().stream()
                    .collect(Collectors.toMap(MetricData::getName, Function.identity()));
            final var claimMetric = observed.get("nereus.target.drr.claim.turns");
            assertEquals("1", claimMetric.getUnit());
            final var claims = claimMetric.getLongSumData().getPoints().stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(3, claims.getValue());
            assertTrue(claims.getAttributes().isEmpty());

            final var visitsMetric = observed.get("nereus.target.drr.turn.visits");
            assertEquals("1", visitsMetric.getUnit());
            final var visits = visitsMetric.getHistogramData().getPoints().stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(1, visits.getCount());
            assertEquals(4.0, visits.getSum(), 0.0);
            assertTrue(visits.getAttributes().isEmpty());

            final var schedulingBytes = observed.get("nereus.target.drr.turn.scheduling.bytes");
            assertEquals("By", schedulingBytes.getUnit());
            final var bytePoint = schedulingBytes.getHistogramData().getPoints().stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(128.0, bytePoint.getSum(), 0.0);
            assertTrue(bytePoint.getAttributes().isEmpty());

            final var intervalMetric = observed.get("nereus.target.drr.target.service.interval.nanos");
            assertEquals("ns", intervalMetric.getUnit());
            final var interval = intervalMetric
                    .getHistogramData()
                    .getPoints()
                    .stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(1, interval.getCount());
            assertEquals(17.0, interval.getSum(), 0.0);
            assertTrue(interval.getAttributes().isEmpty());

            final var refreshCalls = observed.get("nereus.target.drr.turn.queue_refresh.calls");
            assertEquals("1", refreshCalls.getUnit());
            final var refreshCallPoint = refreshCalls.getHistogramData().getPoints().stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(1, refreshCallPoint.getCount());
            assertEquals(2.0, refreshCallPoint.getSum(), 0.0);
            assertTrue(refreshCallPoint.getAttributes().isEmpty());

            final var headProbeCalls = observed.get("nereus.target.drr.turn.head_probe.calls");
            assertEquals("1", headProbeCalls.getUnit());
            final var headProbeCallPoint = headProbeCalls.getHistogramData().getPoints().stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(1, headProbeCallPoint.getCount());
            assertEquals(1.0, headProbeCallPoint.getSum(), 0.0);
            assertTrue(headProbeCallPoint.getAttributes().isEmpty());

            final var scanPages = observed.get("nereus.target.inventory.scan.pages");
            assertEquals("1", scanPages.getUnit());
            final var scanPagePoint = scanPages.getHistogramData().getPoints().stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(1, scanPagePoint.getCount());
            assertEquals(5.0, scanPagePoint.getSum(), 0.0);
            assertTrue(scanPagePoint.getAttributes().isEmpty());

            final var budgetRecords = observed.get("nereus.target.inventory.scan.budget.records");
            assertEquals("1", budgetRecords.getUnit());
            final var budgetRecordPoint = budgetRecords.getHistogramData().getPoints().stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(1, budgetRecordPoint.getCount());
            assertEquals(9.0, budgetRecordPoint.getSum(), 0.0);
            assertTrue(budgetRecordPoint.getAttributes().isEmpty());

            final var budgetBytes = observed.get("nereus.target.inventory.scan.budget.bytes");
            assertEquals("By", budgetBytes.getUnit());
            final var budgetBytePoint = budgetBytes.getHistogramData().getPoints().stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(1, budgetBytePoint.getCount());
            assertEquals(256.0, budgetBytePoint.getSum(), 0.0);
            assertTrue(budgetBytePoint.getAttributes().isEmpty());

            final var rebuildDuration = observed.get("nereus.target.inventory.rebuild.duration");
            assertEquals("ns", rebuildDuration.getUnit());
            final var rebuildDurationPoint = rebuildDuration.getHistogramData().getPoints().stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(1, rebuildDurationPoint.getCount());
            assertEquals(21.0, rebuildDurationPoint.getSum(), 0.0);
            assertTrue(rebuildDurationPoint.getAttributes().isEmpty());

            final var depthMetric = observed.get("nereus.target.inventory.active.message.depth");
            assertEquals("1", depthMetric.getUnit());
            final var depthPoint = depthMetric.getHistogramData().getPoints().stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(1, depthPoint.getCount());
            assertEquals(17.0, depthPoint.getSum(), 0.0);
            assertTrue(depthPoint.getAttributes().isEmpty());

            final var unavailableMetric = observed.get("nereus.target.inventory.active.message.depth.unavailable");
            assertEquals("1", unavailableMetric.getUnit());
            final var unavailablePoint = unavailableMetric.getLongSumData().getPoints().stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(2, unavailablePoint.getValue());
            assertTrue(unavailablePoint.getAttributes().isEmpty());

            final var physicalPublishDuration = observed.get("nereus.target.physical.publish.stage.duration");
            assertEquals("ns", physicalPublishDuration.getUnit());
            final var physicalPublishDurationPoint = physicalPublishDuration
                    .getHistogramData()
                    .getPoints()
                    .stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(1, physicalPublishDurationPoint.getCount());
            assertEquals(37.0, physicalPublishDurationPoint.getSum(), 0.0);
            assertTrue(physicalPublishDurationPoint.getAttributes().isEmpty());

            final var physicalPublishUnavailable = observed.get("nereus.target.physical.publish.stage.unavailable");
            assertEquals("1", physicalPublishUnavailable.getUnit());
            final var physicalPublishUnavailablePoint = physicalPublishUnavailable
                    .getLongSumData()
                    .getPoints()
                    .stream()
                    .findFirst()
                    .orElseThrow();
            assertEquals(1, physicalPublishUnavailablePoint.getValue());
            assertTrue(physicalPublishUnavailablePoint.getAttributes().isEmpty());
        }
    }
}
