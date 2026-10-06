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
                        new Limits(8, 72), new OpenTelemetryMetricExporter(provider.get("nereus-delay")))) {
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
            metrics.stopAccepting();
            assertTrue(metrics.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(4, metrics.snapshot().exported());

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
        }
    }
}
