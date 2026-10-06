package com.nereusstream.delay.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.Metric;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.MetricEvent;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.OfferResult;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BoundedAsyncMetricExporterTest {
    @Test
    void boundsPendingRecordsAndBytesIncludingTheInFlightExport() throws Exception {
        final CountDownLatch enteredExporter = new CountDownLatch(1);
        final CountDownLatch releaseExporter = new CountDownLatch(1);
        final var exported = new CopyOnWriteArrayList<MetricEvent>();
        try (var metrics = new BoundedAsyncMetricExporter(
                new BoundedAsyncMetricExporter.Limits(2, 18), event -> {
                    if (event.metric() == Metric.TARGET_DRR_TURN_VISITS) {
                        enteredExporter.countDown();
                        if (!releaseExporter.await(5, TimeUnit.SECONDS)) {
                            throw new IOException("test exporter release timed out");
                        }
                    }
                    exported.add(event);
                })) {
            assertEquals(
                    OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_DRR_TURN_VISITS, 1)));
            assertTrue(enteredExporter.await(5, TimeUnit.SECONDS));
            assertEquals(
                    OfferResult.ACCEPTED,
                    metrics.record(new MetricEvent(Metric.TARGET_DRR_CLAIM_TURNS, 1)));
            assertEquals(
                    OfferResult.DROPPED_FULL,
                    metrics.record(new MetricEvent(Metric.TARGET_DRR_STOP_NORMAL, 1)));
            assertEquals(2, metrics.snapshot().pendingRecords());
            assertEquals(18, metrics.snapshot().pendingBytes());

            releaseExporter.countDown();
            metrics.stopAccepting();
            assertTrue(metrics.awaitTermination(Duration.ofSeconds(5)));
            final var snapshot = metrics.snapshot();
            assertEquals(2, snapshot.accepted());
            assertEquals(2, snapshot.exported());
            assertEquals(1, snapshot.droppedFull());
            assertEquals(1, snapshot.gapEvents());
            assertEquals(2, exported.size());
        }
    }

    @Test
    void exportFailureIsRecordedAsAGapAndDoesNotStopLaterMetrics() {
        final AtomicInteger calls = new AtomicInteger();
        try (var metrics = new BoundedAsyncMetricExporter(new BoundedAsyncMetricExporter.Limits(4, 36), event -> {
            if (calls.getAndIncrement() == 0) {
                throw new IOException("expected test exporter failure");
            }
        })) {
            metrics.record(new MetricEvent(Metric.TARGET_DRR_CLAIM_TURNS, 1));
            metrics.record(new MetricEvent(Metric.TARGET_DRR_STOP_NORMAL, 1));
            metrics.stopAccepting();
            assertTrue(metrics.awaitTermination(Duration.ofSeconds(5)));
            final var snapshot = metrics.snapshot();
            assertEquals(2, calls.get());
            assertEquals(1, snapshot.exportFailures());
            assertEquals(1, snapshot.exported());
            assertEquals(1, snapshot.gapEvents());
            assertTrue(snapshot.firstExportFailure() instanceof IOException);
        }
    }

    @Test
    void rejectedOfferAfterCloseIsCountedAsUnknownCoverage() {
        try (var metrics = new BoundedAsyncMetricExporter(
                new BoundedAsyncMetricExporter.Limits(1, 9), event -> {})) {
            metrics.stopAccepting();
            assertEquals(
                    OfferResult.DROPPED_CLOSED,
                    metrics.record(new MetricEvent(Metric.TARGET_DRR_STOP_NORMAL, 1)));
            assertTrue(metrics.awaitTermination(Duration.ofSeconds(5)));
            final var snapshot = metrics.snapshot();
            assertEquals(1, snapshot.rejectedAfterClose());
            assertEquals(1, snapshot.gapEvents());
            assertTrue(snapshot.closed());
        }
    }
}
