package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.SloObservationOutbox;
import java.util.List;
import java.util.Objects;

/**
 * At-least-once delivery from a durable Shard SLO outbox to a restart-safe
 * local collector projection.
 *
 * <p>Call this from an exporter worker, away from the source mutation path.
 * Each collector merge is durably published before the exact outbox record is
 * acknowledged. A crash between those operations leaves an idempotently
 * replayable source record.</p>
 *
 * <p>This adapter does not provide production authorization, rolling-window
 * policy, or metric publication authority.</p>
 */
public final class SloObservationOutboxDelivery {
    private final SloObservationOutboxStore outbox;
    private final PersistentSloObservationCollector collector;

    public SloObservationOutboxDelivery(
            final SloObservationOutboxStore outbox, final PersistentSloObservationCollector collector) {
        this.outbox = Objects.requireNonNull(outbox, "outbox");
        this.collector = Objects.requireNonNull(collector, "collector");
    }

    /**
     * Delivers one key-ordered page within the outbox's configured record and
     * value-byte bounds.
     *
     * @return the number of records merged into the collector; a failure
     *         propagates before the corresponding outbox record is deleted
     */
    public int deliverPage(final int maxRecords, final long maxValueBytes) {
        final List<SloObservationOutbox> page = outbox.scan(maxRecords, maxValueBytes);
        for (SloObservationOutbox observation : page) {
            collector.merge(observation, observation.start().objective().requiredDirection());
            outbox.deleteAfterCollectorAck(observation.sampleId(), observation.recordDigest());
        }
        return page.size();
    }
}
