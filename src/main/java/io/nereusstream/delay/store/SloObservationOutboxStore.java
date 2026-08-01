package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.SloObservationOutboxV1;
import io.nereusstream.delay.protocol.SloSampleFinalV1;
import io.nereusstream.delay.protocol.SloSampleStartV1;
import io.nereusstream.delay.protocol.SloThresholdDirectionV1;

import java.util.Objects;

/**
 * Single-shard durable SLO outbox access.
 *
 * <p>The store deliberately uses the shard's synchronous WriteBatch boundary.
 * It does not export or acknowledge a collector; those operations must remain
 * outside the business-state transaction.</p>
 */
public final class SloObservationOutboxStore {
    public static final int VALUE_TYPE = 9;

    private final ShardStore store;

    public SloObservationOutboxStore(final ShardStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /** Returns the exact local projection, or {@code null} when no Start exists. */
    public synchronized SloObservationOutboxV1 get(final byte[] sampleId) {
        final ValueEnvelope.Decoded value = store.getValue(ColumnFamily.META, KeyCodec.metaSloOutbox(sampleId),
                VALUE_TYPE);
        return value == null ? null : SloObservationOutboxV1.decode(value.payload());
    }

    /**
     * Persists a Start before the measured operation can lose ownership. An
     * exact retry is idempotent; a different Start for one sample identity is
     * an integrity failure.
     */
    public synchronized SloObservationOutboxV1 ensureStart(final SloSampleStartV1 start) {
        Objects.requireNonNull(start, "start");
        final byte[] key = KeyCodec.metaSloOutbox(start.sampleId());
        final SloObservationOutboxV1 existing = get(start.sampleId());
        if (existing != null) {
            if (!existing.start().equals(start)) {
                throw new IllegalStateException("SLO sample identity has different Start bytes");
            }
            return existing;
        }
        final SloObservationOutboxV1 created = SloObservationOutboxV1.open(start);
        persist(key, created);
        return created;
    }

    /**
     * Merges and durably replaces the exact sample projection in one synced
     * batch. A missing Start is rejected so callers cannot create a Final-only
     * denominator.
     */
    public synchronized SloObservationOutboxV1 mergeFinal(final SloSampleFinalV1 finalObservation,
                                                           final SloThresholdDirectionV1 direction) {
        Objects.requireNonNull(finalObservation, "finalObservation");
        Objects.requireNonNull(direction, "direction");
        final SloObservationOutboxV1 existing = get(finalObservation.sampleId());
        if (existing == null) {
            throw new IllegalStateException("cannot persist SLO Final without a durable Start");
        }
        final SloObservationOutboxV1 merged = existing.mergeFinal(finalObservation, direction);
        persist(KeyCodec.metaSloOutbox(merged.sampleId()), merged);
        return merged;
    }

    private void persist(final byte[] key, final SloObservationOutboxV1 outbox) {
        store.write(batch -> batch.putValue(ColumnFamily.META, VALUE_TYPE, key, outbox.canonicalBytes()));
    }
}
