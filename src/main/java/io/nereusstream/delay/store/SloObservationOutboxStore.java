package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.SloObservationOutboxV1;
import io.nereusstream.delay.protocol.SloObjectiveV1;
import io.nereusstream.delay.protocol.SloSampleFinalV1;
import io.nereusstream.delay.protocol.SloSampleStartV1;
import io.nereusstream.delay.protocol.SloThresholdDirectionV1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    private final SloObservationOutboxLimits limits;

    /**
     * Compatibility constructor for embedded callers that supply the
     * capacity envelope at a higher layer. Production wiring must use the
     * limit-aware constructor so a shard cannot grow an unbounded outbox.
     */
    public SloObservationOutboxStore(final ShardStore store) {
        this(store, null);
    }

    public SloObservationOutboxStore(final ShardStore store, final SloObservationOutboxLimits limits) {
        this.store = Objects.requireNonNull(store, "store");
        this.limits = limits;
    }

    /** Returns the exact local projection, or {@code null} when no Start exists. */
    public synchronized SloObservationOutboxV1 get(final byte[] sampleId) {
        final ValueEnvelope.Decoded value = store.getValue(ColumnFamily.META, KeyCodec.metaSloOutbox(sampleId),
                VALUE_TYPE);
        if (value == null) {
            return null;
        }
        final SloObservationOutboxV1 outbox = SloObservationOutboxV1.decode(value.payload());
        if (!Bytes.constantTimeEquals(sampleId, outbox.sampleId())) {
            throw new IllegalStateException("SLO_OUTBOX key/value sample identity mismatch");
        }
        return outbox;
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
            if (limits != null) {
                usage();
            }
            return existing;
        }
        final SloObservationOutboxV1 created = SloObservationOutboxV1.open(start);
        requireCapacity(ValueEnvelope.encode(VALUE_TYPE, created.canonicalBytes()).length, 1);
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
        requireNoDueExclusion(finalObservation, existing);
        final SloObservationOutboxV1 merged = existing.mergeFinal(finalObservation, direction);
        requireReplacementCapacity(existing, merged);
        persist(KeyCodec.metaSloOutbox(merged.sampleId()), merged);
        return merged;
    }

    /**
     * Merges a Final whose ALL_ACCEPTED due sample may carry an exclusion.
     * The paired HEALTHY objective is required at this durable boundary so a
     * caller cannot persist a reason that is absent from the immutable SLO
     * catalog pair.
     */
    public synchronized SloObservationOutboxV1 mergeFinal(final SloSampleFinalV1 finalObservation,
                                                           final SloThresholdDirectionV1 direction,
                                                           final SloObjectiveV1 healthyObjective) {
        Objects.requireNonNull(finalObservation, "finalObservation");
        Objects.requireNonNull(direction, "direction");
        Objects.requireNonNull(healthyObjective, "healthyObjective");
        final SloObservationOutboxV1 existing = get(finalObservation.sampleId());
        if (existing == null) {
            throw new IllegalStateException("cannot persist SLO Final without a durable Start");
        }
        final SloObservationOutboxV1 merged = existing.mergeFinal(finalObservation, direction, healthyObjective);
        requireReplacementCapacity(existing, merged);
        persist(KeyCodec.metaSloOutbox(merged.sampleId()), merged);
        return merged;
    }

    /** Returns a bounded key-order snapshot for at-least-once export retry. */
    public synchronized List<SloObservationOutboxV1> scan(final int limit) {
        return scan(limit, limits == null ? Long.MAX_VALUE : limits.maxBytes());
    }

    /**
     * Returns a key-order snapshot bounded by both record count and encoded
     * ValueEnvelope bytes. A first record that cannot fit fails closed; later
     * records are left for the next export turn after earlier acknowledgements.
     */
    public synchronized List<SloObservationOutboxV1> scan(final int limit, final long maxBytes) {
        if (limit <= 0) {
            throw new IllegalArgumentException("SLO outbox scan limit must be positive");
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("SLO outbox scan byte limit must be positive");
        }
        if (limits != null && limit > limits.maxRecords()) {
            throw new IllegalArgumentException("SLO outbox scan exceeds the configured record budget");
        }
        if (limits != null && maxBytes > limits.maxBytes()) {
            throw new IllegalArgumentException("SLO outbox scan exceeds the configured byte budget");
        }
        final List<ShardStore.KeyValue> entries = store.scan(ColumnFamily.META,
                new byte[]{8, 1}, new byte[]{8, 2}, limit);
        final List<SloObservationOutboxV1> result = new ArrayList<>(entries.size());
        long totalBytes = 0;
        for (ShardStore.KeyValue entry : entries) {
            final long encodedBytes = entry.value().length;
            if (encodedBytes > maxBytes - totalBytes) {
                if (result.isEmpty()) {
                    throw new IllegalStateException("SLO outbox record exceeds the export byte budget");
                }
                break;
            }
            try {
                totalBytes = Math.addExact(totalBytes, encodedBytes);
            } catch (ArithmeticException exception) {
                throw new IllegalStateException("SLO outbox export byte usage overflow", exception);
            }
            result.add(decodeEntry(entry));
        }
        return List.copyOf(result);
    }

    /**
     * Returns a strict usage snapshot for local metrics and admission checks.
     * When limits are supplied, a corrupt or over-capacity existing projection
     * fails closed.
     */
    public synchronized Usage usage() {
        final int scanLimit = limits == null || limits.maxRecords() == Integer.MAX_VALUE
                ? Integer.MAX_VALUE : limits.maxRecords() + 1;
        final List<ShardStore.KeyValue> entries = store.scan(ColumnFamily.META,
                new byte[]{8, 1}, new byte[]{8, 2}, scanLimit);
        if (limits != null && entries.size() > limits.maxRecords()) {
            throw new IllegalStateException("SLO outbox record capacity is already exceeded");
        }
        long totalBytes = 0;
        for (ShardStore.KeyValue entry : entries) {
            decodeEntry(entry);
            try {
                totalBytes = Math.addExact(totalBytes, entry.value().length);
            } catch (ArithmeticException exception) {
                throw new IllegalStateException("SLO outbox byte usage overflow", exception);
            }
        }
        if (limits != null && totalBytes > limits.maxBytes()) {
            throw new IllegalStateException("SLO outbox byte capacity is already exceeded");
        }
        return new Usage(entries.size(), totalBytes);
    }

    /**
     * Deletes one record only after an external collector has acknowledged
     * the exact bytes. A changed record is an integrity failure, not a reason
     * to delete a newer observation.
     */
    public synchronized boolean deleteAfterCollectorAck(final byte[] sampleId, final byte[] recordDigest) {
        final SloObservationOutboxV1 existing = get(sampleId);
        if (existing == null) {
            return false;
        }
        if (!Arrays.equals(existing.recordDigest(), recordDigest)) {
            throw new IllegalStateException("SLO collector acknowledgement does not match record digest");
        }
        store.write(batch -> batch.delete(ColumnFamily.META, KeyCodec.metaSloOutbox(sampleId)));
        return true;
    }

    private void persist(final byte[] key, final SloObservationOutboxV1 outbox) {
        store.write(batch -> batch.putValue(ColumnFamily.META, VALUE_TYPE, key, outbox.canonicalBytes()));
    }

    private void requireReplacementCapacity(final SloObservationOutboxV1 existing,
                                             final SloObservationOutboxV1 replacement) {
        if (limits == null) {
            return;
        }
        final Usage usage = usage();
        if (existing.equals(replacement)) {
            return;
        }
        final int oldBytes = ValueEnvelope.encode(VALUE_TYPE, existing.canonicalBytes()).length;
        final int newBytes = ValueEnvelope.encode(VALUE_TYPE, replacement.canonicalBytes()).length;
        if (usage.recordCount() <= 0 || usage.encodedBytes() < oldBytes) {
            throw new IllegalStateException("SLO outbox replacement is missing its existing record");
        }
        try {
            final long withoutExisting = Math.subtractExact(usage.encodedBytes(), oldBytes);
            requireCapacity(newBytes, 0, usage.recordCount() - 1, withoutExisting);
        } catch (ArithmeticException exception) {
            throw new IllegalStateException("SLO outbox byte usage overflow", exception);
        }
    }

    private void requireCapacity(final long addedBytes, final int addedRecords) {
        if (limits == null) {
            return;
        }
        final Usage usage = usage();
        requireCapacity(addedBytes, addedRecords, usage.recordCount(), usage.encodedBytes());
    }

    private void requireCapacity(final long addedBytes, final int addedRecords,
                                 final int currentRecords, final long currentBytes) {
        if (addedBytes < 0 || addedRecords < 0) {
            throw new IllegalArgumentException("SLO outbox capacity delta must be non-negative");
        }
        final long nextRecords;
        final long nextBytes;
        try {
            nextRecords = Math.addExact((long) currentRecords, addedRecords);
            nextBytes = Math.addExact(currentBytes, addedBytes);
        } catch (ArithmeticException exception) {
            throw new IllegalStateException("SLO outbox capacity arithmetic overflow", exception);
        }
        if (nextRecords > limits.maxRecords() || nextBytes > limits.maxBytes()) {
            throw new IllegalStateException("SLO outbox capacity exceeded");
        }
    }

    private static SloObservationOutboxV1 decodeEntry(final ShardStore.KeyValue entry) {
        final byte[] key = entry.key();
        if (key.length != 34 || key[0] != 8 || key[1] != 1) {
            throw new IllegalStateException("invalid SLO_OUTBOX key shape");
        }
        final SloObservationOutboxV1 outbox = SloObservationOutboxV1.decode(
                ValueEnvelope.decode(entry.value(), VALUE_TYPE).payload());
        if (!Bytes.constantTimeEquals(Arrays.copyOfRange(key, 2, key.length), outbox.sampleId())) {
            throw new IllegalStateException("SLO_OUTBOX key/value sample identity mismatch");
        }
        return outbox;
    }

    public record Usage(int recordCount, long encodedBytes) {
        public Usage {
            if (recordCount < 0 || encodedBytes < 0) {
                throw new IllegalArgumentException("SLO outbox usage cannot be negative");
            }
        }
    }

    private static void requireNoDueExclusion(final SloSampleFinalV1 incoming,
                                              final SloObservationOutboxV1 existing) {
        if (incoming.exclusionReason() != null
                || existing.finalObservation() != null && existing.finalObservation().exclusionReason() != null) {
            throw new IllegalArgumentException(
                    "SLO due exclusions require the paired HEALTHY objective at the durable merge boundary");
        }
    }
}
