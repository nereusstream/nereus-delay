package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.SloAuthoritativeStartFactory;
import com.nereusstream.delay.protocol.SloObjectiveV1;
import com.nereusstream.delay.protocol.SloObservationOutboxV1;
import com.nereusstream.delay.protocol.SloPathV1;
import com.nereusstream.delay.protocol.SloSampleFinalV1;
import com.nereusstream.delay.protocol.SloSampleStartV1;
import com.nereusstream.delay.protocol.SloThresholdDirectionV1;
import com.nereusstream.delay.protocol.SourcePosition;
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
    private final SloObservationOutboxExportRate exportRate;

    /**
     * Compatibility constructor for embedded callers that supply the
     * capacity envelope at a higher layer. Production wiring must use the
     * limit-aware constructor so a shard cannot grow an unbounded outbox.
     */
    public SloObservationOutboxStore(final ShardStore store) {
        this(store, null, null);
    }

    public SloObservationOutboxStore(final ShardStore store, final SloObservationOutboxLimits limits) {
        this(store, limits, null);
    }

    /** Creates a store with explicit capacity and process-local export-rate bounds. */
    public SloObservationOutboxStore(
            final ShardStore store,
            final SloObservationOutboxLimits limits,
            final SloObservationOutboxExportRate exportRate) {
        this.store = Objects.requireNonNull(store, "store");
        this.limits = limits;
        this.exportRate = exportRate;
    }

    /** Returns the exact local projection, or {@code null} when no Start exists. */
    public synchronized SloObservationOutboxV1 get(final byte[] sampleId) {
        final ValueEnvelope.Decoded value =
                store.getValue(ColumnFamily.META, KeyCodec.metaSloOutbox(sampleId), VALUE_TYPE);
        if (value == null) {
            return null;
        }
        final SloObservationOutboxV1 outbox = SloObservationOutboxV1.decode(value.payload());
        if (!Bytes.constantTimeEquals(sampleId, outbox.sampleId())) {
            throw new IllegalStateException("SLO_OUTBOX key/value sample identity mismatch");
        }
        validateShardBoundStart(outbox.start());
        return outbox;
    }

    /**
     * Persists a Start before the measured operation can lose ownership. An
     * exact retry is idempotent; a different Start for one sample identity is
     * an integrity failure.
     */
    public synchronized SloObservationOutboxV1 ensureStart(final SloSampleStartV1 start) {
        Objects.requireNonNull(start, "start");
        validateShardBoundStart(start);
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
     * Materializes a command-applied Start from the exact typed Source
     * Position authority projection.
     *
     * <p>This is a convenience around {@link #ensureStart(SloSampleStartV1)};
     * it does not discover a Source Position or infer a Broker timestamp from
     * an arbitrary command payload.</p>
     */
    public synchronized SloObservationOutboxV1 ensureCommandAppliedStart(
            final SloObjectiveV1 objective, final SourcePosition sourcePosition) {
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (!store.shardId().equals(sourcePosition.shardId())) {
            throw new IllegalArgumentException("command-applied Source Position belongs to another shard");
        }
        return ensureStart(SloAuthoritativeStartFactory.commandApplied(objective, sourcePosition));
    }

    /**
     * Materializes a due-admission Start from the exact typed Message/
     * eligibility authority projection.
     *
     * <p>The caller supplies the generation, managed path, semantic start and
     * evidence digest. In particular, this method never substitutes
     * {@code deliverAt} for a managed handoff {@code actionAt}, and it cannot
     * create a native-path due-admission sample.</p>
     */
    public synchronized SloObservationOutboxV1 ensureDueAdmissionStart(
            final SloObjectiveV1 objective,
            final DelayMessageId delayMessageId,
            final long generation,
            final SloPathV1 path,
            final long pathStartEpochMs,
            final byte[] semanticEvidenceSha256) {
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (!store.shardId().equals(delayMessageId.routingId().shardId())) {
            throw new IllegalArgumentException("due-admission Message ID belongs to another shard");
        }
        return ensureStart(SloAuthoritativeStartFactory.dueAdmission(
                objective, delayMessageId, generation, path, pathStartEpochMs, semanticEvidenceSha256));
    }

    /**
     * Reconciles a bounded set of Starts reconstructed by the shard's
     * authoritative Message/Admission/Lane/Recovery projection.
     *
     * <p>The store never derives a Start from an arbitrary message and never
     * removes an existing Final.  Inputs are sorted by the canonical sample
     * identity, exact duplicate inputs collapse to one entry, and a different
     * Start for one sample identity fails before any write.  All newly missing
     * Starts are then written in one synchronous RocksDB batch so a recovery
     * turn cannot leave a partially materialized denominator.</p>
     *
     * @return the resulting key-ordered projections, including preserved
     *         existing Finals
     */
    public synchronized List<SloObservationOutboxV1> reconcileDurableStarts(
            final Iterable<SloSampleStartV1> authoritativeStarts) {
        final List<SloObservationOutboxV1> result = new ArrayList<>();
        store.write(batch -> result.addAll(reconcileDurableStartsInBatch(batch, authoritativeStarts)));
        return List.copyOf(result);
    }

    /**
     * Adds the exact missing Starts to a caller-owned synchronous RocksDB
     * batch. This is the source-apply/recovery integration seam: the caller
     * can put Message, Admission, Source Position and SLO projections in one
     * WriteBatch, so a crash cannot commit one boundary without the other.
     *
     * <p>All authoritative Starts for one apply turn must be supplied in this
     * call. The method performs all conflict and capacity checks before it
     * appends anything to {@code batch}; it never reads an arbitrary business
     * message and never removes an existing Final. The caller must invoke this
     * method at most once for a given batch, because RocksDB does not expose
     * uncommitted batch reads to this store.</p>
     *
     * @return the resulting key-ordered projections, including preserved
     *         existing Finals
     * @throws org.rocksdb.RocksDBException when the native batch rejects an
     *         appended value
     */
    public synchronized List<SloObservationOutboxV1> reconcileDurableStartsInBatch(
            final ShardStore.Batch batch, final Iterable<SloSampleStartV1> authoritativeStarts)
            throws org.rocksdb.RocksDBException {
        Objects.requireNonNull(batch, "batch");
        if (!batch.belongsTo(store)) {
            throw new IllegalArgumentException("SLO outbox batch belongs to another ShardStore");
        }
        Objects.requireNonNull(authoritativeStarts, "authoritativeStarts");
        final List<SloSampleStartV1> sorted = new ArrayList<>();
        for (SloSampleStartV1 start : authoritativeStarts) {
            final SloSampleStartV1 checked = Objects.requireNonNull(start, "authoritativeStarts contains null");
            validateShardBoundStart(checked);
            sorted.add(checked);
        }
        sorted.sort((left, right) -> Arrays.compareUnsigned(left.sampleId(), right.sampleId()));

        final List<SloSampleStartV1> unique = new ArrayList<>(sorted.size());
        for (SloSampleStartV1 start : sorted) {
            if (unique.isEmpty()) {
                unique.add(start);
                continue;
            }
            final SloSampleStartV1 previous = unique.get(unique.size() - 1);
            if (!Arrays.equals(previous.sampleId(), start.sampleId())) {
                unique.add(start);
            } else if (!previous.equals(start)) {
                throw new IllegalStateException("SLO sample identity has conflicting authoritative Starts");
            }
        }

        final Usage currentUsage = limits == null ? null : usage();
        final List<SloObservationOutboxV1> result = new ArrayList<>(unique.size());
        final List<SloObservationOutboxV1> missing = new ArrayList<>();
        long missingBytes = 0;
        for (SloSampleStartV1 start : unique) {
            final SloObservationOutboxV1 existing = get(start.sampleId());
            if (existing != null) {
                if (!existing.start().equals(start)) {
                    throw new IllegalStateException("SLO sample identity has different durable Start bytes");
                }
                result.add(existing);
                continue;
            }
            final SloObservationOutboxV1 created = SloObservationOutboxV1.open(start);
            missing.add(created);
            result.add(created);
            try {
                missingBytes =
                        Math.addExact(missingBytes, ValueEnvelope.encode(VALUE_TYPE, created.canonicalBytes()).length);
            } catch (ArithmeticException exception) {
                throw new IllegalStateException("SLO outbox reconciliation byte usage overflow", exception);
            }
        }
        if (limits != null) {
            requireCapacity(missingBytes, missing.size(), currentUsage.recordCount(), currentUsage.encodedBytes());
        }
        if (!missing.isEmpty()) {
            for (SloObservationOutboxV1 created : missing) {
                batch.putValue(
                        ColumnFamily.META,
                        VALUE_TYPE,
                        KeyCodec.metaSloOutbox(created.sampleId()),
                        created.canonicalBytes());
            }
        }
        return List.copyOf(result);
    }

    /**
     * Merges and durably replaces the exact sample projection in one synced
     * batch. A missing Start is rejected so callers cannot create a Final-only
     * denominator.
     */
    public synchronized SloObservationOutboxV1 mergeFinal(
            final SloSampleFinalV1 finalObservation, final SloThresholdDirectionV1 direction) {
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
     * Compatibility merge for non-excluded projections.  An excluded due
     * projection is rejected because this overload lacks the exact
     * ALL_ACCEPTED companion objective required by the durable boundary.
     */
    public synchronized SloObservationOutboxV1 mergeFinal(
            final SloSampleFinalV1 finalObservation,
            final SloThresholdDirectionV1 direction,
            final SloObjectiveV1 healthyObjective) {
        Objects.requireNonNull(finalObservation, "finalObservation");
        Objects.requireNonNull(direction, "direction");
        Objects.requireNonNull(healthyObjective, "healthyObjective");
        final SloObservationOutboxV1 existing = get(finalObservation.sampleId());
        if (existing == null) {
            throw new IllegalStateException("cannot persist SLO Final without a durable Start");
        }
        if (finalObservation.exclusionReason() != null
                || existing.finalObservation() != null
                        && existing.finalObservation().exclusionReason() != null) {
            throw new IllegalArgumentException(
                    "SLO due exclusions require the exact ALL_ACCEPTED companion at the durable merge boundary");
        }
        final SloObservationOutboxV1 merged = existing.mergeFinal(finalObservation, direction, healthyObjective);
        requireReplacementCapacity(existing, merged);
        persist(KeyCodec.metaSloOutbox(merged.sampleId()), merged);
        return merged;
    }

    /**
     * Merges a Final whose due exclusion is authorized by the exact immutable
     * HEALTHY/ALL_ACCEPTED objective pair.  Both objective digests and all
     * companion policy fields are checked before the replacement WriteBatch.
     */
    public synchronized SloObservationOutboxV1 mergeFinal(
            final SloSampleFinalV1 finalObservation,
            final SloThresholdDirectionV1 direction,
            final SloObjectiveV1 healthyObjective,
            final SloObjectiveV1 allAcceptedObjective) {
        Objects.requireNonNull(finalObservation, "finalObservation");
        Objects.requireNonNull(direction, "direction");
        Objects.requireNonNull(healthyObjective, "healthyObjective");
        Objects.requireNonNull(allAcceptedObjective, "allAcceptedObjective");
        final SloObservationOutboxV1 existing = get(finalObservation.sampleId());
        if (existing == null) {
            throw new IllegalStateException("cannot persist SLO Final without a durable Start");
        }
        final SloObservationOutboxV1 merged =
                existing.mergeFinal(finalObservation, direction, healthyObjective, allAcceptedObjective);
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
        final List<ShardStore.KeyValue> entries =
                store.scan(ColumnFamily.META, new byte[] {8, 1}, new byte[] {8, 2}, limit);
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
        if (exportRate != null && !result.isEmpty() && !exportRate.tryAcquire(result.size(), totalBytes)) {
            throw new IllegalStateException("SLO outbox export rate budget exceeded");
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
                ? Integer.MAX_VALUE
                : limits.maxRecords() + 1;
        final List<ShardStore.KeyValue> entries =
                store.scan(ColumnFamily.META, new byte[] {8, 1}, new byte[] {8, 2}, scanLimit);
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

    private void requireReplacementCapacity(
            final SloObservationOutboxV1 existing, final SloObservationOutboxV1 replacement) {
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

    private void requireCapacity(
            final long addedBytes, final int addedRecords, final int currentRecords, final long currentBytes) {
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

    private SloObservationOutboxV1 decodeEntry(final ShardStore.KeyValue entry) {
        final byte[] key = entry.key();
        if (key.length != 34 || key[0] != 8 || key[1] != 1) {
            throw new IllegalStateException("invalid SLO_OUTBOX key shape");
        }
        final SloObservationOutboxV1 outbox = SloObservationOutboxV1.decode(
                ValueEnvelope.decode(entry.value(), VALUE_TYPE).payload());
        if (!Bytes.constantTimeEquals(Arrays.copyOfRange(key, 2, key.length), outbox.sampleId())) {
            throw new IllegalStateException("SLO_OUTBOX key/value sample identity mismatch");
        }
        validateShardBoundStart(outbox.start());
        return outbox;
    }

    /**
     * Applies the local Shard fence to the typed SLO branches whose identity
     * carries a Source Position or self-routing Message ID. Opaque legacy Due
     * projections remain readable because older compatibility fixtures did not
     * carry a decodable self-routing ID; typed authority factory Starts are
     * always decoded and checked here.
     */
    private void validateShardBoundStart(final SloSampleStartV1 start) {
        switch (start.objective()) {
            case COMMAND_APPLIED_LATENCY -> {
                final SourcePosition position = start.eventIdentity().commandAppliedSourcePosition();
                if (!store.shardId().equals(position.shardId())) {
                    throw new IllegalArgumentException("SLO command-applied Start belongs to another shard");
                }
            }
            case DUE_ADMISSION_LAG -> {
                final DelayMessageId messageId;
                try {
                    messageId = start.eventIdentity().dueAdmissionMessageId();
                } catch (IllegalArgumentException legacyOpaqueStart) {
                    return;
                }
                if (!store.shardId().equals(messageId.routingId().shardId())) {
                    throw new IllegalArgumentException("SLO due-admission Start belongs to another shard");
                }
            }
            default -> {
                // Other objectives do not embed a typed Source/Message route.
            }
        }
    }

    public record Usage(int recordCount, long encodedBytes) {
        public Usage {
            if (recordCount < 0 || encodedBytes < 0) {
                throw new IllegalArgumentException("SLO outbox usage cannot be negative");
            }
        }
    }

    private static void requireNoDueExclusion(final SloSampleFinalV1 incoming, final SloObservationOutboxV1 existing) {
        if (incoming.exclusionReason() != null
                || existing.finalObservation() != null
                        && existing.finalObservation().exclusionReason() != null) {
            throw new IllegalArgumentException(
                    "SLO due exclusions require the paired HEALTHY objective at the durable merge boundary");
        }
    }
}
