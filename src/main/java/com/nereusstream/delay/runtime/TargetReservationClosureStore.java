package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.TargetCloseCursorRecord;
import com.nereusstream.delay.protocol.TargetCloseRecord;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One bounded materialization of a source-ordered Close; independent of later TIME_FENCE progress. */
public final class TargetReservationClosureStore {
    private final TargetReservationExpiryStore terminals;
    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;

    public TargetReservationClosureStore(
            TargetStoreBackend backend,
            TargetQuotaScope scope,
            byte[] lineage,
            int maximumDomains,
            TargetReservationControls.Authority controls) {
        terminals = new TargetReservationExpiryStore(backend, scope, lineage, maximumDomains, controls);
        this.backend = backend;
        this.scope = scope;
        this.lineage = Bytes.copy(lineage);
    }

    /**
     * Read the first remaining active reservation for one durably closed Target, without a Shard-wide scan.
     * This is discovery only: empty proves no remaining indexed reservations, not completion of all Close obligations.
     */
    public Optional<TargetReservationRecord> discover(
            BoundedReadBudget budget, TargetPartitionId target, TargetStoreBackend.ReadAuthority reads) {
        Objects.requireNonNull(target, "target");
        return backend.guardedRead(
                budget,
                reader -> {
                    if (reader.source() == null || !reader.shardId().equals(scope.shard())) {
                        throw new IllegalStateException("Target Close discovery requires its established Shard source");
                    }
                    final byte[] markerKey = TargetKeyCodec.close(target);
                    final byte[] raw = reader.get(ColumnFamily.META, markerKey);
                    if (raw == null) {
                        if (reader.get(ColumnFamily.META, TargetKeyCodec.closeCursor(target)) != null) {
                            throw new IllegalStateException("Close cursor exists without its first marker");
                        }
                        return Optional.empty();
                    }
                    final var marker = TargetCloseRecord.decodeForStore(
                            markerKey,
                            TargetValueEnvelope.decode(raw, TargetCloseRecord.VALUE_TYPE)
                                    .payload(),
                            scope.shard(),
                            lineage);
                    marker.mutation().requireAtOrBefore(reader.aggregate().mutation());
                    final var queue = TargetQueueState.decode(TargetValueEnvelope.decode(
                                    reader.get(ColumnFamily.META, TargetKeyCodec.state(target)),
                                    TargetQueueState.VALUE_TYPE)
                            .payload());
                    marker.requireQueue(queue);
                    final byte[] cursorKey = TargetKeyCodec.closeCursor(target);
                    final var cursor = TargetCloseCursorRecord.decodeForStore(
                            cursorKey,
                            TargetValueEnvelope.decode(
                                            reader.get(ColumnFamily.META, cursorKey),
                                            TargetCloseCursorRecord.VALUE_TYPE)
                                    .payload(),
                            marker,
                            lineage);
                    cursor.mutation().requireAtOrBefore(reader.aggregate().mutation());
                    final var row = reader.first(
                            ColumnFamily.ID,
                            TargetKeyCodec.targetReservationPrefix(target),
                            TargetKeyCodec.targetReservationUpperBound(target),
                            List.of());
                    if (row == null) {
                        return Optional.empty();
                    }
                    if (cursor.complete()
                            || cursor.afterMessageId() != null
                                    && Arrays.compareUnsigned(
                                                    row.key(),
                                                    TargetKeyCodec.targetReservation(
                                                            target, new DelayMessageId(cursor.afterMessageId())))
                                            <= 0) {
                        throw new IllegalStateException("Close cursor disagrees with active reservation order");
                    }
                    final var reservation = TargetReservationRecord.decode(
                            TargetValueEnvelope.decode(row.value(), TargetReservationRecord.VALUE_TYPE)
                                    .payload());
                    if (!Arrays.equals(row.key(), reservation.targetIndexKey())
                            || !reservation.locator().target().equals(target)
                            || reservation.status() != PayloadReservationStatus.RESERVED
                            || !Arrays.equals(reservation.recoveryLineage(), lineage)
                            || !Arrays.equals(
                                    reservation.locator().accountingIncarnation(), queue.accountingIncarnation())
                            || !reservation
                                    .locator()
                                    .messageId()
                                    .routingId()
                                    .shardId()
                                    .equals(scope.shard())
                            || reservation
                                            .prepareAnchor()
                                            .source()
                                            .compareTo(marker.mutation().source())
                                    >= 0) {
                        throw new IllegalStateException(
                                "Target Close index differs from its frozen reservation/marker");
                    }
                    reservation.mutation().requireAtOrBefore(marker.mutation());
                    for (byte[] key : List.of(reservation.key(), reservation.lookupKey())) {
                        if (!Arrays.equals(row.value(), reader.get(ColumnFamily.ID, key))) {
                            throw new IllegalStateException(
                                    "Target Close index differs from its primary/lookup projection");
                        }
                    }
                    return Optional.of(reservation);
                },
                reads);
    }

    /**
     * Revalidate all reservation/closure projections, retain payload and atomically account the terminal record.
     * False is a guarded no-op; failures require fresh discovery. Never releases external objects or advances source.
     */
    public boolean materialize(
            BoundedReadBudget budget,
            byte[] reservationId,
            TargetStoreBackend.ReadAuthority reads,
            TargetStoreBackend.CommitAuthority writes,
            TargetQuotaDelta.ReservationClosureAuthority authority) {
        Objects.requireNonNull(authority, "closureAuthority");
        return terminals.materializeTerminal(budget, reservationId, reads, writes, authority::requireAuthorized, true);
    }

    /** Persist an empty scan as a local cursor mutation, with exact Target and tenant accounting. */
    public boolean completeEmpty(
            BoundedReadBudget budget,
            TargetPartitionId target,
            TargetStoreBackend.ReadAuthority reads,
            TargetStoreBackend.CommitAuthority writes,
            TargetQuotaDelta.CloseCursorAuthority authority) {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(writes, "writes");
        Objects.requireNonNull(authority, "cursorAuthority");
        final boolean pending = backend.guardedRead(
                budget,
                reader -> {
                    final byte[] raw = reader.get(ColumnFamily.META, TargetKeyCodec.close(target));
                    if (raw == null) {
                        return false;
                    }
                    final var marker = TargetCloseRecord.decodeForStore(
                            TargetKeyCodec.close(target),
                            TargetValueEnvelope.decode(raw, TargetCloseRecord.VALUE_TYPE)
                                    .payload(),
                            scope.shard(),
                            lineage);
                    final var cursor = loadCursor(reader, target, marker);
                    final var row = reader.first(
                            ColumnFamily.ID,
                            TargetKeyCodec.targetReservationPrefix(target),
                            TargetKeyCodec.targetReservationUpperBound(target),
                            List.of());
                    if (cursor.complete() && row != null) {
                        throw new IllegalStateException("completed Close cursor retains an active reservation");
                    }
                    return !cursor.complete() && row == null;
                },
                reads);
        if (!pending) {
            return false;
        }
        final var plan = backend.prepare(budget, reader -> {
            if (reader.source() == null || !reader.shardId().equals(scope.shard())) {
                throw new IllegalStateException("Close completion requires its established Shard source");
            }
            final byte[] markerKey = TargetKeyCodec.close(target);
            final var marker = TargetCloseRecord.decodeForStore(
                    markerKey,
                    TargetValueEnvelope.decode(reader.get(ColumnFamily.META, markerKey), TargetCloseRecord.VALUE_TYPE)
                            .payload(),
                    scope.shard(),
                    lineage);
            final var queue = TargetQueueState.decode(TargetValueEnvelope.decode(
                            reader.get(ColumnFamily.META, TargetKeyCodec.state(target)), TargetQueueState.VALUE_TYPE)
                    .payload());
            marker.requireQueue(queue);
            marker.mutation().requireAtOrBefore(reader.aggregate().mutation());
            final var cursor = loadCursor(reader, target, marker);
            if (cursor.complete()
                    || reader.first(
                                    ColumnFamily.ID,
                                    TargetKeyCodec.targetReservationPrefix(target),
                                    TargetKeyCodec.targetReservationUpperBound(target),
                                    List.of())
                            != null) {
                throw new IllegalStateException("Close cursor changed before empty completion; rediscover");
            }
            cursor.mutation().requireAtOrBefore(reader.aggregate().mutation());
            final byte[] digest = Bytes.sha256(
                    Bytes.utf8("nereus-delay-target-close-cursor-empty\0"),
                    cursor.canonicalBytes(),
                    marker.canonicalBytes(),
                    reader.aggregate().mutation().canonicalBytes());
            final var stamp = new TargetQuotaMutation(
                    reader.sourceSequence(),
                    reader.source(),
                    digest,
                    TargetQuotaMutation.increment(reader.aggregate().mutation().localOrdinal()),
                    false,
                    false,
                    true);
            final var next = cursor.completeEmpty(stamp);
            final var edit = reader.replace(
                    ColumnFamily.META, cursor.key(), TargetCloseCursorRecord.VALUE_TYPE, next.canonicalBytes());
            return terminals.account(reader, List.of(edit), stamp, authority::requireAuthorized);
        });
        backend.commit(plan, writes);
        return true;
    }

    private TargetCloseCursorRecord loadCursor(
            TargetStoreBackend.Reader reader, TargetPartitionId target, TargetCloseRecord marker) {
        final byte[] key = TargetKeyCodec.closeCursor(target);
        return TargetCloseCursorRecord.decodeForStore(
                key,
                TargetValueEnvelope.decode(reader.get(ColumnFamily.META, key), TargetCloseCursorRecord.VALUE_TYPE)
                        .payload(),
                marker,
                lineage);
    }
}
