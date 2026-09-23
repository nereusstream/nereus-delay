package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetCloseRecord;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
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
                    final var row = reader.first(
                            ColumnFamily.ID,
                            TargetKeyCodec.targetReservationPrefix(target),
                            TargetKeyCodec.targetReservationUpperBound(target),
                            List.of());
                    if (row == null) {
                        return Optional.empty();
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
}
