package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Objects;

/** One bounded materialization of a source-ordered Close; independent of later TIME_FENCE progress. */
public final class TargetReservationClosureStore {
    private final TargetReservationExpiryStore terminals;

    public TargetReservationClosureStore(
            TargetStoreBackend backend,
            TargetQuotaScope scope,
            byte[] lineage,
            int maximumDomains,
            TargetReservationControls.Authority controls) {
        terminals = new TargetReservationExpiryStore(backend, scope, lineage, maximumDomains, controls);
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
