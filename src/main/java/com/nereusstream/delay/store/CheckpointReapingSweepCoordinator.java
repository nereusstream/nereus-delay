package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import com.nereusstream.delay.protocol.CheckpointUploadStateV1;
import java.util.Objects;

/**
 * Bounded handoff from an exact REAPING intent to the provider prefix sweep.
 *
 * <p>The caller supplies a typed old-Owner proof and the bounded provider
 * quiescence proof. The intent authority wins the PENDING_UPLOAD to REAPING
 * CAS before any provider call. A retry reuses the same pending identity and
 * reaping evidence, so a provider response loss leaves an exact REAPING state
 * that can safely invoke the idempotent final-empty sweep again. This class
 * does not infer Owner abandonment/session loss or provider quiescence from a
 * deadline.</p>
 */
public final class CheckpointReapingSweepCoordinator {
    private final CheckpointUploadIntentAuthority intentAuthority;
    private final CheckpointPrefixSweepAdapter prefixSweep;

    public CheckpointReapingSweepCoordinator(
            final CheckpointUploadIntentAuthority intentAuthority, final CheckpointPrefixSweepAdapter prefixSweep) {
        this.intentAuthority = Objects.requireNonNull(intentAuthority, "intentAuthority");
        this.prefixSweep = Objects.requireNonNull(prefixSweep, "prefixSweep");
    }

    /**
     * Competes for the exact REAPING successor, proves the certified
     * quiescence horizons, rereads that successor before provider I/O, and
     * sweeps only its derived checkpoint prefix.
     */
    public CheckpointReapingSweepResult reap(
            final CheckpointUploadIntentV1 expectedPending,
            final RecoveryCatalogAuthority catalog,
            final CheckpointReapingOwnerProof ownerProof,
            final CheckpointReapingQuiescenceProof quiescence,
            final int maxVersions) {
        Objects.requireNonNull(expectedPending, "expectedPending");
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(ownerProof, "ownerProof");
        Objects.requireNonNull(quiescence, "quiescence");
        if (expectedPending.state() != CheckpointUploadStateV1.PENDING_UPLOAD) {
            throw new IllegalArgumentException("checkpoint reaping requires a PENDING_UPLOAD intent");
        }
        CheckpointReapingOwnerProofGuard.require(expectedPending, ownerProof);
        final CheckpointUploadIntentV1 reaping =
                intentAuthority.beginReaping(expectedPending, quiescence.reapingEvidence(), catalog);
        if (reaping.state() != CheckpointUploadStateV1.REAPING) {
            throw new IllegalStateException("checkpoint reaping authority returned a non-REAPING state");
        }
        CheckpointReapingQuiescenceGuard.require(expectedPending, reaping, ownerProof, quiescence);
        final CheckpointUploadIntentV1 current = intentAuthority
                .current(reaping)
                .orElseThrow(
                        () -> new IllegalStateException("checkpoint REAPING intent disappeared before provider I/O"));
        if (!current.equals(reaping)) {
            throw new IllegalStateException("checkpoint REAPING intent changed before provider I/O");
        }
        final CheckpointPrefixSweepResult sweep = Objects.requireNonNull(
                prefixSweep.sweep(new CheckpointPrefixSweepRequest(
                        reaping.objectStoreProfile(),
                        reaping.recoveryLineageId(),
                        reaping.checkpointId(),
                        maxVersions)),
                "checkpoint prefix sweep result");
        return new CheckpointReapingSweepResult(reaping, sweep);
    }
}
