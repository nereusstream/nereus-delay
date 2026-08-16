package io.nereusstream.delay.store;

import io.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import io.nereusstream.delay.ownership.OwnerLease;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

import java.util.Objects;
import java.util.Optional;

/**
 * Local composition boundary for issuing a typed old-Owner proof.
 *
 * <p>The issuer delegates the actual lease/session CAS and reread to
 * {@link OxiaOwnerLeaseStore}. It does not create a cross-record Oxia
 * transaction with the checkpoint intent; that production authority remains
 * an explicit integration boundary.</p>
 */
public final class CheckpointReapingOwnerProofIssuer {
    private CheckpointReapingOwnerProofIssuer() {
    }

    /** Releases the exact recorded lease and proves the authority no longer exposes it. */
    public static CheckpointReapingOwnerProof explicitOwnerAbandon(
            final CheckpointUploadIntentV1 pending,
            final OxiaOwnerLeaseStore authority,
            final OwnerLease expectedLease,
            final TrustedUtcIntervalEvidence observedAt) {
        validateInputs(pending, authority, expectedLease, observedAt);
        if (!authority.release(expectedLease)) {
            throw new IllegalStateException("exact Owner lease was not released");
        }
        final Optional<OwnerLease> current = authority.current(expectedLease.shardId());
        if (current.isPresent()) {
            throw new IllegalStateException("Owner lease remains current after explicit abandonment");
        }
        return new CheckpointReapingOwnerProof(pending.intentDigest(), pending.owner(),
                pending.sourceStoreIncarnation(), expectedLease,
                CheckpointReapingOwnerProof.Kind.EXACT_OWNER_EXPLICIT_ABANDON, null, observedAt);
    }

    /** Reads the exact lease authority and proves that the recorded identity is no longer current. */
    public static CheckpointReapingOwnerProof proveRecordedOwnerNotCurrent(
            final CheckpointUploadIntentV1 pending,
            final OxiaOwnerLeaseStore authority,
            final OwnerLease recordedLease,
            final TrustedUtcIntervalEvidence observedAt) {
        validateInputs(pending, authority, recordedLease, observedAt);
        final Optional<OwnerLease> current = authority.current(recordedLease.shardId());
        if (current.isPresent() && recordedLease.sameIdentity(current.orElseThrow())) {
            throw new IllegalStateException("recorded Owner lease remains current");
        }
        return new CheckpointReapingOwnerProof(pending.intentDigest(), pending.owner(),
                pending.sourceStoreIncarnation(), recordedLease,
                CheckpointReapingOwnerProof.Kind.RECORDED_OWNER_NOT_CURRENT,
                current.orElse(null), observedAt);
    }

    private static void validateInputs(final CheckpointUploadIntentV1 pending,
                                       final OxiaOwnerLeaseStore authority,
                                       final OwnerLease lease,
                                       final TrustedUtcIntervalEvidence observedAt) {
        Objects.requireNonNull(pending, "pending");
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(observedAt, "observedAt");
        if (pending.state() != CheckpointUploadStateV1.PENDING_UPLOAD) {
            throw new IllegalArgumentException("Owner proof requires a PENDING_UPLOAD intent");
        }
        if (!pending.shard().shardId().equals(lease.shardId())
                || pending.owner().ownerEpoch() != lease.ownerEpoch()) {
            throw new IllegalArgumentException("Owner proof lease does not match the pending Owner/shard");
        }
        if (lease.context() == null) {
            throw new IllegalArgumentException("Owner proof lease must carry assignment/session context");
        }
        observedAt.requireEarliestAtLeast(pending.uploadDeadlineEpochMs());
    }
}
