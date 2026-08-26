package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.Objects;
import java.util.Optional;

/**
 * Read-only necessary-condition check for entering checkpoint orphan reaping.
 * A deadline is only one input: an already published catalog entry or an
 * active recovery pin remains a hard protection. Oxia CAS, owner-abandonment
 * proof, provider-request quiescence and exact-version deletion remain outside
 * this local predicate.
 */
public final class CheckpointReapingGuard {
    private CheckpointReapingGuard() {}

    public enum Decision {
        INTENT_NOT_PENDING,
        UPLOAD_DEADLINE_NOT_CLOSED,
        CATALOG_STATE_UNAVAILABLE,
        PUBLISHED_CATALOG_PROTECTION,
        RECOVERY_PIN_STATE_UNAVAILABLE,
        RECOVERY_PIN_PROTECTION,
        REAPING_ALLOWED
    }

    public static Decision evaluate(
            final CheckpointUploadIntent pending,
            final TrustedUtcIntervalEvidence evidence,
            final RecoveryCatalogAuthority catalog) {
        Objects.requireNonNull(pending, "pending");
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(catalog, "catalog");
        if (pending.state() != CheckpointUploadState.PENDING_UPLOAD) {
            return Decision.INTENT_NOT_PENDING;
        }
        try {
            evidence.requireEarliestAtLeast(pending.uploadDeadlineEpochMs());
        } catch (IllegalArgumentException exception) {
            return Decision.UPLOAD_DEADLINE_NOT_CLOSED;
        }
        try {
            if (catalog.manifest(pending.checkpointId()).isPresent()) {
                return Decision.PUBLISHED_CATALOG_PROTECTION;
            }
        } catch (RuntimeException | Error exception) {
            return Decision.CATALOG_STATE_UNAVAILABLE;
        }
        final Optional<RecoveryPin> active;
        try {
            active = Objects.requireNonNull(catalog.activeRecoveryPin(), "activeRecoveryPin");
        } catch (RuntimeException | Error exception) {
            return Decision.RECOVERY_PIN_STATE_UNAVAILABLE;
        }
        if (active.isPresent() && protectsPending(active.orElseThrow(), pending)) {
            return Decision.RECOVERY_PIN_PROTECTION;
        }
        return Decision.REAPING_ALLOWED;
    }

    private static boolean protectsPending(final RecoveryPin pin, final CheckpointUploadIntent pending) {
        return matches(
                        pin.candidate().recoveryLineageId(),
                        pin.candidate().checkpointId(),
                        pending.recoveryLineageId(),
                        pending.checkpointId())
                || matches(
                        pin.observedFloor().recoveryLineageId(),
                        pin.observedFloor().checkpointId(),
                        pending.recoveryLineageId(),
                        pending.checkpointId());
    }

    private static boolean matches(
            final byte[] lineage,
            final byte[] checkpoint,
            final byte[] pendingLineage,
            final byte[] pendingCheckpoint) {
        return java.util.Arrays.equals(lineage, pendingLineage)
                && java.util.Arrays.equals(checkpoint, pendingCheckpoint);
    }
}
