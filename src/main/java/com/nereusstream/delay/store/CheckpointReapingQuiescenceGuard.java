package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import java.util.Arrays;
import java.util.Objects;

/** Pure fail-closed gate for the three bounded REAPING quiescence horizons. */
public final class CheckpointReapingQuiescenceGuard {
    private CheckpointReapingQuiescenceGuard() {}

    public enum Decision {
        PENDING_INTENT_MISMATCH,
        REAPING_STATE_MISMATCH,
        REAPING_EVIDENCE_MISMATCH,
        OWNER_PROOF_MISMATCH,
        OWNER_GUARD_EVIDENCE_MISMATCH,
        OWNER_GUARD_NOT_CLOSED,
        REQUEST_QUIESCENCE_NOT_ELAPSED,
        OLD_OWNER_GUARD_NOT_CLOSED,
        PROVIDER_OWNERSHIP_NOT_CLOSED,
        QUIESCENCE_PROVEN
    }

    public static Decision evaluate(
            final CheckpointUploadIntent expectedPending,
            final CheckpointUploadIntent reaping,
            final CheckpointReapingOwnerProof ownerProof,
            final CheckpointReapingQuiescenceProof proof) {
        Objects.requireNonNull(expectedPending, "expectedPending");
        Objects.requireNonNull(reaping, "reaping");
        Objects.requireNonNull(ownerProof, "ownerProof");
        Objects.requireNonNull(proof, "proof");
        if (!Bytes.constantTimeEquals(expectedPending.intentDigest(), proof.pendingIntentDigest())
                || expectedPending.state() != CheckpointUploadState.PENDING_UPLOAD
                || !sameStableIdentity(expectedPending, reaping)) {
            return Decision.PENDING_INTENT_MISMATCH;
        }
        if (reaping.state() != CheckpointUploadState.REAPING) {
            return Decision.REAPING_STATE_MISMATCH;
        }
        if (!proof.reapingEvidence().equals(reaping.reapingStartedAt())) {
            return Decision.REAPING_EVIDENCE_MISMATCH;
        }
        if (CheckpointReapingOwnerProofGuard.evaluate(expectedPending, ownerProof)
                != CheckpointReapingOwnerProofGuard.Decision.OWNER_PROOF_ACCEPTED) {
            return Decision.OWNER_PROOF_MISMATCH;
        }
        if (!Bytes.constantTimeEquals(ownerProof.proofDigest(), proof.oldOwnerGuardEvidenceDigest())) {
            return Decision.OWNER_GUARD_EVIDENCE_MISMATCH;
        }
        if (proof.oldOwnerGuardClosedAt().earliestEpochMs()
                < ownerProof.observedAt().earliestEpochMs()) {
            return Decision.OWNER_GUARD_NOT_CLOSED;
        }
        final long requestBoundary;
        try {
            requestBoundary =
                    Math.addExact(reaping.reapingStartedAt().latestEpochMs(), proof.requestQuiescenceHorizonMs());
        } catch (ArithmeticException overflow) {
            return Decision.REQUEST_QUIESCENCE_NOT_ELAPSED;
        }
        if (proof.observedAt().earliestEpochMs() < requestBoundary) {
            return Decision.REQUEST_QUIESCENCE_NOT_ELAPSED;
        }
        if (proof.observedAt().earliestEpochMs() < proof.oldOwnerGuardClosedAt().earliestEpochMs()) {
            return Decision.OLD_OWNER_GUARD_NOT_CLOSED;
        }
        if (proof.observedAt().earliestEpochMs()
                < proof.providerOwnershipClosedAt().earliestEpochMs()) {
            return Decision.PROVIDER_OWNERSHIP_NOT_CLOSED;
        }
        return Decision.QUIESCENCE_PROVEN;
    }

    public static void require(
            final CheckpointUploadIntent expectedPending,
            final CheckpointUploadIntent reaping,
            final CheckpointReapingOwnerProof ownerProof,
            final CheckpointReapingQuiescenceProof proof) {
        final Decision decision = evaluate(expectedPending, reaping, ownerProof, proof);
        if (decision != Decision.QUIESCENCE_PROVEN) {
            throw new IllegalStateException("checkpoint reaping quiescence rejected: " + decision);
        }
    }

    private static boolean sameStableIdentity(
            final CheckpointUploadIntent pending, final CheckpointUploadIntent reaping) {
        return pending.shard().equals(reaping.shard())
                && Arrays.equals(pending.recoveryLineageId(), reaping.recoveryLineageId())
                && Arrays.equals(pending.checkpointId(), reaping.checkpointId())
                && pending.owner().equals(reaping.owner())
                && Arrays.equals(pending.sourceStoreIncarnation(), reaping.sourceStoreIncarnation())
                && Arrays.equals(pending.uploadToken(), reaping.uploadToken())
                && pending.baseCatalogGeneration() == reaping.baseCatalogGeneration()
                && Arrays.equals(pending.parentCheckpointId(), reaping.parentCheckpointId())
                && Arrays.equals(pending.parentManifestSha256(), reaping.parentManifestSha256())
                && pending.objectStoreProfile().equals(reaping.objectStoreProfile())
                && pending.checkpointCreatedAt().equals(reaping.checkpointCreatedAt())
                && pending.uploadDeadlineEpochMs() == reaping.uploadDeadlineEpochMs();
    }
}
