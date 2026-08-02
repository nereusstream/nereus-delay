package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.SourcePosition;

import java.util.List;
import java.util.Optional;

/**
 * Authority boundary for the immutable checkpoint catalog and Recovery
 * Floor.  The embedded implementation is deterministic; production wiring
 * supplies the same CAS/read contract through Oxia.
 */
public interface RecoveryCatalogAuthority {
    RecoveryCatalog.Publication publish(CheckpointManifest manifest, long expectedCatalogGeneration);

    RecoveryFloor advanceFloor(byte[] checkpointId, long expectedCatalogGeneration, byte[] evidenceCursorDigest);

    /** Advances a typed Floor while preserving same-generation cursor dominance. */
    default RecoveryFloorRefV1 advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                             final List<EvidenceCursorV1> evidenceCursors) {
        throw new UnsupportedOperationException("typed Recovery Floor CAS is not implemented");
    }

    /**
     * Publishes a complete manifest only after an exact PUBLISHED upload intent
     * has bound its identity to the requested catalog generation. Production
     * implementations must perform this check in the same Oxia transaction.
     */
    default RecoveryCatalog.Publication publishUploadedCheckpoint(
            final CheckpointUploadIntentV1 publishedIntent, final CheckpointManifest manifest,
            final long expectedCatalogGeneration) {
        throw new UnsupportedOperationException("upload-intent/catalog CAS is not implemented");
    }

    Optional<CheckpointManifest> manifest(byte[] checkpointId);

    Optional<RecoveryFloor> currentFloor();

    /** Returns the typed Floor when the authority has one; legacy projections may be empty. */
    default Optional<RecoveryFloorRefV1> currentFloorRef() {
        return Optional.empty();
    }

    void validatePublishedRestoreCandidate(CheckpointManifest candidate);

    Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(byte[] candidateCheckpointId,
                                                               long requiredMutationSequence,
                                                               SourcePosition... requiredPositions);

    /**
     * Creates the local/test projection of a session-bound recovery pin.
     * Production implementations must perform the exact Owner Lease/session
     * and catalog-generation CAS in Oxia before returning success.
     */
    default RecoveryPinV1 createRecoveryPin(final RecoveryPinV1 pin) {
        throw new UnsupportedOperationException("session-bound RecoveryPin CAS is not implemented");
    }

    /** Releases exactly the pin value that was created by this authority. */
    default void releaseRecoveryPin(final RecoveryPinV1 pin) {
        throw new UnsupportedOperationException("session-bound RecoveryPin CAS is not implemented");
    }

    /** Returns the active local pin when the implementation exposes one. */
    default Optional<RecoveryPinV1> activeRecoveryPin() {
        throw new UnsupportedOperationException("session-bound RecoveryPin read is not implemented");
    }
}
