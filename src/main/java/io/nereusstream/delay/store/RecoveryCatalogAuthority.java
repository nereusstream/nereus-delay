package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.SourcePosition;

import java.util.Optional;

/**
 * Authority boundary for the immutable checkpoint catalog and Recovery
 * Floor.  The embedded implementation is deterministic; production wiring
 * supplies the same CAS/read contract through Oxia.
 */
public interface RecoveryCatalogAuthority {
    RecoveryCatalog.Publication publish(CheckpointManifest manifest, long expectedCatalogGeneration);

    RecoveryFloor advanceFloor(byte[] checkpointId, long expectedCatalogGeneration, byte[] evidenceCursorDigest);

    Optional<CheckpointManifest> manifest(byte[] checkpointId);

    Optional<RecoveryFloor> currentFloor();

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
