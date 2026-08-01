package io.nereusstream.delay.store;

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
}
