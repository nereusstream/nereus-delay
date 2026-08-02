package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.SourcePosition;

import java.util.Objects;
import java.util.List;
import java.util.Optional;

/**
 * Validation boundary for an Oxia-backed checkpoint catalog.  The backend
 * owns the actual immutable publication and CAS records; this class rejects
 * response loss/malformed identity rather than treating a convenient local
 * cache as catalog authority.
 */
public final class OxiaRecoveryCatalog implements RecoveryCatalogAuthority {
    private final CasBackend backend;

    public OxiaRecoveryCatalog(final CasBackend backend) {
        this.backend = Objects.requireNonNull(backend, "backend");
    }

    /** Uses the deterministic in-memory catalog as a local adapter/test backend. */
    public OxiaRecoveryCatalog(final RecoveryCatalog backend) {
        this(new DelegatingBackend(backend));
    }

    @Override
    public RecoveryCatalog.Publication publish(final CheckpointManifest manifest,
                                               final long expectedCatalogGeneration) {
        Objects.requireNonNull(manifest, "manifest");
        if (expectedCatalogGeneration < 0) {
            throw new IllegalArgumentException("catalog generation must be non-negative");
        }
        final RecoveryCatalog.Publication result = Objects.requireNonNull(
                backend.publish(manifest, expectedCatalogGeneration), "Oxia publish result");
        validatePublicationIdentity(manifest, result);
        if (result.catalogGeneration() < expectedCatalogGeneration) {
            throw new IllegalStateException("Oxia catalog generation regressed");
        }
        return result;
    }

    @Override
    public RecoveryFloor advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                      final byte[] evidenceCursorDigest) {
        Bytes.requireLength(checkpointId, 16, "checkpointId");
        if (expectedCatalogGeneration < 0) {
            throw new IllegalArgumentException("catalog generation must be non-negative");
        }
        Bytes.requireLength(evidenceCursorDigest, 32, "evidenceCursorDigest");
        final RecoveryFloor result = Objects.requireNonNull(
                backend.advanceFloor(checkpointId, expectedCatalogGeneration, evidenceCursorDigest),
                "Oxia floor result");
        if (!Bytes.constantTimeEquals(checkpointId, result.checkpointId())
                || result.catalogGeneration() <= expectedCatalogGeneration) {
            throw new IllegalStateException("Oxia floor result is not bound to the requested CAS");
        }
        if (!Bytes.constantTimeEquals(evidenceCursorDigest, result.evidenceCursorDigest())) {
            throw new IllegalStateException("Oxia floor result changed evidence cursor digest");
        }
        return result;
    }

    @Override
    public RecoveryFloorRefV1 advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                            final List<EvidenceCursorV1> evidenceCursors) {
        Bytes.requireLength(checkpointId, 16, "checkpointId");
        if (expectedCatalogGeneration < 0) {
            throw new IllegalArgumentException("catalog generation must be non-negative");
        }
        final RecoveryFloorRefV1 result = Objects.requireNonNull(backend.advanceFloor(checkpointId,
                expectedCatalogGeneration, evidenceCursors), "Oxia typed Floor result");
        if (!Bytes.constantTimeEquals(checkpointId, result.checkpointId())
                || result.catalogGeneration() <= expectedCatalogGeneration) {
            throw new IllegalStateException("Oxia typed Floor result is not bound to the requested CAS");
        }
        return result;
    }

    @Override
    public RecoveryCatalog.Publication publishUploadedCheckpoint(final CheckpointUploadIntentV1 publishedIntent,
                                                                  final CheckpointManifest manifest,
                                                                  final long expectedCatalogGeneration) {
        final RecoveryCatalog.Publication result = Objects.requireNonNull(backend.publishUploadedCheckpoint(
                Objects.requireNonNull(publishedIntent, "publishedIntent"),
                Objects.requireNonNull(manifest, "manifest"), expectedCatalogGeneration),
                "Oxia upload-intent publication result");
        validatePublicationIdentity(manifest, result);
        if (result.catalogGeneration() < expectedCatalogGeneration) {
            throw new IllegalStateException("Oxia upload-intent publication regressed catalog generation");
        }
        return result;
    }

    @Override
    public Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
        Bytes.requireLength(checkpointId, 16, "checkpointId");
        final Optional<CheckpointManifest> result = Objects.requireNonNull(backend.manifest(checkpointId),
                "Oxia manifest result");
        result.ifPresent(manifest -> {
            if (!Bytes.constantTimeEquals(checkpointId, manifest.checkpointId())) {
                throw new IllegalStateException("Oxia manifest result has another checkpoint identity");
            }
        });
        return result;
    }

    @Override
    public Optional<RecoveryFloor> currentFloor() {
        return Objects.requireNonNull(backend.currentFloor(), "Oxia current floor result");
    }

    @Override
    public Optional<RecoveryFloorRefV1> currentFloorRef() {
        return Objects.requireNonNull(backend.currentFloorRef(), "Oxia typed Floor result");
    }

    @Override
    public void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
        backend.validatePublishedRestoreCandidate(Objects.requireNonNull(candidate, "candidate"));
    }

    @Override
    public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(final byte[] candidateCheckpointId,
                                                                       final long requiredMutationSequence,
                                                                       final SourcePosition... requiredPositions) {
        Bytes.requireLength(candidateCheckpointId, 16, "candidateCheckpointId");
        if (requiredMutationSequence < 0) {
            throw new IllegalArgumentException("required mutation sequence must be non-negative");
        }
        final Optional<RecoveryCatalog.FloorCoverage> result = Objects.requireNonNull(
                backend.proveFloorCoverage(candidateCheckpointId, requiredMutationSequence, requiredPositions),
                "Oxia floor coverage result");
        result.ifPresent(coverage -> {
            if (!Bytes.constantTimeEquals(candidateCheckpointId, coverage.candidate().checkpointId())) {
                throw new IllegalStateException("Oxia floor coverage returned another candidate");
            }
        });
        return result;
    }

    @Override
    public RecoveryPinV1 createRecoveryPin(final RecoveryPinV1 pin) {
        final RecoveryPinV1 result = Objects.requireNonNull(backend.createRecoveryPin(
                Objects.requireNonNull(pin, "pin")), "Oxia RecoveryPin result");
        if (!result.equals(pin)) {
            throw new IllegalStateException("Oxia RecoveryPin result changed identity/value");
        }
        return result;
    }

    @Override
    public void releaseRecoveryPin(final RecoveryPinV1 pin) {
        backend.releaseRecoveryPin(Objects.requireNonNull(pin, "pin"));
    }

    @Override
    public Optional<RecoveryPinV1> activeRecoveryPin() {
        return Objects.requireNonNull(backend.activeRecoveryPin(), "Oxia RecoveryPin result");
    }

    private static void validatePublicationIdentity(final CheckpointManifest requested,
                                                    final RecoveryCatalog.Publication result) {
        if (!requested.shardId().equals(result.manifest().shardId())
                || !Bytes.constantTimeEquals(requested.checkpointId(), result.manifest().checkpointId())
                || !Bytes.constantTimeEquals(requested.manifestSha256(), result.manifest().manifestSha256())) {
            throw new IllegalStateException("Oxia catalog publication changed checkpoint identity");
        }
    }

    /** Minimal CAS/read surface implemented by the real Oxia client. */
    public interface CasBackend {
        RecoveryCatalog.Publication publish(CheckpointManifest manifest, long expectedCatalogGeneration);

        default RecoveryCatalog.Publication publishUploadedCheckpoint(
                final CheckpointUploadIntentV1 publishedIntent, final CheckpointManifest manifest,
                final long expectedCatalogGeneration) {
            throw new UnsupportedOperationException("upload-intent/catalog CAS is not implemented");
        }

        RecoveryFloor advanceFloor(byte[] checkpointId, long expectedCatalogGeneration,
                                   byte[] evidenceCursorDigest);

        default RecoveryFloorRefV1 advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                                 final List<EvidenceCursorV1> evidenceCursors) {
            throw new UnsupportedOperationException("typed Recovery Floor CAS is not implemented");
        }

        Optional<CheckpointManifest> manifest(byte[] checkpointId);

        Optional<RecoveryFloor> currentFloor();

        default Optional<RecoveryFloorRefV1> currentFloorRef() {
            throw new UnsupportedOperationException("typed Recovery Floor read is not implemented");
        }

        void validatePublishedRestoreCandidate(CheckpointManifest candidate);

        Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(byte[] candidateCheckpointId,
                                                                   long requiredMutationSequence,
                                                                   SourcePosition... requiredPositions);

        default RecoveryPinV1 createRecoveryPin(final RecoveryPinV1 pin) {
            throw new UnsupportedOperationException("session-bound RecoveryPin CAS is not implemented");
        }

        default void releaseRecoveryPin(final RecoveryPinV1 pin) {
            throw new UnsupportedOperationException("session-bound RecoveryPin CAS is not implemented");
        }

        default Optional<RecoveryPinV1> activeRecoveryPin() {
            throw new UnsupportedOperationException("session-bound RecoveryPin read is not implemented");
        }
    }

    private static final class DelegatingBackend implements CasBackend {
        private final RecoveryCatalog delegate;

        private DelegatingBackend(final RecoveryCatalog delegate) {
            this.delegate = Objects.requireNonNull(delegate, "backend");
        }

        @Override
        public RecoveryCatalog.Publication publish(final CheckpointManifest manifest,
                                                   final long expectedCatalogGeneration) {
            return delegate.publish(manifest, expectedCatalogGeneration);
        }

        @Override
        public RecoveryCatalog.Publication publishUploadedCheckpoint(final CheckpointUploadIntentV1 publishedIntent,
                                                                       final CheckpointManifest manifest,
                                                                       final long expectedCatalogGeneration) {
            return delegate.publishUploadedCheckpoint(publishedIntent, manifest, expectedCatalogGeneration);
        }

        @Override
        public RecoveryFloor advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                          final byte[] evidenceCursorDigest) {
            return delegate.advanceFloor(checkpointId, expectedCatalogGeneration, evidenceCursorDigest);
        }

        @Override
        public RecoveryFloorRefV1 advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                                final List<EvidenceCursorV1> evidenceCursors) {
            return delegate.advanceFloor(checkpointId, expectedCatalogGeneration, evidenceCursors);
        }

        @Override
        public Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
            return delegate.manifest(checkpointId);
        }

        @Override
        public Optional<RecoveryFloor> currentFloor() {
            return delegate.currentFloor();
        }

        @Override
        public Optional<RecoveryFloorRefV1> currentFloorRef() {
            return delegate.currentFloorRef();
        }

        @Override
        public void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
            delegate.validatePublishedRestoreCandidate(candidate);
        }

        @Override
        public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(final byte[] candidateCheckpointId,
                                                                           final long requiredMutationSequence,
                                                                           final SourcePosition... requiredPositions) {
            return delegate.proveFloorCoverage(candidateCheckpointId, requiredMutationSequence, requiredPositions);
        }

        @Override
        public RecoveryPinV1 createRecoveryPin(final RecoveryPinV1 pin) {
            return delegate.createRecoveryPin(pin);
        }

        @Override
        public void releaseRecoveryPin(final RecoveryPinV1 pin) {
            delegate.releaseRecoveryPin(pin);
        }

        @Override
        public Optional<RecoveryPinV1> activeRecoveryPin() {
            return delegate.activeRecoveryPin();
        }
    }
}
