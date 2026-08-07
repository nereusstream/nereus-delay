package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.SourcePosition;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Objects;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
        validatePublicationFloor(manifest, result);
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
        validateScalarFloorIdentity(result, publishedManifest(checkpointId));
        return result;
    }

    @Override
    public RecoveryFloorRefV1 advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                            final List<EvidenceCursorV1> evidenceCursors) {
        Bytes.requireLength(checkpointId, 16, "checkpointId");
        if (expectedCatalogGeneration < 0) {
            throw new IllegalArgumentException("catalog generation must be non-negative");
        }
        Objects.requireNonNull(evidenceCursors, "evidenceCursors");
        final RecoveryFloorRefV1 result = Objects.requireNonNull(backend.advanceFloor(checkpointId,
                expectedCatalogGeneration, evidenceCursors), "Oxia typed Floor result");
        if (!Bytes.constantTimeEquals(checkpointId, result.checkpointId())
                || result.catalogGeneration() <= expectedCatalogGeneration) {
            throw new IllegalStateException("Oxia typed Floor result is not bound to the requested CAS");
        }
        if (!result.evidenceCursors().equals(evidenceCursors)) {
            throw new IllegalStateException("Oxia typed Floor result changed evidence cursors");
        }
        validateTypedFloorIdentity(result, publishedManifest(checkpointId));
        return result;
    }

    @Override
    public RecoveryCatalog.Publication publishUploadedCheckpoint(final CheckpointUploadIntentV1 publishedIntent,
                                                                  final CheckpointManifest manifest,
                                                                  final long expectedCatalogGeneration) {
        final CheckpointUploadIntentV1 intent = Objects.requireNonNull(publishedIntent, "publishedIntent");
        final CheckpointManifest requested = Objects.requireNonNull(manifest, "manifest");
        validateUploadedPublicationRequest(intent, requested, expectedCatalogGeneration);
        final RecoveryCatalog.Publication result = Objects.requireNonNull(backend.publishUploadedCheckpoint(
                intent, requested, expectedCatalogGeneration),
                "Oxia upload-intent publication result");
        validatePublicationIdentity(requested, result);
        validatePublicationFloor(requested, result);
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
        final Optional<RecoveryFloor> result = Objects.requireNonNull(backend.currentFloor(),
                "Oxia current floor result");
        result.ifPresent(floor -> validateScalarFloorIdentity(floor, publishedManifest(floor.checkpointId())));
        return result;
    }

    @Override
    public Optional<RecoveryFloorRefV1> currentFloorRef() {
        final Optional<RecoveryFloorRefV1> result = Objects.requireNonNull(backend.currentFloorRef(),
                "Oxia typed Floor result");
        result.ifPresent(floor -> validateTypedFloorIdentity(floor, publishedManifest(floor.checkpointId())));
        return result;
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
        Objects.requireNonNull(requiredPositions, "requiredPositions");
        for (SourcePosition requiredPosition : requiredPositions) {
            Objects.requireNonNull(requiredPosition, "required source position");
        }
        final Optional<RecoveryCatalog.FloorCoverage> result = Objects.requireNonNull(
                backend.proveFloorCoverage(candidateCheckpointId, requiredMutationSequence, requiredPositions),
                "Oxia floor coverage result");
        result.ifPresent(coverage -> {
            if (!Bytes.constantTimeEquals(candidateCheckpointId, coverage.candidate().checkpointId())) {
                throw new IllegalStateException("Oxia floor coverage returned another candidate");
            }
            final CheckpointManifest candidate = publishedManifest(candidateCheckpointId);
            validateManifestIdentity(coverage.candidate(), candidate,
                    "Oxia floor coverage changed candidate manifest");
            final CheckpointManifest floorManifest = publishedManifest(coverage.floor().checkpointId());
            validateScalarFloorIdentity(coverage.floor(), floorManifest);
            if (!candidate.shardId().equals(floorManifest.shardId())
                    || coverage.floor().includedMutationSequence() < requiredMutationSequence) {
                throw new IllegalStateException("Oxia floor coverage does not cover the requested boundary");
            }
            for (SourcePosition requiredPosition : requiredPositions) {
                if (!coversPosition(coverage.floor().appliedSourcePosition(), requiredPosition)) {
                    throw new IllegalStateException("Oxia floor coverage has an unbound source boundary");
                }
            }
            final List<CheckpointManifest> ancestry = coverage.ancestry();
            if (ancestry.isEmpty()
                    || !Bytes.constantTimeEquals(ancestry.get(ancestry.size() - 1).checkpointId(),
                    candidate.checkpointId())
                    || !Bytes.constantTimeEquals(ancestry.get(ancestry.size() - 1).manifestSha256(),
                    candidate.manifestSha256())) {
                throw new IllegalStateException("Oxia floor coverage ancestry does not end at candidate");
            }
            validateFloorCoverageAncestry(ancestry, candidate, coverage.floor());
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

    /**
     * Mirrors the local catalog's request-to-manifest binding before an
     * external Oxia CAS is attempted.  The remote backend must not become the
     * first place that discovers a malformed or cross-shard upload intent.
     */
    private static void validateUploadedPublicationRequest(final CheckpointUploadIntentV1 intent,
                                                           final CheckpointManifest manifest,
                                                           final long expectedCatalogGeneration) {
        if (intent.state() != CheckpointUploadStateV1.PUBLISHED || intent.publishedManifest() == null) {
            throw new IllegalArgumentException("catalog publication requires a PUBLISHED upload intent");
        }
        if (expectedCatalogGeneration < 0) {
            throw new IllegalArgumentException("catalog generation must be non-negative");
        }
        if (expectedCatalogGeneration != intent.baseCatalogGeneration()) {
            throw new IllegalStateException("upload intent base catalog generation does not match publication CAS");
        }
        if (!intent.shard().shardId().equals(manifest.shardId())
                || !Bytes.constantTimeEquals(intent.recoveryLineageId(), manifest.recoveryLineageId())
                || !Bytes.constantTimeEquals(intent.checkpointId(), manifest.checkpointId())) {
            throw new IllegalArgumentException("upload intent and manifest shard/checkpoint identity differ");
        }
        final CheckpointResourceV1 resource = intent.publishedManifest();
        if (!Bytes.constantTimeEquals(resource.manifestSha256(), manifest.manifestSha256())
                || resource.manifestLength() != manifest.canonicalJsonBytes().length) {
            throw new IllegalArgumentException("published manifest object identity does not match manifest bytes");
        }
        if (!Bytes.constantTimeEquals(intent.owner().deploymentId(), manifest.createdBy().deploymentId())
                || !Bytes.constantTimeEquals(intent.owner().workerRunId(), manifest.createdBy().workerRunId())
                || intent.owner().ownerEpoch() != manifest.createdBy().ownerEpoch()) {
            throw new IllegalArgumentException("upload intent owner does not match manifest creator");
        }
        if (!Bytes.constantTimeEquals(intent.sourceStoreIncarnation(), uuidBytes(manifest.sourceStoreIncarnation()))) {
            throw new IllegalArgumentException("upload intent store incarnation does not match manifest");
        }
        final CheckpointManifest.ParentCheckpoint parent = manifest.parentCheckpoint();
        if (!sameBytes(intent.parentCheckpointId(), parent == null ? null : parent.checkpointId())
                || !sameHashHex(intent.parentManifestSha256(), parent == null ? null : parent.manifestSha256())) {
            throw new IllegalArgumentException("upload intent parent checkpoint identity does not match manifest");
        }
    }

    private void validatePublicationFloor(final CheckpointManifest requested,
                                          final RecoveryCatalog.Publication result) {
        final RecoveryFloor publicationFloor = result.floor();
        if (publicationFloor == null) {
            return;
        }
        if (publicationFloor.catalogGeneration() > result.catalogGeneration()) {
            throw new IllegalStateException("Oxia publication returned a Floor newer than its catalog generation");
        }
        final CheckpointManifest floorManifest = publishedManifest(publicationFloor.checkpointId());
        if (!requested.shardId().equals(floorManifest.shardId())) {
            throw new IllegalStateException("Oxia publication returned a Floor for another shard");
        }
        validateScalarFloorIdentity(publicationFloor, floorManifest);
    }

    private static void validateManifestIdentity(final CheckpointManifest actual,
                                                 final CheckpointManifest expected,
                                                 final String message) {
        if (!actual.shardId().equals(expected.shardId())
                || !Bytes.constantTimeEquals(actual.checkpointId(), expected.checkpointId())
                || !Bytes.constantTimeEquals(actual.recoveryLineageId(), expected.recoveryLineageId())
                || !Bytes.constantTimeEquals(actual.manifestSha256(), expected.manifestSha256())
                || actual.lineageGeneration() != expected.lineageGeneration()
                || !actual.appliedShardLogPosition().equals(expected.appliedShardLogPosition())
                || actual.shardMutationSequence() != expected.shardMutationSequence()
                || !actual.evidenceCursors().equals(expected.evidenceCursors())) {
            throw new IllegalStateException(message);
        }
    }

    private static boolean coversPosition(final SourcePosition covered, final SourcePosition required) {
        try {
            final int order = covered.compareTo(required);
            return order > 0 || (order == 0
                    && Bytes.constantTimeEquals(covered.canonicalBytes(), required.canonicalBytes()));
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    /** Validates that the backend returned a real published parent chain. */
    private void validateFloorCoverageAncestry(final List<CheckpointManifest> ancestry,
                                               final CheckpointManifest candidate,
                                               final RecoveryFloor floor) {
        final Set<String> seenCheckpointIds = new HashSet<>();
        CheckpointManifest previous = null;
        boolean floorFound = false;
        for (CheckpointManifest ancestor : ancestry) {
            final CheckpointManifest published = publishedManifest(ancestor.checkpointId());
            validateManifestIdentity(ancestor, published,
                    "Oxia floor coverage ancestry changed a published manifest");
            if (!candidate.shardId().equals(ancestor.shardId())
                    || !Bytes.constantTimeEquals(candidate.recoveryLineageId(), ancestor.recoveryLineageId())
                    || !seenCheckpointIds.add(Bytes.hex(ancestor.checkpointId()))) {
                throw new IllegalStateException("Oxia floor coverage ancestry has an invalid identity or cycle");
            }
            if (previous != null) {
                validateAncestryLink(previous, ancestor);
            }
            if (Bytes.constantTimeEquals(ancestor.checkpointId(), floor.checkpointId())
                    && Bytes.constantTimeEquals(ancestor.manifestSha256(), floor.manifestSha256())) {
                floorFound = true;
            }
            previous = ancestor;
        }
        if (!floorFound) {
            throw new IllegalStateException("Oxia floor coverage ancestry omits the returned Floor");
        }
    }

    private static void validateAncestryLink(final CheckpointManifest parent,
                                             final CheckpointManifest child) {
        final CheckpointManifest.ParentCheckpoint parentRef = child.parentCheckpoint();
        if (parentRef == null
                || !Bytes.constantTimeEquals(parentRef.checkpointId(), parent.checkpointId())
                || !parentRef.manifestSha256().equals(Bytes.hex(parent.manifestSha256()))
                || !Bytes.constantTimeEquals(parent.recoveryLineageId(), child.recoveryLineageId())) {
            throw new IllegalStateException("Oxia floor coverage ancestry has a broken parent link");
        }
        try {
            if (child.lineageGeneration() != Math.addExact(parent.lineageGeneration(), 1)) {
                throw new IllegalStateException("Oxia floor coverage ancestry has a broken lineage link");
            }
            if (child.appliedShardLogPosition().compareTo(parent.appliedShardLogPosition()) <= 0
                    || child.shardMutationSequence() <= parent.shardMutationSequence()) {
                throw new IllegalStateException("Oxia floor coverage ancestry does not advance its boundary");
            }
        } catch (IllegalArgumentException | ArithmeticException exception) {
            throw new IllegalStateException("Oxia floor coverage ancestry has an invalid source link", exception);
        }
        for (EvidenceCursorV1 parentCursor : parent.evidenceCursors()) {
            final EvidenceCursorV1 childCursor = child.evidenceCursors().stream()
                    .filter(cursor -> cursor.sameIdentity(parentCursor)).findFirst().orElse(null);
            if (childCursor == null || !childCursor.dominates(parentCursor)) {
                throw new IllegalStateException("Oxia floor coverage ancestry regresses evidence cursors");
            }
        }
    }

    private static boolean sameBytes(final byte[] left, final byte[] right) {
        return left == null ? right == null : right != null && Bytes.constantTimeEquals(left, right);
    }

    private static boolean sameHashHex(final byte[] left, final String right) {
        return left == null ? right == null : right != null && Bytes.hex(left).equals(right);
    }

    private static byte[] uuidBytes(final java.util.UUID value) {
        return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }

    private CheckpointManifest publishedManifest(final byte[] checkpointId) {
        return manifest(checkpointId).orElseThrow(() ->
                new IllegalStateException("Oxia Floor result refers to a missing checkpoint manifest"));
    }

    private static void validateScalarFloorIdentity(final RecoveryFloor result,
                                                     final CheckpointManifest manifest) {
        if (!Bytes.constantTimeEquals(result.recoveryLineageId(), manifest.recoveryLineageId())
                || !Bytes.constantTimeEquals(result.checkpointId(), manifest.checkpointId())
                || !Bytes.constantTimeEquals(result.manifestSha256(), manifest.manifestSha256())
                || result.catalogGeneration() <= 0
                || !result.appliedSourcePosition().equals(manifest.appliedShardLogPosition())
                || result.includedMutationSequence() != manifest.shardMutationSequence()) {
            throw new IllegalStateException("Oxia Floor result changed checkpoint identity or boundary");
        }
    }

    private static void validateTypedFloorIdentity(final RecoveryFloorRefV1 result,
                                                    final CheckpointManifest manifest) {
        if (!Bytes.constantTimeEquals(result.recoveryLineageId(), manifest.recoveryLineageId())
                || !Bytes.constantTimeEquals(result.checkpointId(), manifest.checkpointId())
                || !Bytes.constantTimeEquals(result.manifestSha256(), manifest.manifestSha256())
                || !result.appliedSourcePosition().equals(manifest.appliedShardLogPosition())
                || result.includedMutationSequence() != manifest.shardMutationSequence()
                || !result.evidenceCursors().equals(manifest.evidenceCursors())) {
            throw new IllegalStateException("Oxia typed Floor result changed checkpoint identity or boundary");
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
