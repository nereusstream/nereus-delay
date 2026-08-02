package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.RecoveryCandidateKindV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.SourcePosition;

import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Deterministic local model of the V1 checkpoint catalog. Production wiring
 * supplies the same CAS semantics through Oxia; this class deliberately has no
 * network or object-store side effects.
 */
public final class RecoveryCatalog implements RecoveryCatalogAuthority {
    private final Map<String, CheckpointManifest> manifests = new HashMap<>();
    private final Map<String, io.nereusstream.delay.protocol.CheckpointResourceV1> manifestResources = new HashMap<>();
    private long catalogGeneration;
    private RecoveryFloor floor;
    private RecoveryFloorRefV1 typedFloorRef;
    private io.nereusstream.delay.protocol.ShardId catalogShard;
    private RecoveryPinV1 activeRecoveryPin;

    public synchronized Publication publish(final CheckpointManifest manifest, final long expectedCatalogGeneration) {
        Objects.requireNonNull(manifest, "manifest");
        if (catalogShard != null && !catalogShard.equals(manifest.shardId())) {
            throw new IllegalArgumentException("checkpoint catalog is bound to a different shard");
        }
        final String key = key(manifest.checkpointId());
        final CheckpointManifest existing = manifests.get(key);
        if (existing != null) {
            if (!Bytes.constantTimeEquals(existing.manifestSha256(), manifest.manifestSha256())) {
                throw new IllegalStateException("checkpoint identity conflict");
            }
            return new Publication(existing, catalogGeneration, floor);
        }
        if (expectedCatalogGeneration != catalogGeneration) {
            throw new IllegalStateException("checkpoint catalog generation conflict");
        }
        if (manifest.parentCheckpoint() == null && manifest.lineageGeneration() != 0) {
            throw new IllegalArgumentException("genesis checkpoint must have lineage generation zero");
        }
        validateParent(manifest);
        if (catalogShard == null) {
            catalogShard = manifest.shardId();
        }
        catalogGeneration = Math.addExact(catalogGeneration, 1);
        manifests.put(key, manifest);
        return new Publication(manifest, catalogGeneration, floor);
    }

    /** Advances the floor only to a published checkpoint in the same lineage. */
    public synchronized RecoveryFloor advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                                    final byte[] evidenceCursorDigest) {
        Objects.requireNonNull(checkpointId, "checkpointId");
        Bytes.requireLength(evidenceCursorDigest, 32, "evidenceCursorDigest");
        if (typedFloorRef != null) {
            throw new IllegalStateException("typed Recovery Floor requires a typed successor");
        }
        if (floor != null && expectedCatalogGeneration != Long.MAX_VALUE
                && floor.catalogGeneration() == expectedCatalogGeneration + 1
                && Bytes.constantTimeEquals(floor.checkpointId(), checkpointId)
                && Bytes.constantTimeEquals(floor.evidenceCursorDigest(), evidenceCursorDigest)) {
            final CheckpointManifest reread = manifests.get(key(checkpointId));
            if (reread != null && Bytes.constantTimeEquals(reread.recoveryLineageId(), floor.recoveryLineageId())
                    && Bytes.constantTimeEquals(reread.manifestSha256(), floor.manifestSha256())
                    && reread.appliedShardLogPosition().equals(floor.appliedSourcePosition())
                    && reread.shardMutationSequence() == floor.includedMutationSequence()) {
                return floor;
            }
        }
        if (expectedCatalogGeneration != catalogGeneration) {
            throw new IllegalStateException("checkpoint catalog generation conflict");
        }
        final CheckpointManifest candidate = manifests.get(key(checkpointId));
        if (candidate == null) {
            throw new IllegalArgumentException("checkpoint is not published");
        }
        // This also proves that the candidate is a descendant of the current
        // floor rather than merely sharing its lineage and a higher position.
        if (floor != null) {
            recoverySet(checkpointId);
        }
        if (floor != null) {
            if (!Bytes.constantTimeEquals(floor.recoveryLineageId(), candidate.recoveryLineageId())) {
                throw new IllegalArgumentException("floor lineage differs from candidate");
            }
            if (candidate.appliedShardLogPosition().compareTo(floor.appliedSourcePosition()) < 0
                    || candidate.shardMutationSequence() < floor.includedMutationSequence()) {
                throw new IllegalArgumentException("recovery floor cannot regress");
            }
        }
        catalogGeneration = Math.addExact(catalogGeneration, 1);
        floor = RecoveryFloor.create(candidate.recoveryLineageId(), candidate.checkpointId(),
                candidate.manifestSha256(), catalogGeneration, candidate.appliedShardLogPosition(),
                candidate.shardMutationSequence(), evidenceCursorDigest);
        return floor;
    }

    /**
     * Advances a typed Recovery Floor and rejects a missing or regressing
     * cursor for every identity already protected by the previous Floor.
     * The legacy {@link RecoveryFloor} field remains a scalar local projection
     * for existing GC callers; {@link #currentFloorRef()} is the authority for
     * the complete typed cursor set.
     */
    @Override
    public synchronized RecoveryFloorRefV1 advanceFloor(final byte[] checkpointId,
                                                          final long expectedCatalogGeneration,
                                                          final List<EvidenceCursorV1> evidenceCursors) {
        Objects.requireNonNull(checkpointId, "checkpointId");
        Objects.requireNonNull(evidenceCursors, "evidenceCursors");
        if (typedFloorRef != null && expectedCatalogGeneration != Long.MAX_VALUE
                && typedFloorRef.catalogGeneration() == expectedCatalogGeneration + 1
                && Bytes.constantTimeEquals(typedFloorRef.checkpointId(), checkpointId)
                && typedFloorRef.evidenceCursors().equals(evidenceCursors)) {
            final CheckpointManifest reread = manifests.get(key(checkpointId));
            if (reread != null && Bytes.constantTimeEquals(reread.recoveryLineageId(),
                    typedFloorRef.recoveryLineageId())
                    && Bytes.constantTimeEquals(reread.manifestSha256(), typedFloorRef.manifestSha256())
                    && reread.appliedShardLogPosition().equals(typedFloorRef.appliedSourcePosition())
                    && reread.shardMutationSequence() == typedFloorRef.includedMutationSequence()) {
                return typedFloorRef;
            }
        }
        if (expectedCatalogGeneration != catalogGeneration) {
            throw new IllegalStateException("checkpoint catalog generation conflict");
        }
        final CheckpointManifest candidate = manifests.get(key(checkpointId));
        if (candidate == null) {
            throw new IllegalArgumentException("checkpoint is not published");
        }
        if (!candidate.evidenceCursors().equals(evidenceCursors)) {
            throw new IllegalArgumentException("typed Recovery Floor cursors do not match checkpoint manifest");
        }
        if (floor != null) {
            recoverySet(checkpointId);
            if (!Bytes.constantTimeEquals(floor.recoveryLineageId(), candidate.recoveryLineageId())
                    || candidate.appliedShardLogPosition().compareTo(floor.appliedSourcePosition()) < 0
                    || candidate.shardMutationSequence() < floor.includedMutationSequence()) {
                throw new IllegalArgumentException("recovery floor cannot regress");
            }
        }
        final RecoveryFloorRefV1 next = new RecoveryFloorRefV1(candidate.recoveryLineageId(), candidate.checkpointId(),
                candidate.manifestSha256(), Math.addExact(catalogGeneration, 1),
                candidate.appliedShardLogPosition(), candidate.shardMutationSequence(), evidenceCursors);
        if (typedFloorRef != null) {
            for (EvidenceCursorV1 previous : typedFloorRef.evidenceCursors()) {
                final EvidenceCursorV1 successor = next.evidenceCursors().stream()
                        .filter(cursor -> cursor.sameIdentity(previous)).findFirst().orElse(null);
                if (successor == null || !successor.dominates(previous)) {
                    throw new IllegalArgumentException("typed Recovery Floor cursor regressed or disappeared");
                }
            }
        }
        catalogGeneration = next.catalogGeneration();
        typedFloorRef = next;
        floor = RecoveryFloor.create(next.recoveryLineageId(), next.checkpointId(), next.manifestSha256(),
                next.catalogGeneration(), next.appliedSourcePosition(), next.includedMutationSequence(),
                next.floorDigest());
        return next;
    }

    /**
     * Binds local catalog publication to a complete PUBLISHED upload intent.
     * The production equivalent must compare the same identities and the
     * active Owner Lease/session in one Oxia transaction.
     */
    @Override
    public synchronized Publication publishUploadedCheckpoint(final CheckpointUploadIntentV1 publishedIntent,
                                                               final CheckpointManifest manifest,
                                                               final long expectedCatalogGeneration) {
        Objects.requireNonNull(publishedIntent, "publishedIntent");
        Objects.requireNonNull(manifest, "manifest");
        if (publishedIntent.state() != CheckpointUploadStateV1.PUBLISHED
                || publishedIntent.publishedManifest() == null) {
            throw new IllegalArgumentException("catalog publication requires a PUBLISHED upload intent");
        }
        if (expectedCatalogGeneration < 0) {
            throw new IllegalArgumentException("catalog generation must be non-negative");
        }
        if (expectedCatalogGeneration != publishedIntent.baseCatalogGeneration()) {
            throw new IllegalStateException("upload intent base catalog generation does not match publication CAS");
        }
        if (!publishedIntent.shard().shardId().equals(manifest.shardId())
                || !Bytes.constantTimeEquals(publishedIntent.recoveryLineageId(), manifest.recoveryLineageId())
                || !Bytes.constantTimeEquals(publishedIntent.checkpointId(), manifest.checkpointId())) {
            throw new IllegalArgumentException("upload intent and manifest shard/checkpoint identity differ");
        }
        final io.nereusstream.delay.protocol.CheckpointResourceV1 resource = publishedIntent.publishedManifest();
        if (!Bytes.constantTimeEquals(resource.manifestSha256(), manifest.manifestSha256())
                || resource.manifestLength() != manifest.canonicalJsonBytes().length) {
            throw new IllegalArgumentException("published manifest object identity does not match manifest bytes");
        }
        if (!Bytes.constantTimeEquals(publishedIntent.owner().deploymentId(), manifest.createdBy().deploymentId())
                || !Bytes.constantTimeEquals(publishedIntent.owner().workerRunId(), manifest.createdBy().workerRunId())
                || publishedIntent.owner().ownerEpoch() != manifest.createdBy().ownerEpoch()) {
            throw new IllegalArgumentException("upload intent owner does not match manifest creator");
        }
        if (!Bytes.constantTimeEquals(publishedIntent.sourceStoreIncarnation(), uuidBytes(manifest.sourceStoreIncarnation()))) {
            throw new IllegalArgumentException("upload intent store incarnation does not match manifest");
        }
        final CheckpointManifest.ParentCheckpoint parent = manifest.parentCheckpoint();
        if (!sameBytes(publishedIntent.parentCheckpointId(), parent == null ? null : parent.checkpointId())
                || !sameHashHex(publishedIntent.parentManifestSha256(),
                parent == null ? null : parent.manifestSha256())) {
            throw new IllegalArgumentException("upload intent parent checkpoint identity does not match manifest");
        }

        // A successful Oxia CAS may have returned no response. Once the exact
        // manifest is already in the catalog, an exact checkpoint reread is
        // success even if another catalog operation advanced the
        // generation in the meantime. A same-ID/different-hash value remains
        // an integrity conflict and is rejected by the normal publish path.
        final CheckpointManifest existing = manifests.get(key(manifest.checkpointId()));
        if (existing != null) {
            if (!Bytes.constantTimeEquals(existing.manifestSha256(), manifest.manifestSha256())) {
                throw new IllegalStateException("checkpoint identity conflict");
            }
            final io.nereusstream.delay.protocol.CheckpointResourceV1 existingResource =
                    manifestResources.get(key(manifest.checkpointId()));
            if (existingResource != null && !existingResource.equals(resource)) {
                throw new IllegalStateException("checkpoint object identity conflict");
            }
            if (existingResource == null) {
                // A legacy local publish may have recorded the manifest before
                // this upload-intent projection was introduced. Bind the exact
                // immutable object identity on the first compatible reread.
                manifestResources.put(key(manifest.checkpointId()), resource);
            }
            return new Publication(existing, catalogGeneration, floor);
        }
        final Publication publication = publish(manifest, expectedCatalogGeneration);
        manifestResources.put(key(manifest.checkpointId()), resource);
        return publication;
    }

    public synchronized Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
        return Optional.ofNullable(manifests.get(key(checkpointId)));
    }

    public synchronized Optional<RecoveryFloor> currentFloor() {
        return Optional.ofNullable(floor);
    }

    @Override
    public synchronized Optional<RecoveryFloorRefV1> currentFloorRef() {
        return Optional.ofNullable(typedFloorRef);
    }

    /**
     * Validates that an exact manifest is catalog-published and still belongs
     * to the floor-bounded recovery set.  It performs no catalog mutation and
     * is the local counterpart of an Oxia recovery-candidate read/verification.
     */
    public synchronized void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
        Objects.requireNonNull(candidate, "candidate");
        final CheckpointManifest published = manifests.get(key(candidate.checkpointId()));
        if (published == null || !Bytes.constantTimeEquals(published.manifestSha256(), candidate.manifestSha256())) {
            throw new IllegalArgumentException("checkpoint is not the published catalog manifest");
        }
        recoverySet(candidate.checkpointId());
    }

    /** Returns the newest checkpoint in the floor-bounded recovery set. */
    public synchronized CheckpointManifest selectRecoveryCandidate(final byte[] checkpointId) {
        final List<CheckpointManifest> set = recoverySet(checkpointId);
        if (set.isEmpty()) {
            throw new IllegalStateException("published recovery set is empty");
        }
        return set.get(set.size() - 1);
    }

    /** Returns the floor-to-candidate ancestry in replay order. */
    public synchronized List<CheckpointManifest> recoverySet(final byte[] checkpointId) {
        final CheckpointManifest candidate = manifests.get(key(checkpointId));
        if (candidate == null) {
            throw new IllegalArgumentException("checkpoint is not published");
        }
        final List<CheckpointManifest> reverse = new ArrayList<>();
        CheckpointManifest cursor = candidate;
        while (cursor != null) {
            reverse.add(cursor);
            final CheckpointManifest.ParentCheckpoint parent = cursor.parentCheckpoint();
            if (parent == null) {
                break;
            }
            cursor = manifests.get(key(parent.checkpointId()));
            if (cursor == null || !parent.manifestSha256().equals(Bytes.hex(cursor.manifestSha256()))) {
                throw new IllegalStateException("checkpoint ancestry is incomplete or tampered");
            }
        }
        Collections.reverse(reverse);
        if (floor != null) {
            final int floorIndex = indexOf(reverse, floor.checkpointId());
            if (floorIndex < 0) {
                throw new IllegalStateException("candidate is not a descendant of current recovery floor");
            }
            return List.copyOf(reverse.subList(floorIndex, reverse.size()));
        }
        return List.copyOf(reverse);
    }

    /**
     * Proves that an exact published candidate is a descendant of the current
     * Recovery Floor and that the Floor covers the supplied source/mutation
     * boundary.  This is deliberately a read-only local proof: it does not
     * perform an Oxia CAS, publish an object, or authorize an external delete.
     *
     * <p>The candidate is part of the proof even though the sequence/source
     * checks are made against the Floor.  A scalar position or mutation
     * sequence from an unrelated checkpoint branch must never be accepted as
     * a substitute for the parent-hash ancestry.</p>
     */
    public synchronized Optional<FloorCoverage> proveFloorCoverage(final byte[] candidateCheckpointId,
                                                                     final long requiredMutationSequence,
                                                                     final SourcePosition... requiredPositions) {
        Objects.requireNonNull(candidateCheckpointId, "candidateCheckpointId");
        if (requiredMutationSequence < 0) {
            throw new IllegalArgumentException("required mutation sequence must be non-negative");
        }
        Objects.requireNonNull(requiredPositions, "requiredPositions");
        for (SourcePosition position : requiredPositions) {
            Objects.requireNonNull(position, "required source position");
        }
        final RecoveryFloor currentFloor = floor;
        if (currentFloor == null) {
            return Optional.empty();
        }
        final CheckpointManifest candidate = manifests.get(key(candidateCheckpointId));
        if (candidate == null) {
            return Optional.empty();
        }
        final List<CheckpointManifest> ancestry;
        try {
            ancestry = recoverySet(candidateCheckpointId);
        } catch (IllegalArgumentException | IllegalStateException exception) {
            return Optional.empty();
        }
        final int floorIndex = indexOf(ancestry, currentFloor.checkpointId());
        if (floorIndex < 0 || !Bytes.constantTimeEquals(ancestry.get(floorIndex).manifestSha256(),
                currentFloor.manifestSha256())) {
            return Optional.empty();
        }
        if (currentFloor.includedMutationSequence() < requiredMutationSequence) {
            return Optional.empty();
        }
        for (SourcePosition requiredPosition : requiredPositions) {
            try {
                if (currentFloor.appliedSourcePosition().compareTo(requiredPosition) < 0) {
                    return Optional.empty();
                }
            } catch (IllegalArgumentException exception) {
                return Optional.empty();
            }
        }
        return Optional.of(new FloorCoverage(currentFloor, candidate, ancestry));
    }

    public synchronized long catalogGeneration() {
        return catalogGeneration;
    }

    /**
     * Creates the bounded local projection of the Registry Recovery Pin.
     * This validates the same immutable identities available to the local
     * catalog; the Oxia-backed implementation must additionally bind the
     * exact Owner Lease/session in one transaction.
     */
    @Override
    public synchronized RecoveryPinV1 createRecoveryPin(final RecoveryPinV1 pin) {
        Objects.requireNonNull(pin, "pin");
        if (catalogShard == null || !new ShardSubjectV1(catalogShard).equals(pin.shard())) {
            throw new IllegalArgumentException("RecoveryPin shard is not the catalog shard");
        }
        if (pin.observedCatalogGeneration() != catalogGeneration) {
            throw new IllegalStateException("RecoveryPin catalog generation is stale");
        }
        if (floor == null || !matchesFloor(pin)) {
            throw new IllegalStateException("RecoveryPin does not match the current Recovery Floor");
        }
        final CheckpointManifest candidate = manifests.get(key(pin.candidate().checkpointId()));
        if (candidate == null
                || !Bytes.constantTimeEquals(candidate.manifestSha256(), pin.candidate().manifestSha256())
                || !Bytes.constantTimeEquals(candidate.recoveryLineageId(), pin.candidate().recoveryLineageId())) {
            throw new IllegalArgumentException("RecoveryPin candidate is not the published manifest");
        }
        recoverySet(candidate.checkpointId());
        if (pin.candidate().kind() == RecoveryCandidateKindV1.CATALOG_CHECKPOINT
                && pin.candidate().storeIncarnation() != null) {
            throw new IllegalArgumentException("catalog RecoveryPin candidate carries a Store Incarnation");
        }
        if (activeRecoveryPin != null) {
            if (activeRecoveryPin.equals(pin)) {
                return activeRecoveryPin;
            }
            throw new IllegalStateException("a RecoveryPin is already active for the shard");
        }
        activeRecoveryPin = pin;
        return pin;
    }

    /** Releases the exact session-bound pin value; a different value fails closed. */
    @Override
    public synchronized void releaseRecoveryPin(final RecoveryPinV1 pin) {
        Objects.requireNonNull(pin, "pin");
        if (activeRecoveryPin == null) {
            throw new IllegalStateException("no RecoveryPin is active");
        }
        if (!activeRecoveryPin.equals(pin)) {
            throw new IllegalStateException("RecoveryPin identity/value mismatch");
        }
        activeRecoveryPin = null;
    }

    @Override
    public synchronized Optional<RecoveryPinV1> activeRecoveryPin() {
        return Optional.ofNullable(activeRecoveryPin);
    }

    private boolean matchesFloor(final RecoveryPinV1 pin) {
        final io.nereusstream.delay.protocol.RecoveryFloorRefV1 observed = pin.observedFloor();
        if (typedFloorRef != null) {
            return typedFloorRef.equals(observed);
        }
        final CheckpointManifest floorManifest = manifests.get(key(floor.checkpointId()));
        return floorManifest != null
                && floorManifest.evidenceCursors().equals(observed.evidenceCursors())
                && Bytes.constantTimeEquals(observed.recoveryLineageId(), floor.recoveryLineageId())
                && Bytes.constantTimeEquals(observed.checkpointId(), floor.checkpointId())
                && Bytes.constantTimeEquals(observed.manifestSha256(), floor.manifestSha256())
                && observed.catalogGeneration() == floor.catalogGeneration()
                && observed.includedMutationSequence() == floor.includedMutationSequence()
                && observed.appliedSourcePosition().equals(floor.appliedSourcePosition());
    }

    private void validateParent(final CheckpointManifest manifest) {
        final CheckpointManifest.ParentCheckpoint parentRef = manifest.parentCheckpoint();
        if (parentRef == null) {
            return;
        }
        final CheckpointManifest parent = manifests.get(key(parentRef.checkpointId()));
        if (parent == null || !parentRef.manifestSha256().equals(Bytes.hex(parent.manifestSha256()))) {
            throw new IllegalArgumentException("checkpoint parent is not published or hash mismatches");
        }
        if (!Bytes.constantTimeEquals(parent.recoveryLineageId(), manifest.recoveryLineageId())
                || manifest.lineageGeneration() != Math.addExact(parent.lineageGeneration(), 1)) {
            throw new IllegalArgumentException("checkpoint lineage does not extend parent");
        }
        for (EvidenceCursorV1 parentCursor : parent.evidenceCursors()) {
            final EvidenceCursorV1 childCursor = manifest.evidenceCursors().stream()
                    .filter(cursor -> cursor.sameIdentity(parentCursor)).findFirst().orElse(null);
            if (childCursor == null || !childCursor.dominates(parentCursor)) {
                throw new IllegalArgumentException("checkpoint evidence cursor regressed or disappeared");
            }
        }
        final SourcePosition position = manifest.appliedShardLogPosition();
        if (position.compareTo(parent.appliedShardLogPosition()) <= 0
                || manifest.shardMutationSequence() <= parent.shardMutationSequence()) {
            throw new IllegalArgumentException("checkpoint source position does not advance parent");
        }
    }

    private static int indexOf(final List<CheckpointManifest> values, final byte[] checkpointId) {
        for (int index = 0; index < values.size(); index++) {
            if (Bytes.constantTimeEquals(values.get(index).checkpointId(), checkpointId)) {
                return index;
            }
        }
        return -1;
    }

    private static String key(final byte[] checkpointId) {
        Bytes.requireLength(checkpointId, 16, "checkpointId");
        return Bytes.hex(checkpointId);
    }

    private static boolean sameBytes(final byte[] left, final byte[] right) {
        return left == null ? right == null : right != null && Bytes.constantTimeEquals(left, right);
    }

    private static boolean sameHashHex(final byte[] left, final String right) {
        return left == null ? right == null : right != null && Bytes.hex(left).equals(right);
    }

    private static byte[] uuidBytes(final java.util.UUID value) {
        return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits())
                .array();
    }

    public record Publication(CheckpointManifest manifest, long catalogGeneration, RecoveryFloor floor) {
        public Publication {
            Objects.requireNonNull(manifest, "manifest");
            if (catalogGeneration < 0) {
                throw new IllegalArgumentException("catalog generation must be non-negative");
            }
        }
    }

    /** Exact local evidence returned by {@link #proveFloorCoverage(byte[], long, SourcePosition...)}. */
    public record FloorCoverage(RecoveryFloor floor, CheckpointManifest candidate,
                                List<CheckpointManifest> ancestry) {
        public FloorCoverage {
            Objects.requireNonNull(floor, "floor");
            Objects.requireNonNull(candidate, "candidate");
            if (ancestry == null || ancestry.isEmpty()) {
                throw new IllegalArgumentException("floor coverage ancestry must not be empty");
            }
            ancestry = List.copyOf(ancestry);
        }
    }
}
