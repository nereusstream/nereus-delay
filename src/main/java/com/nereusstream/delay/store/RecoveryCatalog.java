package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.RecoveryCandidateKind;
import com.nereusstream.delay.protocol.RecoveryCandidateRef;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SourcePosition;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Deterministic local model of the checkpoint catalog. Production wiring
 * supplies the same CAS semantics through Oxia; this class deliberately has no
 * network or object-store side effects.
 */
public final class RecoveryCatalog implements RecoveryCatalogAuthority {
    private final Map<String, CheckpointManifest> manifests = new HashMap<>();
    private final Map<String, CheckpointResource> manifestResources = new HashMap<>();
    private long catalogGeneration;
    private RecoveryFloor floor;
    private RecoveryFloorRef typedFloorRef;
    private com.nereusstream.delay.protocol.ShardId catalogShard;
    private RecoveryPin activeRecoveryPin;

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
        catalogGeneration = nextCatalogGeneration(catalogGeneration);
        manifests.put(key, manifest);
        return new Publication(manifest, catalogGeneration, floor);
    }

    /** Advances the floor only to a published checkpoint in the same lineage. */
    public synchronized RecoveryFloor advanceFloor(
            final byte[] checkpointId, final long expectedCatalogGeneration, final byte[] evidenceCursorDigest) {
        Objects.requireNonNull(checkpointId, "checkpointId");
        Bytes.requireLength(evidenceCursorDigest, 32, "evidenceCursorDigest");
        if (typedFloorRef != null) {
            throw new IllegalStateException("typed Recovery Floor requires a typed successor");
        }
        if (floor != null
                && hasNextCatalogGeneration(expectedCatalogGeneration)
                && floor.catalogGeneration() == nextCatalogGeneration(expectedCatalogGeneration)
                && Bytes.constantTimeEquals(floor.checkpointId(), checkpointId)
                && Bytes.constantTimeEquals(floor.evidenceCursorDigest(), evidenceCursorDigest)) {
            final CheckpointManifest reread = manifests.get(key(checkpointId));
            if (reread != null
                    && Bytes.constantTimeEquals(reread.recoveryLineageId(), floor.recoveryLineageId())
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
                    || Long.compareUnsigned(candidate.shardMutationSequence(), floor.includedMutationSequence()) < 0) {
                throw new IllegalArgumentException("recovery floor cannot regress");
            }
        }
        catalogGeneration = nextCatalogGeneration(catalogGeneration);
        floor = RecoveryFloor.create(
                candidate.recoveryLineageId(),
                candidate.checkpointId(),
                candidate.manifestSha256(),
                catalogGeneration,
                candidate.appliedShardLogPosition(),
                candidate.shardMutationSequence(),
                evidenceCursorDigest);
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
    public synchronized RecoveryFloorRef advanceFloor(
            final byte[] checkpointId,
            final long expectedCatalogGeneration,
            final List<EvidenceCursor> evidenceCursors) {
        Objects.requireNonNull(checkpointId, "checkpointId");
        Objects.requireNonNull(evidenceCursors, "evidenceCursors");
        if (typedFloorRef != null
                && hasNextCatalogGeneration(expectedCatalogGeneration)
                && typedFloorRef.catalogGeneration() == nextCatalogGeneration(expectedCatalogGeneration)
                && Bytes.constantTimeEquals(typedFloorRef.checkpointId(), checkpointId)
                && typedFloorRef.evidenceCursors().equals(evidenceCursors)) {
            final CheckpointManifest reread = manifests.get(key(checkpointId));
            if (reread != null
                    && Bytes.constantTimeEquals(reread.recoveryLineageId(), typedFloorRef.recoveryLineageId())
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
                    || Long.compareUnsigned(candidate.shardMutationSequence(), floor.includedMutationSequence()) < 0) {
                throw new IllegalArgumentException("recovery floor cannot regress");
            }
        }
        final RecoveryFloorRef next = new RecoveryFloorRef(
                candidate.recoveryLineageId(),
                candidate.checkpointId(),
                candidate.manifestSha256(),
                nextCatalogGeneration(catalogGeneration),
                candidate.appliedShardLogPosition(),
                candidate.shardMutationSequence(),
                evidenceCursors);
        if (typedFloorRef != null) {
            for (EvidenceCursor previous : typedFloorRef.evidenceCursors()) {
                final EvidenceCursor successor = next.evidenceCursors().stream()
                        .filter(cursor -> cursor.sameIdentity(previous))
                        .findFirst()
                        .orElse(null);
                if (successor == null || !successor.dominates(previous)) {
                    throw new IllegalArgumentException("typed Recovery Floor cursor regressed or disappeared");
                }
            }
        }
        catalogGeneration = next.catalogGeneration();
        typedFloorRef = next;
        floor = RecoveryFloor.create(
                next.recoveryLineageId(),
                next.checkpointId(),
                next.manifestSha256(),
                next.catalogGeneration(),
                next.appliedSourcePosition(),
                next.includedMutationSequence(),
                next.floorDigest());
        return next;
    }

    /**
     * Binds local catalog publication to a complete PUBLISHED upload intent.
     * The production equivalent must compare the same identities and the
     * active Owner Lease/session in one Oxia transaction.
     */
    @Override
    public synchronized Publication publishUploadedCheckpoint(
            final CheckpointUploadIntent publishedIntent,
            final CheckpointManifest manifest,
            final long expectedCatalogGeneration) {
        Objects.requireNonNull(publishedIntent, "publishedIntent");
        Objects.requireNonNull(manifest, "manifest");
        if (publishedIntent.state() != CheckpointUploadState.PUBLISHED || publishedIntent.publishedManifest() == null) {
            throw new IllegalArgumentException("catalog publication requires a PUBLISHED upload intent");
        }
        if (expectedCatalogGeneration != publishedIntent.baseCatalogGeneration()) {
            throw new IllegalStateException("upload intent base catalog generation does not match publication CAS");
        }
        if (!publishedIntent.shard().shardId().equals(manifest.shardId())
                || !Bytes.constantTimeEquals(publishedIntent.recoveryLineageId(), manifest.recoveryLineageId())
                || !Bytes.constantTimeEquals(publishedIntent.checkpointId(), manifest.checkpointId())) {
            throw new IllegalArgumentException("upload intent and manifest shard/checkpoint identity differ");
        }
        final com.nereusstream.delay.protocol.CheckpointResource resource = publishedIntent.publishedManifest();
        if (!Bytes.constantTimeEquals(resource.manifestSha256(), manifest.manifestSha256())
                || resource.manifestLength() != manifest.canonicalJsonBytes().length) {
            throw new IllegalArgumentException("published manifest object identity does not match manifest bytes");
        }
        if (!Bytes.constantTimeEquals(
                        publishedIntent.owner().deploymentId(),
                        manifest.createdBy().deploymentId())
                || !Bytes.constantTimeEquals(
                        publishedIntent.owner().workerRunId(),
                        manifest.createdBy().workerRunId())
                || publishedIntent.owner().ownerEpoch() != manifest.createdBy().ownerEpoch()) {
            throw new IllegalArgumentException("upload intent owner does not match manifest creator");
        }
        if (!Bytes.constantTimeEquals(
                publishedIntent.sourceStoreIncarnation(), uuidBytes(manifest.sourceStoreIncarnation()))) {
            throw new IllegalArgumentException("upload intent store incarnation does not match manifest");
        }
        final CheckpointManifest.ParentCheckpoint parent = manifest.parentCheckpoint();
        if (!sameBytes(publishedIntent.parentCheckpointId(), parent == null ? null : parent.checkpointId())
                || !sameHashHex(
                        publishedIntent.parentManifestSha256(), parent == null ? null : parent.manifestSha256())) {
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
            final com.nereusstream.delay.protocol.CheckpointResource existingResource =
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
    public synchronized Optional<RecoveryFloorRef> currentFloorRef() {
        return Optional.ofNullable(typedFloorRef);
    }

    /**
     * Validates that an exact manifest is catalog-published and still belongs
     * to the floor-bounded recovery set. It performs no catalog mutation and
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

    /**
     * Validates the four local meta/RECOVERY projections before a Store can be
     * considered for reuse. This is deliberately read-only: a production
     * Oxia implementation must bind the same checks to its catalog/Floor
     * transaction and current Owner Lease/session.
     */
    @Override
    public synchronized void validateLocalStoreRecovery(
            final com.nereusstream.delay.protocol.ShardId shardId, final StoreRecoveryMetadata localMetadata) {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(localMetadata, "localMetadata");
        if (!localMetadata.hasReusableProof()
                || localMetadata.lineageBase() == null
                || localMetadata.lineageBase().kind() != RecoveryCandidateKind.LOCAL_STORE
                || localMetadata.lastObservedFloor() == null
                || typedFloorRef == null) {
            throw new IllegalArgumentException("local Store lacks a complete recovery-reuse projection");
        }
        if (!localMetadata.lastObservedFloor().equals(typedFloorRef)
                || localMetadata.catalogGeneration() != typedFloorRef.catalogGeneration()) {
            throw new IllegalStateException("local Store Recovery Floor observation is stale");
        }
        final RecoveryCandidateRef candidate = localMetadata.lineageBase();
        final CheckpointManifest manifest = manifests.get(key(candidate.checkpointId()));
        if (manifest == null
                || !manifest.shardId().equals(shardId)
                || !Bytes.constantTimeEquals(manifest.recoveryLineageId(), candidate.recoveryLineageId())
                || !Bytes.constantTimeEquals(manifest.manifestSha256(), candidate.manifestSha256())) {
            throw new IllegalArgumentException("local Store base candidate is not the exact published manifest");
        }
        if (localMetadata.installState() == null
                || !java.util.Arrays.equals(
                        localMetadata.installState().storeIncarnation(), candidate.storeIncarnation())
                || !java.util.Arrays.equals(localMetadata.installState().checkpointId(), candidate.checkpointId())) {
            throw new IllegalArgumentException("local Store install state does not match its base candidate");
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
        final List<CheckpointManifest> reverse = fullAncestry(checkpointId);
        if (floor != null) {
            final int floorIndex = indexOf(reverse, floor.checkpointId());
            if (floorIndex < 0) {
                throw new IllegalStateException("candidate is not a descendant of current recovery floor");
            }
            return List.copyOf(reverse.subList(floorIndex, reverse.size()));
        }
        return List.copyOf(reverse);
    }

    /** Returns the complete published parent-hash ancestry without applying the current Floor. */
    private List<CheckpointManifest> fullAncestry(final byte[] checkpointId) {
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
        return reverse;
    }

    /**
     * Proves that an exact published candidate is a descendant of the current
     * Recovery Floor and that the Floor covers the supplied source/mutation
     * boundary. This is deliberately a read-only local proof: it does not
     * perform an Oxia CAS, publish an object, or authorize an external delete.
     *
     * <p>The candidate is part of the proof even though the sequence/source
     * checks are made against the Floor. A scalar position or mutation
     * sequence from an unrelated checkpoint branch must never be accepted as
     * a substitute for the parent-hash ancestry.</p>
     */
    public synchronized Optional<FloorCoverage> proveFloorCoverage(
            final byte[] candidateCheckpointId,
            final long requiredMutationSequence,
            final SourcePosition... requiredPositions) {
        Objects.requireNonNull(candidateCheckpointId, "candidateCheckpointId");
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
        if (floorIndex < 0
                || !Bytes.constantTimeEquals(
                        ancestry.get(floorIndex).manifestSha256(), currentFloor.manifestSha256())) {
            return Optional.empty();
        }
        if (Long.compareUnsigned(currentFloor.includedMutationSequence(), requiredMutationSequence) < 0) {
            return Optional.empty();
        }
        for (SourcePosition requiredPosition : requiredPositions) {
            if (!coversPosition(currentFloor.appliedSourcePosition(), requiredPosition)) {
                return Optional.empty();
            }
        }
        return Optional.of(new FloorCoverage(currentFloor, candidate, ancestry));
    }

    public synchronized long catalogGeneration() {
        return catalogGeneration;
    }

    /** Returns an immutable local snapshot for a crash-durable wrapper. */
    synchronized Snapshot snapshot() {
        return new Snapshot(
                catalogGeneration,
                catalogShard,
                new ArrayList<>(manifests.values()),
                new HashMap<>(manifestResources),
                floor,
                typedFloorRef,
                activeRecoveryPin);
    }

    /** Restores a previously validated local snapshot without performing CAS. */
    static RecoveryCatalog fromSnapshot(final Snapshot snapshot) {
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.installSnapshot(Objects.requireNonNull(snapshot, "snapshot"));
        return catalog;
    }

    private synchronized void installSnapshot(final Snapshot snapshot) {
        final boolean hasPublishedState = !snapshot.manifests().isEmpty()
                || snapshot.floor() != null
                || snapshot.typedFloorRef() != null
                || snapshot.activeRecoveryPin() != null
                || !snapshot.manifestResources().isEmpty();
        if (snapshot.catalogGeneration() == 0 && hasPublishedState) {
            throw new IllegalArgumentException("snapshot has published state at catalog generation zero");
        }
        if (snapshot.catalogGeneration() != 0 && snapshot.manifests().isEmpty()) {
            throw new IllegalArgumentException("snapshot has a catalog generation without a manifest");
        }
        manifests.clear();
        manifestResources.clear();
        catalogGeneration = snapshot.catalogGeneration();
        catalogShard = snapshot.catalogShard();
        floor = snapshot.floor();
        typedFloorRef = snapshot.typedFloorRef();
        activeRecoveryPin = snapshot.activeRecoveryPin();
        for (CheckpointManifest manifest : snapshot.manifests()) {
            Objects.requireNonNull(manifest, "snapshot manifest");
            final String key = key(manifest.checkpointId());
            if (manifests.put(key, manifest) != null) {
                throw new IllegalArgumentException("snapshot contains duplicate checkpoint");
            }
            if (catalogShard != null && !catalogShard.equals(manifest.shardId())) {
                throw new IllegalArgumentException("snapshot contains another shard");
            }
            if (catalogShard == null) {
                catalogShard = manifest.shardId();
            }
        }
        for (CheckpointManifest manifest : manifests.values()) {
            validateParent(manifest);
        }
        for (Map.Entry<String, CheckpointResource> entry :
                snapshot.manifestResources().entrySet()) {
            final CheckpointManifest manifest = manifests.get(entry.getKey());
            final CheckpointResource resource = Objects.requireNonNull(entry.getValue(), "snapshot resource");
            if (manifest == null
                    || !Bytes.constantTimeEquals(resource.checkpointId(), manifest.checkpointId())
                    || !Bytes.constantTimeEquals(resource.recoveryLineageId(), manifest.recoveryLineageId())
                    || !Bytes.constantTimeEquals(resource.manifestSha256(), manifest.manifestSha256())) {
                throw new IllegalArgumentException("snapshot resource identity does not match manifest");
            }
            manifestResources.put(entry.getKey(), resource);
        }
        if (floor != null) {
            validateFloorProjection(floor);
        }
        if (typedFloorRef != null) {
            validateTypedFloorProjection(typedFloorRef);
        }
        if (floor != null && typedFloorRef != null) {
            if (!Bytes.constantTimeEquals(floor.recoveryLineageId(), typedFloorRef.recoveryLineageId())
                    || !Bytes.constantTimeEquals(floor.checkpointId(), typedFloorRef.checkpointId())
                    || !Bytes.constantTimeEquals(floor.manifestSha256(), typedFloorRef.manifestSha256())
                    || floor.catalogGeneration() != typedFloorRef.catalogGeneration()
                    || !floor.appliedSourcePosition().equals(typedFloorRef.appliedSourcePosition())
                    || floor.includedMutationSequence() != typedFloorRef.includedMutationSequence()
                    || !Bytes.constantTimeEquals(floor.evidenceCursorDigest(), typedFloorRef.floorDigest())) {
                throw new IllegalArgumentException("snapshot scalar and typed Floors disagree");
            }
        }
        if (activeRecoveryPin != null) {
            validatePinProjection(activeRecoveryPin);
        }
    }

    private void validateFloorProjection(final RecoveryFloor value) {
        final CheckpointManifest manifest = manifests.get(key(value.checkpointId()));
        if (manifest == null
                || !Bytes.constantTimeEquals(value.recoveryLineageId(), manifest.recoveryLineageId())
                || !Bytes.constantTimeEquals(value.manifestSha256(), manifest.manifestSha256())
                || !value.appliedSourcePosition().equals(manifest.appliedShardLogPosition())
                || value.includedMutationSequence() != manifest.shardMutationSequence()
                || Long.compareUnsigned(value.catalogGeneration(), catalogGeneration) > 0) {
            throw new IllegalArgumentException("snapshot scalar Floor does not match a published manifest");
        }
    }

    private void validateTypedFloorProjection(final RecoveryFloorRef value) {
        final CheckpointManifest manifest = manifests.get(key(value.checkpointId()));
        if (manifest == null
                || !Bytes.constantTimeEquals(value.recoveryLineageId(), manifest.recoveryLineageId())
                || !Bytes.constantTimeEquals(value.manifestSha256(), manifest.manifestSha256())
                || !value.appliedSourcePosition().equals(manifest.appliedShardLogPosition())
                || value.includedMutationSequence() != manifest.shardMutationSequence()
                || !value.evidenceCursors().equals(manifest.evidenceCursors())
                || Long.compareUnsigned(value.catalogGeneration(), catalogGeneration) > 0) {
            throw new IllegalArgumentException("snapshot typed Floor does not match a published manifest");
        }
    }

    private void validatePinProjection(final RecoveryPin pin) {
        if (catalogShard == null
                || !new ShardSubject(catalogShard).equals(pin.shard())
                || Long.compareUnsigned(pin.observedCatalogGeneration(), catalogGeneration) > 0
                || pin.observedCatalogGeneration() != pin.observedFloor().catalogGeneration()) {
            throw new IllegalArgumentException("snapshot RecoveryPin shard/generation mismatch");
        }
        if (floor == null) {
            throw new IllegalArgumentException("snapshot RecoveryPin has no current Recovery Floor");
        }
        final RecoveryFloorRef observedFloor = pin.observedFloor();
        final CheckpointManifest observedFloorManifest = manifests.get(key(observedFloor.checkpointId()));
        if (observedFloorManifest == null
                || !Bytes.constantTimeEquals(
                        observedFloorManifest.recoveryLineageId(), observedFloor.recoveryLineageId())
                || !Bytes.constantTimeEquals(observedFloorManifest.manifestSha256(), observedFloor.manifestSha256())
                || !observedFloorManifest.appliedShardLogPosition().equals(observedFloor.appliedSourcePosition())
                || observedFloorManifest.shardMutationSequence() != observedFloor.includedMutationSequence()
                || !observedFloorManifest.evidenceCursors().equals(observedFloor.evidenceCursors())) {
            throw new IllegalArgumentException("snapshot RecoveryPin observed Floor is not a published manifest");
        }
        final CheckpointManifest candidate = manifests.get(key(pin.candidate().checkpointId()));
        if (candidate == null
                || !Bytes.constantTimeEquals(
                        candidate.manifestSha256(), pin.candidate().manifestSha256())
                || !Bytes.constantTimeEquals(
                        candidate.recoveryLineageId(), pin.candidate().recoveryLineageId())) {
            throw new IllegalArgumentException("snapshot RecoveryPin candidate is not published");
        }
        final List<CheckpointManifest> ancestry = fullAncestry(candidate.checkpointId());
        final int observedFloorIndex = indexOf(ancestry, observedFloor.checkpointId());
        if (observedFloorIndex < 0
                || !Bytes.constantTimeEquals(
                        ancestry.get(observedFloorIndex).manifestSha256(), observedFloor.manifestSha256())) {
            throw new IllegalArgumentException(
                    "snapshot RecoveryPin candidate does not descend from its observed Floor");
        }
        final List<CheckpointManifest> currentFloorAncestry = fullAncestry(floor.checkpointId());
        final int currentObservedFloorIndex = indexOf(currentFloorAncestry, observedFloor.checkpointId());
        if (currentObservedFloorIndex < 0
                || !Bytes.constantTimeEquals(
                        currentFloorAncestry.get(currentObservedFloorIndex).manifestSha256(),
                        observedFloor.manifestSha256())) {
            throw new IllegalArgumentException(
                    "snapshot RecoveryPin observed Floor is not an ancestor of current Floor");
        }
        final boolean currentFloorOnCandidateBranch = indexOf(ancestry, floor.checkpointId()) >= 0
                || indexOf(currentFloorAncestry, candidate.checkpointId()) >= 0;
        if (!currentFloorOnCandidateBranch) {
            throw new IllegalArgumentException("snapshot RecoveryPin current Floor is on another branch");
        }
        if (pin.candidate().kind() == RecoveryCandidateKind.CATALOG_CHECKPOINT
                && pin.candidate().storeIncarnation() != null) {
            throw new IllegalArgumentException("snapshot catalog RecoveryPin carries a Store Incarnation");
        }
    }

    /**
     * Creates the bounded local projection of the Registry Recovery Pin.
     * This validates the same immutable identities available to the local
     * catalog; the Oxia-backed implementation must additionally bind the
     * exact Owner Lease/session in one transaction.
     */
    @Override
    public synchronized RecoveryPin createRecoveryPin(final RecoveryPin pin) {
        Objects.requireNonNull(pin, "pin");
        if (catalogShard == null || !new ShardSubject(catalogShard).equals(pin.shard())) {
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
                || !Bytes.constantTimeEquals(
                        candidate.manifestSha256(), pin.candidate().manifestSha256())
                || !Bytes.constantTimeEquals(
                        candidate.recoveryLineageId(), pin.candidate().recoveryLineageId())) {
            throw new IllegalArgumentException("RecoveryPin candidate is not the published manifest");
        }
        recoverySet(candidate.checkpointId());
        if (pin.candidate().kind() == RecoveryCandidateKind.CATALOG_CHECKPOINT
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
    public synchronized void releaseRecoveryPin(final RecoveryPin pin) {
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
    public synchronized Optional<RecoveryPin> activeRecoveryPin() {
        return Optional.ofNullable(activeRecoveryPin);
    }

    private boolean matchesFloor(final RecoveryPin pin) {
        final com.nereusstream.delay.protocol.RecoveryFloorRef observed = pin.observedFloor();
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
                || manifest.lineageGeneration() != nextLineageGeneration(parent.lineageGeneration())) {
            throw new IllegalArgumentException("checkpoint lineage does not extend parent");
        }
        for (EvidenceCursor parentCursor : parent.evidenceCursors()) {
            final EvidenceCursor childCursor = manifest.evidenceCursors().stream()
                    .filter(cursor -> cursor.sameIdentity(parentCursor))
                    .findFirst()
                    .orElse(null);
            if (childCursor == null || !childCursor.dominates(parentCursor)) {
                throw new IllegalArgumentException("checkpoint evidence cursor regressed or disappeared");
            }
        }
        final SourcePosition position = manifest.appliedShardLogPosition();
        if (position.compareTo(parent.appliedShardLogPosition()) <= 0
                || Long.compareUnsigned(manifest.shardMutationSequence(), parent.shardMutationSequence()) <= 0) {
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

    /**
     * A Floor strictly after a required position covers it. If the order
     * token is equal, the canonical position bytes must also be equal; Kafka
     * offset or Pulsar ledger/entry/batch equality alone is not an integrity
     * proof for a retained source position.
     */
    private static boolean coversPosition(final SourcePosition covered, final SourcePosition required) {
        try {
            final int order = covered.compareTo(required);
            return order > 0
                    || (order == 0 && Bytes.constantTimeEquals(covered.canonicalBytes(), required.canonicalBytes()));
        } catch (IllegalArgumentException exception) {
            return false;
        }
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
        return ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    record Snapshot(
            long catalogGeneration,
            ShardId catalogShard,
            List<CheckpointManifest> manifests,
            Map<String, CheckpointResource> manifestResources,
            RecoveryFloor floor,
            RecoveryFloorRef typedFloorRef,
            RecoveryPin activeRecoveryPin) {
        Snapshot {
            manifests = List.copyOf(Objects.requireNonNull(manifests, "manifests"));
            manifestResources = Map.copyOf(Objects.requireNonNull(manifestResources, "manifestResources"));
        }
    }

    public record Publication(CheckpointManifest manifest, long catalogGeneration, RecoveryFloor floor) {
        public Publication {
            Objects.requireNonNull(manifest, "manifest");
        }
    }

    private static boolean hasNextCatalogGeneration(final long generation) {
        return generation != -1L;
    }

    static long nextLineageGeneration(final long generation) {
        if (generation == -1L) {
            throw new IllegalArgumentException("checkpoint lineage generation exhausted");
        }
        return generation + 1;
    }

    private static long nextCatalogGeneration(final long generation) {
        if (!hasNextCatalogGeneration(generation)) {
            throw new IllegalStateException("checkpoint catalog generation exhausted");
        }
        return generation + 1;
    }

    /** Exact local evidence returned by {@link #proveFloorCoverage(byte[], long, SourcePosition...)}. */
    public record FloorCoverage(RecoveryFloor floor, CheckpointManifest candidate, List<CheckpointManifest> ancestry) {
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
