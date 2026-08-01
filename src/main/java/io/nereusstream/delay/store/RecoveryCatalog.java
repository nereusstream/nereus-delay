package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.SourcePosition;

import java.util.ArrayList;
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
public final class RecoveryCatalog {
    private final Map<String, CheckpointManifest> manifests = new HashMap<>();
    private long catalogGeneration;
    private RecoveryFloor floor;

    public synchronized Publication publish(final CheckpointManifest manifest, final long expectedCatalogGeneration) {
        Objects.requireNonNull(manifest, "manifest");
        if (expectedCatalogGeneration != catalogGeneration) {
            throw new IllegalStateException("checkpoint catalog generation conflict");
        }
        final String key = key(manifest.checkpointId());
        final CheckpointManifest existing = manifests.get(key);
        if (existing != null) {
            if (!Bytes.constantTimeEquals(existing.manifestSha256(), manifest.manifestSha256())) {
                throw new IllegalStateException("checkpoint identity conflict");
            }
            return new Publication(existing, catalogGeneration, floor);
        }
        validateParent(manifest);
        catalogGeneration = Math.addExact(catalogGeneration, 1);
        manifests.put(key, manifest);
        return new Publication(manifest, catalogGeneration, floor);
    }

    /** Advances the floor only to a published checkpoint in the same lineage. */
    public synchronized RecoveryFloor advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                                    final byte[] evidenceCursorDigest) {
        Objects.requireNonNull(checkpointId, "checkpointId");
        Bytes.requireLength(evidenceCursorDigest, 32, "evidenceCursorDigest");
        if (expectedCatalogGeneration != catalogGeneration) {
            throw new IllegalStateException("checkpoint catalog generation conflict");
        }
        final CheckpointManifest candidate = manifests.get(key(checkpointId));
        if (candidate == null) {
            throw new IllegalArgumentException("checkpoint is not published");
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

    public synchronized Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
        return Optional.ofNullable(manifests.get(key(checkpointId)));
    }

    public synchronized Optional<RecoveryFloor> currentFloor() {
        return Optional.ofNullable(floor);
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

    public synchronized long catalogGeneration() {
        return catalogGeneration;
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

    public record Publication(CheckpointManifest manifest, long catalogGeneration, RecoveryFloor floor) {
        public Publication {
            Objects.requireNonNull(manifest, "manifest");
            if (catalogGeneration < 0) {
                throw new IllegalArgumentException("catalog generation must be non-negative");
            }
        }
    }
}
