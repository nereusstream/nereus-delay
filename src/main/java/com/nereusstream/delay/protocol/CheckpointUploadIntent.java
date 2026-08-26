package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Canonical checkpoint upload intent state machine projection.
 *
 * <p>The codec validates the mutually exclusive local state branches. It
 * does not perform the Oxia CAS, Object Store upload, lease/session check or
 * reaping authority described by the design.</p>
 */
public final class CheckpointUploadIntent {
    private static final int ID_LENGTH = 16;
    private static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-checkpoint-upload-intent\0");

    private final ShardSubject shard;
    private final byte[] recoveryLineageId;
    private final byte[] checkpointId;
    private final OwnerIdentity owner;
    private final byte[] sourceStoreIncarnation;
    private final byte[] uploadToken;
    private final long baseCatalogGeneration;
    private final byte[] parentCheckpointId;
    private final byte[] parentManifestSha256;
    private final ProfileRef objectStoreProfile;
    private final TrustedUtcIntervalEvidence checkpointCreatedAt;
    private final long uploadDeadlineEpochMs;
    private final CheckpointUploadState state;
    private final long stateRevision;
    private final CheckpointResource publishedManifest;
    private final TrustedUtcIntervalEvidence reapingStartedAt;
    private final byte[] intentDigest;

    public CheckpointUploadIntent(
            final ShardSubject shard,
            final byte[] recoveryLineageId,
            final byte[] checkpointId,
            final OwnerIdentity owner,
            final byte[] sourceStoreIncarnation,
            final byte[] uploadToken,
            final long baseCatalogGeneration,
            final byte[] parentCheckpointId,
            final byte[] parentManifestSha256,
            final ProfileRef objectStoreProfile,
            final TrustedUtcIntervalEvidence checkpointCreatedAt,
            final long uploadDeadlineEpochMs,
            final CheckpointUploadState state,
            final long stateRevision,
            final CheckpointResource publishedManifest,
            final TrustedUtcIntervalEvidence reapingStartedAt) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.recoveryLineageId = nonZeroFixed(recoveryLineageId, ID_LENGTH, "recoveryLineageId");
        this.checkpointId = nonZeroFixed(checkpointId, ID_LENGTH, "checkpointId");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.sourceStoreIncarnation = nonZeroFixed(sourceStoreIncarnation, ID_LENGTH, "sourceStoreIncarnation");
        this.uploadToken = nonZeroFixed(uploadToken, HASH_LENGTH, "uploadToken");
        if (baseCatalogGeneration == 0) {
            throw new IllegalArgumentException("baseCatalogGeneration must be nonzero");
        }
        this.baseCatalogGeneration = baseCatalogGeneration;
        if ((parentCheckpointId == null) != (parentManifestSha256 == null)) {
            throw new IllegalArgumentException("parent checkpoint fields must be present together");
        }
        this.parentCheckpointId =
                parentCheckpointId == null ? null : nonZeroFixed(parentCheckpointId, ID_LENGTH, "parentCheckpointId");
        this.parentManifestSha256 =
                parentManifestSha256 == null ? null : fixed(parentManifestSha256, HASH_LENGTH, "parentManifestSha256");
        this.objectStoreProfile = requireObjectStore(objectStoreProfile);
        this.checkpointCreatedAt = Objects.requireNonNull(checkpointCreatedAt, "checkpointCreatedAt");
        if (uploadDeadlineEpochMs < 0) {
            throw new IllegalArgumentException("uploadDeadlineEpochMs must be non-negative");
        }
        this.uploadDeadlineEpochMs = uploadDeadlineEpochMs;
        this.state = Objects.requireNonNull(state, "state");
        if (stateRevision == 0) {
            throw new IllegalArgumentException("stateRevision must be nonzero");
        }
        this.stateRevision = stateRevision;
        if (publishedManifest != null
                && (!Arrays.equals(recoveryLineageId, publishedManifest.recoveryLineageId())
                        || !Arrays.equals(checkpointId, publishedManifest.checkpointId())
                        || !objectStoreProfile.equals(publishedManifest.objectStoreProfile()))) {
            throw new IllegalArgumentException("published manifest identity does not match upload intent");
        }
        switch (state) {
            case PENDING_UPLOAD -> {
                if (publishedManifest != null || reapingStartedAt != null) {
                    throw new IllegalArgumentException("PENDING_UPLOAD cannot carry publication/reaping evidence");
                }
            }
            case PUBLISHED -> {
                if (publishedManifest == null || reapingStartedAt != null) {
                    throw new IllegalArgumentException("PUBLISHED requires manifest and forbids reaping evidence");
                }
            }
            case REAPING -> {
                if (publishedManifest != null || reapingStartedAt == null) {
                    throw new IllegalArgumentException("REAPING requires reaping evidence and forbids manifest");
                }
            }
        }
        this.publishedManifest = publishedManifest;
        this.reapingStartedAt = reapingStartedAt;
        this.intentDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToSeventeen());
    }

    public ShardSubject shard() {
        return shard;
    }

    public byte[] recoveryLineageId() {
        return Bytes.copy(recoveryLineageId);
    }

    public byte[] checkpointId() {
        return Bytes.copy(checkpointId);
    }

    public OwnerIdentity owner() {
        return owner;
    }

    public byte[] sourceStoreIncarnation() {
        return Bytes.copy(sourceStoreIncarnation);
    }

    public byte[] uploadToken() {
        return Bytes.copy(uploadToken);
    }

    public long baseCatalogGeneration() {
        return baseCatalogGeneration;
    }

    public byte[] parentCheckpointId() {
        return copyNullable(parentCheckpointId);
    }

    public byte[] parentManifestSha256() {
        return copyNullable(parentManifestSha256);
    }

    public ProfileRef objectStoreProfile() {
        return objectStoreProfile;
    }

    public TrustedUtcIntervalEvidence checkpointCreatedAt() {
        return checkpointCreatedAt;
    }

    public long uploadDeadlineEpochMs() {
        return uploadDeadlineEpochMs;
    }

    public CheckpointUploadState state() {
        return state;
    }

    public long stateRevision() {
        return stateRevision;
    }

    public CheckpointResource publishedManifest() {
        return publishedManifest;
    }

    public TrustedUtcIntervalEvidence reapingStartedAt() {
        return reapingStartedAt;
    }

    public byte[] intentDigest() {
        return Bytes.copy(intentDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToSeventeen());
            CanonicalProtobuf.bytes(output, 18, intentDigest);
        });
    }

    public static CheckpointUploadIntent decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "CheckpointUploadIntent");
        if (fields.size() < 14
                || fields.get(0).number() != 1
                || fields.get(fields.size() - 1).number() != 18) {
            throw new IllegalArgumentException("invalid CheckpointUploadIntent field order");
        }
        int index = 0;
        require(fields.get(index), 1);
        if (QueryCodecSupport.uint32(fields.get(index++), 1) != 1) {
            throw new IllegalArgumentException("unsupported CheckpointUploadIntent version");
        }
        final ShardSubject shard = ShardSubject.decode(QueryCodecSupport.nested(fields.get(index++), 2));
        final byte[] lineage = QueryCodecSupport.fixed(fields.get(index++), 3, ID_LENGTH);
        final byte[] checkpoint = QueryCodecSupport.fixed(fields.get(index++), 4, ID_LENGTH);
        final OwnerIdentity owner = OwnerIdentity.decode(QueryCodecSupport.nested(fields.get(index++), 5));
        final byte[] store = QueryCodecSupport.fixed(fields.get(index++), 6, ID_LENGTH);
        final byte[] token = QueryCodecSupport.fixed(fields.get(index++), 7, HASH_LENGTH);
        final long baseGeneration = QueryCodecSupport.uint64Bits(fields.get(index++), 8);
        byte[] parentCheckpoint = null;
        byte[] parentManifest = null;
        if (fields.get(index).number() == 9) {
            parentCheckpoint = QueryCodecSupport.fixed(fields.get(index++), 9, ID_LENGTH);
            parentManifest = QueryCodecSupport.fixed(fields.get(index++), 10, HASH_LENGTH);
        }
        final ProfileRef profile =
                requireObjectStoreProfile(ProfileRef.decode(QueryCodecSupport.nested(fields.get(index++), 11)));
        final TrustedUtcIntervalEvidence created =
                TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(fields.get(index++), 12));
        final long deadline = QueryCodecSupport.uint(fields.get(index++), 13);
        final CheckpointUploadState state =
                CheckpointUploadState.fromWire(QueryCodecSupport.uint(fields.get(index++), 14));
        final long revision = QueryCodecSupport.uint64Bits(fields.get(index++), 15);
        CheckpointResource resource = null;
        if (fields.get(index).number() == 16) {
            resource = CheckpointResource.decode(QueryCodecSupport.nested(fields.get(index++), 16));
        }
        TrustedUtcIntervalEvidence reaping = null;
        if (fields.get(index).number() == 17) {
            reaping = TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(fields.get(index++), 17));
        }
        if (index != fields.size() - 1) {
            throw new IllegalArgumentException("CheckpointUploadIntent has unexpected optional fields");
        }
        final CheckpointUploadIntent result = new CheckpointUploadIntent(
                shard,
                lineage,
                checkpoint,
                owner,
                store,
                token,
                baseGeneration,
                parentCheckpoint,
                parentManifest,
                profile,
                created,
                deadline,
                state,
                revision,
                resource,
                reaping);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(index), 18, HASH_LENGTH);
        if (!Bytes.constantTimeEquals(digest, result.intentDigest)) {
            throw new IllegalArgumentException("CheckpointUploadIntent digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CheckpointUploadIntent");
        return result;
    }

    private byte[] fieldsOneToSeventeen() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, shard.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, recoveryLineageId);
            CanonicalProtobuf.bytes(output, 4, checkpointId);
            CanonicalProtobuf.bytes(output, 5, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 6, sourceStoreIncarnation);
            CanonicalProtobuf.bytes(output, 7, uploadToken);
            CanonicalProtobuf.uint64Bits(output, 8, baseCatalogGeneration);
            if (parentCheckpointId != null) {
                CanonicalProtobuf.bytes(output, 9, parentCheckpointId);
                CanonicalProtobuf.bytes(output, 10, parentManifestSha256);
            }
            CanonicalProtobuf.bytes(output, 11, objectStoreProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 12, checkpointCreatedAt.canonicalBytes());
            CanonicalProtobuf.int64(output, 13, uploadDeadlineEpochMs);
            CanonicalProtobuf.uint32(output, 14, state.wireValue());
            CanonicalProtobuf.uint64Bits(output, 15, stateRevision);
            if (publishedManifest != null) {
                CanonicalProtobuf.bytes(output, 16, publishedManifest.canonicalBytes());
            }
            if (reapingStartedAt != null) {
                CanonicalProtobuf.bytes(output, 17, reapingStartedAt.canonicalBytes());
            }
        });
    }

    private static void require(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number) {
            throw new IllegalArgumentException("CheckpointUploadIntent missing field " + number);
        }
    }

    private static ProfileRef requireObjectStore(final ProfileRef profile) {
        return requireObjectStoreProfile(Objects.requireNonNull(profile, "objectStoreProfile"));
    }

    private static ProfileRef requireObjectStoreProfile(final ProfileRef profile) {
        if (profile.profileKind() != ProfileKind.OBJECT_STORE) {
            throw new IllegalArgumentException("checkpoint upload intent requires an OBJECT_STORE profile");
        }
        return profile;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static byte[] nonZeroFixed(final byte[] value, final int length, final String name) {
        final byte[] result = fixed(value, length, name);
        if (Arrays.stream(toIntArray(result)).allMatch(item -> item == 0)) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return result;
    }

    private static byte[] copyNullable(final byte[] value) {
        return value == null ? null : Bytes.copy(value);
    }

    private static int[] toIntArray(final byte[] value) {
        final int[] result = new int[value.length];
        for (int index = 0; index < value.length; index++) {
            result[index] = value[index];
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CheckpointUploadIntent that
                && shard.equals(that.shard)
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
