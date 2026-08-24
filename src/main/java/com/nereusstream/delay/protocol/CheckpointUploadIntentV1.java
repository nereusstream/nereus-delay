package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Canonical checkpoint upload intent state machine projection.
 *
 * <p>The codec validates the mutually exclusive local state branches.  It
 * does not perform the Oxia CAS, Object Store upload, lease/session check or
 * reaping authority described by the V1 design.</p>
 */
public final class CheckpointUploadIntentV1 {
    private static final int ID_LENGTH = 16;
    private static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-checkpoint-upload-intent-v1\0");

    private final ShardSubjectV1 shard;
    private final byte[] recoveryLineageId;
    private final byte[] checkpointId;
    private final OwnerIdentityV1 owner;
    private final byte[] sourceStoreIncarnation;
    private final byte[] uploadToken;
    private final long baseCatalogGeneration;
    private final byte[] parentCheckpointId;
    private final byte[] parentManifestSha256;
    private final ProfileRefV1 objectStoreProfile;
    private final TrustedUtcIntervalEvidence checkpointCreatedAt;
    private final long uploadDeadlineEpochMs;
    private final CheckpointUploadStateV1 state;
    private final long stateRevision;
    private final CheckpointResourceV1 publishedManifest;
    private final TrustedUtcIntervalEvidence reapingStartedAt;
    private final byte[] intentDigest;

    public CheckpointUploadIntentV1(
            final ShardSubjectV1 shard,
            final byte[] recoveryLineageId,
            final byte[] checkpointId,
            final OwnerIdentityV1 owner,
            final byte[] sourceStoreIncarnation,
            final byte[] uploadToken,
            final long baseCatalogGeneration,
            final byte[] parentCheckpointId,
            final byte[] parentManifestSha256,
            final ProfileRefV1 objectStoreProfile,
            final TrustedUtcIntervalEvidence checkpointCreatedAt,
            final long uploadDeadlineEpochMs,
            final CheckpointUploadStateV1 state,
            final long stateRevision,
            final CheckpointResourceV1 publishedManifest,
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

    public ShardSubjectV1 shard() {
        return shard;
    }

    public byte[] recoveryLineageId() {
        return Bytes.copy(recoveryLineageId);
    }

    public byte[] checkpointId() {
        return Bytes.copy(checkpointId);
    }

    public OwnerIdentityV1 owner() {
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

    public ProfileRefV1 objectStoreProfile() {
        return objectStoreProfile;
    }

    public TrustedUtcIntervalEvidence checkpointCreatedAt() {
        return checkpointCreatedAt;
    }

    public long uploadDeadlineEpochMs() {
        return uploadDeadlineEpochMs;
    }

    public CheckpointUploadStateV1 state() {
        return state;
    }

    public long stateRevision() {
        return stateRevision;
    }

    public CheckpointResourceV1 publishedManifest() {
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

    public static CheckpointUploadIntentV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "CheckpointUploadIntentV1");
        if (fields.size() < 14
                || fields.get(0).number() != 1
                || fields.get(fields.size() - 1).number() != 18) {
            throw new IllegalArgumentException("invalid CheckpointUploadIntentV1 field order");
        }
        int index = 0;
        require(fields.get(index), 1);
        if (QueryCodecSupport.uint32(fields.get(index++), 1) != 1) {
            throw new IllegalArgumentException("unsupported CheckpointUploadIntentV1 version");
        }
        final ShardSubjectV1 shard = ShardSubjectV1.decode(QueryCodecSupport.nested(fields.get(index++), 2));
        final byte[] lineage = QueryCodecSupport.fixed(fields.get(index++), 3, ID_LENGTH);
        final byte[] checkpoint = QueryCodecSupport.fixed(fields.get(index++), 4, ID_LENGTH);
        final OwnerIdentityV1 owner = OwnerIdentityV1.decode(QueryCodecSupport.nested(fields.get(index++), 5));
        final byte[] store = QueryCodecSupport.fixed(fields.get(index++), 6, ID_LENGTH);
        final byte[] token = QueryCodecSupport.fixed(fields.get(index++), 7, HASH_LENGTH);
        final long baseGeneration = QueryCodecSupport.uint64Bits(fields.get(index++), 8);
        byte[] parentCheckpoint = null;
        byte[] parentManifest = null;
        if (fields.get(index).number() == 9) {
            parentCheckpoint = QueryCodecSupport.fixed(fields.get(index++), 9, ID_LENGTH);
            parentManifest = QueryCodecSupport.fixed(fields.get(index++), 10, HASH_LENGTH);
        }
        final ProfileRefV1 profile =
                requireObjectStoreProfile(ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(index++), 11)));
        final TrustedUtcIntervalEvidence created =
                TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(fields.get(index++), 12));
        final long deadline = QueryCodecSupport.uint(fields.get(index++), 13);
        final CheckpointUploadStateV1 state =
                CheckpointUploadStateV1.fromWire(QueryCodecSupport.uint(fields.get(index++), 14));
        final long revision = QueryCodecSupport.uint64Bits(fields.get(index++), 15);
        CheckpointResourceV1 resource = null;
        if (fields.get(index).number() == 16) {
            resource = CheckpointResourceV1.decode(QueryCodecSupport.nested(fields.get(index++), 16));
        }
        TrustedUtcIntervalEvidence reaping = null;
        if (fields.get(index).number() == 17) {
            reaping = TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(fields.get(index++), 17));
        }
        if (index != fields.size() - 1) {
            throw new IllegalArgumentException("CheckpointUploadIntentV1 has unexpected optional fields");
        }
        final CheckpointUploadIntentV1 result = new CheckpointUploadIntentV1(
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
            throw new IllegalArgumentException("CheckpointUploadIntentV1 digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CheckpointUploadIntentV1");
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
            throw new IllegalArgumentException("CheckpointUploadIntentV1 missing field " + number);
        }
    }

    private static ProfileRefV1 requireObjectStore(final ProfileRefV1 profile) {
        return requireObjectStoreProfile(Objects.requireNonNull(profile, "objectStoreProfile"));
    }

    private static ProfileRefV1 requireObjectStoreProfile(final ProfileRefV1 profile) {
        if (profile.profileKind() != ProfileKindV1.OBJECT_STORE) {
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
        return other instanceof CheckpointUploadIntentV1 that
                && shard.equals(that.shard)
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
