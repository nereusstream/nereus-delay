package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Profile target with the optional all-or-none secret-rotation precondition tuple. */
public final class ProfileControlTargetV1 {
    public static final int HASH_LENGTH = 32;

    private final ProfileRefV1 profile;
    private final Long expectedSecretGeneration;
    private final byte[] expectedBindingDigest;
    private final Long expectedBindingHeadRevision;

    public ProfileControlTargetV1(final ProfileRefV1 profile) {
        this(profile, null, null, null);
    }

    public ProfileControlTargetV1(
            final ProfileRefV1 profile,
            final long expectedSecretGeneration,
            final byte[] expectedBindingDigest,
            final long expectedBindingHeadRevision) {
        this(
                profile,
                Long.valueOf(expectedSecretGeneration),
                expectedBindingDigest,
                Long.valueOf(expectedBindingHeadRevision));
    }

    private ProfileControlTargetV1(
            final ProfileRefV1 profile,
            final Long expectedSecretGeneration,
            final byte[] expectedBindingDigest,
            final Long expectedBindingHeadRevision) {
        this.profile = Objects.requireNonNull(profile, "profile");
        final boolean any = expectedSecretGeneration != null
                || expectedBindingDigest != null
                || expectedBindingHeadRevision != null;
        if (any
                != (expectedSecretGeneration != null
                        && expectedBindingDigest != null
                        && expectedBindingHeadRevision != null)) {
            throw new IllegalArgumentException("Profile rotation preconditions must be all present or all absent");
        }
        if (expectedSecretGeneration != null && (expectedSecretGeneration == 0 || expectedBindingHeadRevision == 0)) {
            throw new IllegalArgumentException("Profile rotation secret generation and head revision must be non-zero");
        }
        if (expectedBindingDigest != null) {
            Bytes.requireLength(expectedBindingDigest, HASH_LENGTH, "expectedBindingDigest");
        }
        this.expectedSecretGeneration = expectedSecretGeneration;
        this.expectedBindingDigest = expectedBindingDigest == null ? null : Bytes.copy(expectedBindingDigest);
        this.expectedBindingHeadRevision = expectedBindingHeadRevision;
    }

    public ProfileRefV1 profile() {
        return profile;
    }

    public Long expectedSecretGeneration() {
        return expectedSecretGeneration;
    }

    public byte[] expectedBindingDigest() {
        return expectedBindingDigest == null ? null : Bytes.copy(expectedBindingDigest);
    }

    public Long expectedBindingHeadRevision() {
        return expectedBindingHeadRevision;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            if (expectedSecretGeneration != null) {
                CanonicalProtobuf.uint64Bits(output, 2, expectedSecretGeneration);
                CanonicalProtobuf.bytes(output, 3, expectedBindingDigest);
                CanonicalProtobuf.uint64Bits(output, 4, expectedBindingHeadRevision);
            }
        });
    }

    public static ProfileControlTargetV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ProfileControlTargetV1");
        if (fields.size() != 1 && fields.size() != 4) {
            throw new IllegalArgumentException("invalid ProfileControlTargetV1 field count");
        }
        if (fields.get(0).number() != 1
                || (fields.size() == 4
                        && (fields.get(1).number() != 2
                                || fields.get(2).number() != 3
                                || fields.get(3).number() != 4))) {
            throw new IllegalArgumentException("invalid ProfileControlTargetV1 field order");
        }
        final ProfileControlTargetV1 result = fields.size() == 1
                ? new ProfileControlTargetV1(ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)))
                : new ProfileControlTargetV1(
                        ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                        QueryCodecSupport.uint(fields.get(1), 2),
                        QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                        QueryCodecSupport.uint64Bits(fields.get(3), 4));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileControlTargetV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProfileControlTargetV1 that
                && profile.equals(that.profile)
                && Objects.equals(expectedSecretGeneration, that.expectedSecretGeneration)
                && Arrays.equals(expectedBindingDigest, that.expectedBindingDigest)
                && Objects.equals(expectedBindingHeadRevision, that.expectedBindingHeadRevision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                profile, expectedSecretGeneration, Arrays.hashCode(expectedBindingDigest), expectedBindingHeadRevision);
    }
}
