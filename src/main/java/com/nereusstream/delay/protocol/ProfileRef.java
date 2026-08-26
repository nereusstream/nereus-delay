package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable public reference to one versioned semantic Profile. */
public final class ProfileRef {
    public static final int SEMANTIC_HASH_LENGTH = 32;

    private final byte[] profileId;
    private final long version;
    private final byte[] semanticHash;
    private final ProfileKind profileKind;

    public ProfileRef(
            final byte[] profileId, final long version, final byte[] semanticHash, final ProfileKind profileKind) {
        Objects.requireNonNull(profileId, "profileId");
        if (profileId.length == 0) {
            throw new IllegalArgumentException("profileId must not be empty");
        }
        if (version == 0) {
            throw new IllegalArgumentException("profile version must be nonzero");
        }
        Bytes.requireLength(semanticHash, SEMANTIC_HASH_LENGTH, "profileSemanticHash");
        this.profileId = Bytes.copy(profileId);
        this.version = version;
        this.semanticHash = Bytes.copy(semanticHash);
        this.profileKind = Objects.requireNonNull(profileKind, "profileKind");
    }

    public byte[] profileId() {
        return Bytes.copy(profileId);
    }

    public long version() {
        return version;
    }

    public byte[] semanticHash() {
        return Bytes.copy(semanticHash);
    }

    public ProfileKind profileKind() {
        return profileKind;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profileId);
            CanonicalProtobuf.uint64Bits(output, 2, version);
            CanonicalProtobuf.bytes(output, 3, semanticHash);
            CanonicalProtobuf.uint32(output, 4, profileKind.wireValue());
        });
    }

    public static ProfileRef decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ProfileRef");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "ProfileRef");
        final ProfileRef result = new ProfileRef(
                QueryCodecSupport.bytes(fields.get(0), 1),
                QueryCodecSupport.uint64Bits(fields.get(1), 2),
                QueryCodecSupport.fixed(fields.get(2), 3, SEMANTIC_HASH_LENGTH),
                ProfileKind.fromWire(QueryCodecSupport.uint(fields.get(3), 4)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileRef");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ProfileRef that)) {
            return false;
        }
        return version == that.version
                && profileKind == that.profileKind
                && Arrays.equals(profileId, that.profileId)
                && Arrays.equals(semanticHash, that.semanticHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(profileId), version, Arrays.hashCode(semanticHash), profileKind);
    }
}
