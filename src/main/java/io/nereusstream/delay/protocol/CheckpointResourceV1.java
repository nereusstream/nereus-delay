package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Exact immutable Object Store identity for a published checkpoint manifest. */
public final class CheckpointResourceV1 {
    private static final int ID_LENGTH = 16;
    private static final int HASH_LENGTH = 32;

    private final byte[] recoveryLineageId;
    private final byte[] checkpointId;
    private final ProfileRefV1 objectStoreProfile;
    private final byte[] container;
    private final byte[] objectKey;
    private final byte[] immutableVersion;
    private final long manifestLength;
    private final byte[] manifestSha256;

    public CheckpointResourceV1(final byte[] recoveryLineageId, final byte[] checkpointId,
                                final ProfileRefV1 objectStoreProfile, final byte[] container,
                                final byte[] objectKey, final byte[] immutableVersion,
                                final long manifestLength, final byte[] manifestSha256) {
        this.recoveryLineageId = nonZeroFixed(recoveryLineageId, ID_LENGTH, "recoveryLineageId");
        this.checkpointId = nonZeroFixed(checkpointId, ID_LENGTH, "checkpointId");
        this.objectStoreProfile = Objects.requireNonNull(objectStoreProfile, "objectStoreProfile");
        if (objectStoreProfile.profileKind() != ProfileKindV1.OBJECT_STORE) {
            throw new IllegalArgumentException("checkpoint resource requires an OBJECT_STORE profile");
        }
        this.container = nonEmpty(container, "container");
        this.objectKey = nonEmpty(objectKey, "objectKey");
        this.immutableVersion = nonEmpty(immutableVersion, "immutableVersion");
        if (manifestLength < 0) {
            throw new IllegalArgumentException("manifestLength must be non-negative");
        }
        this.manifestLength = manifestLength;
        this.manifestSha256 = fixed(manifestSha256, HASH_LENGTH, "manifestSha256");
    }

    public byte[] recoveryLineageId() {
        return Bytes.copy(recoveryLineageId);
    }

    public byte[] checkpointId() {
        return Bytes.copy(checkpointId);
    }

    public ProfileRefV1 objectStoreProfile() {
        return objectStoreProfile;
    }

    public byte[] container() {
        return Bytes.copy(container);
    }

    public byte[] objectKey() {
        return Bytes.copy(objectKey);
    }

    public byte[] immutableVersion() {
        return Bytes.copy(immutableVersion);
    }

    public long manifestLength() {
        return manifestLength;
    }

    public byte[] manifestSha256() {
        return Bytes.copy(manifestSha256);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, recoveryLineageId);
            CanonicalProtobuf.bytes(output, 2, checkpointId);
            CanonicalProtobuf.bytes(output, 3, objectStoreProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, container);
            CanonicalProtobuf.bytes(output, 5, objectKey);
            CanonicalProtobuf.bytes(output, 6, immutableVersion);
            CanonicalProtobuf.uint64(output, 7, manifestLength);
            CanonicalProtobuf.bytes(output, 8, manifestSha256);
        });
    }

    public static CheckpointResourceV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "CheckpointResourceV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8}, "CheckpointResourceV1");
        final CheckpointResourceV1 result = new CheckpointResourceV1(
                QueryCodecSupport.fixed(fields.get(0), 1, ID_LENGTH),
                QueryCodecSupport.fixed(fields.get(1), 2, ID_LENGTH),
                ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(2), 3)),
                QueryCodecSupport.bytes(fields.get(3), 4),
                QueryCodecSupport.bytes(fields.get(4), 5),
                QueryCodecSupport.bytes(fields.get(5), 6),
                QueryCodecSupport.uint(fields.get(6), 7),
                QueryCodecSupport.fixed(fields.get(7), 8, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CheckpointResourceV1");
        return result;
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

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
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
        return other instanceof CheckpointResourceV1 that
                && manifestLength == that.manifestLength
                && objectStoreProfile.equals(that.objectStoreProfile)
                && Arrays.equals(recoveryLineageId, that.recoveryLineageId)
                && Arrays.equals(checkpointId, that.checkpointId)
                && Arrays.equals(container, that.container)
                && Arrays.equals(objectKey, that.objectKey)
                && Arrays.equals(immutableVersion, that.immutableVersion)
                && Arrays.equals(manifestSha256, that.manifestSha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(recoveryLineageId), Arrays.hashCode(checkpointId), objectStoreProfile,
                Arrays.hashCode(container), Arrays.hashCode(objectKey), Arrays.hashCode(immutableVersion),
                manifestLength, Arrays.hashCode(manifestSha256));
    }
}
