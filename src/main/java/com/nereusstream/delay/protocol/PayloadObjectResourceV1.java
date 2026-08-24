package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical typed value for the Registry PayloadObjectResourceV1 branch. */
public final class PayloadObjectResourceV1 {
    private static final int HASH_LENGTH = 32;

    private final ProfileRefV1 objectStoreProfile;
    private final byte[] container;
    private final byte[] objectKey;
    private final byte[] immutableVersion;
    private final byte[] etag;
    private final long length;
    private final byte[] payloadSha256;

    public PayloadObjectResourceV1(
            final ProfileRefV1 objectStoreProfile,
            final byte[] container,
            final byte[] objectKey,
            final byte[] immutableVersion,
            final byte[] etag,
            final long length,
            final byte[] payloadSha256) {
        this.objectStoreProfile = Objects.requireNonNull(objectStoreProfile, "objectStoreProfile");
        if (objectStoreProfile.profileKind() != ProfileKindV1.OBJECT_STORE) {
            throw new IllegalArgumentException("payload object requires an OBJECT_STORE profile");
        }
        this.container = nonEmpty(container, "container");
        this.objectKey = nonEmpty(objectKey, "objectKey");
        this.immutableVersion = nonEmpty(immutableVersion, "immutableVersion");
        this.etag = optional(etag);
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        this.length = length;
        this.payloadSha256 = fixed(payloadSha256, HASH_LENGTH, "payloadSha256");
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

    public byte[] etag() {
        return etag == null ? null : Bytes.copy(etag);
    }

    public long length() {
        return length;
    }

    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, objectStoreProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, container);
            CanonicalProtobuf.bytes(output, 3, objectKey);
            CanonicalProtobuf.bytes(output, 4, immutableVersion);
            if (etag != null) {
                CanonicalProtobuf.bytes(output, 5, etag);
            }
            CanonicalProtobuf.uint64(output, 6, length);
            CanonicalProtobuf.bytes(output, 7, payloadSha256);
        });
    }

    public byte[] exactResourceCanonicalBytes() {
        return CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, ResourceKind.PAYLOAD_OBJECT.wireValue(), canonicalBytes()));
    }

    public static PayloadObjectResourceV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "PayloadObjectResourceV1");
        if (fields.size() != 6 && fields.size() != 7) {
            throw new IllegalArgumentException("PayloadObjectResourceV1 has an unexpected field count");
        }
        QueryCodecSupport.requireNumbers(
                fields,
                fields.size() == 6 ? new int[] {1, 2, 3, 4, 6, 7} : new int[] {1, 2, 3, 4, 5, 6, 7},
                "PayloadObjectResourceV1");
        int index = 0;
        final ProfileRefV1 profile = ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(index++), 1));
        final byte[] container = QueryCodecSupport.bytes(fields.get(index++), 2);
        final byte[] objectKey = QueryCodecSupport.bytes(fields.get(index++), 3);
        final byte[] immutableVersion = QueryCodecSupport.bytes(fields.get(index++), 4);
        final byte[] etag = fields.size() == 7 ? QueryCodecSupport.bytes(fields.get(index++), 5) : null;
        final long length = QueryCodecSupport.uint(fields.get(index++), 6);
        final byte[] payloadSha256 = QueryCodecSupport.fixed(fields.get(index), 7, HASH_LENGTH);
        final PayloadObjectResourceV1 result = new PayloadObjectResourceV1(
                profile, container, objectKey, immutableVersion, etag, length, payloadSha256);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadObjectResourceV1");
        return result;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    private static byte[] optional(final byte[] value) {
        return value == null || value.length == 0 ? null : Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadObjectResourceV1 that
                && objectStoreProfile.equals(that.objectStoreProfile)
                && Arrays.equals(container, that.container)
                && Arrays.equals(objectKey, that.objectKey)
                && Arrays.equals(immutableVersion, that.immutableVersion)
                && Arrays.equals(etag, that.etag)
                && length == that.length
                && Arrays.equals(payloadSha256, that.payloadSha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                objectStoreProfile,
                Arrays.hashCode(container),
                Arrays.hashCode(objectKey),
                Arrays.hashCode(immutableVersion),
                Arrays.hashCode(etag),
                length,
                Arrays.hashCode(payloadSha256));
    }
}
