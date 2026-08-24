package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Immutable Object Store identity used by a committed large payload. */
public final class CommittedPayloadDescriptorV1 {
    public static final int HASH_LENGTH = 32;

    private final ProfileRefV1 objectStoreProfile;
    private final byte[] container;
    private final byte[] objectKey;
    private final byte[] immutableObjectVersion;
    private final byte[] etag;
    private final long length;
    private final byte[] payloadSha256;
    private final byte[] reservationId;
    private final byte[] proofId;

    public CommittedPayloadDescriptorV1(
            final ProfileRefV1 objectStoreProfile,
            final byte[] container,
            final byte[] objectKey,
            final byte[] immutableObjectVersion,
            final byte[] etag,
            final long length,
            final byte[] payloadSha256,
            final byte[] reservationId,
            final byte[] proofId) {
        this.objectStoreProfile = Objects.requireNonNull(objectStoreProfile, "objectStoreProfile");
        if (objectStoreProfile.profileKind() != ProfileKindV1.OBJECT_STORE) {
            throw new IllegalArgumentException("objectStoreProfile must have OBJECT_STORE kind");
        }
        this.container = nonEmpty(container, "container");
        this.objectKey = nonEmpty(objectKey, "objectKey");
        this.immutableObjectVersion = nonEmpty(immutableObjectVersion, "immutableObjectVersion");
        this.etag = etag == null ? null : Bytes.copy(etag);
        if (length < 0) {
            throw new IllegalArgumentException("payload length must be non-negative");
        }
        this.length = length;
        Bytes.requireLength(payloadSha256, HASH_LENGTH, "payloadSha256");
        this.payloadSha256 = Bytes.copy(payloadSha256);
        this.reservationId = fixedNonZero(reservationId, "reservationId");
        this.proofId = fixedNonZero(proofId, "proofId");
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

    public byte[] immutableObjectVersion() {
        return Bytes.copy(immutableObjectVersion);
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

    public byte[] reservationId() {
        return Bytes.copy(reservationId);
    }

    public byte[] proofId() {
        return Bytes.copy(proofId);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, objectStoreProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, container);
            CanonicalProtobuf.bytes(output, 3, objectKey);
            CanonicalProtobuf.bytes(output, 4, immutableObjectVersion);
            if (etag != null) {
                CanonicalProtobuf.bytes(output, 5, etag);
            }
            CanonicalProtobuf.uint64(output, 6, length);
            CanonicalProtobuf.bytes(output, 7, payloadSha256);
            CanonicalProtobuf.bytes(output, 8, reservationId);
            CanonicalProtobuf.bytes(output, 9, proofId);
        });
    }

    public static CommittedPayloadDescriptorV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "CommittedPayloadDescriptorV1");
        if (fields.size() != 8 && fields.size() != 9) {
            throw new IllegalArgumentException("CommittedPayloadDescriptorV1 has invalid field count");
        }
        int index = 0;
        final ProfileRefV1 profile = ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(index++), 1));
        final byte[] container = QueryCodecSupport.bytes(fields.get(index++), 2);
        final byte[] objectKey = QueryCodecSupport.bytes(fields.get(index++), 3);
        final byte[] objectVersion = QueryCodecSupport.bytes(fields.get(index++), 4);
        byte[] etag = null;
        if (fields.get(index).number() == 5) {
            etag = QueryCodecSupport.bytes(fields.get(index++), 5);
        }
        final long length = QueryCodecSupport.uint(fields.get(index++), 6);
        final byte[] hash = QueryCodecSupport.fixed(fields.get(index++), 7, HASH_LENGTH);
        final byte[] reservation = QueryCodecSupport.fixed(fields.get(index++), 8, HASH_LENGTH);
        final byte[] proof = QueryCodecSupport.fixed(fields.get(index), 9, HASH_LENGTH);
        final CommittedPayloadDescriptorV1 result = new CommittedPayloadDescriptorV1(
                profile, container, objectKey, objectVersion, etag, length, hash, reservation, proof);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CommittedPayloadDescriptorV1");
        return result;
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    private static byte[] fixedNonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        boolean nonZero = false;
        for (byte current : value) {
            nonZero |= current != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CommittedPayloadDescriptorV1 that
                && length == that.length
                && objectStoreProfile.equals(that.objectStoreProfile)
                && Arrays.equals(container, that.container)
                && Arrays.equals(objectKey, that.objectKey)
                && Arrays.equals(immutableObjectVersion, that.immutableObjectVersion)
                && Arrays.equals(etag, that.etag)
                && Arrays.equals(payloadSha256, that.payloadSha256)
                && Arrays.equals(reservationId, that.reservationId)
                && Arrays.equals(proofId, that.proofId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                objectStoreProfile,
                Arrays.hashCode(container),
                Arrays.hashCode(objectKey),
                Arrays.hashCode(immutableObjectVersion),
                Arrays.hashCode(etag),
                length,
                Arrays.hashCode(payloadSha256),
                Arrays.hashCode(reservationId),
                Arrays.hashCode(proofId));
    }
}
