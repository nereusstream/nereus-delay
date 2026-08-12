package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Immutable object-store identity retained by a committed large payload. */
public record PayloadReference(
        byte[] objectStoreProfileHash,
        byte[] container,
        byte[] objectKey,
        byte[] immutableObjectVersion,
        byte[] etag,
        long length,
        byte[] payloadSha256,
        byte[] reservationId,
        byte[] proofId) {
    private static final int LEGACY_VERSION = 1;
    private static final int VERSION = 2;

    public PayloadReference {
        Bytes.requireLength(objectStoreProfileHash, 32, "objectStoreProfileHash");
        requireNonEmpty(container, "container");
        requireNonEmpty(objectKey, "objectKey");
        requireNonEmpty(immutableObjectVersion, "immutableObjectVersion");
        Bytes.requireLength(payloadSha256, 32, "payloadSha256");
        if (length < 0) {
            throw new IllegalArgumentException("payload length must be non-negative");
        }
        if ((reservationId == null) != (proofId == null)) {
            throw new IllegalArgumentException("payload commit identity must be present as a pair");
        }
        if (reservationId != null) {
            reservationId = fixedNonZero(reservationId, "reservationId");
            proofId = fixedNonZero(proofId, "proofId");
        }
        objectStoreProfileHash = Bytes.copy(objectStoreProfileHash);
        container = Bytes.copy(container);
        objectKey = Bytes.copy(objectKey);
        immutableObjectVersion = Bytes.copy(immutableObjectVersion);
        etag = optionalEtag(etag);
        payloadSha256 = Bytes.copy(payloadSha256);
    }

    /** Compatibility constructor for local/legacy projections without committed proof identity. */
    public PayloadReference(final byte[] objectStoreProfileHash, final byte[] container, final byte[] objectKey,
                            final byte[] immutableObjectVersion, final byte[] etag, final long length,
                            final byte[] payloadSha256) {
        this(objectStoreProfileHash, container, objectKey, immutableObjectVersion, etag, length, payloadSha256,
                null, null);
    }

    /** Projects the Registry committed-payload descriptor without losing an absent etag. */
    public static PayloadReference fromDescriptor(final CommittedPayloadDescriptorV1 descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        return new PayloadReference(descriptor.objectStoreProfile().semanticHash(), descriptor.container(),
                descriptor.objectKey(), descriptor.immutableObjectVersion(), descriptor.etag(), descriptor.length(),
                descriptor.payloadSha256(), descriptor.reservationId(), descriptor.proofId());
    }

    private static void requireNonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
    }

    public byte[] objectStoreProfileHash() {
        return Bytes.copy(objectStoreProfileHash);
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

    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    @Override
    public byte[] reservationId() {
        return reservationId == null ? null : Bytes.copy(reservationId);
    }

    @Override
    public byte[] proofId() {
        return proofId == null ? null : Bytes.copy(proofId);
    }

    public boolean hasCommitIdentity() {
        return reservationId != null;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PayloadReference that)) {
            return false;
        }
        return length == that.length && Arrays.equals(objectStoreProfileHash, that.objectStoreProfileHash)
                && Arrays.equals(container, that.container) && Arrays.equals(objectKey, that.objectKey)
                && Arrays.equals(immutableObjectVersion, that.immutableObjectVersion)
                && Arrays.equals(etag, that.etag) && Arrays.equals(payloadSha256, that.payloadSha256)
                && Arrays.equals(reservationId, that.reservationId) && Arrays.equals(proofId, that.proofId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(objectStoreProfileHash), Arrays.hashCode(container),
                Arrays.hashCode(objectKey), Arrays.hashCode(immutableObjectVersion), Arrays.hashCode(etag), length,
                Arrays.hashCode(payloadSha256), Arrays.hashCode(reservationId), Arrays.hashCode(proofId));
    }

    public byte[] encode() {
        final byte[] base = Bytes.concat(Bytes.u32be(hasCommitIdentity() ? VERSION : LEGACY_VERSION),
                objectStoreProfileHash, Bytes.lp32(container), Bytes.lp32(objectKey),
                Bytes.lp32(immutableObjectVersion), Bytes.lp32(etag == null ? new byte[0] : etag),
                Bytes.u64be(length), payloadSha256);
        return hasCommitIdentity() ? Bytes.concat(base, reservationId, proofId) : base;
    }

    public static PayloadReference decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.remaining() < 4 + 32 + 4 * 4 + 8 + 32) {
            throw new IllegalArgumentException("payload reference is truncated");
        }
        final int versionNumber = input.getInt();
        if (versionNumber != LEGACY_VERSION && versionNumber != VERSION) {
            throw new IllegalArgumentException("unsupported payload reference version");
        }
        final byte[] profile = readFixed(input, 32);
        final byte[] container = readLp32(input);
        final byte[] key = readLp32(input);
        final byte[] version = readLp32(input);
        final byte[] etag = readLp32(input);
        final long length = input.getLong();
        final byte[] sha = readFixed(input, 32);
        final byte[] reservationId = versionNumber == VERSION ? readFixed(input, 32) : null;
        final byte[] proofId = versionNumber == VERSION ? readFixed(input, 32) : null;
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("payload reference has trailing bytes");
        }
        final PayloadReference result = new PayloadReference(profile, container, key, version,
                etag.length == 0 ? null : etag, length, sha, reservationId, proofId);
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical payload reference");
        }
        return result;
    }

    private static byte[] readFixed(final ByteBuffer input, final int length) {
        if (input.remaining() < length) {
            throw new IllegalArgumentException("truncated payload reference");
        }
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static byte[] readLp32(final ByteBuffer input) {
        if (input.remaining() < 4) {
            throw new IllegalArgumentException("truncated payload reference length");
        }
        final long length = Integer.toUnsignedLong(input.getInt());
        if (length > input.remaining()) {
            throw new IllegalArgumentException("payload reference length outside value");
        }
        return readFixed(input, Math.toIntExact(length));
    }

    private static byte[] optionalEtag(final byte[] value) {
        return value == null || value.length == 0 ? null : Bytes.copy(value);
    }

    private static byte[] fixedNonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, 32, name);
        boolean nonZero = false;
        for (byte current : value) {
            nonZero |= current != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return Bytes.copy(value);
    }
}
