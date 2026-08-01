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
        byte[] payloadSha256) {
    public PayloadReference {
        Bytes.requireLength(objectStoreProfileHash, 32, "objectStoreProfileHash");
        requireNonEmpty(container, "container");
        requireNonEmpty(objectKey, "objectKey");
        requireNonEmpty(immutableObjectVersion, "immutableObjectVersion");
        Objects.requireNonNull(etag, "etag");
        Bytes.requireLength(payloadSha256, 32, "payloadSha256");
        if (length < 0) {
            throw new IllegalArgumentException("payload length must be non-negative");
        }
        objectStoreProfileHash = Bytes.copy(objectStoreProfileHash);
        container = Bytes.copy(container);
        objectKey = Bytes.copy(objectKey);
        immutableObjectVersion = Bytes.copy(immutableObjectVersion);
        etag = Bytes.copy(etag);
        payloadSha256 = Bytes.copy(payloadSha256);
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
        return Bytes.copy(etag);
    }

    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PayloadReference that)) {
            return false;
        }
        return length == that.length && Arrays.equals(objectStoreProfileHash, that.objectStoreProfileHash)
                && Arrays.equals(container, that.container) && Arrays.equals(objectKey, that.objectKey)
                && Arrays.equals(immutableObjectVersion, that.immutableObjectVersion)
                && Arrays.equals(etag, that.etag) && Arrays.equals(payloadSha256, that.payloadSha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(objectStoreProfileHash), Arrays.hashCode(container),
                Arrays.hashCode(objectKey), Arrays.hashCode(immutableObjectVersion), Arrays.hashCode(etag), length,
                Arrays.hashCode(payloadSha256));
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(1), objectStoreProfileHash, Bytes.lp32(container), Bytes.lp32(objectKey),
                Bytes.lp32(immutableObjectVersion), Bytes.lp32(etag), Bytes.u64be(length), payloadSha256);
    }

    public static PayloadReference decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.remaining() < 4 + 32 + 4 * 4 + 8 + 32) {
            throw new IllegalArgumentException("payload reference is truncated");
        }
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported payload reference version");
        }
        final byte[] profile = readFixed(input, 32);
        final byte[] container = readLp32(input);
        final byte[] key = readLp32(input);
        final byte[] version = readLp32(input);
        final byte[] etag = readLp32(input);
        final long length = input.getLong();
        final byte[] sha = readFixed(input, 32);
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("payload reference has trailing bytes");
        }
        final PayloadReference result = new PayloadReference(profile, container, key, version, etag, length, sha);
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
}
