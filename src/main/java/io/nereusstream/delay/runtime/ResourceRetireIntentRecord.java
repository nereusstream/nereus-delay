package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ResourceKind;
import io.nereusstream.delay.protocol.ResourceRetireIntentBody;
import io.nereusstream.delay.protocol.SourcePositionCodec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Durable gc_cf registration for one source-ordered resource retire intent. */
public record ResourceRetireIntentRecord(
        byte[] mutationId,
        byte[] mutationHash,
        ResourceKind resourceKind,
        byte[] resourceIdentity,
        byte[] resourceIdentityHash,
        long expectedResourceStateVersion,
        byte[] protections,
        byte[] appliedSourcePosition) {
    public static final int VALUE_TYPE = 6;
    private static final int HASH_LENGTH = 32;

    public ResourceRetireIntentRecord {
        Bytes.requireLength(mutationId, HASH_LENGTH, "mutationId");
        Bytes.requireLength(mutationHash, HASH_LENGTH, "mutationHash");
        Objects.requireNonNull(resourceKind, "resourceKind");
        requireNonEmpty(resourceIdentity, "resourceIdentity");
        Bytes.requireLength(resourceIdentityHash, HASH_LENGTH, "resourceIdentityHash");
        ResourceRetireIntentBody.decodeResourceIdentity(resourceKind, resourceIdentity);
        if (!Bytes.constantTimeEquals(resourceIdentityHash,
                Bytes.sha256(Bytes.utf8("nereus-delay-resource-identity-v1\0"), resourceIdentity))) {
            throw new IllegalArgumentException("resource identity hash does not match canonical identity");
        }
        if (expectedResourceStateVersion < 0) {
            throw new IllegalArgumentException("expected resource state version must be non-negative");
        }
        requireNonEmpty(protections, "protections");
        requireNonEmpty(appliedSourcePosition, "appliedSourcePosition");
        SourcePositionCodec.decode(appliedSourcePosition);
        mutationId = Bytes.copy(mutationId);
        mutationHash = Bytes.copy(mutationHash);
        resourceIdentity = Bytes.copy(resourceIdentity);
        resourceIdentityHash = Bytes.copy(resourceIdentityHash);
        protections = Bytes.copy(protections);
        appliedSourcePosition = Bytes.copy(appliedSourcePosition);
    }

    @Override
    public byte[] mutationId() {
        return Bytes.copy(mutationId);
    }

    @Override
    public byte[] mutationHash() {
        return Bytes.copy(mutationHash);
    }

    @Override
    public byte[] resourceIdentity() {
        return Bytes.copy(resourceIdentity);
    }

    @Override
    public byte[] resourceIdentityHash() {
        return Bytes.copy(resourceIdentityHash);
    }

    @Override
    public byte[] protections() {
        return Bytes.copy(protections);
    }

    @Override
    public byte[] appliedSourcePosition() {
        return Bytes.copy(appliedSourcePosition);
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(1), mutationId, mutationHash, Bytes.u8(resourceKind.wireValue()),
                Bytes.lp32(resourceIdentity), resourceIdentityHash, Bytes.u64be(expectedResourceStateVersion),
                Bytes.lp32(protections), Bytes.lp32(appliedSourcePosition));
    }

    public static ResourceRetireIntentRecord decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(Objects.requireNonNull(encoded, "encoded"));
        requireRemaining(input, 4 + HASH_LENGTH * 2 + 1 + 4 + HASH_LENGTH + 8 + 4 + 4);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported resource retire intent version");
        }
        final byte[] mutationId = readFixed(input, HASH_LENGTH, "mutationId");
        final byte[] mutationHash = readFixed(input, HASH_LENGTH, "mutationHash");
        final ResourceKind kind = ResourceKind.fromWire(input.get() & 0xff);
        final byte[] identity = readLp32(input, "resourceIdentity");
        final byte[] identityHash = readFixed(input, HASH_LENGTH, "resourceIdentityHash");
        final long expectedVersion = readU64(input, "expectedResourceStateVersion");
        final byte[] protections = readLp32(input, "protections");
        final byte[] source = readLp32(input, "appliedSourcePosition");
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing resource retire intent bytes");
        }
        final ResourceRetireIntentRecord result = new ResourceRetireIntentRecord(mutationId, mutationHash, kind,
                identity, identityHash, expectedVersion, protections, source);
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical resource retire intent");
        }
        return result;
    }

    private static void requireNonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
    }

    private static long readU64(final ByteBuffer input, final String name) {
        requireRemaining(input, Long.BYTES);
        final long value = input.getLong();
        if (value < 0) {
            throw new IllegalArgumentException(name + " exceeds the supported unsigned range");
        }
        return value;
    }

    private static byte[] readLp32(final ByteBuffer input, final String name) {
        requireRemaining(input, Integer.BYTES);
        final long length = Integer.toUnsignedLong(input.getInt());
        if (length > input.remaining()) {
            throw new IllegalArgumentException(name + " length outside record");
        }
        return readFixed(input, Math.toIntExact(length), name);
    }

    private static byte[] readFixed(final ByteBuffer input, final int length, final String name) {
        requireRemaining(input, length);
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static void requireRemaining(final ByteBuffer input, final int length) {
        if (length < 0 || input.remaining() < length) {
            throw new IllegalArgumentException("resource retire intent is truncated");
        }
    }
}
