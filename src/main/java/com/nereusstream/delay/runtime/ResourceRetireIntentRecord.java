package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ResourceKind;
import com.nereusstream.delay.protocol.ResourceRetireIntentBody;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
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
        long appliedMutationSequence,
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
        if (!Bytes.constantTimeEquals(
                resourceIdentityHash, Bytes.sha256(Bytes.utf8("nereus-delay-resource-identity\0"), resourceIdentity))) {
            throw new IllegalArgumentException("resource identity hash does not match canonical identity");
        }
        final ResourceRetireIntentBody.ProtectionSet protectionSet =
                ResourceRetireIntentBody.ProtectionSet.decodeCanonical(protections);
        requireNonEmpty(appliedSourcePosition, "appliedSourcePosition");
        final SourcePosition appliedPosition = SourcePositionCodec.decode(appliedSourcePosition);
        for (ResourceRetireIntentBody.ProtectionRef reference : protectionSet.references()) {
            if (reference.minimumSourcePosition().length == 0) {
                continue;
            }
            final SourcePosition protectionPosition = SourcePositionCodec.decode(reference.minimumSourcePosition());
            if (!appliedPosition.shardId().equals(protectionPosition.shardId())) {
                throw new IllegalArgumentException("protection source position belongs to another shard");
            }
        }
        protections = protectionSet.canonicalBytes();
        mutationId = Bytes.copy(mutationId);
        mutationHash = Bytes.copy(mutationHash);
        resourceIdentity = Bytes.copy(resourceIdentity);
        resourceIdentityHash = Bytes.copy(resourceIdentityHash);
        appliedSourcePosition = appliedPosition.canonicalBytes();
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
        return Bytes.concat(
                Bytes.u32be(2),
                mutationId,
                mutationHash,
                Bytes.u8(resourceKind.wireValue()),
                Bytes.lp32(resourceIdentity),
                resourceIdentityHash,
                Bytes.u64beBits(expectedResourceStateVersion),
                Bytes.u64beBits(appliedMutationSequence),
                Bytes.lp32(protections),
                Bytes.lp32(appliedSourcePosition));
    }

    public static ResourceRetireIntentRecord decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(Objects.requireNonNull(encoded, "encoded"));
        requireRemaining(input, 4 + HASH_LENGTH * 2 + 1 + 4 + HASH_LENGTH + 8 + 4 + 4);
        final int version = input.getInt();
        if (version != 1 && version != 2) {
            throw new IllegalArgumentException("unsupported resource retire intent version");
        }
        final byte[] mutationId = readFixed(input, HASH_LENGTH, "mutationId");
        final byte[] mutationHash = readFixed(input, HASH_LENGTH, "mutationHash");
        final ResourceKind kind = ResourceKind.fromWire(input.get() & 0xff);
        final byte[] identity = readLp32(input, "resourceIdentity");
        final byte[] identityHash = readFixed(input, HASH_LENGTH, "resourceIdentityHash");
        final long expectedVersion = readRawU64(input, "expectedResourceStateVersion");
        final long mutationSequence = version == 2 ? readU64(input, "appliedMutationSequence") : 0;
        final byte[] protections = readLp32(input, "protections");
        final byte[] source = readLp32(input, "appliedSourcePosition");
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing resource retire intent bytes");
        }
        final ResourceRetireIntentRecord result = new ResourceRetireIntentRecord(
                mutationId,
                mutationHash,
                kind,
                identity,
                identityHash,
                expectedVersion,
                mutationSequence,
                protections,
                source);
        if (version == 1 ? !Arrays.equals(encoded, result.encodeLegacy()) : !Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical resource retire intent");
        }
        return result;
    }

    private byte[] encodeLegacy() {
        return Bytes.concat(
                Bytes.u32be(1),
                mutationId,
                mutationHash,
                Bytes.u8(resourceKind.wireValue()),
                Bytes.lp32(resourceIdentity),
                resourceIdentityHash,
                Bytes.u64beBits(expectedResourceStateVersion),
                Bytes.lp32(protections),
                Bytes.lp32(appliedSourcePosition));
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
        return value;
    }

    private static long readRawU64(final ByteBuffer input, final String name) {
        requireRemaining(input, Long.BYTES);
        return input.getLong();
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
