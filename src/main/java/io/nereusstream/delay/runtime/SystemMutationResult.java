package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Durable result and dedupe projection for one source-ordered System Mutation. */
public record SystemMutationResult(
        byte[] mutationId,
        byte[] mutationHash,
        SystemMutationType mutationType,
        long retryUntilEpochMs,
        byte[] authorIdentity,
        ApplyStatus applyStatus,
        StableCode stableCode,
        byte[] appliedSourcePosition) {
    public static final int VALUE_TYPE = 4;
    private static final int HASH_LENGTH = SystemMutation.HASH_LENGTH;

    public SystemMutationResult {
        Bytes.requireLength(mutationId, HASH_LENGTH, "mutationId");
        Bytes.requireLength(mutationHash, HASH_LENGTH, "mutationHash");
        Objects.requireNonNull(mutationType, "mutationType");
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntil must be non-negative");
        }
        if (authorIdentity == null || authorIdentity.length == 0) {
            throw new IllegalArgumentException("authorIdentity must not be empty");
        }
        Objects.requireNonNull(applyStatus, "applyStatus");
        Objects.requireNonNull(stableCode, "stableCode");
        Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        mutationId = Bytes.copy(mutationId);
        mutationHash = Bytes.copy(mutationHash);
        authorIdentity = Bytes.copy(authorIdentity);
        // Keep System Mutation results subject to the same canonical source
        // anchor fence as Command results before they enter a durable value.
        appliedSourcePosition = SourcePositionCodec.decode(appliedSourcePosition).canonicalBytes();
    }

    public static SystemMutationResult from(final SystemMutation mutation, final ApplyStatus status,
                                             final StableCode code, final byte[] sourcePosition) {
        Objects.requireNonNull(mutation, "mutation");
        return new SystemMutationResult(mutation.systemMutationId(), mutation.mutationHash(), mutation.type(),
                mutation.retryUntilEpochMs(), mutation.authorIdentity(), status, code, sourcePosition);
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
    public byte[] authorIdentity() {
        return Bytes.copy(authorIdentity);
    }

    @Override
    public byte[] appliedSourcePosition() {
        return Bytes.copy(appliedSourcePosition);
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(1), mutationId, mutationHash, Bytes.u32be(mutationType.wireValue()),
                Bytes.u64be(retryUntilEpochMs), new byte[]{(byte) applyStatus.wireValue()},
                Bytes.u32be(stableCode.wireValue()), Bytes.lp32(authorIdentity), Bytes.lp32(appliedSourcePosition));
    }

    public static SystemMutationResult decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(Objects.requireNonNull(encoded, "encoded"));
        requireRemaining(input, 4 + HASH_LENGTH * 2 + 4 + 8 + 1 + 4 + 4 + 4);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported system mutation result version");
        }
        final byte[] id = readFixed(input, HASH_LENGTH, "mutationId");
        final byte[] hash = readFixed(input, HASH_LENGTH, "mutationHash");
        final long type = readU32(input, "mutationType");
        final long retryUntil = readU64(input, "retryUntil");
        final int status = input.get() & 0xff;
        final ApplyStatus applyStatus = switch (status) {
            case 1 -> ApplyStatus.APPLIED;
            case 2 -> ApplyStatus.REJECTED;
            default -> throw new IllegalArgumentException("unknown system mutation apply status");
        };
        final long stableCodeValue = readU32(input, "stableCode");
        if (stableCodeValue > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("stableCode exceeds local runtime range");
        }
        final StableCode code = StableCode.fromWire((int) stableCodeValue);
        final byte[] author = readLp32(input, "authorIdentity");
        final byte[] source = readLp32(input, "appliedSourcePosition");
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing system mutation result bytes");
        }
        final SystemMutationResult result = new SystemMutationResult(id, hash,
                SystemMutationType.fromWire(type), retryUntil, author, applyStatus, code, source);
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical system mutation result");
        }
        return result;
    }

    private static long readU32(final ByteBuffer input, final String name) {
        requireRemaining(input, 4);
        return Integer.toUnsignedLong(input.getInt());
    }

    private static long readU64(final ByteBuffer input, final String name) {
        requireRemaining(input, 8);
        final long value = input.getLong();
        if (value < 0) {
            throw new IllegalArgumentException(name + " exceeds signed range");
        }
        return value;
    }

    private static byte[] readFixed(final ByteBuffer input, final int length, final String name) {
        requireRemaining(input, length);
        final byte[] value = new byte[length];
        input.get(value);
        return value;
    }

    private static byte[] readLp32(final ByteBuffer input, final String name) {
        final long length = readU32(input, name + " length");
        if (length > input.remaining()) {
            throw new IllegalArgumentException(name + " length outside result");
        }
        return readFixed(input, Math.toIntExact(length), name);
    }

    private static void requireRemaining(final ByteBuffer input, final int length) {
        if (length < 0 || input.remaining() < length) {
            throw new IllegalArgumentException("system mutation result is truncated");
        }
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof SystemMutationResult that)) {
            return false;
        }
        return retryUntilEpochMs == that.retryUntilEpochMs && mutationType == that.mutationType
                && applyStatus == that.applyStatus && stableCode == that.stableCode
                && Arrays.equals(mutationId, that.mutationId) && Arrays.equals(mutationHash, that.mutationHash)
                && Arrays.equals(authorIdentity, that.authorIdentity)
                && Arrays.equals(appliedSourcePosition, that.appliedSourcePosition);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(mutationType, retryUntilEpochMs, applyStatus, stableCode);
        result = 31 * result + Arrays.hashCode(mutationId);
        result = 31 * result + Arrays.hashCode(mutationHash);
        result = 31 * result + Arrays.hashCode(authorIdentity);
        result = 31 * result + Arrays.hashCode(appliedSourcePosition);
        return result;
    }
}
