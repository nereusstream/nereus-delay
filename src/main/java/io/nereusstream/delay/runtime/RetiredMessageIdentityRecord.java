package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Compact {@code id_cf/MESSAGE} value retained after a Message's full
 * generation/history projection has been reclaimed.
 *
 * <p>The key remains the normal MESSAGE locator and the CF-local value
 * envelope remains type {@code 1}; the payload version distinguishes this
 * branch from {@link MessageRecord} versions 1--4.  Keeping the branch on the
 * same key makes the identity fence atomic with physical history cleanup and
 * lets query distinguish a retired identity from an unknown one.</p>
 */
public record RetiredMessageIdentityRecord(
        DelayMessageId messageId,
        long messageIdentityReuseUntilEpochMs,
        long retirementMutationSequence,
        byte[] appliedSourcePosition) {
    public static final int VERSION = 5;

    public RetiredMessageIdentityRecord {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        if (messageIdentityReuseUntilEpochMs < 0 || retirementMutationSequence == 0
                || appliedSourcePosition.length == 0) {
            throw new IllegalArgumentException("invalid retired Message identity record");
        }
        appliedSourcePosition = Bytes.copy(appliedSourcePosition);
    }

    @Override
    public byte[] appliedSourcePosition() {
        return Bytes.copy(appliedSourcePosition);
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(VERSION), messageId.bytes(),
                Bytes.u64be(messageIdentityReuseUntilEpochMs),
                Bytes.u64beBits(retirementMutationSequence), Bytes.lp32(appliedSourcePosition));
    }

    /** Returns whether the CF-local MESSAGE payload selects this branch. */
    public static boolean isEncoded(final byte[] encoded) {
        return encoded != null && encoded.length >= Integer.BYTES
                && ByteBuffer.wrap(encoded, 0, Integer.BYTES).getInt() == VERSION;
    }

    public static RetiredMessageIdentityRecord decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        requireRemaining(input, Integer.BYTES, "version");
        if (input.getInt() != VERSION) {
            throw new IllegalArgumentException("unsupported retired Message identity version");
        }
        final byte[] messageId = readBytes(input, DelayMessageId.LENGTH, "messageId");
        final long reuseUntil = readLong(input, "messageIdentityReuseUntil");
        final long retirementSequence = readLong(input, "retirementMutationSequence");
        final long sourceLength = Integer.toUnsignedLong(readInt(input, "source position length"));
        if (sourceLength > input.remaining()) {
            throw new IllegalArgumentException("retired Message source position is truncated");
        }
        final byte[] source = readBytes(input, Math.toIntExact(sourceLength), "source position");
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing retired Message identity bytes");
        }
        final RetiredMessageIdentityRecord result = new RetiredMessageIdentityRecord(
                new DelayMessageId(messageId), reuseUntil, retirementSequence, source);
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical retired Message identity record");
        }
        return result;
    }

    private static int readInt(final ByteBuffer input, final String name) {
        requireRemaining(input, Integer.BYTES, name);
        return input.getInt();
    }

    private static long readLong(final ByteBuffer input, final String name) {
        requireRemaining(input, Long.BYTES, name);
        return input.getLong();
    }

    private static byte[] readBytes(final ByteBuffer input, final int length, final String name) {
        requireRemaining(input, length, name);
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static void requireRemaining(final ByteBuffer input, final int length, final String name) {
        if (length < 0 || input.remaining() < length) {
            throw new IllegalArgumentException("retired Message " + name + " is truncated");
        }
    }
}
