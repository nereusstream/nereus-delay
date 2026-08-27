package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Compact {@code id_cf/MESSAGE} value retained after a Message's full
 * generation/history projection has been reclaimed.
 *
 * <p>The key remains the normal MESSAGE locator and the CF-local value
 * envelope remains type {@code 1}; the payload version distinguishes this
 * branch from {@link MessageRecord} versions 1--4. Keeping the branch on the
 * same key makes the identity fence atomic with physical history cleanup and
 * lets query distinguish a retired identity from an unknown one.</p>
 */
public record RetiredMessageIdentityRecord(
        DelayMessageId messageId,
        long messageIdentityReuseUntilEpochMs,
        long retirementMutationSequence,
        byte[] appliedSourcePosition) {
    /** Current discriminator; generation-5 MessageRecord now owns payload version 5. */
    public static final int VERSION = 6;
    /** Reader-only discriminator emitted by the pre-generation-5 implementation. */
    private static final int LEGACY_VERSION = 5;

    public RetiredMessageIdentityRecord {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        if (messageIdentityReuseUntilEpochMs < 0
                || retirementMutationSequence == 0
                || appliedSourcePosition.length == 0) {
            throw new IllegalArgumentException("invalid retired Message identity record");
        }
        final SourcePosition decodedSourcePosition = SourcePositionCodec.decode(appliedSourcePosition);
        if (!messageId.routingId().shardId().equals(decodedSourcePosition.shardId())) {
            throw new IllegalArgumentException("retired Message source position belongs to another shard");
        }
        appliedSourcePosition = decodedSourcePosition.canonicalBytes();
    }

    @Override
    public byte[] appliedSourcePosition() {
        return Bytes.copy(appliedSourcePosition);
    }

    public byte[] encode() {
        return encode(VERSION);
    }

    private byte[] encode(final int version) {
        return Bytes.concat(
                Bytes.u32be(version),
                messageId.bytes(),
                Bytes.u64be(messageIdentityReuseUntilEpochMs),
                Bytes.u64beBits(retirementMutationSequence),
                Bytes.lp32(appliedSourcePosition));
    }

    /** Returns whether the CF-local MESSAGE payload selects this branch. */
    public static boolean isEncoded(final byte[] encoded) {
        if (encoded == null || encoded.length < Integer.BYTES) {
            return false;
        }
        final int version = ByteBuffer.wrap(encoded, 0, Integer.BYTES).getInt();
        if (version == VERSION) {
            return true;
        }
        // Version 5 collides with the current MessageRecord generation.  A
        // legacy tombstone is selected only after its complete canonical
        // shape has been decoded; an ordinary generation-5 MessageRecord is
        // therefore never misclassified by its first four bytes alone.
        if (version != LEGACY_VERSION) {
            return false;
        }
        try {
            decode(encoded);
            return true;
        } catch (IllegalArgumentException notRetiredIdentity) {
            return false;
        }
    }

    public static RetiredMessageIdentityRecord decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        requireRemaining(input, Integer.BYTES, "version");
        final int version = input.getInt();
        if (version != VERSION && version != LEGACY_VERSION) {
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
        final RetiredMessageIdentityRecord result =
                new RetiredMessageIdentityRecord(new DelayMessageId(messageId), reuseUntil, retirementSequence, source);
        if (!Arrays.equals(encoded, result.encode(version))) {
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
