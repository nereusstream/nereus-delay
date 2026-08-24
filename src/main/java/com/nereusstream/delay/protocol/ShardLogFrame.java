package com.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Fixed NDL1 frame around one canonical Shard Log envelope. */
public final class ShardLogFrame {
    public static final int MAGIC = 0x4e444c31;
    public static final int HEADER_LENGTH = 12;
    public static final int TRAILER_LENGTH = 4;
    public static final int FRAME_VERSION = 1;
    public static final int CLIENT_COMMAND_KIND = 1;
    public static final int SYSTEM_MUTATION_KIND = 2;

    private ShardLogFrame() {}

    public static byte[] encodeClientCommand(final byte[] canonicalEnvelope) {
        return encode(CLIENT_COMMAND_KIND, canonicalEnvelope);
    }

    public static byte[] encode(final int recordKind, final byte[] canonicalEnvelope) {
        if (recordKind != CLIENT_COMMAND_KIND && recordKind != SYSTEM_MUTATION_KIND) {
            throw new IllegalArgumentException("invalid Shard Log record kind");
        }
        if (canonicalEnvelope.length > Integer.MAX_VALUE - HEADER_LENGTH - TRAILER_LENGTH) {
            throw new IllegalArgumentException("Shard Log envelope is too large");
        }
        final ByteBuffer header = ByteBuffer.allocate(HEADER_LENGTH);
        header.putInt(MAGIC)
                .put((byte) FRAME_VERSION)
                .put((byte) recordKind)
                .putShort((short) 0)
                .putInt(canonicalEnvelope.length);
        return Bytes.concat(
                header.array(), canonicalEnvelope, Bytes.crc32cbe(Bytes.concat(header.array(), canonicalEnvelope)));
    }

    public static Decoded decode(final byte[] frame) {
        if (frame.length < HEADER_LENGTH + TRAILER_LENGTH) {
            throw new IllegalArgumentException("Shard Log frame is truncated");
        }
        final ByteBuffer header = ByteBuffer.wrap(frame, 0, HEADER_LENGTH);
        if (header.getInt() != MAGIC || (header.get() & 0xff) != FRAME_VERSION) {
            throw new IllegalArgumentException("invalid Shard Log magic/version");
        }
        final int recordKind = header.get() & 0xff;
        if (recordKind != CLIENT_COMMAND_KIND && recordKind != SYSTEM_MUTATION_KIND) {
            throw new IllegalArgumentException("invalid Shard Log record kind");
        }
        if (header.getShort() != 0) {
            throw new IllegalArgumentException("unknown Shard Log flags");
        }
        final long payloadLength = Integer.toUnsignedLong(header.getInt());
        if (payloadLength != frame.length - HEADER_LENGTH - TRAILER_LENGTH) {
            throw new IllegalArgumentException("Shard Log payload length mismatch");
        }
        final byte[] payload = Arrays.copyOfRange(frame, HEADER_LENGTH, frame.length - TRAILER_LENGTH);
        final long expected = Bytes.crc32c(frame, 0, frame.length - TRAILER_LENGTH);
        final long actual = Bytes.readU32be(frame, frame.length - TRAILER_LENGTH);
        if (expected != actual) {
            throw new IllegalArgumentException("Shard Log CRC mismatch");
        }
        return new Decoded(recordKind, payload);
    }

    public record Decoded(int recordKind, byte[] canonicalEnvelope) {
        public Decoded {
            canonicalEnvelope = Bytes.copy(canonicalEnvelope);
        }

        @Override
        public byte[] canonicalEnvelope() {
            return Bytes.copy(canonicalEnvelope);
        }
    }
}
