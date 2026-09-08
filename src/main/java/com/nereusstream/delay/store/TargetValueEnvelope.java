package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** NV framing for the explicitly selected Target Store reader, including shared fixed metadata. */
public final class TargetValueEnvelope {
    public static final int MAX_REGISTERED_VALUE_TYPE = 36;

    private TargetValueEnvelope() {}

    public static byte[] encode(final int type, final byte[] payload) {
        requireType(type);
        Objects.requireNonNull(payload, "payload");
        final int length = Math.addExact(payload.length, 12);
        final byte[] encoded = ByteBuffer.allocate(length)
                .putShort((short) 0x4e56)
                .put((byte) type)
                .put((byte) 1)
                .putInt(payload.length)
                .put(payload)
                .array();
        ByteBuffer.wrap(encoded).putInt(length - 4, (int) Bytes.crc32c(encoded, 0, length - 4));
        return encoded;
    }

    public static ValueEnvelope.Decoded decode(final byte[] encoded, final int expectedType) {
        requireType(expectedType);
        final var decoded = decodeAny(encoded);
        if (decoded.valueType() != expectedType) {
            throw new IllegalArgumentException("Target NV type mismatch");
        }
        return decoded;
    }

    public static ValueEnvelope.Decoded decodeAny(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length < 12) {
            throw new IllegalArgumentException("Target NV is truncated");
        }
        final var input = ByteBuffer.wrap(encoded);
        if (Short.toUnsignedInt(input.getShort()) != 0x4e56) {
            throw new IllegalArgumentException("Target NV magic mismatch");
        }
        final int type = Byte.toUnsignedInt(input.get());
        requireType(type);
        if (input.get() != 1
                || Integer.toUnsignedLong(input.getInt()) != encoded.length - 12L
                || Bytes.readU32be(encoded, encoded.length - 4) != Bytes.crc32c(encoded, 0, encoded.length - 4)) {
            throw new IllegalArgumentException("Target NV version/length/CRC mismatch");
        }
        return new ValueEnvelope.Decoded(type, Arrays.copyOfRange(encoded, 8, encoded.length - 4));
    }

    private static void requireType(final int type) {
        if (type <= 0 || type > MAX_REGISTERED_VALUE_TYPE) {
            throw new IllegalArgumentException("unregistered Target NV type");
        }
    }
}
