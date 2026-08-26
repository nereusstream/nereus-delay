package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** NV value envelope: type, version, length, canonical payload and CRC32C. */
public final class ValueEnvelope {
    private static final int PREFIX_LENGTH = 8;
    private static final int MAGIC = 0x4e56;
    /** Highest payload-schema discriminator registered by the current design. */
    public static final int MAX_REGISTERED_VALUE_TYPE = 11;

    private ValueEnvelope() {}

    public static byte[] encode(final int valueType, final byte[] payload) {
        if (!isRegisteredType(valueType) || payload.length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("invalid value envelope");
        }
        final ByteBuffer prefix = ByteBuffer.allocate(PREFIX_LENGTH);
        prefix.putShort((short) MAGIC).put((byte) valueType).put((byte) 1).putInt(payload.length);
        return Bytes.concat(prefix.array(), payload, Bytes.crc32cbe(Bytes.concat(prefix.array(), payload)));
    }

    public static Decoded decode(final byte[] encoded, final int expectedType) {
        if (!isRegisteredType(expectedType)) {
            throw new IllegalArgumentException("unknown value envelope type");
        }
        final Decoded decoded = decodeAny(encoded);
        if (decoded.valueType() != expectedType) {
            throw new IllegalArgumentException("value envelope type/version mismatch");
        }
        return decoded;
    }

    /**
     * Decodes and validates the envelope without assuming its CF-local
     * payload type. Callers that share one key namespace across multiple
     * value branches can inspect the registered discriminator and then apply
     * the branch-specific codec. Unknown types still fail closed.
     */
    public static Decoded decodeAny(final byte[] encoded) {
        if (encoded.length < PREFIX_LENGTH + 4) {
            throw new IllegalArgumentException("value envelope is truncated");
        }
        final ByteBuffer prefix = ByteBuffer.wrap(encoded, 0, PREFIX_LENGTH);
        if (Short.toUnsignedInt(prefix.getShort()) != MAGIC) {
            throw new IllegalArgumentException("value envelope type/version mismatch");
        }
        final int valueType = prefix.get() & 0xff;
        if (!isRegisteredType(valueType) || (prefix.get() & 0xff) != 1) {
            throw new IllegalArgumentException("value envelope type/version mismatch");
        }
        final long payloadLength = Integer.toUnsignedLong(prefix.getInt());
        if (payloadLength != encoded.length - PREFIX_LENGTH - 4) {
            throw new IllegalArgumentException("value envelope length mismatch");
        }
        final long expectedCrc = Bytes.crc32c(encoded, 0, encoded.length - 4);
        final long actualCrc = Bytes.readU32be(encoded, encoded.length - 4);
        if (expectedCrc != actualCrc) {
            throw new IllegalArgumentException("value envelope CRC mismatch");
        }
        return new Decoded(valueType, Arrays.copyOfRange(encoded, PREFIX_LENGTH, encoded.length - 4));
    }

    private static boolean isRegisteredType(final int valueType) {
        return valueType > 0 && valueType <= MAX_REGISTERED_VALUE_TYPE;
    }

    public record Decoded(int valueType, byte[] payload) {
        public Decoded {
            payload = Bytes.copy(payload);
        }

        @Override
        public byte[] payload() {
            return Bytes.copy(payload);
        }
    }
}
