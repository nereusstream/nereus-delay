package io.nereusstream.delay.protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Fixed-width, checked byte operations used by the V1 codecs. */
public final class Bytes {
    private static final HexFormat HEX = HexFormat.of();

    private Bytes() {
    }

    public static byte[] utf8(final String value) {
        Objects.requireNonNull(value, "value");
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] copy(final byte[] value) {
        return Arrays.copyOf(Objects.requireNonNull(value, "value"), value.length);
    }

    public static byte[] u8(final int value) {
        if (value < 0 || value > 0xff) {
            throw new IllegalArgumentException("u8 out of range: " + value);
        }
        return new byte[]{(byte) value};
    }

    public static byte[] u16be(final int value) {
        if (value < 0 || value > 0xffff) {
            throw new IllegalArgumentException("u16 out of range: " + value);
        }
        return ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) value).array();
    }

    public static byte[] u32be(final long value) {
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException("u32 out of range: " + value);
        }
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt((int) value).array();
    }

    public static byte[] u64be(final long value) {
        if (value < 0) {
            throw new IllegalArgumentException("u64 out of range: " + value);
        }
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
    }

    public static byte[] i64be(final long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
    }

    public static long readU32be(final byte[] bytes, final int offset) {
        requireRange(bytes, offset, 4);
        return Integer.toUnsignedLong(ByteBuffer.wrap(bytes, offset, 4).order(ByteOrder.BIG_ENDIAN).getInt());
    }

    public static long readU64be(final byte[] bytes, final int offset) {
        requireRange(bytes, offset, 8);
        return ByteBuffer.wrap(bytes, offset, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    }

    public static long readI64be(final byte[] bytes, final int offset) {
        return readU64be(bytes, offset);
    }

    public static byte[] lp32(final byte[] value) {
        Objects.requireNonNull(value, "value");
        return concat(u32be(value.length), value);
    }

    public static byte[] concat(final byte[]... values) {
        Objects.requireNonNull(values, "values");
        int length = 0;
        for (byte[] value : values) {
            if (value == null) {
                throw new NullPointerException("null byte array");
            }
            length = Math.addExact(length, value.length);
        }
        final ByteArrayOutputStream output = new ByteArrayOutputStream(length);
        try {
            for (byte[] value : values) {
                output.write(value);
            }
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
        return output.toByteArray();
    }

    public static byte[] sha256(final byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    public static byte[] sha256(final byte[]... values) {
        return sha256(concat(values));
    }

    public static long crc32c(final byte[] value) {
        final CRC32C crc = new CRC32C();
        crc.update(value, 0, value.length);
        return crc.getValue();
    }

    public static long crc32c(final byte[] value, final int offset, final int length) {
        requireRange(value, offset, length);
        final CRC32C crc = new CRC32C();
        crc.update(value, offset, length);
        return crc.getValue();
    }

    public static byte[] crc32cbe(final byte[] value) {
        return u32be(crc32c(value));
    }

    public static String hex(final byte[] value) {
        return HEX.formatHex(value);
    }

    public static byte[] hexToBytes(final String value) {
        return HEX.parseHex(value);
    }

    public static boolean constantTimeEquals(final byte[] left, final byte[] right) {
        return MessageDigest.isEqual(left, right);
    }

    public static void requireLength(final byte[] value, final int expected, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length != expected) {
            throw new IllegalArgumentException(name + " must be " + expected + " bytes, got " + value.length);
        }
    }

    public static void requireRange(final byte[] value, final int offset, final int length) {
        if (offset < 0 || length < 0 || offset > value.length - length) {
            throw new IllegalArgumentException("byte range outside value");
        }
    }
}
