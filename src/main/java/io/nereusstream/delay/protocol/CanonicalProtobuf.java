package io.nereusstream.delay.protocol;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/** Small canonical-protobuf writer for the closed V1 envelope subset. */
public final class CanonicalProtobuf {
    private CanonicalProtobuf() {
    }

    public static byte[] message(final FieldWriter writer) {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        writer.writeTo(output);
        return output.toByteArray();
    }

    public static void uint32(final ByteArrayOutputStream output, final int field, final long value) {
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException("canonical uint32 is outside unsigned 32-bit range");
        }
        tag(output, field, 0);
        varint(output, value);
    }

    /** Encodes a complete protobuf unsigned-32 bit pattern carried by a Java int. */
    public static void uint32Bits(final ByteArrayOutputStream output, final int field, final int value) {
        tag(output, field, 0);
        varintBits(output, Integer.toUnsignedLong(value));
    }

    public static void uint64(final ByteArrayOutputStream output, final int field, final long value) {
        tag(output, field, 0);
        varint(output, value);
    }

    /** Encodes a complete protobuf unsigned-64 bit pattern carried by a Java long. */
    public static void uint64Bits(final ByteArrayOutputStream output, final int field, final long value) {
        tag(output, field, 0);
        varintBits(output, value);
    }

    public static void int64(final ByteArrayOutputStream output, final int field, final long value) {
        tag(output, field, 0);
        varint(output, value);
    }

    public static void bytes(final ByteArrayOutputStream output, final int field, final byte[] value) {
        tag(output, field, 2);
        varint(output, value.length);
        output.writeBytes(value);
    }

    private static void tag(final ByteArrayOutputStream output, final int field, final int wireType) {
        if (field <= 0 || field > 0x1fff) {
            throw new IllegalArgumentException("protobuf field out of range");
        }
        varint(output, ((long) field << 3) | wireType);
    }

    private static void varint(final ByteArrayOutputStream output, final long value) {
        if (value < 0) {
            throw new IllegalArgumentException("canonical V1 varints require non-negative values");
        }
        varintBits(output, value);
    }

    private static void varintBits(final ByteArrayOutputStream output, final long value) {
        long remaining = value;
        while ((remaining & ~0x7fL) != 0) {
            output.write((int) ((remaining & 0x7fL) | 0x80L));
            remaining >>>= 7;
        }
        output.write((int) remaining);
    }

    @FunctionalInterface
    public interface FieldWriter {
        void writeTo(ByteArrayOutputStream output);
    }

    /** Strict reader used to reject non-canonical envelope bytes. */
    public static final class Reader {
        private final byte[] bytes;
        private int offset;
        private int previousField;

        private final boolean allowRepeatedFields;

        public Reader(final byte[] bytes) {
            this(bytes, false);
        }

        /**
         * Creates a reader for a canonical message that may contain repeated fields.
         *
         * <p>Repeated fields must still be contiguous and strictly non-decreasing.  The
         * default constructor remains strict so the existing closed messages continue to
         * reject duplicate singular fields.</p>
         */
        public Reader(final byte[] bytes, final boolean allowRepeatedFields) {
            this.bytes = Bytes.copy(bytes);
            this.allowRepeatedFields = allowRepeatedFields;
        }

        public boolean hasRemaining() {
            return offset < bytes.length;
        }

        public Field next() {
            if (!hasRemaining()) {
                throw new IllegalArgumentException("missing protobuf field");
            }
            final long rawTag = readVarint();
            final long rawField = rawTag >>> 3;
            if (rawField <= 0 || rawField > 0x1fff) {
                throw new IllegalArgumentException("protobuf field out of range");
            }
            final int field = (int) rawField;
            final int wireType = (int) (rawTag & 7);
            if ((!allowRepeatedFields && field <= previousField)
                    || (allowRepeatedFields && field < previousField)) {
                throw new IllegalArgumentException("protobuf fields are not strictly increasing");
            }
            previousField = field;
            return new Field(field, wireType, readValue(wireType));
        }

        private byte[] readValue(final int wireType) {
            if (wireType == 0) {
                final int start = offset;
                readVarint();
                return Arrays.copyOfRange(bytes, start, offset);
            }
            if (wireType == 2) {
                final long length = readVarint();
                // Length prefixes are bounded local sizes, not arbitrary raw
                // uint64 fields.  A high-bit varint is negative in Java's
                // signed view; rejecting it before the int cast prevents a
                // malicious 2^63+ length from being narrowed to zero (or a
                // small positive value) and accepted as an empty payload.
                if (length < 0 || length > Integer.MAX_VALUE || offset > bytes.length - (int) length) {
                    throw new IllegalArgumentException("protobuf length outside payload");
                }
                final byte[] value = Arrays.copyOfRange(bytes, offset, offset + (int) length);
                offset += (int) length;
                return value;
            }
            throw new IllegalArgumentException("unsupported canonical protobuf wire type: " + wireType);
        }

        private long readVarint() {
            long value = 0;
            int shift = 0;
            while (shift < 64) {
                if (offset >= bytes.length) {
                    throw new IllegalArgumentException("truncated protobuf varint");
                }
                final int current = bytes[offset++] & 0xff;
                if (shift == 63 && current > 1) {
                    throw new IllegalArgumentException("protobuf varint overflow");
                }
                value |= (long) (current & 0x7f) << shift;
                if ((current & 0x80) == 0) {
                    if (shift > 0 && current == 0) {
                        throw new IllegalArgumentException("non-minimal protobuf varint");
                    }
                    return value;
                }
                shift += 7;
            }
            throw new IllegalArgumentException("protobuf varint overflow");
        }

        public record Field(int number, int wireType, byte[] rawValue) {
            public Field {
                rawValue = Bytes.copy(rawValue);
            }

            @Override
            public byte[] rawValue() {
                return Bytes.copy(rawValue);
            }

            public long unsignedValue() {
                if (wireType != 0) {
                    throw new IllegalStateException("field is not varint");
                }
                long result = 0;
                int shift = 0;
                for (byte currentByte : rawValue) {
                    final int current = currentByte & 0xff;
                    result |= (long) (current & 0x7f) << shift;
                    if ((current & 0x80) == 0) {
                        return result;
                    }
                    shift += 7;
                }
                throw new IllegalArgumentException("invalid varint");
            }
        }
    }
}
