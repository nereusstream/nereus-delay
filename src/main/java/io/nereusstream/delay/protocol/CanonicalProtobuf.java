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
        tag(output, field, 0);
        varint(output, value);
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

        public Reader(final byte[] bytes) {
            this.bytes = Bytes.copy(bytes);
        }

        public boolean hasRemaining() {
            return offset < bytes.length;
        }

        public Field next() {
            if (!hasRemaining()) {
                throw new IllegalArgumentException("missing protobuf field");
            }
            final long rawTag = readVarint();
            final int field = Math.toIntExact(rawTag >>> 3);
            final int wireType = (int) (rawTag & 7);
            if (field <= previousField || field == 0) {
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
                if (length > Integer.MAX_VALUE || offset > bytes.length - (int) length) {
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
