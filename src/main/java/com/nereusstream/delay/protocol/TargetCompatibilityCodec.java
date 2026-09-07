package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Shared closed-codec bounds for Target execution and control contracts. */
final class TargetCompatibilityCodec {
    private TargetCompatibilityCodec() {}

    static byte[] assigned(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        if (Arrays.equals(value, new byte[length])) {
            throw new IllegalArgumentException(name + " is unassigned");
        }
        return Bytes.copy(value);
    }

    static List<CanonicalProtobuf.Reader.Field> read(
            final byte[] encoded, final int maxBytes, final int maxFields, final boolean repeated, final String name) {
        if (encoded == null || encoded.length > maxBytes) {
            throw new IllegalArgumentException(name + " exceeds its byte bound");
        }
        final var reader = new CanonicalProtobuf.Reader(encoded, repeated);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            if (fields.size() == maxFields) {
                throw new IllegalArgumentException(name + " exceeds its field bound");
            }
            fields.add(reader.next());
        }
        return fields;
    }
}
