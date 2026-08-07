package io.nereusstream.delay.protocol;

import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CanonicalProtobufTest {
    @Test
    void uint32AcceptsOnlyUnsigned32BitValues() {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();

        assertDoesNotThrow(() -> CanonicalProtobuf.uint32(output, 1, 0));
        assertDoesNotThrow(() -> CanonicalProtobuf.uint32(output, 2, 0xffff_ffffL));
        assertThrows(IllegalArgumentException.class, () -> CanonicalProtobuf.uint32(output, 3, -1));
        assertThrows(IllegalArgumentException.class,
                () -> CanonicalProtobuf.uint32(output, 4, 0x1_0000_0000L));
    }

    @Test
    void uint64BitsRoundTripsTheHighBitPattern() {
        final java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        CanonicalProtobuf.uint64Bits(output, 1, Long.MIN_VALUE);
        final CanonicalProtobuf.Reader.Field field = new CanonicalProtobuf.Reader(output.toByteArray()).next();
        assertEquals(Long.MIN_VALUE, field.unsignedValue());
    }

    @Test
    void readerRejectsFieldNumbersOutsideRegistryRange() {
        assertThrows(IllegalArgumentException.class,
                () -> new CanonicalProtobuf.Reader(new byte[]{(byte) 0x80, (byte) 0x80, 0x04, 0}).next());
    }
}
