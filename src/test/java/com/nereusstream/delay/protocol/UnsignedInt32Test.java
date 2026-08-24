package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class UnsignedInt32Test {
    @Test
    void comparesAndProjectsRawHighBitValues() {
        final int highBit = (int) 0x8000_0000L;

        assertEquals(0x8000_0000L, UnsignedInt32.toLong(highBit));
        assertEquals(0, UnsignedInt32.compare(highBit, highBit));
        assertEquals(1, UnsignedInt32.compare(highBit, 1));
        assertEquals(-1, UnsignedInt32.compare(1, highBit));
    }

    @Test
    void successorFencesTheAllOnesPattern() {
        assertEquals((int) 0x8000_0000L, UnsignedInt32.successor(0x7fff_ffff));
        assertThrows(ArithmeticException.class, () -> UnsignedInt32.successor(-1));
    }
}
