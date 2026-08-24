package com.nereusstream.delay.protocol;

/** Small helpers for values whose wire type is an unsigned {@code uint32}. */
public final class UnsignedInt32 {
    private UnsignedInt32() {}

    /** Returns the complete unsigned value represented by the raw Java int bits. */
    public static long toLong(final int bits) {
        return Integer.toUnsignedLong(bits);
    }

    /** Compares two raw {@code uint32} bit patterns in wire order. */
    public static int compare(final int left, final int right) {
        return Integer.compareUnsigned(left, right);
    }

    /** Returns the checked successor, fencing before the all-ones value wraps. */
    public static int successor(final int value) {
        if (value == -1) {
            throw new ArithmeticException("uint32 successor exhausted");
        }
        return value + 1;
    }
}
