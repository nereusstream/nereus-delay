package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Closed, zero-explicit capacity vector.
 *
 * <p>Amounts are represented as non-negative Java {@code long}s. This is the
 * checked local representation of the protocol's uint64 values; values above
 * {@link Long#MAX_VALUE} are rejected rather than silently wrapping.</p>
 */
public final class CapacityVector {
    public static final int ACCOUNTING_VERSION = 1;
    public static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-capacity-vector\0");

    private final long[] amounts;
    private final byte[] digest;

    public CapacityVector(final long[] amounts) {
        Objects.requireNonNull(amounts, "amounts");
        if (amounts.length != CapacityDimension.COUNT) {
            throw new IllegalArgumentException("capacity vector must contain all 66 dimensions");
        }
        this.amounts = Arrays.copyOf(amounts, amounts.length);
        for (long amount : this.amounts) {
            if (amount < 0) {
                throw new IllegalArgumentException("capacity amounts must be non-negative");
            }
        }
        this.digest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneAndTwo());
    }

    private CapacityVector(final long[] amounts, final byte[] digest) {
        this.amounts = Arrays.copyOf(amounts, amounts.length);
        this.digest = Bytes.copy(digest);
    }

    public static CapacityVector empty() {
        return new CapacityVector(new long[CapacityDimension.COUNT]);
    }

    public long amount(final CapacityDimension dimension) {
        return amounts[Objects.requireNonNull(dimension, "dimension").wireValue() - 1];
    }

    public long[] amounts() {
        return Arrays.copyOf(amounts, amounts.length);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public boolean isZero() {
        for (long amount : amounts) {
            if (amount != 0) {
                return false;
            }
        }
        return true;
    }

    public CapacityVector add(final CapacityVector other) {
        Objects.requireNonNull(other, "other");
        final long[] result = new long[CapacityDimension.COUNT];
        for (int index = 0; index < result.length; index++) {
            result[index] = Math.addExact(amounts[index], other.amounts[index]);
        }
        return new CapacityVector(result);
    }

    public CapacityVector subtract(final CapacityVector other) {
        Objects.requireNonNull(other, "other");
        final long[] result = new long[CapacityDimension.COUNT];
        for (int index = 0; index < result.length; index++) {
            if (amounts[index] < other.amounts[index]) {
                throw new IllegalStateException("capacity vector underflow at dimension " + (index + 1));
            }
            result[index] = amounts[index] - other.amounts[index];
        }
        return new CapacityVector(result);
    }

    /** Returns whether this vector can cover every amount in {@code required}. */
    public boolean covers(final CapacityVector required) {
        Objects.requireNonNull(required, "required");
        for (int index = 0; index < amounts.length; index++) {
            if (amounts[index] < required.amounts[index]) {
                return false;
            }
        }
        return true;
    }

    public byte[] canonicalBytes() {
        final byte[] fields = fieldsOneAndTwo();
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fields);
            CanonicalProtobuf.bytes(output, 3, digest);
        });
    }

    public static CapacityVector decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final java.util.ArrayList<CanonicalProtobuf.Reader.Field> fields = new java.util.ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() != CapacityDimension.COUNT + 2
                || fields.get(0).number() != 1
                || fields.get(fields.size() - 1).number() != 3) {
            throw new IllegalArgumentException("CapacityVector must contain version, 66 amounts and digest");
        }
        final CanonicalProtobuf.Reader.Field version = fields.get(0);
        if (version.wireType() != 0 || version.unsignedValue() != ACCOUNTING_VERSION) {
            throw new IllegalArgumentException("unsupported CapacityVector accounting version");
        }
        final long[] amounts = new long[CapacityDimension.COUNT];
        for (int index = 0; index < CapacityDimension.COUNT; index++) {
            final CanonicalProtobuf.Reader.Field amountField = fields.get(index + 1);
            if (amountField.number() != 2 || amountField.wireType() != 2) {
                throw new IllegalArgumentException("CapacityVector amount fields are not repeated field 2");
            }
            final List<CanonicalProtobuf.Reader.Field> amount =
                    QueryCodecSupport.read(amountField.rawValue(), "CapacityAmount");
            QueryCodecSupport.requireNumbers(amount, new int[] {1, 2}, "CapacityAmount");
            final long dimension = QueryCodecSupport.uint(amount.get(0), 1);
            if (dimension != index + 1) {
                throw new IllegalArgumentException("CapacityVector dimensions must be complete and ordered");
            }
            final long value = QueryCodecSupport.uint(amount.get(1), 2);
            if (value < 0) {
                throw new IllegalArgumentException("CapacityAmount uint64 exceeds local range");
            }
            amounts[index] = value;
        }
        final byte[] digest = QueryCodecSupport.fixed(fields.get(fields.size() - 1), 3, HASH_LENGTH);
        final CapacityVector result = new CapacityVector(amounts);
        if (!Arrays.equals(digest, result.digest)) {
            throw new IllegalArgumentException("CapacityVector digest mismatch");
        }
        final CapacityVector canonical = new CapacityVector(amounts, digest);
        QueryCodecSupport.requireCanonical(encoded, canonical.canonicalBytes(), "CapacityVector");
        return canonical;
    }

    private byte[] fieldsOneAndTwo() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, ACCOUNTING_VERSION);
            for (CapacityDimension dimension : CapacityDimension.values()) {
                final long amount = amounts[dimension.wireValue() - 1];
                CanonicalProtobuf.bytes(output, 2, CanonicalProtobuf.message(value -> {
                    CanonicalProtobuf.uint32(value, 1, dimension.wireValue());
                    CanonicalProtobuf.uint64(value, 2, amount);
                }));
            }
        });
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CapacityVector that
                && Arrays.equals(amounts, that.amounts)
                && Arrays.equals(digest, that.digest);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(amounts) + Arrays.hashCode(digest);
    }
}
