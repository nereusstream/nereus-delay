package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Closed, zero-explicit V1 capacity vector.
 *
 * <p>Amounts are represented as non-negative Java {@code long}s. This is the
 * checked local representation of the protocol's uint64 values; values above
 * {@link Long#MAX_VALUE} are rejected rather than silently wrapping.</p>
 */
public final class CapacityVectorV1 {
    public static final int ACCOUNTING_VERSION = 1;
    public static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-capacity-vector-v1\0");

    private final long[] amounts;
    private final byte[] digest;

    public CapacityVectorV1(final long[] amounts) {
        Objects.requireNonNull(amounts, "amounts");
        if (amounts.length != CapacityDimensionV1.COUNT) {
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

    private CapacityVectorV1(final long[] amounts, final byte[] digest) {
        this.amounts = Arrays.copyOf(amounts, amounts.length);
        this.digest = Bytes.copy(digest);
    }

    public static CapacityVectorV1 empty() {
        return new CapacityVectorV1(new long[CapacityDimensionV1.COUNT]);
    }

    public long amount(final CapacityDimensionV1 dimension) {
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

    public CapacityVectorV1 add(final CapacityVectorV1 other) {
        Objects.requireNonNull(other, "other");
        final long[] result = new long[CapacityDimensionV1.COUNT];
        for (int index = 0; index < result.length; index++) {
            result[index] = Math.addExact(amounts[index], other.amounts[index]);
        }
        return new CapacityVectorV1(result);
    }

    public CapacityVectorV1 subtract(final CapacityVectorV1 other) {
        Objects.requireNonNull(other, "other");
        final long[] result = new long[CapacityDimensionV1.COUNT];
        for (int index = 0; index < result.length; index++) {
            if (amounts[index] < other.amounts[index]) {
                throw new IllegalStateException("capacity vector underflow at dimension " + (index + 1));
            }
            result[index] = amounts[index] - other.amounts[index];
        }
        return new CapacityVectorV1(result);
    }

    /** Returns whether this vector can cover every amount in {@code required}. */
    public boolean covers(final CapacityVectorV1 required) {
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

    public static CapacityVectorV1 decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final java.util.ArrayList<CanonicalProtobuf.Reader.Field> fields = new java.util.ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() != CapacityDimensionV1.COUNT + 2
                || fields.get(0).number() != 1
                || fields.get(fields.size() - 1).number() != 3) {
            throw new IllegalArgumentException("CapacityVectorV1 must contain version, 66 amounts and digest");
        }
        final CanonicalProtobuf.Reader.Field version = fields.get(0);
        if (version.wireType() != 0 || version.unsignedValue() != ACCOUNTING_VERSION) {
            throw new IllegalArgumentException("unsupported CapacityVectorV1 accounting version");
        }
        final long[] amounts = new long[CapacityDimensionV1.COUNT];
        for (int index = 0; index < CapacityDimensionV1.COUNT; index++) {
            final CanonicalProtobuf.Reader.Field amountField = fields.get(index + 1);
            if (amountField.number() != 2 || amountField.wireType() != 2) {
                throw new IllegalArgumentException("CapacityVectorV1 amount fields are not repeated field 2");
            }
            final List<CanonicalProtobuf.Reader.Field> amount =
                    QueryCodecSupport.read(amountField.rawValue(), "CapacityAmountV1");
            QueryCodecSupport.requireNumbers(amount, new int[] {1, 2}, "CapacityAmountV1");
            final long dimension = QueryCodecSupport.uint(amount.get(0), 1);
            if (dimension != index + 1) {
                throw new IllegalArgumentException("CapacityVectorV1 dimensions must be complete and ordered");
            }
            final long value = QueryCodecSupport.uint(amount.get(1), 2);
            if (value < 0) {
                throw new IllegalArgumentException("CapacityAmountV1 uint64 exceeds local range");
            }
            amounts[index] = value;
        }
        final byte[] digest = QueryCodecSupport.fixed(fields.get(fields.size() - 1), 3, HASH_LENGTH);
        final CapacityVectorV1 result = new CapacityVectorV1(amounts);
        if (!Arrays.equals(digest, result.digest)) {
            throw new IllegalArgumentException("CapacityVectorV1 digest mismatch");
        }
        final CapacityVectorV1 canonical = new CapacityVectorV1(amounts, digest);
        QueryCodecSupport.requireCanonical(encoded, canonical.canonicalBytes(), "CapacityVectorV1");
        return canonical;
    }

    private byte[] fieldsOneAndTwo() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, ACCOUNTING_VERSION);
            for (CapacityDimensionV1 dimension : CapacityDimensionV1.values()) {
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
        return other instanceof CapacityVectorV1 that
                && Arrays.equals(amounts, that.amounts)
                && Arrays.equals(digest, that.digest);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(amounts) + Arrays.hashCode(digest);
    }
}
