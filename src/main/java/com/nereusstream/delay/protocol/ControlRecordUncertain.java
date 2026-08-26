package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Registration response used when Oxia persistence is unknown. */
public final class ControlRecordUncertain {
    public static final int HASH_LENGTH = 32;

    private final byte[] operationId;
    private final byte[] preparedDigest;
    private final StableError error;

    public ControlRecordUncertain(final byte[] operationId, final byte[] preparedDigest, final StableError error) {
        this.operationId = nonZero(operationId, "operationId");
        this.preparedDigest = fixed(preparedDigest, "preparedDigest");
        final StableError result = Objects.requireNonNull(error, "error");
        if (result.stage() != FailureStage.CONTROL) {
            throw new IllegalArgumentException("Control registration error must use CONTROL stage");
        }
        this.error = result;
    }

    public byte[] operationId() {
        return Bytes.copy(operationId);
    }

    public byte[] preparedDigest() {
        return Bytes.copy(preparedDigest);
    }

    public StableError error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, operationId);
            CanonicalProtobuf.bytes(output, 2, preparedDigest);
            CanonicalProtobuf.bytes(output, 3, error.canonicalBytes());
        });
    }

    public static ControlRecordUncertain decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ControlRecordUncertain");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "ControlRecordUncertain");
        final ControlRecordUncertain result = new ControlRecordUncertain(
                QueryCodecSupport.fixed(fields.get(0), 1, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                StableError.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlRecordUncertain");
        return result;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        final byte[] result = fixed(value, name);
        for (byte current : result) {
            if (current != 0) {
                return result;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlRecordUncertain that
                && Arrays.equals(operationId, that.operationId)
                && Arrays.equals(preparedDigest, that.preparedDigest)
                && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(operationId), Arrays.hashCode(preparedDigest), error);
    }
}
