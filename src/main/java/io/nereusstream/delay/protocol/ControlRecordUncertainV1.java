package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Registration response used when Oxia persistence is unknown. */
public final class ControlRecordUncertainV1 {
    public static final int HASH_LENGTH = 32;

    private final byte[] operationId;
    private final byte[] preparedDigest;
    private final StableErrorV1 error;

    public ControlRecordUncertainV1(final byte[] operationId, final byte[] preparedDigest,
                                    final StableErrorV1 error) {
        this.operationId = nonZero(operationId, "operationId");
        this.preparedDigest = fixed(preparedDigest, "preparedDigest");
        final StableErrorV1 result = Objects.requireNonNull(error, "error");
        if (result.stage() != FailureStageV1.CONTROL) {
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

    public StableErrorV1 error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, operationId);
            CanonicalProtobuf.bytes(output, 2, preparedDigest);
            CanonicalProtobuf.bytes(output, 3, error.canonicalBytes());
        });
    }

    public static ControlRecordUncertainV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "ControlRecordUncertainV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3}, "ControlRecordUncertainV1");
        final ControlRecordUncertainV1 result = new ControlRecordUncertainV1(
                QueryCodecSupport.fixed(fields.get(0), 1, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                StableErrorV1.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlRecordUncertainV1");
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
        return other instanceof ControlRecordUncertainV1 that && Arrays.equals(operationId, that.operationId)
                && Arrays.equals(preparedDigest, that.preparedDigest) && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(operationId), Arrays.hashCode(preparedDigest), error);
    }
}
