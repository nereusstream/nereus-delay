package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Native possible-persistence branch; retry reuses the exact prepared bytes. */
public final class NativeEnqueueUncertainV1 {
    private final NativePreparedRefV1 nativePrepared;
    private final byte[] physicalEnqueueAttemptId;
    private final StableErrorV1 error;

    public NativeEnqueueUncertainV1(final NativePreparedRefV1 nativePrepared,
                                    final byte[] physicalEnqueueAttemptId, final StableErrorV1 error) {
        this.nativePrepared = Objects.requireNonNull(nativePrepared, "nativePrepared");
        this.physicalEnqueueAttemptId = nonZero(physicalEnqueueAttemptId);
        final StableErrorV1 checked = Objects.requireNonNull(error, "error");
        if (checked.command() != null
                || (checked.nativePrepared() != null && !checked.nativePrepared().equals(nativePrepared))
                || checked.retryability() != RetryabilityV1.RETRY_EXACT_BYTES) {
            throw new IllegalArgumentException("native uncertain error does not bind exact-byte retry");
        }
        this.error = checked;
    }

    public NativePreparedRefV1 nativePrepared() {
        return nativePrepared;
    }

    public byte[] physicalEnqueueAttemptId() {
        return Bytes.copy(physicalEnqueueAttemptId);
    }

    public StableErrorV1 error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, nativePrepared.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, physicalEnqueueAttemptId);
            CanonicalProtobuf.bytes(output, 3, error.canonicalBytes());
        });
    }

    public static NativeEnqueueUncertainV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "NativeEnqueueUncertainV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3}, "NativeEnqueueUncertainV1");
        final NativeEnqueueUncertainV1 result = new NativeEnqueueUncertainV1(
                NativePreparedRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, NonPersistenceProofV1.ATTEMPT_ID_LENGTH),
                StableErrorV1.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "NativeEnqueueUncertainV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof NativeEnqueueUncertainV1 that && nativePrepared.equals(that.nativePrepared)
                && Arrays.equals(physicalEnqueueAttemptId, that.physicalEnqueueAttemptId)
                && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nativePrepared, Arrays.hashCode(physicalEnqueueAttemptId), error);
    }

    private static byte[] nonZero(final byte[] value) {
        Bytes.requireLength(value, NonPersistenceProofV1.ATTEMPT_ID_LENGTH, "physicalEnqueueAttemptId");
        for (byte item : value) {
            if (item != 0) {
                return Bytes.copy(value);
            }
        }
        throw new IllegalArgumentException("physicalEnqueueAttemptId must be non-zero");
    }
}
