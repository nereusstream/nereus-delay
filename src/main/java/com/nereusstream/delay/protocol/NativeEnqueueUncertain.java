package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Native possible-persistence branch; retry reuses the exact prepared bytes. */
public final class NativeEnqueueUncertain {
    private final NativePreparedRef nativePrepared;
    private final byte[] physicalEnqueueAttemptId;
    private final StableError error;

    public NativeEnqueueUncertain(
            final NativePreparedRef nativePrepared, final byte[] physicalEnqueueAttemptId, final StableError error) {
        this.nativePrepared = Objects.requireNonNull(nativePrepared, "nativePrepared");
        this.physicalEnqueueAttemptId = nonZero(physicalEnqueueAttemptId);
        final StableError checked = Objects.requireNonNull(error, "error");
        if (checked.stage() != FailureStage.ENQUEUE
                || checked.command() != null
                || (checked.nativePrepared() != null
                        && !checked.nativePrepared().equals(nativePrepared))
                || checked.retryability() != Retryability.RETRY_EXACT_BYTES) {
            throw new IllegalArgumentException("native uncertain enqueue error does not bind exact-byte retry");
        }
        this.error = checked;
    }

    public NativePreparedRef nativePrepared() {
        return nativePrepared;
    }

    public byte[] physicalEnqueueAttemptId() {
        return Bytes.copy(physicalEnqueueAttemptId);
    }

    public StableError error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, nativePrepared.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, physicalEnqueueAttemptId);
            CanonicalProtobuf.bytes(output, 3, error.canonicalBytes());
        });
    }

    public static NativeEnqueueUncertain decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "NativeEnqueueUncertain");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "NativeEnqueueUncertain");
        final NativeEnqueueUncertain result = new NativeEnqueueUncertain(
                NativePreparedRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, NonPersistenceProof.ATTEMPT_ID_LENGTH),
                StableError.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "NativeEnqueueUncertain");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof NativeEnqueueUncertain that
                && nativePrepared.equals(that.nativePrepared)
                && Arrays.equals(physicalEnqueueAttemptId, that.physicalEnqueueAttemptId)
                && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nativePrepared, Arrays.hashCode(physicalEnqueueAttemptId), error);
    }

    private static byte[] nonZero(final byte[] value) {
        Bytes.requireLength(value, NonPersistenceProof.ATTEMPT_ID_LENGTH, "physicalEnqueueAttemptId");
        for (byte item : value) {
            if (item != 0) {
                return Bytes.copy(value);
            }
        }
        throw new IllegalArgumentException("physicalEnqueueAttemptId must be non-zero");
    }
}
