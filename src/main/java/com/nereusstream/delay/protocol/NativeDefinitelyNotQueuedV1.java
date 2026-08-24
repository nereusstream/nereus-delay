package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Native definitive non-persistence branch. */
public final class NativeDefinitelyNotQueuedV1 {
    private final NativePreparedRefV1 nativePrepared;
    private final NonPersistenceProofV1 proof;
    private final StableErrorV1 error;

    public NativeDefinitelyNotQueuedV1(
            final NativePreparedRefV1 nativePrepared, final NonPersistenceProofV1 proof, final StableErrorV1 error) {
        this.nativePrepared = Objects.requireNonNull(nativePrepared, "nativePrepared");
        this.proof = Objects.requireNonNull(proof, "proof");
        if (!Arrays.equals(nativePrepared.submissionHash(), proof.preparedHash())) {
            throw new IllegalArgumentException("native proof does not bind submission hash");
        }
        final StableErrorV1 checked = Objects.requireNonNull(error, "error");
        if (checked.stage() != FailureStageV1.ENQUEUE
                || checked.command() != null
                || (checked.nativePrepared() != null
                        && !checked.nativePrepared().equals(nativePrepared))) {
            throw new IllegalArgumentException("native enqueue error does not bind the native prepared ref");
        }
        this.error = checked;
    }

    public NativePreparedRefV1 nativePrepared() {
        return nativePrepared;
    }

    public NonPersistenceProofV1 proof() {
        return proof;
    }

    public StableErrorV1 error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, nativePrepared.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, proof.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, error.canonicalBytes());
        });
    }

    public static NativeDefinitelyNotQueuedV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "NativeDefinitelyNotQueuedV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "NativeDefinitelyNotQueuedV1");
        final NativeDefinitelyNotQueuedV1 result = new NativeDefinitelyNotQueuedV1(
                NativePreparedRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                NonPersistenceProofV1.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                StableErrorV1.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "NativeDefinitelyNotQueuedV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof NativeDefinitelyNotQueuedV1 that
                && nativePrepared.equals(that.nativePrepared)
                && proof.equals(that.proof)
                && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nativePrepared, proof, error);
    }
}
