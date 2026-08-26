package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Native definitive non-persistence branch. */
public final class NativeDefinitelyNotQueued {
    private final NativePreparedRef nativePrepared;
    private final NonPersistenceProof proof;
    private final StableError error;

    public NativeDefinitelyNotQueued(
            final NativePreparedRef nativePrepared, final NonPersistenceProof proof, final StableError error) {
        this.nativePrepared = Objects.requireNonNull(nativePrepared, "nativePrepared");
        this.proof = Objects.requireNonNull(proof, "proof");
        if (!Arrays.equals(nativePrepared.submissionHash(), proof.preparedHash())) {
            throw new IllegalArgumentException("native proof does not bind submission hash");
        }
        final StableError checked = Objects.requireNonNull(error, "error");
        if (checked.stage() != FailureStage.ENQUEUE
                || checked.command() != null
                || (checked.nativePrepared() != null
                        && !checked.nativePrepared().equals(nativePrepared))) {
            throw new IllegalArgumentException("native enqueue error does not bind the native prepared ref");
        }
        this.error = checked;
    }

    public NativePreparedRef nativePrepared() {
        return nativePrepared;
    }

    public NonPersistenceProof proof() {
        return proof;
    }

    public StableError error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, nativePrepared.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, proof.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, error.canonicalBytes());
        });
    }

    public static NativeDefinitelyNotQueued decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "NativeDefinitelyNotQueued");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "NativeDefinitelyNotQueued");
        final NativeDefinitelyNotQueued result = new NativeDefinitelyNotQueued(
                NativePreparedRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                NonPersistenceProof.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                StableError.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "NativeDefinitelyNotQueued");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof NativeDefinitelyNotQueued that
                && nativePrepared.equals(that.nativePrepared)
                && proof.equals(that.proof)
                && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nativePrepared, proof, error);
    }
}
