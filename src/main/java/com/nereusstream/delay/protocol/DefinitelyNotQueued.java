package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Managed definitive non-persistence branch. */
public final class DefinitelyNotQueued {
    private final CanonicalCommandQueuedReceipt.PreparedCommandRef command;
    private final NonPersistenceProof proof;
    private final StableError error;

    public DefinitelyNotQueued(
            final CanonicalCommandQueuedReceipt.PreparedCommandRef command,
            final NonPersistenceProof proof,
            final StableError error) {
        this.command = Objects.requireNonNull(command, "command");
        this.proof = Objects.requireNonNull(proof, "proof");
        if (!Arrays.equals(command.frameSha256(), proof.preparedHash())) {
            throw new IllegalArgumentException("managed proof does not bind PreparedCommand frame hash");
        }
        this.error = requireCommandError(error, command);
    }

    public CanonicalCommandQueuedReceipt.PreparedCommandRef command() {
        return command;
    }

    public NonPersistenceProof proof() {
        return proof;
    }

    public StableError error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, command.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, proof.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, error.canonicalBytes());
        });
    }

    public static DefinitelyNotQueued decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "DefinitelyNotQueued");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "DefinitelyNotQueued");
        final DefinitelyNotQueued result = new DefinitelyNotQueued(
                CanonicalCommandQueuedReceipt.PreparedCommandRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                NonPersistenceProof.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                StableError.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "DefinitelyNotQueued");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof DefinitelyNotQueued that
                && command.equals(that.command)
                && proof.equals(that.proof)
                && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(command, proof, error);
    }

    private static StableError requireCommandError(
            final StableError error, final CanonicalCommandQueuedReceipt.PreparedCommandRef command) {
        final StableError checked = Objects.requireNonNull(error, "error");
        if (checked.stage() != FailureStage.ENQUEUE
                || checked.nativePrepared() != null
                || (checked.command() != null && !checked.command().equals(command))) {
            throw new IllegalArgumentException("managed enqueue error does not bind the managed prepared command");
        }
        return checked;
    }
}
