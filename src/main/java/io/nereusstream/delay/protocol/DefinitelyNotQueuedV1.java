package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Managed definitive non-persistence branch. */
public final class DefinitelyNotQueuedV1 {
    private final CommandQueuedReceiptV1.PreparedCommandRef command;
    private final NonPersistenceProofV1 proof;
    private final StableErrorV1 error;

    public DefinitelyNotQueuedV1(final CommandQueuedReceiptV1.PreparedCommandRef command,
                                 final NonPersistenceProofV1 proof, final StableErrorV1 error) {
        this.command = Objects.requireNonNull(command, "command");
        this.proof = Objects.requireNonNull(proof, "proof");
        if (!Arrays.equals(command.frameSha256(), proof.preparedHash())) {
            throw new IllegalArgumentException("managed proof does not bind PreparedCommand frame hash");
        }
        this.error = requireCommandError(error, command);
    }

    public CommandQueuedReceiptV1.PreparedCommandRef command() {
        return command;
    }

    public NonPersistenceProofV1 proof() {
        return proof;
    }

    public StableErrorV1 error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, command.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, proof.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, error.canonicalBytes());
        });
    }

    public static DefinitelyNotQueuedV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "DefinitelyNotQueuedV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3}, "DefinitelyNotQueuedV1");
        final DefinitelyNotQueuedV1 result = new DefinitelyNotQueuedV1(
                CommandQueuedReceiptV1.PreparedCommandRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                NonPersistenceProofV1.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                StableErrorV1.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "DefinitelyNotQueuedV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof DefinitelyNotQueuedV1 that && command.equals(that.command)
                && proof.equals(that.proof) && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(command, proof, error);
    }

    private static StableErrorV1 requireCommandError(final StableErrorV1 error,
                                                      final CommandQueuedReceiptV1.PreparedCommandRef command) {
        final StableErrorV1 checked = Objects.requireNonNull(error, "error");
        if (checked.stage() != FailureStageV1.ENQUEUE
                || checked.nativePrepared() != null
                || (checked.command() != null && !checked.command().equals(command))) {
            throw new IllegalArgumentException("managed enqueue error does not bind the managed prepared command");
        }
        return checked;
    }
}
