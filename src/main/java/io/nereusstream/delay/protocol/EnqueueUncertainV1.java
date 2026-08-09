package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Managed possible-persistence branch; retry reuses exact prepared bytes. */
public final class EnqueueUncertainV1 {
    private final CommandQueuedReceiptV1.PreparedCommandRef command;
    private final byte[] physicalEnqueueAttemptId;
    private final StableErrorV1 error;

    public EnqueueUncertainV1(final CommandQueuedReceiptV1.PreparedCommandRef command,
                              final byte[] physicalEnqueueAttemptId, final StableErrorV1 error) {
        this.command = Objects.requireNonNull(command, "command");
        this.physicalEnqueueAttemptId = nonZero(physicalEnqueueAttemptId);
        this.error = requireCommandError(error, command);
        if (error.retryability() != RetryabilityV1.RETRY_EXACT_BYTES) {
            throw new IllegalArgumentException("managed uncertain outcome must allow exact-byte retry");
        }
    }

    public CommandQueuedReceiptV1.PreparedCommandRef command() {
        return command;
    }

    public byte[] physicalEnqueueAttemptId() {
        return Bytes.copy(physicalEnqueueAttemptId);
    }

    public StableErrorV1 error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, command.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, physicalEnqueueAttemptId);
            CanonicalProtobuf.bytes(output, 3, error.canonicalBytes());
        });
    }

    public static EnqueueUncertainV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "EnqueueUncertainV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3}, "EnqueueUncertainV1");
        final EnqueueUncertainV1 result = new EnqueueUncertainV1(
                CommandQueuedReceiptV1.PreparedCommandRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, NonPersistenceProofV1.ATTEMPT_ID_LENGTH),
                StableErrorV1.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "EnqueueUncertainV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof EnqueueUncertainV1 that && command.equals(that.command)
                && Arrays.equals(physicalEnqueueAttemptId, that.physicalEnqueueAttemptId)
                && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(command, Arrays.hashCode(physicalEnqueueAttemptId), error);
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
