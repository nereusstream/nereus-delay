package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Managed possible-persistence branch; retry reuses exact prepared bytes. */
public final class EnqueueUncertain {
    private final CanonicalCommandQueuedReceipt.PreparedCommandRef command;
    private final byte[] physicalEnqueueAttemptId;
    private final StableError error;

    public EnqueueUncertain(
            final CanonicalCommandQueuedReceipt.PreparedCommandRef command,
            final byte[] physicalEnqueueAttemptId,
            final StableError error) {
        this.command = Objects.requireNonNull(command, "command");
        this.physicalEnqueueAttemptId = nonZero(physicalEnqueueAttemptId);
        this.error = requireCommandError(error, command);
        if (error.retryability() != Retryability.RETRY_EXACT_BYTES) {
            throw new IllegalArgumentException("managed uncertain outcome must allow exact-byte retry");
        }
    }

    public CanonicalCommandQueuedReceipt.PreparedCommandRef command() {
        return command;
    }

    public byte[] physicalEnqueueAttemptId() {
        return Bytes.copy(physicalEnqueueAttemptId);
    }

    public StableError error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, command.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, physicalEnqueueAttemptId);
            CanonicalProtobuf.bytes(output, 3, error.canonicalBytes());
        });
    }

    public static EnqueueUncertain decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "EnqueueUncertain");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "EnqueueUncertain");
        final EnqueueUncertain result = new EnqueueUncertain(
                CanonicalCommandQueuedReceipt.PreparedCommandRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, NonPersistenceProof.ATTEMPT_ID_LENGTH),
                StableError.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "EnqueueUncertain");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof EnqueueUncertain that
                && command.equals(that.command)
                && Arrays.equals(physicalEnqueueAttemptId, that.physicalEnqueueAttemptId)
                && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(command, Arrays.hashCode(physicalEnqueueAttemptId), error);
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
