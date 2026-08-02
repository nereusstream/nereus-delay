package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/**
 * Registry-shaped CommitLargeSchedule client body.
 *
 * <p>The body/common-field boundary and nested proof are canonical V1. The
 * decoder never accepts a proof whose reservation or message identity
 * disagrees with the body.</p>
 */
public final class CommitLargeScheduleBodyV1 {
    private static final int COMMAND_TYPE = 3;
    private static final int HASH_LENGTH = 32;

    private final DelayMessageId delayMessageId;
    private final long retryUntilEpochMs;
    private final byte[] reservationId;
    private final PayloadCommitProofV1 proof;

    public CommitLargeScheduleBodyV1(final DelayMessageId delayMessageId, final long retryUntilEpochMs,
                                     final byte[] reservationId, final PayloadCommitProofV1 proof) {
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntil must be non-negative");
        }
        this.retryUntilEpochMs = retryUntilEpochMs;
        Bytes.requireLength(reservationId, HASH_LENGTH, "reservationId");
        this.reservationId = Bytes.copy(reservationId);
        this.proof = Objects.requireNonNull(proof, "proof");
        if (!delayMessageId.equals(proof.delayMessageId())) {
            throw new IllegalArgumentException("CommitLargeSchedule proof message identity mismatch");
        }
        if (!java.util.Arrays.equals(this.reservationId, proof.reservationId())) {
            throw new IllegalArgumentException("CommitLargeSchedule proof reservation identity mismatch");
        }
    }

    public DelayMessageId delayMessageId() {
        return delayMessageId;
    }

    public long retryUntilEpochMs() {
        return retryUntilEpochMs;
    }

    public byte[] reservationId() {
        return Bytes.copy(reservationId);
    }

    public PayloadCommitProofV1 proof() {
        return proof;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, delayMessageId.bytes());
            CanonicalProtobuf.uint32(output, 2, COMMAND_TYPE);
            CanonicalProtobuf.int64(output, 3, retryUntilEpochMs);
            CanonicalProtobuf.bytes(output, 10, reservationId);
            CanonicalProtobuf.bytes(output, 11, proof.canonicalBytes());
        });
    }

    public static CommitLargeScheduleBodyV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "CommitLargeScheduleBodyV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 10, 11}, "CommitLargeScheduleBodyV1");
        if (QueryCodecSupport.uint(fields.get(1), 2) != COMMAND_TYPE) {
            throw new IllegalArgumentException("CommitLargeScheduleBodyV1 has the wrong command type");
        }
        final CommitLargeScheduleBodyV1 result = new CommitLargeScheduleBodyV1(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(0), 1, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint(fields.get(2), 3),
                QueryCodecSupport.fixed(fields.get(3), 10, HASH_LENGTH),
                PayloadCommitProofV1.decode(QueryCodecSupport.nested(fields.get(4), 11)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CommitLargeScheduleBodyV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CommitLargeScheduleBodyV1 that && retryUntilEpochMs == that.retryUntilEpochMs
                && delayMessageId.equals(that.delayMessageId)
                && java.util.Arrays.equals(reservationId, that.reservationId)
                && proof.equals(that.proof);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delayMessageId, retryUntilEpochMs, java.util.Arrays.hashCode(reservationId), proof);
    }
}
