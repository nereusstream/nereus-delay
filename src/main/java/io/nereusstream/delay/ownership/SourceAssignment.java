package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourceActivationBarrier;

import java.util.Objects;

/**
 * Local projection of a source assignment accepted by the external broker
 * adapter.  The assignment identity is deliberately separate from the
 * activation barrier; both are required before a shard can become active.
 */
public record SourceAssignment(
        ShardId shardId,
        byte[] assignmentId,
        long assignmentEpoch,
        SourceActivationBarrier activationBarrier) {
    public static final int ID_LENGTH = 32;

    public SourceAssignment {
        Objects.requireNonNull(shardId, "shardId");
        Bytes.requireLength(assignmentId, ID_LENGTH, "assignmentId");
        if (assignmentEpoch <= 0) {
            throw new IllegalArgumentException("assignmentEpoch must be positive");
        }
        Objects.requireNonNull(activationBarrier, "activationBarrier");
        if (!shardId.equals(activationBarrier.shardId())) {
            throw new IllegalArgumentException("source assignment barrier belongs to another shard");
        }
        boolean nonZero = false;
        for (byte value : assignmentId) {
            if (value != 0) {
                nonZero = true;
                break;
            }
        }
        if (!nonZero) {
            throw new IllegalArgumentException("assignmentId must be non-zero");
        }
        assignmentId = Bytes.copy(assignmentId);
    }

    @Override
    public byte[] assignmentId() {
        return Bytes.copy(assignmentId);
    }
}
