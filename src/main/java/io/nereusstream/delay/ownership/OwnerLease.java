package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ShardId;

import java.util.Objects;

/** Oxia-backed ownership projection used for local fencing. */
public record OwnerLease(
        ShardId shardId,
        String ownerId,
        long ownerEpoch,
        byte[] leaseToken,
        long expiresAtEpochMs,
        OwnerLeaseContext context,
        ShardLifecycleState state) {
    public OwnerLease(final ShardId shardId, final String ownerId, final long ownerEpoch,
                      final byte[] leaseToken, final long expiresAtEpochMs) {
        this(shardId, ownerId, ownerEpoch, leaseToken, expiresAtEpochMs, null, ShardLifecycleState.ACQUIRING);
    }

    public OwnerLease {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(ownerId, "ownerId");
        Objects.requireNonNull(state, "state");
        Bytes.requireLength(leaseToken, 32, "leaseToken");
        if (ownerId.isBlank() || ownerEpoch <= 0 || expiresAtEpochMs < 0) {
            throw new IllegalArgumentException("invalid owner lease");
        }
        leaseToken = Bytes.copy(leaseToken);
    }

    @Override
    public byte[] leaseToken() {
        return Bytes.copy(leaseToken);
    }

    public byte[] sourceAssignmentId() {
        return context == null ? null : context.sourceAssignmentId();
    }

    public byte[] sessionIdentity() {
        return context == null ? null : context.sessionIdentity();
    }

    /** Returns whether the immutable fencing and assignment/session identity is unchanged. */
    public boolean sameIdentity(final OwnerLease other) {
        return other != null && shardId.equals(other.shardId()) && ownerId.equals(other.ownerId())
                && ownerEpoch == other.ownerEpoch()
                && Bytes.constantTimeEquals(leaseToken, other.leaseToken())
                && Objects.equals(context, other.context());
    }

    public boolean validAt(final long nowEpochMs) {
        return nowEpochMs < expiresAtEpochMs;
    }
}
