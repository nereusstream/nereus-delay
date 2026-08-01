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
        long expiresAtEpochMs) {
    public OwnerLease {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(ownerId, "ownerId");
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

    public boolean validAt(final long nowEpochMs) {
        return nowEpochMs < expiresAtEpochMs;
    }
}

