package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Public-safe result for one Shard control target. */
public final class ShardControlResult {
    private final ShardSubject shard;
    private final ShardLifecycleState lifecycle;
    private final Long ownerEpoch;
    private final StableCode stableCode;

    public ShardControlResult(
            final ShardSubject shard,
            final ShardLifecycleState lifecycle,
            final Long ownerEpoch,
            final StableCode stableCode) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        if (ownerEpoch != null && ownerEpoch == 0) {
            throw new IllegalArgumentException("ownerEpoch must be nonzero when present");
        }
        this.ownerEpoch = ownerEpoch;
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
    }

    public ShardSubject shard() {
        return shard;
    }

    public ShardLifecycleState lifecycle() {
        return lifecycle;
    }

    public Long ownerEpoch() {
        return ownerEpoch;
    }

    public StableCode stableCode() {
        return stableCode;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, lifecycle.wireValue());
            if (ownerEpoch != null) {
                CanonicalProtobuf.uint64Bits(output, 3, ownerEpoch);
            }
            CanonicalProtobuf.uint32(output, 4, stableCode.wireValue());
        });
    }

    public static ShardControlResult decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ShardControlResult");
        if (fields.size() != 3 && fields.size() != 4) {
            throw new IllegalArgumentException("invalid ShardControlResult field count");
        }
        if (fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || (fields.size() == 4 && fields.get(2).number() != 3)
                || fields.get(fields.size() - 1).number() != 4) {
            throw new IllegalArgumentException("invalid ShardControlResult field order");
        }
        final ShardControlResult result = new ShardControlResult(
                ShardSubject.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                ShardLifecycleState.fromWire(QueryCodecSupport.uint(fields.get(1), 2)),
                fields.size() == 4 ? QueryCodecSupport.uint(fields.get(2), 3) : null,
                StableCode.fromWire(QueryCodecSupport.uint32(fields.get(fields.size() - 1), 4)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ShardControlResult");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ShardControlResult that
                && shard.equals(that.shard)
                && lifecycle == that.lifecycle
                && Objects.equals(ownerEpoch, that.ownerEpoch)
                && stableCode == that.stableCode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shard, lifecycle, ownerEpoch, stableCode);
    }
}
