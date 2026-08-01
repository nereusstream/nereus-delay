package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Durable shard-local usage projection for hard pending grants. */
public record ShardQuota(long pendingMessages, long pendingBytes, long laneCount, long usageRevision) {
    public ShardQuota {
        if (pendingMessages < 0 || pendingBytes < 0 || laneCount < 0 || usageRevision < 0) {
            throw new IllegalArgumentException("invalid shard quota usage");
        }
    }

    public static ShardQuota empty() {
        return new ShardQuota(0, 0, 0, 0);
    }

    public ShardQuota addSchedule(final long bytes, final boolean newLane) {
        return new ShardQuota(Math.addExact(pendingMessages, 1), Math.addExact(pendingBytes, bytes),
                Math.addExact(laneCount, newLane ? 1 : 0), Math.addExact(usageRevision, 1));
    }

    public ShardQuota removeSchedule(final long bytes) {
        if (pendingMessages <= 0 || pendingBytes < bytes) {
            throw new IllegalStateException("shard quota usage underflow");
        }
        return new ShardQuota(pendingMessages - 1, pendingBytes - bytes, laneCount,
                Math.addExact(usageRevision, 1));
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(1), Bytes.u64be(pendingMessages), Bytes.u64be(pendingBytes),
                Bytes.u64be(laneCount), Bytes.u64be(usageRevision));
    }

    public static ShardQuota decode(final byte[] encoded) {
        if (encoded.length != 4 + 8 * 4) {
            throw new IllegalArgumentException("invalid shard quota length");
        }
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported shard quota version");
        }
        final ShardQuota result = new ShardQuota(input.getLong(), input.getLong(), input.getLong(), input.getLong());
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical shard quota");
        }
        return result;
    }
}
