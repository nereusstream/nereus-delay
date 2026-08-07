package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Durable shard-local usage projection for hard pending grants. */
public record ShardQuota(long pendingMessages, long pendingBytes, long reservationMessages,
                         long reservationBytes, long laneCount, long usageRevision) {
    public ShardQuota {
        if (pendingMessages < 0 || pendingBytes < 0 || reservationMessages < 0 || reservationBytes < 0
                || laneCount < 0 || usageRevision < 0) {
            throw new IllegalArgumentException("invalid shard quota usage");
        }
    }

    public static ShardQuota empty() {
        return new ShardQuota(0, 0, 0, 0, 0, 0);
    }

    public ShardQuota addSchedule(final long bytes, final boolean newLane) {
        requireNonNegativeBytes(bytes);
        return new ShardQuota(Math.addExact(pendingMessages, 1), Math.addExact(pendingBytes, bytes),
                reservationMessages, reservationBytes, Math.addExact(laneCount, newLane ? 1 : 0),
                Math.addExact(usageRevision, 1));
    }

    public ShardQuota removeSchedule(final long bytes) {
        requireNonNegativeBytes(bytes);
        if (pendingMessages <= 0 || pendingBytes < bytes) {
            throw new IllegalStateException("shard quota usage underflow");
        }
        return new ShardQuota(pendingMessages - 1, pendingBytes - bytes, reservationMessages, reservationBytes,
                laneCount, Math.addExact(usageRevision, 1));
    }

    /** Removes the unadmitted message charge transferred by one Lane close. */
    public ShardQuota removeSchedules(final long messages, final long bytes) {
        if (messages < 0 || bytes < 0 || pendingMessages < messages || pendingBytes < bytes) {
            throw new IllegalStateException("shard quota message usage underflow");
        }
        if (messages == 0 && bytes == 0) {
            return this;
        }
        return new ShardQuota(pendingMessages - messages, pendingBytes - bytes, reservationMessages,
                reservationBytes, laneCount, Math.addExact(usageRevision, 1));
    }

    public ShardQuota addReservation(final long bytes, final boolean newLane) {
        requireNonNegativeBytes(bytes);
        return new ShardQuota(pendingMessages, pendingBytes, Math.addExact(reservationMessages, 1),
                Math.addExact(reservationBytes, bytes), Math.addExact(laneCount, newLane ? 1 : 0),
                Math.addExact(usageRevision, 1));
    }

    public ShardQuota removeReservation(final long bytes) {
        requireNonNegativeBytes(bytes);
        if (reservationMessages <= 0 || reservationBytes < bytes) {
            throw new IllegalStateException("shard quota reservation underflow");
        }
        return new ShardQuota(pendingMessages, pendingBytes, reservationMessages - 1,
                reservationBytes - bytes, laneCount, Math.addExact(usageRevision, 1));
    }

    /** Removes the uncommitted reservation charge transferred by one Lane close. */
    public ShardQuota removeReservations(final long messages, final long bytes) {
        if (messages < 0 || bytes < 0 || reservationMessages < messages || reservationBytes < bytes) {
            throw new IllegalStateException("shard quota reservation usage underflow");
        }
        if (messages == 0 && bytes == 0) {
            return this;
        }
        return new ShardQuota(pendingMessages, pendingBytes, reservationMessages - messages,
                reservationBytes - bytes, laneCount, Math.addExact(usageRevision, 1));
    }

    public ShardQuota commitReservation(final long bytes) {
        requireNonNegativeBytes(bytes);
        if (reservationMessages <= 0 || reservationBytes < bytes) {
            throw new IllegalStateException("shard quota reservation underflow");
        }
        return new ShardQuota(Math.addExact(pendingMessages, 1), Math.addExact(pendingBytes, bytes),
                reservationMessages - 1, reservationBytes - bytes, laneCount, Math.addExact(usageRevision, 1));
    }

    private static void requireNonNegativeBytes(final long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("quota bytes must be non-negative");
        }
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(2), Bytes.u64be(pendingMessages), Bytes.u64be(pendingBytes),
                Bytes.u64be(reservationMessages), Bytes.u64be(reservationBytes), Bytes.u64be(laneCount),
                Bytes.u64be(usageRevision));
    }

    public static ShardQuota decode(final byte[] encoded) {
        if (encoded.length != 4 + 8 * 4 && encoded.length != 4 + 8 * 6) {
            throw new IllegalArgumentException("invalid shard quota length");
        }
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        final int version = input.getInt();
        final ShardQuota result;
        if (version == 1 && encoded.length == 4 + 8 * 4) {
            result = new ShardQuota(input.getLong(), input.getLong(), 0, 0, input.getLong(), input.getLong());
        } else if (version == 2 && encoded.length == 4 + 8 * 6) {
            result = new ShardQuota(input.getLong(), input.getLong(), input.getLong(), input.getLong(),
                    input.getLong(), input.getLong());
        } else {
            throw new IllegalArgumentException("unsupported shard quota version");
        }
        if (!Arrays.equals(encoded, result.encode())) {
            if (version != 1) {
                throw new IllegalArgumentException("non-canonical shard quota");
            }
        }
        return result;
    }
}
