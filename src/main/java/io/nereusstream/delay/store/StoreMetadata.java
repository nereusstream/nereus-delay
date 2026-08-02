package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;

import java.nio.ByteBuffer;
import java.util.UUID;

/** Durable identity written before a shard DB is exposed to command application. */
public record StoreMetadata(
        int storeFormatVersion,
        ShardId shardId,
        byte[] storeIncarnation,
        byte[] dbIdentity) {
    public StoreMetadata {
        if (storeFormatVersion != 1) {
            throw new IllegalArgumentException("unsupported store format version");
        }
        requireNonZero(storeIncarnation, 16, "storeIncarnation");
        requireNonZero(dbIdentity, 32, "dbIdentity");
        storeIncarnation = Bytes.copy(storeIncarnation);
        dbIdentity = Bytes.copy(dbIdentity);
    }

    @Override
    public byte[] storeIncarnation() {
        return Bytes.copy(storeIncarnation);
    }

    @Override
    public byte[] dbIdentity() {
        return Bytes.copy(dbIdentity);
    }

    public byte[] encode() {
        final ByteBuffer result = ByteBuffer.allocate(4 + 16 + 4 + 16 + 32);
        result.putInt(storeFormatVersion).put(shardId.routeIncarnation().bytes()).putInt(shardId.partition())
                .put(storeIncarnation).put(dbIdentity);
        return result.array();
    }

    public static StoreMetadata decode(final byte[] bytes) {
        if (bytes.length != 4 + 16 + 4 + 16 + 32) {
            throw new IllegalArgumentException("invalid store metadata length");
        }
        final ByteBuffer input = ByteBuffer.wrap(bytes);
        final int format = input.getInt();
        final byte[] route = new byte[16];
        input.get(route);
        final int partition = input.getInt();
        final byte[] store = new byte[16];
        input.get(store);
        final byte[] db = new byte[32];
        input.get(db);
        return new StoreMetadata(format, new ShardId(new RouteIncarnation(route), partition), store, db);
    }

    public UUID storeIncarnationUuid() {
        final ByteBuffer input = ByteBuffer.wrap(storeIncarnation);
        return new UUID(input.getLong(), input.getLong());
    }

    private static void requireNonZero(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        for (byte item : value) {
            if (item != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
