package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * V1 self-routing ID: format byte, route UUID, partition, UUID-like logical
 * identity and CRC32C over the first 37 bytes.
 */
public final class SelfRoutingId extends FixedBytes {
    public static final int LENGTH = 41;
    private static final int FORMAT_VERSION = 1;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final ShardId shardId;
    private final UUID logicalId;

    private SelfRoutingId(final byte[] bytes, final ShardId shardId, final UUID logicalId) {
        super(bytes, LENGTH, "selfRoutingId");
        this.shardId = shardId;
        this.logicalId = logicalId;
    }

    public static SelfRoutingId random(final ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId");
        final UUID logical = uuidV7();
        final byte[] prefix = new byte[37];
        final ByteBuffer buffer = ByteBuffer.wrap(prefix);
        buffer.put((byte) FORMAT_VERSION);
        buffer.put(shardId.routeIncarnation().bytes());
        buffer.putInt(shardId.partition());
        buffer.putLong(logical.getMostSignificantBits());
        buffer.putLong(logical.getLeastSignificantBits());
        return new SelfRoutingId(Bytes.concat(prefix, Bytes.crc32cbe(prefix)), shardId, logical);
    }

    public static SelfRoutingId decode(final byte[] bytes) {
        Bytes.requireLength(bytes, LENGTH, "selfRoutingId");
        if ((bytes[0] & 0xff) != FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported self-routing ID version");
        }
        final long expected = Bytes.crc32c(bytes, 0, 37);
        final long actual = Bytes.readU32be(bytes, 37);
        if (expected != actual) {
            throw new IllegalArgumentException("self-routing ID CRC mismatch");
        }
        final byte[] route = new byte[16];
        System.arraycopy(bytes, 1, route, 0, route.length);
        final int partition = ByteBuffer.wrap(bytes, 17, 4).getInt();
        final ByteBuffer logical = ByteBuffer.wrap(bytes, 21, 16);
        return new SelfRoutingId(bytes, new ShardId(new RouteIncarnation(route), partition),
                new UUID(logical.getLong(), logical.getLong()));
    }

    private static UUID uuidV7() {
        final long millis = Instant.now().toEpochMilli();
        final long randomA = RANDOM.nextLong();
        final long randomB = RANDOM.nextLong();
        final long most = (millis << 16) | 0x7000L | ((randomA >>> 48) & 0x0fffL);
        final long least = (randomB & 0x3fff_ffff_ffff_ffffL) | 0x8000_0000_0000_0000L;
        return new UUID(most, least);
    }

    public ShardId shardId() {
        return shardId;
    }

    public UUID logicalId() {
        return logicalId;
    }

    public static long crc32c(final byte[] bytes, final int offset, final int length) {
        final java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(bytes, offset, length);
        return crc.getValue();
    }
}

