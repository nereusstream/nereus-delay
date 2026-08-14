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
    private static final int LOGICAL_UUID_OFFSET = 21;
    private static final int LOGICAL_UUID_VERSION = 7;
    private static final int UUID_BYTES = 16;
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
        final UUID logical = uuidV7(Instant.now().toEpochMilli(), RANDOM);
        return fromLogicalUuid(shardId, logical);
    }

    /**
     * Builds the existing 41-byte ID format from a caller-supplied UUIDv7.
     * The logical UUID is selected before routing so an unordered schedule can
     * use the same seed in its partition hash and its self-routing identity.
     */
    public static SelfRoutingId fromLogicalUuid(final ShardId shardId, final UUID logicalUuidV7) {
        Objects.requireNonNull(shardId, "shardId");
        requireLogicalUuidV7(logicalUuidV7);
        final byte[] prefix = new byte[37];
        final ByteBuffer buffer = ByteBuffer.wrap(prefix);
        buffer.put((byte) FORMAT_VERSION);
        buffer.put(shardId.routeIncarnation().bytes());
        buffer.putInt(shardId.partition());
        buffer.putLong(logicalUuidV7.getMostSignificantBits());
        buffer.putLong(logicalUuidV7.getLeastSignificantBits());
        return new SelfRoutingId(Bytes.concat(prefix, Bytes.crc32cbe(prefix)), shardId, logicalUuidV7);
    }

    public static SelfRoutingId decode(final byte[] bytes) {
        Bytes.requireLength(bytes, LENGTH, "selfRoutingId");
        if ((bytes[0] & 0xff) != FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported self-routing ID version");
        }
        validateLogicalUuidV7(bytes);
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

    /** Generates a UUIDv7 using the supplied trusted epoch source and entropy source. */
    public static UUID uuidV7(final long epochMs, final SecureRandom random) {
        if (epochMs < 0 || epochMs > 0xffff_ffff_ffffL) {
            throw new IllegalArgumentException("UUIDv7 timestamp is outside the 48-bit range");
        }
        Objects.requireNonNull(random, "random");
        final long randomA = random.nextLong();
        final long randomB = random.nextLong();
        final long most = (epochMs << 16) | 0x7000L | ((randomA >>> 48) & 0x0fffL);
        final long least = (randomB & 0x3fff_ffff_ffff_ffffL) | 0x8000_0000_0000_0000L;
        return new UUID(most, least);
    }

    /** Validates the UUID version and RFC 4122 variant required by V1. */
    public static void requireLogicalUuidV7(final UUID logicalUuidV7) {
        Objects.requireNonNull(logicalUuidV7, "logicalUuidV7");
        final int version = logicalUuidV7.version();
        final int variant = logicalUuidV7.variant();
        if (version != LOGICAL_UUID_VERSION || variant != 2) {
            throw new IllegalArgumentException("logical locator is not a UUIDv7");
        }
    }

    public ShardId shardId() {
        return shardId;
    }

    public UUID logicalId() {
        return logicalId;
    }

    /** Returns the unsigned 48-bit UUIDv7 creation timestamp in epoch ms. */
    public long logicalTimestampEpochMs() {
        return logicalId.getMostSignificantBits() >>> 16;
    }

    public static long crc32c(final byte[] bytes, final int offset, final int length) {
        final java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(bytes, offset, length);
        return crc.getValue();
    }

    /**
     * V1 uses UUIDv7 for the logical locator so its timestamp can participate
     * in first-seen age validation.  The timestamp itself is interpreted by
     * the route policy; this decoder only enforces the UUID version and RFC
     * variant bits that make the locator a UUIDv7 rather than arbitrary bytes.
     */
    private static void validateLogicalUuidV7(final byte[] bytes) {
        final ByteBuffer logical = ByteBuffer.wrap(bytes, LOGICAL_UUID_OFFSET, UUID_BYTES);
        final long most = logical.getLong();
        final long least = logical.getLong();
        final int version = (int) ((most >>> 12) & 0x0f);
        final int variant = (int) ((least >>> 62) & 0x03);
        if (version != LOGICAL_UUID_VERSION || variant != 0x02) {
            throw new IllegalArgumentException("logical locator is not a UUIDv7");
        }
    }

}
