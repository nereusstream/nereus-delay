package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.util.UUID;

/** Immutable 128-bit Route Incarnation identity. */
public final class RouteIncarnation extends FixedBytes {
    public static final int LENGTH = 16;

    public RouteIncarnation(final byte[] bytes) {
        super(bytes, LENGTH, "routeIncarnation");
    }

    public static RouteIncarnation random() {
        return fromUuid(UUID.randomUUID());
    }

    public static RouteIncarnation fromUuid(final UUID uuid) {
        final ByteBuffer buffer = ByteBuffer.allocate(LENGTH);
        buffer.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
        return new RouteIncarnation(buffer.array());
    }

    public UUID uuid() {
        final ByteBuffer buffer = ByteBuffer.wrap(unsafeBytes());
        return new UUID(buffer.getLong(), buffer.getLong());
    }
}

