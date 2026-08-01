package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;

/** Canonical V1 body codecs for the first command application surface. */
public final class CommandBodies {
    private CommandBodies() {
    }

    public static byte[] schedule(final ScheduleIntent intent) {
        return intent.canonicalBytes();
    }

    public static ScheduleIntent decodeSchedule(final byte[] body) {
        final ByteBuffer input = ByteBuffer.wrap(body);
        if (input.remaining() < 4 + 8 + 8 + 32 + 1 + 4) {
            throw new IllegalArgumentException("truncated schedule body");
        }
        final int version = input.getInt();
        if (version != 1) {
            throw new IllegalArgumentException("unsupported schedule body version: " + version);
        }
        final long deliverAt = input.getLong();
        final long expireAt = input.getLong();
        final byte[] lane = new byte[DestinationLaneId.LENGTH];
        input.get(lane);
        final int ordering = input.get() & 0xff;
        final int payloadLength = input.getInt();
        if (payloadLength < 0 || payloadLength != input.remaining()) {
            throw new IllegalArgumentException("schedule payload length mismatch");
        }
        final byte[] payload = new byte[payloadLength];
        input.get(payload);
        final OrderingMode mode = switch (ordering) {
            case 1 -> OrderingMode.BEST_EFFORT;
            case 2 -> OrderingMode.DELIVERY_TIME_FIFO;
            default -> throw new IllegalArgumentException("unknown ordering mode: " + ordering);
        };
        final ScheduleIntent result = new ScheduleIntent(new DestinationLaneId(lane), deliverAt, expireAt, mode, payload);
        if (!java.util.Arrays.equals(body, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical schedule body");
        }
        return result;
    }

    public static byte[] cancel(final int expectedGeneration) {
        final ByteBuffer result = ByteBuffer.allocate(8);
        result.putInt(1).putInt(expectedGeneration).flip();
        return result.array();
    }

    public static int decodeCancel(final byte[] body) {
        if (body.length != 8) {
            throw new IllegalArgumentException("invalid cancel body");
        }
        final ByteBuffer input = ByteBuffer.wrap(body);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported cancel body version");
        }
        return input.getInt();
    }

    public static byte[] reschedule(final int expectedGeneration, final long deliverAt, final long expireAt) {
        if (expectedGeneration < -1 || deliverAt < 0 || expireAt < deliverAt) {
            throw new IllegalArgumentException("invalid reschedule body");
        }
        final ByteBuffer result = ByteBuffer.allocate(4 + 4 + 8 + 8);
        result.putInt(1).putInt(expectedGeneration).putLong(deliverAt).putLong(expireAt);
        return result.array();
    }

    public static RescheduleValues decodeReschedule(final byte[] body) {
        if (body.length != 24) {
            throw new IllegalArgumentException("invalid reschedule body");
        }
        final ByteBuffer input = ByteBuffer.wrap(body);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported reschedule body version");
        }
        final RescheduleValues result = new RescheduleValues(input.getInt(), input.getLong(), input.getLong());
        if (result.deliverAtEpochMs() < 0 || result.expireAtEpochMs() < result.deliverAtEpochMs()) {
            throw new IllegalArgumentException("invalid reschedule window");
        }
        return result;
    }

    public record RescheduleValues(int expectedGeneration, long deliverAtEpochMs, long expireAtEpochMs) {
    }
}

