package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** Decoder for the closed canonical Source Position variants. */
public final class SourcePositionCodec {
    private SourcePositionCodec() {
    }

    public static SourcePosition decode(final byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        final SourcePosition decoded = decodeInternal(bytes);
        if (!Arrays.equals(bytes, decoded.canonicalBytes())) {
            throw new IllegalArgumentException("source position is not canonical");
        }
        return decoded;
    }

    private static SourcePosition decodeInternal(final byte[] bytes) {
        final ByteBuffer input = ByteBuffer.wrap(bytes);
        if (!input.hasRemaining()) {
            throw new IllegalArgumentException("empty source position");
        }
        final int kind = input.get() & 0xff;
        final byte[] route = readFixed(input, 16);
        if (kind == SourcePositionKind.KAFKA.wireValue()) {
            final String cluster = readString(input);
            final UUID topic = readUuid(input);
            final int partition = readInt(input, "partition");
            final long offset = requireNonNegative(readLong(input, "offset"), "offset");
            final int presence = readUnsignedByte(input, "leader epoch presence");
            final Integer leaderEpoch = switch (presence) {
                case 0 -> null;
                case 1 -> readNonNegativeInt(input, "leader epoch");
                default -> throw new IllegalArgumentException("invalid leader epoch presence");
            };
            final long appendTime = requireNonNegative(readLong(input, "brokerLogAppendTime"),
                    "brokerLogAppendTime");
            requireEnd(input);
            return new KafkaSourcePosition(new ShardId(new RouteIncarnation(route), partition), cluster, topic,
                    offset, leaderEpoch, appendTime);
        }
        if (kind == SourcePositionKind.PULSAR.wireValue()) {
            final byte[] resource = readBytes(input);
            final String topic = readString(input);
            final int partition = readInt(input, "partition");
            final long ledger = requireNonNegative(readLong(input, "ledgerId"), "ledgerId");
            final long entry = requireNonNegative(readLong(input, "entryId"), "entryId");
            final int batchIndex = readInt(input, "normalizedBatchIndex");
            final int batchSize = readInt(input, "batchSize");
            final int entryKind = readUnsignedByte(input, "entry kind");
            final long timestamp = requireNonNegative(readLong(input, "brokerEntryTimestamp"),
                    "brokerEntryTimestamp");
            requireEnd(input);
            final PulsarSourcePosition.EntryKind decodedKind = switch (entryKind) {
                case 1 -> PulsarSourcePosition.EntryKind.NON_BATCH;
                case 2 -> PulsarSourcePosition.EntryKind.BATCH;
                default -> throw new IllegalArgumentException("unknown Pulsar entry kind");
            };
            return new PulsarSourcePosition(new ShardId(new RouteIncarnation(route), partition), resource, topic,
                    ledger, entry, batchIndex, batchSize, decodedKind, timestamp);
        }
        throw new IllegalArgumentException("unknown source position kind: " + kind);
    }

    private static byte[] readFixed(final ByteBuffer input, final int length) {
        if (input.remaining() < length) {
            throw new IllegalArgumentException("truncated source position");
        }
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static byte[] readBytes(final ByteBuffer input) {
        final long length = Integer.toUnsignedLong(readInt(input, "length"));
        if (length > input.remaining()) {
            throw new IllegalArgumentException("source position length outside payload");
        }
        return readFixed(input, Math.toIntExact(length));
    }

    private static String readString(final ByteBuffer input) {
        return new String(readBytes(input), StandardCharsets.UTF_8);
    }

    private static UUID readUuid(final ByteBuffer input) {
        final ByteBuffer bytes = ByteBuffer.wrap(readFixed(input, 16));
        return new UUID(bytes.getLong(), bytes.getLong());
    }

    private static long requireNonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " is outside signed V1 range");
        }
        return value;
    }

    private static int readInt(final ByteBuffer input, final String name) {
        requireRemaining(input, Integer.BYTES, name);
        return input.getInt();
    }

    private static long readLong(final ByteBuffer input, final String name) {
        requireRemaining(input, Long.BYTES, name);
        return input.getLong();
    }

    private static int readUnsignedByte(final ByteBuffer input, final String name) {
        requireRemaining(input, 1, name);
        return input.get() & 0xff;
    }

    private static int readNonNegativeInt(final ByteBuffer input, final String name) {
        final long value = Integer.toUnsignedLong(readInt(input, name));
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " is outside signed V1 range");
        }
        return (int) value;
    }

    private static void requireRemaining(final ByteBuffer input, final int length, final String name) {
        if (input.remaining() < length) {
            throw new IllegalArgumentException("truncated source position " + name);
        }
    }

    private static void requireEnd(final ByteBuffer input) {
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing source position bytes");
        }
    }
}
