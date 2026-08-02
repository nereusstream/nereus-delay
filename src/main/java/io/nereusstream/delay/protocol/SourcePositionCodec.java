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
            final int partition = input.getInt();
            final long offset = requireNonNegative(input.getLong(), "offset");
            final int presence = input.get() & 0xff;
            final Integer leaderEpoch = switch (presence) {
                case 0 -> null;
                case 1 -> Math.toIntExact(Integer.toUnsignedLong(input.getInt()));
                default -> throw new IllegalArgumentException("invalid leader epoch presence");
            };
            final long appendTime = requireNonNegative(input.getLong(), "brokerLogAppendTime");
            requireEnd(input);
            return new KafkaSourcePosition(new ShardId(new RouteIncarnation(route), partition), cluster, topic,
                    offset, leaderEpoch, appendTime);
        }
        if (kind == SourcePositionKind.PULSAR.wireValue()) {
            final byte[] resource = readBytes(input);
            final String topic = readString(input);
            final int partition = input.getInt();
            final long ledger = requireNonNegative(input.getLong(), "ledgerId");
            final long entry = requireNonNegative(input.getLong(), "entryId");
            final int batchIndex = input.getInt();
            final int batchSize = input.getInt();
            final int entryKind = input.get() & 0xff;
            final long timestamp = requireNonNegative(input.getLong(), "brokerEntryTimestamp");
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
        final long length = Integer.toUnsignedLong(input.getInt());
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

    private static void requireEnd(final ByteBuffer input) {
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing source position bytes");
        }
    }
}
