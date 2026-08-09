package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.PayloadReference;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Durable reservation record stored in id_cf before a large payload is committed. */
public record PayloadReservation(
        ShardId shardId,
        byte[] reservationId,
        CommandId commandId,
        DelayMessageId delayMessageId,
        byte[] commandHash,
        LargeScheduleIntent intent,
        long reservationExpiryEpochMs,
        PayloadReservationStatus status,
        long stateVersion,
        byte[] sourcePosition,
        PayloadReference committedPayload) {
    public PayloadReservation {
        Objects.requireNonNull(shardId, "shardId");
        Bytes.requireLength(reservationId, 32, "reservationId");
        Objects.requireNonNull(commandId, "commandId");
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        Bytes.requireLength(commandHash, 32, "commandHash");
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(status, "status");
        Bytes.requireLength(sourcePosition, sourcePosition.length, "sourcePosition");
        if (reservationExpiryEpochMs < 0 || stateVersion <= 0 || sourcePosition.length == 0) {
            throw new IllegalArgumentException("invalid payload reservation");
        }
        if (!commandId.routingId().shardId().equals(shardId)
                || !delayMessageId.routingId().shardId().equals(shardId)) {
            throw new IllegalArgumentException("payload reservation identity does not belong to shard");
        }
        if (status == PayloadReservationStatus.COMMITTED && committedPayload == null) {
            throw new IllegalArgumentException("committed reservation needs payload reference");
        }
        if (status != PayloadReservationStatus.COMMITTED && committedPayload != null) {
            throw new IllegalArgumentException("uncommitted reservation cannot carry payload reference");
        }
        if (committedPayload != null
                && (committedPayload.length() != intent.expectedPayloadLength()
                || !Bytes.constantTimeEquals(committedPayload.payloadSha256(), intent.payloadSha256()))) {
            throw new IllegalArgumentException("committed payload does not match reservation intent");
        }
        reservationId = Bytes.copy(reservationId);
        commandHash = Bytes.copy(commandHash);
        sourcePosition = Bytes.copy(sourcePosition);
    }

    @Override
    public byte[] reservationId() {
        return Bytes.copy(reservationId);
    }

    @Override
    public byte[] commandHash() {
        return Bytes.copy(commandHash);
    }

    @Override
    public byte[] sourcePosition() {
        return Bytes.copy(sourcePosition);
    }

    public byte[] encode() {
        final byte[] payload = committedPayload == null ? new byte[0] : committedPayload.encode();
        return Bytes.concat(Bytes.u32be(1), shardId.routeIncarnation().bytes(), Bytes.u32beBits(shardId.partition()),
                reservationId, commandId.bytes(), delayMessageId.bytes(), commandHash, intent.canonicalBytes(),
                Bytes.u64be(reservationExpiryEpochMs), Bytes.u8(status.wireValue()), Bytes.u64be(stateVersion),
                Bytes.lp32(sourcePosition), Bytes.u8(committedPayload == null ? 0 : 1), Bytes.lp32(payload));
    }

    public static PayloadReservation decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (readInt(input, "version") != 1) {
            throw new IllegalArgumentException("unsupported payload reservation version");
        }
        final RouteIncarnation route = new RouteIncarnation(readFixed(input, 16));
        final int partition = readInt(input, "partition");
        final byte[] reservationId = readFixed(input, 32);
        final CommandId commandId = new CommandId(readFixed(input, CommandId.LENGTH));
        final DelayMessageId messageId = new DelayMessageId(readFixed(input, DelayMessageId.LENGTH));
        final byte[] commandHash = readFixed(input, 32);
        final int intentLength = 4 + 8 + 8 + 32 + 1 + 8 + 32 + 8 + 8;
        final LargeScheduleIntent intent = decodeIntent(readFixed(input, intentLength));
        final long expiry = readLong(input, "reservationExpiry");
        final PayloadReservationStatus status = PayloadReservationStatus.fromWire(readUnsignedByte(input, "status"));
        final long stateVersion = readLong(input, "stateVersion");
        final byte[] source = readLp32(input);
        final int hasPayload = readUnsignedByte(input, "payload presence");
        final byte[] payloadBytes = readLp32(input);
        if (hasPayload != 0 && hasPayload != 1 || hasPayload == 0 && payloadBytes.length != 0) {
            throw new IllegalArgumentException("invalid committed payload presence");
        }
        final PayloadReference payload = hasPayload == 0 ? null : PayloadReference.decode(payloadBytes);
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("payload reservation has trailing bytes");
        }
        final PayloadReservation result = new PayloadReservation(new ShardId(route, partition), reservationId,
                commandId, messageId, commandHash, intent, expiry, status, stateVersion, source, payload);
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical payload reservation");
        }
        return result;
    }

    private static LargeScheduleIntent decodeIntent(final byte[] bytes) {
        final ByteBuffer input = ByteBuffer.wrap(bytes);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported large schedule intent version");
        }
        final long deliver = input.getLong();
        final long expire = input.getLong();
        final byte[] lane = readFixed(input, 32);
        final int ordering = input.get() & 0xff;
        final long length = input.getLong();
        final byte[] sha = readFixed(input, 32);
        final long ttl = input.getLong();
        final long trustSet = input.getLong();
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("large schedule intent has trailing bytes");
        }
        final io.nereusstream.delay.protocol.OrderingMode mode = switch (ordering) {
            case 1 -> io.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT;
            case 2 -> io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO;
            default -> throw new IllegalArgumentException("unknown large schedule ordering mode");
        };
        return new LargeScheduleIntent(new io.nereusstream.delay.protocol.DestinationLaneId(lane), deliver, expire,
                mode, length, sha, ttl, trustSet);
    }

    private static byte[] readFixed(final ByteBuffer input, final int length) {
        if (input.remaining() < length) {
            throw new IllegalArgumentException("truncated payload reservation");
        }
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static byte[] readLp32(final ByteBuffer input) {
        if (input.remaining() < 4) {
            throw new IllegalArgumentException("truncated payload reservation length");
        }
        final long length = Integer.toUnsignedLong(input.getInt());
        if (length > input.remaining()) {
            throw new IllegalArgumentException("payload reservation length outside value");
        }
        return readFixed(input, Math.toIntExact(length));
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
        requireRemaining(input, Byte.BYTES, name);
        return input.get() & 0xff;
    }

    private static void requireRemaining(final ByteBuffer input, final int length, final String name) {
        if (length < 0 || input.remaining() < length) {
            throw new IllegalArgumentException("payload reservation " + name + " is truncated");
        }
    }
}
