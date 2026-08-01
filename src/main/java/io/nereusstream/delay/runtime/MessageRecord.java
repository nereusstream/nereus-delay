package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.OrderingMode;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Durable current-generation projection stored in id_cf/MESSAGE. */
public record MessageRecord(
        MessageStatus status,
        int generation,
        long stateVersion,
        long deliverAtEpochMs,
        long expireAtEpochMs,
        DestinationLaneId laneId,
        OrderingMode orderingMode,
        byte[] payload,
        byte[] scheduleSourcePosition) {
    public MessageRecord {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(orderingMode, "orderingMode");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(scheduleSourcePosition, "scheduleSourcePosition");
        if (generation < 0 || stateVersion < 0 || deliverAtEpochMs < 0 || expireAtEpochMs < deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid message record");
        }
        payload = Bytes.copy(payload);
        scheduleSourcePosition = Bytes.copy(scheduleSourcePosition);
    }

    @Override
    public byte[] payload() {
        return Bytes.copy(payload);
    }

    @Override
    public byte[] scheduleSourcePosition() {
        return Bytes.copy(scheduleSourcePosition);
    }

    public byte[] encode() {
        final ByteBuffer result = ByteBuffer.allocate(4 + 1 + 4 + 8 + 8 + 8 + 32 + 1 + 4
                + scheduleSourcePosition.length + 4 + payload.length);
        result.putInt(1).put((byte) status.wireValue()).putInt(generation).putLong(stateVersion)
                .putLong(deliverAtEpochMs).putLong(expireAtEpochMs).put(laneId.bytes()).put((byte) orderingMode.wireValue())
                .putInt(scheduleSourcePosition.length).put(scheduleSourcePosition)
                .putInt(payload.length).put(payload);
        return result.array();
    }

    public static MessageRecord decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.remaining() < 4 + 1 + 4 + 8 + 8 + 8 + 32 + 1 + 4 + 4) {
            throw new IllegalArgumentException("message record is truncated");
        }
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported message record version");
        }
        final MessageStatus status = MessageStatus.fromWire(input.get() & 0xff);
        final int generation = input.getInt();
        final long stateVersion = input.getLong();
        final long deliverAt = input.getLong();
        final long expireAt = input.getLong();
        final byte[] lane = new byte[32];
        input.get(lane);
        final int ordering = input.get() & 0xff;
        final int sourceLength = input.getInt();
        if (sourceLength < 0 || sourceLength > input.remaining()) {
            throw new IllegalArgumentException("invalid source position length");
        }
        final byte[] source = new byte[sourceLength];
        input.get(source);
        final int payloadLength = input.getInt();
        if (payloadLength < 0 || payloadLength != input.remaining()) {
            throw new IllegalArgumentException("invalid message payload length");
        }
        final byte[] payload = new byte[payloadLength];
        input.get(payload);
        final OrderingMode mode = switch (ordering) {
            case 1 -> OrderingMode.BEST_EFFORT;
            case 2 -> OrderingMode.DELIVERY_TIME_FIFO;
            default -> throw new IllegalArgumentException("unknown ordering mode: " + ordering);
        };
        final MessageRecord result = new MessageRecord(status, generation, stateVersion, deliverAt, expireAt,
                new DestinationLaneId(lane), mode, payload, source);
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical message record");
        }
        return result;
    }
}

