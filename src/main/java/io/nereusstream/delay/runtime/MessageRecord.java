package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadReference;

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
        byte[] scheduleSourcePosition,
        PayloadReference payloadReference) {
    public MessageRecord(final MessageStatus status, final int generation, final long stateVersion,
                         final long deliverAtEpochMs, final long expireAtEpochMs, final DestinationLaneId laneId,
                         final OrderingMode orderingMode, final byte[] payload, final byte[] scheduleSourcePosition) {
        this(status, generation, stateVersion, deliverAtEpochMs, expireAtEpochMs, laneId, orderingMode, payload,
                scheduleSourcePosition, null);
    }

    public MessageRecord {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(orderingMode, "orderingMode");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(scheduleSourcePosition, "scheduleSourcePosition");
        if (generation < 0 || stateVersion < 0 || deliverAtEpochMs < 0 || expireAtEpochMs < deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid message record");
        }
        if (payloadReference != null && payload.length != 0) {
            throw new IllegalArgumentException("object-backed message cannot carry inline payload");
        }
        if (payloadReference == null && payload.length == 0) {
            // Empty inline payloads are valid; the branch is intentionally explicit for readability.
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

    public long payloadLength() {
        return payloadReference == null ? payload.length : payloadReference.length();
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof MessageRecord that)) {
            return false;
        }
        return status == that.status && generation == that.generation && stateVersion == that.stateVersion
                && deliverAtEpochMs == that.deliverAtEpochMs && expireAtEpochMs == that.expireAtEpochMs
                && laneId.equals(that.laneId) && orderingMode == that.orderingMode
                && Arrays.equals(payload, that.payload)
                && Arrays.equals(scheduleSourcePosition, that.scheduleSourcePosition)
                && Objects.equals(payloadReference, that.payloadReference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, generation, stateVersion, deliverAtEpochMs, expireAtEpochMs, laneId,
                orderingMode, Arrays.hashCode(payload), Arrays.hashCode(scheduleSourcePosition), payloadReference);
    }

    public byte[] encode() {
        final byte[] reference = payloadReference == null ? new byte[0] : payloadReference.encode();
        final ByteBuffer result = ByteBuffer.allocate(4 + 1 + 4 + 8 + 8 + 8 + 32 + 1 + 1 + 4
                + scheduleSourcePosition.length + 4 + payload.length + 4 + reference.length);
        result.putInt(2).put((byte) status.wireValue()).putInt(generation).putLong(stateVersion)
                .putLong(deliverAtEpochMs).putLong(expireAtEpochMs).put(laneId.bytes()).put((byte) orderingMode.wireValue())
                .put((byte) (payloadReference == null ? 1 : 2))
                .putInt(scheduleSourcePosition.length).put(scheduleSourcePosition);
        if (payloadReference == null) {
            result.putInt(payload.length).put(payload).putInt(0);
        } else {
            result.putInt(0).putInt(reference.length).put(reference);
        }
        return result.array();
    }

    public static MessageRecord decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.remaining() < 4 + 1 + 4 + 8 + 8 + 8 + 32 + 1 + 4 + 4) {
            throw new IllegalArgumentException("message record is truncated");
        }
        final int version = input.getInt();
        if (version != 1 && version != 2) {
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
        final int payloadKind = version == 1 ? 1 : input.get() & 0xff;
        if (payloadKind != 1 && payloadKind != 2) {
            throw new IllegalArgumentException("unknown message payload kind");
        }
        final int sourceLength = input.getInt();
        if (sourceLength < 0 || sourceLength > input.remaining()) {
            throw new IllegalArgumentException("invalid source position length");
        }
        final byte[] source = new byte[sourceLength];
        input.get(source);
        final int payloadLength = input.getInt();
        if (payloadLength < 0 || payloadLength > input.remaining()) {
            throw new IllegalArgumentException("invalid message payload length");
        }
        final byte[] payload = new byte[payloadLength];
        input.get(payload);
        final PayloadReference payloadReference;
        if (version == 1) {
            if (payloadLength != input.remaining()) {
                throw new IllegalArgumentException("invalid message payload length");
            }
            payloadReference = null;
        } else {
            if (payloadKind == 2 && payloadLength != 0) {
                throw new IllegalArgumentException("object payload has inline bytes");
            }
            if (input.remaining() < 4) {
                throw new IllegalArgumentException("missing object payload reference length");
            }
            final int referenceLength = input.getInt();
            if (referenceLength < 0 || referenceLength != input.remaining()) {
                throw new IllegalArgumentException("invalid object payload reference length");
            }
            payloadReference = payloadKind == 1 ? null : PayloadReference.decode(readBytes(input, referenceLength));
            if (payloadKind == 1 && referenceLength != 0) {
                throw new IllegalArgumentException("inline payload has an object reference");
            }
        }
        final OrderingMode mode = switch (ordering) {
            case 1 -> OrderingMode.BEST_EFFORT;
            case 2 -> OrderingMode.DELIVERY_TIME_FIFO;
            default -> throw new IllegalArgumentException("unknown ordering mode: " + ordering);
        };
        final MessageRecord result = new MessageRecord(status, generation, stateVersion, deliverAt, expireAt,
                new DestinationLaneId(lane), mode, payload, source, payloadReference);
        if (!Arrays.equals(encoded, result.encode())) {
            // Version 1 records remain readable, but all new writes use version 2.
            if (version != 1) {
                throw new IllegalArgumentException("non-canonical message record");
            }
        }
        return result;
    }

    private static byte[] readBytes(final ByteBuffer input, final int length) {
        if (length < 0 || length > input.remaining()) {
            throw new IllegalArgumentException("message payload reference outside value");
        }
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }
}
