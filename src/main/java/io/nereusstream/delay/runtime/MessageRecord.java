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
        PayloadReference payloadReference,
        long retryEligibilityAtEpochMs,
        GenerationRuntimeIndex runtimeIndex) {
    public MessageRecord(final MessageStatus status, final int generation, final long stateVersion,
                         final long deliverAtEpochMs, final long expireAtEpochMs, final DestinationLaneId laneId,
                         final OrderingMode orderingMode, final byte[] payload, final byte[] scheduleSourcePosition) {
        this(status, generation, stateVersion, deliverAtEpochMs, expireAtEpochMs, laneId, orderingMode, payload,
                scheduleSourcePosition, null, deliverAtEpochMs, legacyRuntimeIndex(status, stateVersion));
    }

    public MessageRecord(final MessageStatus status, final int generation, final long stateVersion,
                         final long deliverAtEpochMs, final long expireAtEpochMs, final DestinationLaneId laneId,
                         final OrderingMode orderingMode, final byte[] payload, final byte[] scheduleSourcePosition,
                         final PayloadReference payloadReference) {
        this(status, generation, stateVersion, deliverAtEpochMs, expireAtEpochMs, laneId, orderingMode, payload,
                scheduleSourcePosition, payloadReference, deliverAtEpochMs, legacyRuntimeIndex(status, stateVersion));
    }

    /** Compatibility constructor for the pre-runtime-index value shape. */
    public MessageRecord(final MessageStatus status, final int generation, final long stateVersion,
                         final long deliverAtEpochMs, final long expireAtEpochMs, final DestinationLaneId laneId,
                         final OrderingMode orderingMode, final byte[] payload, final byte[] scheduleSourcePosition,
                         final PayloadReference payloadReference, final long retryEligibilityAtEpochMs) {
        this(status, generation, stateVersion, deliverAtEpochMs, expireAtEpochMs, laneId, orderingMode, payload,
                scheduleSourcePosition, payloadReference, retryEligibilityAtEpochMs,
                legacyRuntimeIndex(status, stateVersion));
    }

    public MessageRecord {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(orderingMode, "orderingMode");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(scheduleSourcePosition, "scheduleSourcePosition");
        Objects.requireNonNull(runtimeIndex, "runtimeIndex");
        if (generation < 0 || stateVersion < 0 || deliverAtEpochMs < 0 || expireAtEpochMs < deliverAtEpochMs
                || retryEligibilityAtEpochMs < 0 || retryEligibilityAtEpochMs > expireAtEpochMs) {
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

    public MessageRecord withRuntimeIndex(final GenerationRuntimeIndex nextRuntimeIndex) {
        return new MessageRecord(status, generation, stateVersion, deliverAtEpochMs, expireAtEpochMs, laneId,
                orderingMode, payload, scheduleSourcePosition, payloadReference, retryEligibilityAtEpochMs,
                nextRuntimeIndex);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof MessageRecord that)) {
            return false;
        }
        return status == that.status && generation == that.generation && stateVersion == that.stateVersion
                && deliverAtEpochMs == that.deliverAtEpochMs && expireAtEpochMs == that.expireAtEpochMs
                && retryEligibilityAtEpochMs == that.retryEligibilityAtEpochMs
                && laneId.equals(that.laneId) && orderingMode == that.orderingMode
                && Arrays.equals(payload, that.payload)
                && Arrays.equals(scheduleSourcePosition, that.scheduleSourcePosition)
                && Objects.equals(payloadReference, that.payloadReference)
                && runtimeIndex.equals(that.runtimeIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, generation, stateVersion, deliverAtEpochMs, expireAtEpochMs,
                retryEligibilityAtEpochMs, laneId, orderingMode, Arrays.hashCode(payload),
                Arrays.hashCode(scheduleSourcePosition), payloadReference, runtimeIndex);
    }

    public byte[] encode() {
        final byte[] reference = payloadReference == null ? new byte[0] : payloadReference.encode();
        final byte[] runtime = runtimeIndex.canonicalBytes();
        final boolean legacy = runtimeIndex.isLegacyCompatibility();
        final int version = legacy ? 3 : 4;
        final ByteBuffer result = ByteBuffer.allocate(4 + 1 + 4 + 8 + 8 + 8 + 8 + 32 + 1 + 1 + 4
                + scheduleSourcePosition.length + 4 + payload.length + 4 + reference.length
                + (legacy ? 0 : 4 + runtime.length));
        result.putInt(version).put((byte) status.wireValue()).putInt(generation).putLong(stateVersion)
                .putLong(deliverAtEpochMs).putLong(expireAtEpochMs).putLong(retryEligibilityAtEpochMs)
                .put(laneId.bytes()).put((byte) orderingMode.wireValue())
                .put((byte) (payloadReference == null ? 1 : 2))
                .putInt(scheduleSourcePosition.length).put(scheduleSourcePosition);
        if (payloadReference == null) {
            result.putInt(payload.length).put(payload).putInt(0);
        } else {
            result.putInt(0).putInt(reference.length).put(reference);
        }
        if (!legacy) {
            result.putInt(runtime.length).put(runtime);
        }
        return result.array();
    }

    public static MessageRecord decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        final int version = readInt(input, "version");
        if (version != 1 && version != 2 && version != 3) {
            if (version != 4) {
                throw new IllegalArgumentException("unsupported message record version");
            }
        }
        final MessageStatus status = MessageStatus.fromWire(readUnsignedByte(input, "status"));
        final int generation = readInt(input, "generation");
        final long stateVersion = readLong(input, "stateVersion");
        final long deliverAt = readLong(input, "deliverAt");
        final long expireAt = readLong(input, "expireAt");
        final long retryEligibilityAt = version >= 3 ? readLong(input, "retryEligibilityAt") : deliverAt;
        final byte[] lane = readBytes(input, 32, "lane");
        final int ordering = readUnsignedByte(input, "ordering");
        final int payloadKind = version == 1 ? 1 : readUnsignedByte(input, "payloadKind");
        if (payloadKind != 1 && payloadKind != 2) {
            throw new IllegalArgumentException("unknown message payload kind");
        }
        final int sourceLength = readInt(input, "source position length");
        if (sourceLength < 0 || sourceLength > input.remaining()) {
            throw new IllegalArgumentException("invalid source position length");
        }
        final byte[] source = new byte[sourceLength];
        input.get(source);
        final int payloadLength = readInt(input, "payload length");
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
            final int referenceLength = readInt(input, "object payload reference length");
            if (referenceLength < 0 || referenceLength > input.remaining()
                    || (version < 4 && referenceLength != input.remaining())) {
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
        final GenerationRuntimeIndex runtimeIndex;
        if (version >= 4) {
            final long runtimeLength = Integer.toUnsignedLong(readInt(input, "generation runtime index length"));
            if (runtimeLength > input.remaining()) {
                throw new IllegalArgumentException("invalid generation runtime index length");
            }
            final byte[] runtime = new byte[Math.toIntExact(runtimeLength)];
            input.get(runtime);
            if (input.hasRemaining()) {
                throw new IllegalArgumentException("trailing message runtime index bytes");
            }
            runtimeIndex = GenerationRuntimeIndex.decode(runtime);
        } else {
            runtimeIndex = legacyRuntimeIndex(status, stateVersion);
        }
        final MessageRecord result = new MessageRecord(status, generation, stateVersion, deliverAt, expireAt,
                new DestinationLaneId(lane), mode, payload, source, payloadReference, retryEligibilityAt,
                runtimeIndex);
        if (!Arrays.equals(encoded, result.encode())) {
            // Legacy version 1/2/3 records remain readable, but all new writes use version 4.
            if (version >= 4) {
                throw new IllegalArgumentException("non-canonical message record");
            }
        }
        return result;
    }

    private static GenerationRuntimeIndex legacyRuntimeIndex(final MessageStatus status, final long stateVersion) {
        return GenerationRuntimeIndex.legacyNone(GenerationAggregateState.fromMessageStatus(status),
                Math.max(1, stateVersion));
    }

    private static byte[] readBytes(final ByteBuffer input, final int length) {
        if (length < 0 || length > input.remaining()) {
            throw new IllegalArgumentException("message payload reference outside value");
        }
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static byte[] readBytes(final ByteBuffer input, final int length, final String name) {
        if (length < 0 || length > input.remaining()) {
            throw new IllegalArgumentException("message record " + name + " is truncated");
        }
        final byte[] result = new byte[length];
        input.get(result);
        return result;
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
        if (input.remaining() < length) {
            throw new IllegalArgumentException("message record " + name + " is truncated");
        }
    }
}
