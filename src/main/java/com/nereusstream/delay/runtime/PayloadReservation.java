package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.LargeScheduleIntent;
import com.nereusstream.delay.protocol.PayloadReference;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
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
        PayloadReference committedPayload,
        long receiptAnchorStateVersion,
        byte[] receiptAnchorSourcePosition) {
    private static final int LEGACY_VERSION = 1;
    private static final int VERSION = 2;

    public PayloadReservation {
        Objects.requireNonNull(shardId, "shardId");
        Bytes.requireLength(reservationId, 32, "reservationId");
        Objects.requireNonNull(commandId, "commandId");
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        Bytes.requireLength(commandHash, 32, "commandHash");
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(status, "status");
        sourcePosition = canonicalSourcePosition(shardId, sourcePosition, "sourcePosition");
        receiptAnchorSourcePosition =
                canonicalSourcePosition(shardId, receiptAnchorSourcePosition, "receiptAnchorSourcePosition");
        if (reservationExpiryEpochMs < 0
                || stateVersion <= 0
                || sourcePosition.length == 0
                || receiptAnchorStateVersion <= 0
                || receiptAnchorStateVersion > stateVersion
                || receiptAnchorSourcePosition.length == 0) {
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
        receiptAnchorSourcePosition = Bytes.copy(receiptAnchorSourcePosition);
    }

    /**
     * Compatibility constructor for callers that construct a fresh Prepare
     * reservation or a local test projection without an explicit anchor. The
     * current state is the only safe anchor in that case; durable Shard
     * transitions use {@link #withLifecycle} and retain the original anchor.
     */
    public PayloadReservation(
            final ShardId shardId,
            final byte[] reservationId,
            final CommandId commandId,
            final DelayMessageId delayMessageId,
            final byte[] commandHash,
            final LargeScheduleIntent intent,
            final long reservationExpiryEpochMs,
            final PayloadReservationStatus status,
            final long stateVersion,
            final byte[] sourcePosition,
            final PayloadReference committedPayload) {
        this(
                shardId,
                reservationId,
                commandId,
                delayMessageId,
                commandHash,
                intent,
                reservationExpiryEpochMs,
                status,
                stateVersion,
                sourcePosition,
                committedPayload,
                stateVersion,
                sourcePosition);
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

    @Override
    public byte[] receiptAnchorSourcePosition() {
        return Bytes.copy(receiptAnchorSourcePosition);
    }

    /** Returns the immutable Prepare projection used to issue a receipt. */
    public PayloadReservation receiptAnchor() {
        return new PayloadReservation(
                shardId,
                reservationId,
                commandId,
                delayMessageId,
                commandHash,
                intent,
                reservationExpiryEpochMs,
                PayloadReservationStatus.RESERVED,
                receiptAnchorStateVersion,
                receiptAnchorSourcePosition,
                null,
                receiptAnchorStateVersion,
                receiptAnchorSourcePosition);
    }

    /**
     * Creates a lifecycle projection while retaining the original Prepare
     * receipt anchor. The source/state transition itself remains caller-owned.
     */
    public PayloadReservation withLifecycle(
            final PayloadReservationStatus nextStatus,
            final long nextStateVersion,
            final byte[] nextSourcePosition,
            final PayloadReference nextPayload) {
        return new PayloadReservation(
                shardId,
                reservationId,
                commandId,
                delayMessageId,
                commandHash,
                intent,
                reservationExpiryEpochMs,
                nextStatus,
                nextStateVersion,
                nextSourcePosition,
                nextPayload,
                receiptAnchorStateVersion,
                receiptAnchorSourcePosition);
    }

    /** Rebinds only the internal receipt anchor while preserving this state. */
    public PayloadReservation withReceiptAnchor(final PayloadReservation anchor) {
        Objects.requireNonNull(anchor, "anchor");
        if (!sameReservationIdentity(anchor)) {
            throw new IllegalArgumentException("receipt anchor does not match reservation identity");
        }
        return new PayloadReservation(
                shardId,
                reservationId,
                commandId,
                delayMessageId,
                commandHash,
                intent,
                reservationExpiryEpochMs,
                status,
                stateVersion,
                sourcePosition,
                committedPayload,
                anchor.receiptAnchorStateVersion,
                anchor.receiptAnchorSourcePosition);
    }

    private boolean sameReservationIdentity(final PayloadReservation other) {
        return shardId.equals(other.shardId)
                && Arrays.equals(reservationId, other.reservationId)
                && commandId.equals(other.commandId)
                && delayMessageId.equals(other.delayMessageId)
                && Arrays.equals(commandHash, other.commandHash)
                && intent.equals(other.intent)
                && reservationExpiryEpochMs == other.reservationExpiryEpochMs;
    }

    /**
     * Reservation lifecycle values are source-ordered durable state.  A
     * non-empty byte string is not sufficient evidence of a Source Position:
     * accepting one would let a malformed or foreign-shard reservation carry
     * an untrusted receipt/GC anchor until a later path happened to decode it.
     */
    private static byte[] canonicalSourcePosition(final ShardId shardId, final byte[] encoded, final String name) {
        Objects.requireNonNull(encoded, name);
        final SourcePosition decoded = SourcePositionCodec.decode(encoded);
        if (!shardId.equals(decoded.shardId())) {
            throw new IllegalArgumentException(name + " belongs to another shard");
        }
        return decoded.canonicalBytes();
    }

    public byte[] encode() {
        return encode(VERSION);
    }

    private byte[] encode(final int version) {
        final byte[] payload = committedPayload == null ? new byte[0] : committedPayload.encode();
        final byte[] base = Bytes.concat(
                Bytes.u32be(version),
                shardId.routeIncarnation().bytes(),
                Bytes.u32beBits(shardId.partition()),
                reservationId,
                commandId.bytes(),
                delayMessageId.bytes(),
                commandHash,
                intent.canonicalBytes(),
                Bytes.u64be(reservationExpiryEpochMs),
                Bytes.u8(status.wireValue()),
                Bytes.u64be(stateVersion),
                Bytes.lp32(sourcePosition),
                Bytes.u8(committedPayload == null ? 0 : 1),
                Bytes.lp32(payload));
        return version == LEGACY_VERSION
                ? base
                : Bytes.concat(base, Bytes.u64be(receiptAnchorStateVersion), Bytes.lp32(receiptAnchorSourcePosition));
    }

    public static PayloadReservation decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        final int version = readInt(input, "version");
        if (version != LEGACY_VERSION && version != VERSION) {
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
        final long receiptAnchorStateVersion;
        final byte[] receiptAnchorSourcePosition;
        if (version == VERSION) {
            receiptAnchorStateVersion = readLong(input, "receiptAnchorStateVersion");
            receiptAnchorSourcePosition = readLp32(input);
        } else {
            receiptAnchorStateVersion = stateVersion;
            receiptAnchorSourcePosition = source;
        }
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("payload reservation has trailing bytes");
        }
        final PayloadReservation result = new PayloadReservation(
                new ShardId(route, partition),
                reservationId,
                commandId,
                messageId,
                commandHash,
                intent,
                expiry,
                status,
                stateVersion,
                source,
                payload,
                receiptAnchorStateVersion,
                receiptAnchorSourcePosition);
        if (!Arrays.equals(encoded, result.encode(version))) {
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
        final com.nereusstream.delay.protocol.OrderingMode mode =
                switch (ordering) {
                    case 1 -> com.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT;
                    case 2 -> com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO;
                    default -> throw new IllegalArgumentException("unknown large schedule ordering mode");
                };
        return new LargeScheduleIntent(
                new com.nereusstream.delay.protocol.DestinationLaneId(lane),
                deliver,
                expire,
                mode,
                length,
                sha,
                ttl,
                trustSet);
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
