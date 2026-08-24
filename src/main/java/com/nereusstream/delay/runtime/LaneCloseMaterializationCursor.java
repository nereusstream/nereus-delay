package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Durable, restartable cursor for the local part of a source-ordered Lane close.
 *
 * <p>The close marker owns the semantic decision and transfers the unadmitted
 * quota once.  This value only records where bounded terminal/reservation
 * materialization must resume; it never contains a new source-log decision.</p>
 */
public final class LaneCloseMaterializationCursor {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 11;
    public static final byte SYSTEM_WORK_KIND = 2;
    public static final int HASH_LENGTH = 32;

    public enum Phase {
        MESSAGES(1),
        RESERVATIONS(2);

        private final int wireValue;

        Phase(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }

        public static Phase fromWire(final long value) {
            for (Phase phase : values()) {
                if (phase.wireValue == value) {
                    return phase;
                }
            }
            throw new IllegalArgumentException("unknown Lane close cursor phase: " + value);
        }
    }

    private final DestinationLaneId laneId;
    private final byte[] laneIncarnation;
    private final long closeVersion;
    private final byte[] closeSourcePosition;
    private final Phase phase;
    private final byte[] lastKey;
    private final long transferredPendingMessages;
    private final long transferredPendingBytes;
    private final long transferredReservationMessages;
    private final long transferredReservationBytes;
    private final byte[] digest;

    public LaneCloseMaterializationCursor(
            final DestinationLaneId laneId,
            final byte[] laneIncarnation,
            final long closeVersion,
            final byte[] closeSourcePosition,
            final Phase phase,
            final byte[] lastKey,
            final long transferredPendingMessages,
            final long transferredPendingBytes,
            final long transferredReservationMessages,
            final long transferredReservationBytes) {
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        if (closeVersion <= 0) {
            throw new IllegalArgumentException("closeVersion must be positive");
        }
        Objects.requireNonNull(closeSourcePosition, "closeSourcePosition");
        SourcePositionCodec.decode(closeSourcePosition);
        this.laneIncarnation = Bytes.copy(laneIncarnation);
        this.closeVersion = closeVersion;
        this.closeSourcePosition = Bytes.copy(closeSourcePosition);
        this.phase = Objects.requireNonNull(phase, "phase");
        this.lastKey = lastKey == null ? new byte[0] : Bytes.copy(lastKey);
        validateLastKey(this.phase, this.lastKey);
        if (transferredPendingMessages < 0
                || transferredPendingBytes < 0
                || transferredReservationMessages < 0
                || transferredReservationBytes < 0) {
            throw new IllegalArgumentException("close cursor counters must be non-negative");
        }
        this.transferredPendingMessages = transferredPendingMessages;
        this.transferredPendingBytes = transferredPendingBytes;
        this.transferredReservationMessages = transferredReservationMessages;
        this.transferredReservationBytes = transferredReservationBytes;
        this.digest = computeDigest();
    }

    public DestinationLaneId laneId() {
        return laneId;
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public long closeVersion() {
        return closeVersion;
    }

    public byte[] closeSourcePosition() {
        return Bytes.copy(closeSourcePosition);
    }

    public Phase phase() {
        return phase;
    }

    public byte[] lastKey() {
        return Bytes.copy(lastKey);
    }

    public long transferredPendingMessages() {
        return transferredPendingMessages;
    }

    public long transferredPendingBytes() {
        return transferredPendingBytes;
    }

    public long transferredReservationMessages() {
        return transferredReservationMessages;
    }

    public long transferredReservationBytes() {
        return transferredReservationBytes;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public LaneCloseMaterializationCursor advance(final byte[] nextLastKey) {
        return new LaneCloseMaterializationCursor(
                laneId,
                laneIncarnation,
                closeVersion,
                closeSourcePosition,
                phase,
                nextLastKey,
                transferredPendingMessages,
                transferredPendingBytes,
                transferredReservationMessages,
                transferredReservationBytes);
    }

    public LaneCloseMaterializationCursor nextPhase() {
        if (phase != Phase.MESSAGES) {
            throw new IllegalStateException("Lane close cursor is already in reservation phase");
        }
        return new LaneCloseMaterializationCursor(
                laneId,
                laneIncarnation,
                closeVersion,
                closeSourcePosition,
                Phase.RESERVATIONS,
                null,
                transferredPendingMessages,
                transferredPendingBytes,
                transferredReservationMessages,
                transferredReservationBytes);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, laneId.bytes());
            CanonicalProtobuf.bytes(output, 3, laneIncarnation);
            CanonicalProtobuf.uint64(output, 4, closeVersion);
            CanonicalProtobuf.bytes(output, 5, closeSourcePosition);
            CanonicalProtobuf.uint32(output, 6, phase.wireValue());
            if (lastKey.length != 0) {
                CanonicalProtobuf.bytes(output, 7, lastKey);
            }
            CanonicalProtobuf.uint64(output, 8, transferredPendingMessages);
            CanonicalProtobuf.uint64(output, 9, transferredPendingBytes);
            CanonicalProtobuf.uint64(output, 10, transferredReservationMessages);
            CanonicalProtobuf.uint64(output, 11, transferredReservationBytes);
            CanonicalProtobuf.bytes(output, 12, digest);
        });
    }

    public static LaneCloseMaterializationCursor decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = readAll(new CanonicalProtobuf.Reader(encoded));
        if (fields.size() != 11 && fields.size() != 12) {
            throw new IllegalArgumentException("Lane close cursor has an invalid field count");
        }
        int index = 0;
        if (uint(fields.get(index++), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Lane close cursor version");
        }
        final byte[] lane = bytes(fields.get(index++), 2);
        final byte[] incarnation = bytes(fields.get(index++), 3);
        final long closeVersion = uint(fields.get(index++), 4);
        final byte[] source = bytes(fields.get(index++), 5);
        final Phase phase = Phase.fromWire(uint(fields.get(index++), 6));
        byte[] lastKey = null;
        if (index < fields.size() && fields.get(index).number() == 7) {
            lastKey = bytes(fields.get(index++), 7);
        }
        final long transferredPendingMessages = uint(fields.get(index++), 8);
        final long transferredPendingBytes = uint(fields.get(index++), 9);
        final long transferredReservationMessages = uint(fields.get(index++), 10);
        final long transferredReservationBytes = uint(fields.get(index++), 11);
        final byte[] digest = bytes(fields.get(index++), 12);
        if (index != fields.size()) {
            throw new IllegalArgumentException("unknown Lane close cursor field");
        }
        final LaneCloseMaterializationCursor result = new LaneCloseMaterializationCursor(
                new DestinationLaneId(fixed(lane, 32, "laneId")),
                fixed(incarnation, 16, "laneIncarnation"),
                closeVersion,
                source,
                phase,
                lastKey,
                transferredPendingMessages,
                transferredPendingBytes,
                transferredReservationMessages,
                transferredReservationBytes);
        if (!Arrays.equals(digest, result.digest) || !Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical Lane close cursor");
        }
        return result;
    }

    private byte[] computeDigest() {
        final byte[] fields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, laneId.bytes());
            CanonicalProtobuf.bytes(output, 3, laneIncarnation);
            CanonicalProtobuf.uint64(output, 4, closeVersion);
            CanonicalProtobuf.bytes(output, 5, closeSourcePosition);
            CanonicalProtobuf.uint32(output, 6, phase.wireValue());
            if (lastKey.length != 0) {
                CanonicalProtobuf.bytes(output, 7, lastKey);
            }
            CanonicalProtobuf.uint64(output, 8, transferredPendingMessages);
            CanonicalProtobuf.uint64(output, 9, transferredPendingBytes);
            CanonicalProtobuf.uint64(output, 10, transferredReservationMessages);
            CanonicalProtobuf.uint64(output, 11, transferredReservationBytes);
        });
        return Bytes.sha256(Bytes.utf8("nereus-delay-lane-close-cursor-v1\0"), fields);
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid Lane close cursor varint field " + number);
        }
        return field.unsignedValue();
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid Lane close cursor bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return value;
    }

    private static void validateLastKey(final Phase phase, final byte[] key) {
        if (key.length == 0) {
            return;
        }
        final int expectedLength = phase == Phase.MESSAGES ? 2 + 41 : 2 + 32;
        final byte expectedTag = phase == Phase.MESSAGES ? (byte) 1 : (byte) 2;
        if (key.length != expectedLength || key[0] != expectedTag || key[1] != 1) {
            throw new IllegalArgumentException("Lane close cursor lastKey does not match its phase");
        }
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final CanonicalProtobuf.Reader reader) {
        final List<CanonicalProtobuf.Reader.Field> result = new ArrayList<>();
        while (reader.hasRemaining()) {
            result.add(reader.next());
        }
        return result;
    }
}
