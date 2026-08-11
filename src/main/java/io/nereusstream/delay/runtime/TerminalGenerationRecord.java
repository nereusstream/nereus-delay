package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import java.util.Objects;

/** Immutable generation history retained independently from id_cf/MESSAGE. */
public record TerminalGenerationRecord(
        DelayMessageId messageId,
        int generation,
        MessageStatus status,
        io.nereusstream.delay.protocol.StableCode terminalCode,
        long stateVersion,
        byte[] appliedSourcePosition,
        boolean possibleDestinationDuplicate,
        List<AttemptObligationRef> openObligations) {
    private static final int VERSION = 2;

    public TerminalGenerationRecord(final DelayMessageId messageId, final int generation,
                                    final MessageStatus status,
                                    final io.nereusstream.delay.protocol.StableCode terminalCode,
                                    final long stateVersion, final byte[] appliedSourcePosition,
                                    final boolean possibleDestinationDuplicate) {
        this(messageId, generation, status, terminalCode, stateVersion, appliedSourcePosition,
                possibleDestinationDuplicate, List.of());
    }

    public TerminalGenerationRecord {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(terminalCode, "terminalCode");
        Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        Objects.requireNonNull(openObligations, "openObligations");
        if (stateVersion < 0 || status == MessageStatus.SCHEDULED
                || status == MessageStatus.CLAIMED || status == MessageStatus.PUBLISHING) {
            throw new IllegalArgumentException("invalid terminal generation record");
        }
        appliedSourcePosition = Bytes.copy(appliedSourcePosition);
        final List<AttemptObligationRef> sorted = new ArrayList<>(openObligations);
        sorted.sort(TerminalGenerationRecord::compareObligations);
        for (int index = 0; index < sorted.size(); index++) {
            final AttemptObligationRef obligation = Objects.requireNonNull(sorted.get(index),
                    "null terminal obligation");
            if (obligation.generation() != generation) {
                throw new IllegalArgumentException("terminal obligation generation mismatch");
            }
            if (index > 0 && compareObligations(sorted.get(index - 1), obligation) == 0) {
                throw new IllegalArgumentException("duplicate terminal obligation");
            }
        }
        openObligations = Collections.unmodifiableList(sorted);
    }

    @Override
    public byte[] appliedSourcePosition() {
        return Bytes.copy(appliedSourcePosition);
    }

    @Override
    public List<AttemptObligationRef> openObligations() {
        return openObligations;
    }

    public byte[] encode() {
        final byte[] encodedObligations = Bytes.concat(openObligations.stream()
                .map(obligation -> Bytes.lp32(obligation.canonicalBytes()))
                .toArray(byte[][]::new));
        return Bytes.concat(Bytes.u32be(VERSION), messageId.bytes(), Bytes.u32beBits(generation),
                new byte[]{(byte) status.wireValue()}, Bytes.u32be(terminalCode.wireValue()),
                Bytes.u64be(stateVersion), new byte[]{(byte) (possibleDestinationDuplicate ? 1 : 0)},
                Bytes.lp32(appliedSourcePosition), Bytes.u32be(openObligations.size()), encodedObligations);
    }

    public static TerminalGenerationRecord decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        final int version = readInt(input, "version");
        if (version != 1 && version != VERSION) {
            throw new IllegalArgumentException("unsupported terminal generation version");
        }
        final byte[] message = readBytes(input, DelayMessageId.LENGTH, "messageId");
        final int generation = readInt(input, "generation");
        final MessageStatus status = MessageStatus.fromWire(readUnsignedByte(input, "status"));
        final io.nereusstream.delay.protocol.StableCode code =
                io.nereusstream.delay.protocol.StableCode.fromWire(readInt(input, "terminalCode"));
        final long stateVersion = readLong(input, "stateVersion");
        final int duplicate = readUnsignedByte(input, "duplicate-risk flag");
        if (duplicate > 1) {
            throw new IllegalArgumentException("invalid duplicate-risk flag");
        }
        final long sourceLength = Integer.toUnsignedLong(readInt(input, "source position length"));
        if (sourceLength > input.remaining()) {
            throw new IllegalArgumentException("invalid terminal source position length");
        }
        final byte[] source = readBytes(input, Math.toIntExact(sourceLength), "source position");
        final List<AttemptObligationRef> obligations = new ArrayList<>();
        if (version == VERSION) {
            final long count = Integer.toUnsignedLong(readInt(input, "terminal obligation count"));
            if (count > input.remaining() / 4L) {
                throw new IllegalArgumentException("terminal obligation count is invalid");
            }
            for (long index = 0; index < count; index++) {
                final long length = Integer.toUnsignedLong(readInt(input, "terminal obligation length"));
                if (length > input.remaining()) {
                    throw new IllegalArgumentException("terminal obligation bytes are truncated");
                }
                final byte[] obligation = readBytes(input, Math.toIntExact(length), "terminal obligation");
                obligations.add(AttemptObligationRef.decode(obligation));
            }
        }
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing terminal generation bytes");
        }
        final TerminalGenerationRecord result = new TerminalGenerationRecord(new DelayMessageId(message), generation,
                status, code, stateVersion, source, duplicate == 1, obligations);
        if (version == VERSION && !Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical terminal generation record");
        }
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

    private static byte[] readBytes(final ByteBuffer input, final int length, final String name) {
        requireRemaining(input, length, name);
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static void requireRemaining(final ByteBuffer input, final int length, final String name) {
        if (length < 0 || input.remaining() < length) {
            throw new IllegalArgumentException("terminal generation " + name + " is truncated");
        }
    }

    private static int compareObligations(final AttemptObligationRef left, final AttemptObligationRef right) {
        final int id = compareUnsigned(left.publishAttemptId(), right.publishAttemptId());
        return id != 0 ? id : compareUnsigned(left.encodedInflightKey(), right.encodedInflightKey());
    }

    private static int compareUnsigned(final byte[] left, final byte[] right) {
        final int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            final int comparison = Integer.compare(left[index] & 0xff, right[index] & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
