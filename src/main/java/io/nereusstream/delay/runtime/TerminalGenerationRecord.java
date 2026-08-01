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
        if (generation < 0 || stateVersion < 0 || status == MessageStatus.SCHEDULED
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
        return Bytes.concat(Bytes.u32be(VERSION), messageId.bytes(), Bytes.u32be(generation),
                new byte[]{(byte) status.wireValue()}, Bytes.u32be(terminalCode.wireValue()),
                Bytes.u64be(stateVersion), new byte[]{(byte) (possibleDestinationDuplicate ? 1 : 0)},
                Bytes.lp32(appliedSourcePosition), Bytes.u32be(openObligations.size()), encodedObligations);
    }

    public static TerminalGenerationRecord decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.remaining() < 4 + DelayMessageId.LENGTH + 4 + 1 + 4 + 8 + 1 + 4) {
            throw new IllegalArgumentException("terminal generation record is truncated");
        }
        final int version = input.getInt();
        if (version != 1 && version != VERSION) {
            throw new IllegalArgumentException("unsupported terminal generation version");
        }
        final byte[] message = new byte[DelayMessageId.LENGTH];
        input.get(message);
        final int generation = input.getInt();
        final MessageStatus status = MessageStatus.fromWire(input.get() & 0xff);
        final io.nereusstream.delay.protocol.StableCode code =
                io.nereusstream.delay.protocol.StableCode.fromWire(input.getInt());
        final long stateVersion = input.getLong();
        final int duplicate = input.get() & 0xff;
        if (duplicate > 1) {
            throw new IllegalArgumentException("invalid duplicate-risk flag");
        }
        final long sourceLength = Integer.toUnsignedLong(input.getInt());
        if (sourceLength > input.remaining()) {
            throw new IllegalArgumentException("invalid terminal source position length");
        }
        final byte[] source = new byte[Math.toIntExact(sourceLength)];
        input.get(source);
        final List<AttemptObligationRef> obligations = new ArrayList<>();
        if (version == VERSION) {
            if (input.remaining() < 4) {
                throw new IllegalArgumentException("terminal obligation count is truncated");
            }
            final long count = Integer.toUnsignedLong(input.getInt());
            if (count > input.remaining() / 4L) {
                throw new IllegalArgumentException("terminal obligation count is invalid");
            }
            for (long index = 0; index < count; index++) {
                if (input.remaining() < 4) {
                    throw new IllegalArgumentException("terminal obligation length is truncated");
                }
                final long length = Integer.toUnsignedLong(input.getInt());
                if (length > input.remaining()) {
                    throw new IllegalArgumentException("terminal obligation bytes are truncated");
                }
                final byte[] obligation = new byte[Math.toIntExact(length)];
                input.get(obligation);
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
