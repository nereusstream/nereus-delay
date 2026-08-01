package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;

import java.nio.ByteBuffer;
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
        boolean possibleDestinationDuplicate) {
    public TerminalGenerationRecord {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(terminalCode, "terminalCode");
        Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        if (generation < 0 || stateVersion < 0 || status == MessageStatus.SCHEDULED
                || status == MessageStatus.PUBLISHING) {
            throw new IllegalArgumentException("invalid terminal generation record");
        }
        appliedSourcePosition = Bytes.copy(appliedSourcePosition);
    }

    @Override
    public byte[] appliedSourcePosition() {
        return Bytes.copy(appliedSourcePosition);
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(1), messageId.bytes(), Bytes.u32be(generation),
                new byte[]{(byte) status.wireValue()}, Bytes.u32be(terminalCode.wireValue()),
                Bytes.u64be(stateVersion), new byte[]{(byte) (possibleDestinationDuplicate ? 1 : 0)},
                Bytes.lp32(appliedSourcePosition));
    }

    public static TerminalGenerationRecord decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.remaining() < 4 + DelayMessageId.LENGTH + 4 + 1 + 4 + 8 + 1 + 4) {
            throw new IllegalArgumentException("terminal generation record is truncated");
        }
        if (input.getInt() != 1) {
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
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing terminal generation bytes");
        }
        final TerminalGenerationRecord result = new TerminalGenerationRecord(new DelayMessageId(message), generation,
                status, code, stateVersion, source, duplicate == 1);
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical terminal generation record");
        }
        return result;
    }
}
