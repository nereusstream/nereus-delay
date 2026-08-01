package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DlqExportStateV1;
import io.nereusstream.delay.protocol.SourcePositionCodec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Durable local DLQ-export outbox projection for one terminal generation.
 *
 * <p>V1 currently creates the deterministic {@link DlqExportStateV1#NOT_CONFIGURED}
 * state because no export policy/target is present in the embedded schedule
 * model.  Future export attempts must advance this same record by the
 * source-ordered {@code DLQ_EXPORT_RESULT_V1} state machine; they may not
 * create a second export identity.</p>
 */
public record DlqExportRecord(
        byte[] dlqExportId,
        DelayMessageId messageId,
        int generation,
        long terminalRevision,
        byte[] exportEnvelopeHash,
        DlqExportStateV1 state,
        int physicalAttemptNo,
        byte[] appliedSourcePosition) {
    public static final int VALUE_TYPE = 8;
    private static final int HASH_LENGTH = 32;
    private static final int VERSION = 1;
    private static final byte[] ID_DOMAIN = Bytes.utf8("nereus-delay-dlq-export-id-v1\0");
    private static final byte[] ENVELOPE_DOMAIN = Bytes.utf8("nereus-delay-dlq-export-envelope-v1\0");

    public DlqExportRecord {
        requireNonZero(dlqExportId, "dlqExportId");
        Objects.requireNonNull(messageId, "messageId");
        if (generation < 0 || terminalRevision <= 0) {
            throw new IllegalArgumentException("invalid DLQ export generation/revision");
        }
        Bytes.requireLength(exportEnvelopeHash, HASH_LENGTH, "exportEnvelopeHash");
        Objects.requireNonNull(state, "state");
        if (state == DlqExportStateV1.NOT_CONFIGURED && physicalAttemptNo != 0
                || state != DlqExportStateV1.NOT_CONFIGURED && physicalAttemptNo <= 0) {
            throw new IllegalArgumentException("DLQ export physical attempt does not match state");
        }
        Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        SourcePositionCodec.decode(appliedSourcePosition);
        if (!Arrays.equals(dlqExportId, deriveId(messageId, generation, terminalRevision))) {
            throw new IllegalArgumentException("DLQ export ID does not match terminal identity");
        }
        dlqExportId = Bytes.copy(dlqExportId);
        exportEnvelopeHash = Bytes.copy(exportEnvelopeHash);
        appliedSourcePosition = Bytes.copy(appliedSourcePosition);
    }

    /** Creates the terminal-time state used when no DLQ policy is configured. */
    public static DlqExportRecord notConfigured(final DelayMessageId messageId, final int generation,
                                                final long terminalRevision,
                                                final byte[] appliedSourcePosition) {
        final byte[] id = deriveId(messageId, generation, terminalRevision);
        return new DlqExportRecord(id, messageId, generation, terminalRevision,
                Bytes.sha256(ENVELOPE_DOMAIN, id, messageId.bytes(), Bytes.u32be(generation),
                        Bytes.u64be(terminalRevision)), DlqExportStateV1.NOT_CONFIGURED, 0,
                appliedSourcePosition);
    }

    /** Creates the first pending attempt for a configured export policy. */
    public static DlqExportRecord pending(final DelayMessageId messageId, final int generation,
                                          final long terminalRevision, final byte[] exportEnvelopeHash,
                                          final byte[] appliedSourcePosition) {
        final byte[] id = deriveId(messageId, generation, terminalRevision);
        return new DlqExportRecord(id, messageId, generation, terminalRevision, exportEnvelopeHash,
                DlqExportStateV1.PENDING, 1, appliedSourcePosition);
    }

    public static byte[] deriveId(final DelayMessageId messageId, final int generation,
                                  final long terminalRevision) {
        Objects.requireNonNull(messageId, "messageId");
        if (generation < 0 || terminalRevision <= 0) {
            throw new IllegalArgumentException("invalid DLQ export identity values");
        }
        return Bytes.sha256(ID_DOMAIN, messageId.bytes(), Bytes.u32be(generation), Bytes.u64be(terminalRevision));
    }

    @Override
    public byte[] dlqExportId() {
        return Bytes.copy(dlqExportId);
    }

    @Override
    public byte[] exportEnvelopeHash() {
        return Bytes.copy(exportEnvelopeHash);
    }

    @Override
    public byte[] appliedSourcePosition() {
        return Bytes.copy(appliedSourcePosition);
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(VERSION), dlqExportId, messageId.bytes(), Bytes.u32be(generation),
                Bytes.u64be(terminalRevision), exportEnvelopeHash, Bytes.u8(state.wireValue()),
                Bytes.u32be(physicalAttemptNo), Bytes.lp32(appliedSourcePosition));
    }

    public static DlqExportRecord decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(Objects.requireNonNull(encoded, "encoded"));
        requireRemaining(input, 4 + HASH_LENGTH + DelayMessageId.LENGTH + 4 + 8 + HASH_LENGTH + 1 + 4 + 4);
        if (input.getInt() != VERSION) {
            throw new IllegalArgumentException("unsupported DLQ export record version");
        }
        final byte[] id = readFixed(input, HASH_LENGTH, "dlqExportId");
        final byte[] message = readFixed(input, DelayMessageId.LENGTH, "messageId");
        final int generation = readU32(input, "generation");
        final long terminalRevision = readU64(input, "terminalRevision");
        final byte[] envelope = readFixed(input, HASH_LENGTH, "exportEnvelopeHash");
        final DlqExportStateV1 state = DlqExportStateV1.fromWire(input.get() & 0xff);
        final int attempt = readU32(input, "physicalAttemptNo");
        final byte[] source = readLp32(input, "appliedSourcePosition");
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing DLQ export record bytes");
        }
        final DlqExportRecord result = new DlqExportRecord(id, new DelayMessageId(message), generation,
                terminalRevision, envelope, state, attempt, source);
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical DLQ export record");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof DlqExportRecord that
                && Arrays.equals(dlqExportId, that.dlqExportId)
                && messageId.equals(that.messageId)
                && generation == that.generation
                && terminalRevision == that.terminalRevision
                && Arrays.equals(exportEnvelopeHash, that.exportEnvelopeHash)
                && state == that.state
                && physicalAttemptNo == that.physicalAttemptNo
                && Arrays.equals(appliedSourcePosition, that.appliedSourcePosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(dlqExportId), messageId, generation, terminalRevision,
                Arrays.hashCode(exportEnvelopeHash), state, physicalAttemptNo,
                Arrays.hashCode(appliedSourcePosition));
    }

    private static int readU32(final ByteBuffer input, final String name) {
        requireRemaining(input, 4);
        final long value = Integer.toUnsignedLong(input.getInt());
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " exceeds supported range");
        }
        return (int) value;
    }

    private static long readU64(final ByteBuffer input, final String name) {
        requireRemaining(input, 8);
        final long value = input.getLong();
        if (value < 0) {
            throw new IllegalArgumentException(name + " exceeds supported range");
        }
        return value;
    }

    private static byte[] readLp32(final ByteBuffer input, final String name) {
        final long length = Integer.toUnsignedLong(readRawInt(input, name + " length"));
        if (length > input.remaining()) {
            throw new IllegalArgumentException(name + " length outside record");
        }
        return readFixed(input, Math.toIntExact(length), name);
    }

    private static int readRawInt(final ByteBuffer input, final String name) {
        requireRemaining(input, 4);
        return input.getInt();
    }

    private static byte[] readFixed(final ByteBuffer input, final int length, final String name) {
        requireRemaining(input, length);
        final byte[] value = new byte[length];
        input.get(value);
        return value;
    }

    private static void requireRemaining(final ByteBuffer input, final int length) {
        if (length < 0 || input.remaining() < length) {
            throw new IllegalArgumentException("DLQ export record is truncated");
        }
    }

    private static void requireNonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        for (byte current : value) {
            if (current != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
