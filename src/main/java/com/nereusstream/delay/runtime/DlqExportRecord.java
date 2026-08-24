package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DlqExportStateV1;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Durable local DLQ-export outbox projection for one terminal generation.
 *
 * <p>Terminalization without an export policy creates the deterministic
 * {@link DlqExportStateV1#NOT_CONFIGURED} state. Configured outboxes retain the
 * exact canonical charge projection that their result callbacks must echo;
 * every export attempt advances this same record by the source-ordered
 * {@code DLQ_EXPORT_RESULT_V1} state machine and may not create a second export
 * identity.</p>
 */
public record DlqExportRecord(
        byte[] dlqExportId,
        DelayMessageId messageId,
        int generation,
        long terminalRevision,
        byte[] exportEnvelopeHash,
        byte[] retainedCharge,
        DlqExportStateV1 state,
        int physicalAttemptNo,
        byte[] appliedSourcePosition) {
    public static final int VALUE_TYPE = 8;
    private static final int HASH_LENGTH = 32;
    private static final int VERSION = 2;
    private static final int LEGACY_VERSION = 1;
    private static final byte[] ID_DOMAIN = Bytes.utf8("nereus-delay-dlq-export-id-v1\0");
    private static final byte[] ENVELOPE_DOMAIN = Bytes.utf8("nereus-delay-dlq-export-envelope-v1\0");

    public DlqExportRecord {
        requireNonZero(dlqExportId, "dlqExportId");
        Objects.requireNonNull(messageId, "messageId");
        if (terminalRevision == 0) {
            throw new IllegalArgumentException("invalid DLQ export generation/revision");
        }
        Bytes.requireLength(exportEnvelopeHash, HASH_LENGTH, "exportEnvelopeHash");
        retainedCharge = PublishAdmissionBody.ChargeVector.decodeCanonical(retainedCharge)
                .canonicalBytes();
        Objects.requireNonNull(state, "state");
        if (state == DlqExportStateV1.NOT_CONFIGURED && !Arrays.equals(retainedCharge, emptyChargeCanonical())) {
            throw new IllegalArgumentException("NOT_CONFIGURED DLQ export cannot retain a charge");
        }
        if (state == DlqExportStateV1.NOT_CONFIGURED && physicalAttemptNo != 0
                || state != DlqExportStateV1.NOT_CONFIGURED && physicalAttemptNo == 0) {
            throw new IllegalArgumentException("DLQ export physical attempt does not match state");
        }
        Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        final SourcePosition decodedSourcePosition = SourcePositionCodec.decode(appliedSourcePosition);
        if (!messageId.routingId().shardId().equals(decodedSourcePosition.shardId())) {
            throw new IllegalArgumentException("DLQ export source position belongs to another shard");
        }
        if (!Arrays.equals(dlqExportId, deriveId(messageId, generation, terminalRevision))) {
            throw new IllegalArgumentException("DLQ export ID does not match terminal identity");
        }
        dlqExportId = Bytes.copy(dlqExportId);
        exportEnvelopeHash = Bytes.copy(exportEnvelopeHash);
        retainedCharge = Bytes.copy(retainedCharge);
        appliedSourcePosition = decodedSourcePosition.canonicalBytes();
    }

    /** Source-compatible constructor for the legacy zero-charge projection. */
    public DlqExportRecord(
            final byte[] dlqExportId,
            final DelayMessageId messageId,
            final int generation,
            final long terminalRevision,
            final byte[] exportEnvelopeHash,
            final DlqExportStateV1 state,
            final int physicalAttemptNo,
            final byte[] appliedSourcePosition) {
        this(
                dlqExportId,
                messageId,
                generation,
                terminalRevision,
                exportEnvelopeHash,
                emptyChargeCanonical(),
                state,
                physicalAttemptNo,
                appliedSourcePosition);
    }

    /** Creates the terminal-time state used when no DLQ policy is configured. */
    public static DlqExportRecord notConfigured(
            final DelayMessageId messageId,
            final int generation,
            final long terminalRevision,
            final byte[] appliedSourcePosition) {
        final byte[] id = deriveId(messageId, generation, terminalRevision);
        return new DlqExportRecord(
                id,
                messageId,
                generation,
                terminalRevision,
                Bytes.sha256(
                        ENVELOPE_DOMAIN,
                        id,
                        messageId.bytes(),
                        Bytes.u32beBits(generation),
                        Bytes.u64beBits(terminalRevision)),
                emptyChargeCanonical(),
                DlqExportStateV1.NOT_CONFIGURED,
                0,
                appliedSourcePosition);
    }

    /** Creates the first pending attempt for a configured export policy. */
    public static DlqExportRecord pending(
            final DelayMessageId messageId,
            final int generation,
            final long terminalRevision,
            final byte[] exportEnvelopeHash,
            final byte[] appliedSourcePosition) {
        return pending(
                messageId,
                generation,
                terminalRevision,
                exportEnvelopeHash,
                emptyChargeCanonical(),
                appliedSourcePosition);
    }

    /** Creates a configured export attempt with the exact retained charge projection. */
    public static DlqExportRecord pending(
            final DelayMessageId messageId,
            final int generation,
            final long terminalRevision,
            final byte[] exportEnvelopeHash,
            final byte[] retainedCharge,
            final byte[] appliedSourcePosition) {
        final byte[] id = deriveId(messageId, generation, terminalRevision);
        return new DlqExportRecord(
                id,
                messageId,
                generation,
                terminalRevision,
                exportEnvelopeHash,
                retainedCharge,
                DlqExportStateV1.PENDING,
                1,
                appliedSourcePosition);
    }

    public static byte[] deriveId(final DelayMessageId messageId, final int generation, final long terminalRevision) {
        Objects.requireNonNull(messageId, "messageId");
        if (terminalRevision == 0) {
            throw new IllegalArgumentException("invalid DLQ export identity values");
        }
        return Bytes.sha256(
                ID_DOMAIN, messageId.bytes(), Bytes.u32beBits(generation), Bytes.u64beBits(terminalRevision));
    }

    @Override
    public byte[] dlqExportId() {
        return Bytes.copy(dlqExportId);
    }

    @Override
    public byte[] exportEnvelopeHash() {
        return Bytes.copy(exportEnvelopeHash);
    }

    public byte[] retainedCharge() {
        return Bytes.copy(retainedCharge);
    }

    @Override
    public byte[] appliedSourcePosition() {
        return Bytes.copy(appliedSourcePosition);
    }

    public byte[] encode() {
        return Bytes.concat(
                Bytes.u32be(VERSION),
                dlqExportId,
                messageId.bytes(),
                Bytes.u32beBits(generation),
                Bytes.u64beBits(terminalRevision),
                exportEnvelopeHash,
                Bytes.lp32(retainedCharge),
                Bytes.u8(state.wireValue()),
                Bytes.u32beBits(physicalAttemptNo),
                Bytes.lp32(appliedSourcePosition));
    }

    public static DlqExportRecord decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(Objects.requireNonNull(encoded, "encoded"));
        requireRemaining(input, 4);
        final int version = input.getInt();
        if (version != VERSION && version != LEGACY_VERSION) {
            throw new IllegalArgumentException("unsupported DLQ export record version");
        }
        requireRemaining(input, HASH_LENGTH + DelayMessageId.LENGTH + 4 + 8 + HASH_LENGTH + 1 + 4 + 4);
        final byte[] id = readFixed(input, HASH_LENGTH, "dlqExportId");
        final byte[] message = readFixed(input, DelayMessageId.LENGTH, "messageId");
        final int generation = readU32(input, "generation");
        final long terminalRevision = readU64(input, "terminalRevision");
        final byte[] envelope = readFixed(input, HASH_LENGTH, "exportEnvelopeHash");
        final byte[] retainedCharge = version == VERSION ? readLp32(input, "retainedCharge") : emptyChargeCanonical();
        final DlqExportStateV1 state = DlqExportStateV1.fromWire(input.get() & 0xff);
        final int attempt = readU32(input, "physicalAttemptNo");
        final byte[] source = readLp32(input, "appliedSourcePosition");
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing DLQ export record bytes");
        }
        final DlqExportRecord result = new DlqExportRecord(
                id,
                new DelayMessageId(message),
                generation,
                terminalRevision,
                envelope,
                retainedCharge,
                state,
                attempt,
                source);
        if (!Arrays.equals(encoded, version == VERSION ? result.encode() : result.encodeLegacy())) {
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
                && Arrays.equals(retainedCharge, that.retainedCharge)
                && state == that.state
                && physicalAttemptNo == that.physicalAttemptNo
                && Arrays.equals(appliedSourcePosition, that.appliedSourcePosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                Arrays.hashCode(dlqExportId),
                messageId,
                generation,
                terminalRevision,
                Arrays.hashCode(exportEnvelopeHash),
                Arrays.hashCode(retainedCharge),
                state,
                physicalAttemptNo,
                Arrays.hashCode(appliedSourcePosition));
    }

    private byte[] encodeLegacy() {
        return Bytes.concat(
                Bytes.u32be(LEGACY_VERSION),
                dlqExportId,
                messageId.bytes(),
                Bytes.u32beBits(generation),
                Bytes.u64beBits(terminalRevision),
                exportEnvelopeHash,
                Bytes.u8(state.wireValue()),
                Bytes.u32beBits(physicalAttemptNo),
                Bytes.lp32(appliedSourcePosition));
    }

    private static byte[] emptyChargeCanonical() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                .canonicalBytes();
    }

    private static int readU32(final ByteBuffer input, final String name) {
        requireRemaining(input, 4);
        return input.getInt();
    }

    private static long readU64(final ByteBuffer input, final String name) {
        requireRemaining(input, 8);
        final long value = input.getLong();
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
