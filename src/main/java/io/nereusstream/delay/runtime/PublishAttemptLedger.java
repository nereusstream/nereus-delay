package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.store.KeyCodec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Durable open publish-attempt projection.
 *
 * <p>The admission and outcome bytes are retained verbatim so replay can later validate the full Registry body
 * without reconstructing it from mutable runtime state. Version 1 remains readable for legacy opaque ledgers;
 * canonical source-applied Admissions use version 2 to persist the immutable retry window alongside those bytes.
 * Version 3 is an optional local projection for a target adapter's Pulsar Attempt Journal binding: it records the
 * allocated sequence, the latest acknowledged Journal position and the retirement-pending fence. These fields are
 * not new wire fields in {@code PublishAdmissionV1}; they are only populated after the adapter has an exact local
 * producer identity and durable Journal evidence. This embedded V1 subset does not yet interpret all nested
 * Claim/Certificate/Channel fields.</p>
 */
public final class PublishAttemptLedger {
    public static final int VALUE_TYPE = 8;
    public static final int HASH_LENGTH = 32;
    public static final int INCARNATION_LENGTH = 16;
    private static final int LEGACY_VERSION = 1;
    private static final int RETRY_WINDOW_VERSION = 2;
    private static final int JOURNAL_VERSION = 3;
    /** A legacy ledger does not carry the independently typed retry window. */
    private static final long ABSENT_RETRY_WINDOW = -1L;
    /** A ledger without a target-specific Journal binding has no allocated sequence. */
    private static final long ABSENT_SEQUENCE_ID = -1L;
    private static final int MAPPING_DURABLE_FLAG = 1;
    private static final int RETIREMENT_PENDING_FLAG = 2;
    private static final int MAX_JOURNAL_POSITION_BYTES = 128;

    private final DelayMessageId delayMessageId;
    private final int generation;
    private final byte[] publishAttemptId;
    private final byte[] claimId;
    private final long ownerEpoch;
    private final int attemptNo;
    private final DestinationLaneId laneId;
    private final byte[] laneIncarnation;
    private final byte[] ownerIdentity;
    private final byte[] storeIncarnation;
    private final byte[] preparedPublishHash;
    private final byte[] admissionBytes;
    private final long firstAttemptAtEpochMs;
    private final long retryDeadlineEpochMs;
    private final AttemptLedgerState state;
    private final byte[] outcomeBytes;
    private final byte[] evidenceBytes;
    private final byte[] sourcePosition;
    private final long sequenceId;
    private final boolean mappingDurable;
    private final byte[] journalPosition;
    private final boolean retirementPending;

    public PublishAttemptLedger(final DelayMessageId delayMessageId, final int generation,
                                final byte[] publishAttemptId, final byte[] claimId, final long ownerEpoch,
                                final int attemptNo, final DestinationLaneId laneId, final byte[] laneIncarnation,
                                final byte[] ownerIdentity, final byte[] storeIncarnation,
                                final byte[] preparedPublishHash, final byte[] admissionBytes,
                                final AttemptLedgerState state, final byte[] outcomeBytes,
                                final byte[] evidenceBytes, final byte[] sourcePosition) {
        this(delayMessageId, generation, publishAttemptId, claimId, ownerEpoch, attemptNo, laneId, laneIncarnation,
                ownerIdentity, storeIncarnation, preparedPublishHash, admissionBytes, state, outcomeBytes,
                evidenceBytes, sourcePosition, ABSENT_RETRY_WINDOW, ABSENT_RETRY_WINDOW,
                ABSENT_SEQUENCE_ID, false, new byte[0], false);
    }

    private PublishAttemptLedger(final DelayMessageId delayMessageId, final int generation,
                                 final byte[] publishAttemptId, final byte[] claimId, final long ownerEpoch,
                                 final int attemptNo, final DestinationLaneId laneId, final byte[] laneIncarnation,
                                 final byte[] ownerIdentity, final byte[] storeIncarnation,
                                 final byte[] preparedPublishHash, final byte[] admissionBytes,
                                 final AttemptLedgerState state, final byte[] outcomeBytes,
                                 final byte[] evidenceBytes, final byte[] sourcePosition,
                                 final long firstAttemptAtEpochMs, final long retryDeadlineEpochMs,
                                 final long sequenceId, final boolean mappingDurable,
                                 final byte[] journalPosition, final boolean retirementPending) {
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (ownerEpoch == 0 || attemptNo == 0) {
            throw new IllegalArgumentException("invalid publish attempt generation/owner/attempt");
        }
        this.generation = generation;
        this.publishAttemptId = fixed(publishAttemptId, "publishAttemptId");
        this.claimId = fixed(claimId, "claimId");
        this.ownerEpoch = ownerEpoch;
        this.attemptNo = attemptNo;
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "laneIncarnation");
        this.ownerIdentity = nonEmpty(ownerIdentity, "ownerIdentity");
        this.storeIncarnation = fixed(storeIncarnation, INCARNATION_LENGTH, "storeIncarnation");
        this.preparedPublishHash = fixed(preparedPublishHash, "preparedPublishHash");
        this.admissionBytes = nonEmpty(admissionBytes, "admissionBytes");
        if ((firstAttemptAtEpochMs == ABSENT_RETRY_WINDOW) != (retryDeadlineEpochMs == ABSENT_RETRY_WINDOW)) {
            throw new IllegalArgumentException("retry window fields must be present together");
        }
        if (firstAttemptAtEpochMs < ABSENT_RETRY_WINDOW || retryDeadlineEpochMs < ABSENT_RETRY_WINDOW
                || firstAttemptAtEpochMs >= 0 && retryDeadlineEpochMs < firstAttemptAtEpochMs) {
            throw new IllegalArgumentException("invalid persisted retry window");
        }
        this.firstAttemptAtEpochMs = firstAttemptAtEpochMs;
        this.retryDeadlineEpochMs = retryDeadlineEpochMs;
        this.state = Objects.requireNonNull(state, "state");
        this.outcomeBytes = optional(outcomeBytes);
        this.evidenceBytes = optional(evidenceBytes);
        this.sourcePosition = SourcePositionCodec.decode(sourcePosition).canonicalBytes();
        if (sequenceId < ABSENT_SEQUENCE_ID) {
            throw new IllegalArgumentException("invalid Attempt Journal sequence ID");
        }
        this.sequenceId = sequenceId;
        this.mappingDurable = mappingDurable;
        this.journalPosition = optionalBounded(journalPosition, "journalPosition");
        this.retirementPending = retirementPending;
        if (mappingDurable && (sequenceId == ABSENT_SEQUENCE_ID || this.journalPosition.length == 0)) {
            throw new IllegalArgumentException("durable Journal mapping requires sequence and position");
        }
        if (this.journalPosition.length != 0 && !mappingDurable) {
            throw new IllegalArgumentException("Journal position requires a durable mapping");
        }
        if (retirementPending && (state != AttemptLedgerState.PUBLISHING || !mappingDurable)) {
            throw new IllegalArgumentException("retirement-pending attempt must be a mapped PUBLISHING ledger");
        }
        if (state == AttemptLedgerState.PUBLISHING && (this.outcomeBytes.length != 0 || this.evidenceBytes.length != 0)) {
            throw new IllegalArgumentException("PUBLISHING ledger cannot carry outcome/evidence");
        }
        if (state == AttemptLedgerState.UNCERTAIN && this.outcomeBytes.length == 0) {
            throw new IllegalArgumentException("UNCERTAIN ledger requires an initial outcome");
        }
    }

    public static PublishAttemptLedger publishing(final DelayMessageId delayMessageId, final int generation,
                                                  final byte[] publishAttemptId, final byte[] claimId,
                                                  final long ownerEpoch, final int attemptNo,
                                                  final DestinationLaneId laneId, final byte[] laneIncarnation,
                                                  final byte[] ownerIdentity, final byte[] storeIncarnation,
                                                  final byte[] preparedPublishHash, final byte[] admissionBytes,
                                                  final byte[] sourcePosition) {
        return new PublishAttemptLedger(delayMessageId, generation, publishAttemptId, claimId, ownerEpoch, attemptNo,
                laneId, laneIncarnation, ownerIdentity, storeIncarnation, preparedPublishHash, admissionBytes,
                AttemptLedgerState.PUBLISHING, new byte[0], new byte[0], sourcePosition);
    }

    /** Creates a canonical V2 Admission ledger with the immutable retry window. */
    public static PublishAttemptLedger publishingWithRetryWindow(final DelayMessageId delayMessageId,
                                                                  final int generation,
                                                                  final byte[] publishAttemptId,
                                                                  final byte[] claimId,
                                                                  final long ownerEpoch,
                                                                  final int attemptNo,
                                                                  final DestinationLaneId laneId,
                                                                  final byte[] laneIncarnation,
                                                                  final byte[] ownerIdentity,
                                                                  final byte[] storeIncarnation,
                                                                  final byte[] preparedPublishHash,
                                                                  final byte[] admissionBytes,
                                                                  final long firstAttemptAtEpochMs,
                                                                  final long retryDeadlineEpochMs,
                                                                  final byte[] sourcePosition) {
        return new PublishAttemptLedger(delayMessageId, generation, publishAttemptId, claimId, ownerEpoch, attemptNo,
                laneId, laneIncarnation, ownerIdentity, storeIncarnation, preparedPublishHash, admissionBytes,
                AttemptLedgerState.PUBLISHING, new byte[0], new byte[0], sourcePosition,
                firstAttemptAtEpochMs, retryDeadlineEpochMs, ABSENT_SEQUENCE_ID, false, new byte[0], false);
    }

    public PublishAttemptLedger withUnknownOutcome(final byte[] outcome, final byte[] evidence,
                                                   final byte[] outcomeSourcePosition) {
        if (retirementPending) {
            throw new IllegalStateException("retirement-pending attempt cannot become UNCERTAIN");
        }
        return new PublishAttemptLedger(delayMessageId, generation, publishAttemptId, claimId, ownerEpoch, attemptNo,
                laneId, laneIncarnation, ownerIdentity, storeIncarnation, preparedPublishHash, admissionBytes,
                AttemptLedgerState.UNCERTAIN, outcome, evidence, outcomeSourcePosition,
                firstAttemptAtEpochMs, retryDeadlineEpochMs, sequenceId, mappingDurable, journalPosition, false);
    }

    /** Returns a copy with the adapter-allocated sequence, before Journal append is acknowledged. */
    public PublishAttemptLedger withAllocatedJournalSequence(final long allocatedSequenceId) {
        if (state != AttemptLedgerState.PUBLISHING || allocatedSequenceId < 0) {
            throw new IllegalArgumentException("allocated Journal sequence requires PUBLISHING and non-negative ID");
        }
        if (sequenceId != ABSENT_SEQUENCE_ID && sequenceId != allocatedSequenceId) {
            throw new IllegalStateException("Attempt Journal sequence identity changed");
        }
        return copyWithJournal(allocatedSequenceId, mappingDurable, journalPosition, retirementPending);
    }

    /** Records the exact Journal position after the canonical MAPPED append is durable. */
    public PublishAttemptLedger withDurableJournalMapping(final long mappedSequenceId,
                                                            final byte[] mappedJournalPosition) {
        if (state != AttemptLedgerState.PUBLISHING || mappedSequenceId < 0
                || mappedJournalPosition == null || mappedJournalPosition.length == 0) {
            throw new IllegalArgumentException("durable Journal mapping requires a PUBLISHING attempt and position");
        }
        if (sequenceId != ABSENT_SEQUENCE_ID && sequenceId != mappedSequenceId) {
            throw new IllegalStateException("Attempt Journal sequence identity changed");
        }
        if (mappingDurable) {
            if (sequenceId != mappedSequenceId || !Bytes.constantTimeEquals(journalPosition, mappedJournalPosition)) {
                throw new IllegalStateException("Attempt Journal mapping evidence changed");
            }
            return this;
        }
        return copyWithJournal(mappedSequenceId, true, mappedJournalPosition, retirementPending);
    }

    /** Holds a mapped attempt at the strong-capability retirement barrier. */
    public PublishAttemptLedger withRetirementPending() {
        if (state != AttemptLedgerState.PUBLISHING || !mappingDurable) {
            throw new IllegalStateException("retirement pending requires a durable Journal mapping");
        }
        if (retirementPending) {
            return this;
        }
        return copyWithJournal(sequenceId, true, journalPosition, true);
    }

    /** Records the acknowledged RETIRED_NOT_PUBLISHED position and releases the local fence. */
    public PublishAttemptLedger withDurableRetirement(final byte[] retirementJournalPosition) {
        if (state != AttemptLedgerState.PUBLISHING || !mappingDurable || !retirementPending
                || retirementJournalPosition == null || retirementJournalPosition.length == 0) {
            throw new IllegalStateException("durable retirement requires a pending mapped PUBLISHING attempt");
        }
        return copyWithJournal(sequenceId, true, retirementJournalPosition, false);
    }

    public DelayMessageId delayMessageId() {
        return delayMessageId;
    }

    public int generation() {
        return generation;
    }

    public byte[] publishAttemptId() {
        return Bytes.copy(publishAttemptId);
    }

    public byte[] claimId() {
        return Bytes.copy(claimId);
    }

    public long ownerEpoch() {
        return ownerEpoch;
    }

    public int attemptNo() {
        return attemptNo;
    }

    public DestinationLaneId laneId() {
        return laneId;
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public byte[] ownerIdentity() {
        return Bytes.copy(ownerIdentity);
    }

    public byte[] storeIncarnation() {
        return Bytes.copy(storeIncarnation);
    }

    public byte[] preparedPublishHash() {
        return Bytes.copy(preparedPublishHash);
    }

    public byte[] admissionBytes() {
        return Bytes.copy(admissionBytes);
    }

    /** Returns whether this ledger independently stores the immutable retry window. */
    public boolean hasRetryWindow() {
        return firstAttemptAtEpochMs != ABSENT_RETRY_WINDOW;
    }

    public long firstAttemptAtEpochMs() {
        if (!hasRetryWindow()) {
            throw new IllegalStateException("legacy publish attempt ledger has no retry window");
        }
        return firstAttemptAtEpochMs;
    }

    public long retryDeadlineEpochMs() {
        if (!hasRetryWindow()) {
            throw new IllegalStateException("legacy publish attempt ledger has no retry window");
        }
        return retryDeadlineEpochMs;
    }

    public AttemptLedgerState state() {
        return state;
    }

    public byte[] outcomeBytes() {
        return Bytes.copy(outcomeBytes);
    }

    public byte[] evidenceBytes() {
        return Bytes.copy(evidenceBytes);
    }

    public byte[] sourcePosition() {
        return Bytes.copy(sourcePosition);
    }

    public boolean hasAllocatedJournalSequence() {
        return sequenceId != ABSENT_SEQUENCE_ID;
    }

    public long journalSequenceId() {
        if (!hasAllocatedJournalSequence()) {
            throw new IllegalStateException("Attempt Journal sequence is not allocated");
        }
        return sequenceId;
    }

    public boolean mappingDurable() {
        return mappingDurable;
    }

    public boolean hasJournalPosition() {
        return journalPosition.length != 0;
    }

    public byte[] journalPosition() {
        if (!hasJournalPosition()) {
            throw new IllegalStateException("Attempt Journal position is not recorded");
        }
        return Bytes.copy(journalPosition);
    }

    public boolean retirementPending() {
        return retirementPending;
    }

    public byte[] encodedKey() {
        return KeyCodec.inflight(state == AttemptLedgerState.PUBLISHING ? (byte) 2 : (byte) 3, ownerEpoch,
                publishAttemptId);
    }

    public AttemptObligationRef obligationRef() {
        return new AttemptObligationRef(publishAttemptId, generation, state, encodedKey());
    }

    public byte[] encode() {
        final byte[] retryWindow = (hasRetryWindow() || hasJournalProjection())
                ? Bytes.concat(Bytes.i64be(firstAttemptAtEpochMs), Bytes.i64be(retryDeadlineEpochMs))
                : new byte[0];
        final int version = hasJournalProjection() ? JOURNAL_VERSION
                : hasRetryWindow() ? RETRY_WINDOW_VERSION : LEGACY_VERSION;
        final byte[] journal = hasJournalProjection()
                ? Bytes.concat(Bytes.u64beBits(sequenceId), Bytes.u8(journalFlags()), Bytes.lp32(journalPosition))
                : new byte[0];
        return Bytes.concat(Bytes.u32be(version), delayMessageId.bytes(),
                Bytes.u32beBits(generation), publishAttemptId,
                claimId, Bytes.u64beBits(ownerEpoch), Bytes.u32beBits(attemptNo), laneId.bytes(), laneIncarnation,
                retryWindow, Bytes.lp32(ownerIdentity), storeIncarnation, preparedPublishHash,
                Bytes.lp32(admissionBytes),
                new byte[]{(byte) state.wireValue()}, Bytes.lp32(outcomeBytes), Bytes.lp32(evidenceBytes),
                Bytes.lp32(sourcePosition), journal);
    }

    public static PublishAttemptLedger decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        requireRemaining(input, Integer.BYTES);
        final int version = input.getInt();
        if (version != LEGACY_VERSION && version != RETRY_WINDOW_VERSION && version != JOURNAL_VERSION) {
            throw new IllegalArgumentException("unsupported publish attempt ledger version");
        }
        final byte[] message = readFixed(input, DelayMessageId.LENGTH, "delayMessageId");
        final int generation = readU32Int(input, "generation");
        final byte[] attempt = readFixed(input, HASH_LENGTH, "publishAttemptId");
        final byte[] claim = readFixed(input, HASH_LENGTH, "claimId");
        final long ownerEpoch = readU64(input, "ownerEpoch");
        final int attemptNo = readU32Int(input, "attemptNo");
        final byte[] lane = readFixed(input, 32, "laneId");
        final byte[] laneIncarnation = readFixed(input, INCARNATION_LENGTH, "laneIncarnation");
        final long firstAttemptAt;
        final long retryDeadline;
        if (version == RETRY_WINDOW_VERSION) {
            firstAttemptAt = readI64(input, "firstAttemptAt");
            retryDeadline = readI64(input, "retryDeadline");
        } else if (version == JOURNAL_VERSION) {
            firstAttemptAt = readRetryWindowValue(input, "firstAttemptAt");
            retryDeadline = readRetryWindowValue(input, "retryDeadline");
        } else {
            firstAttemptAt = ABSENT_RETRY_WINDOW;
            retryDeadline = ABSENT_RETRY_WINDOW;
        }
        final byte[] owner = readLp32(input, "ownerIdentity");
        final byte[] store = readFixed(input, INCARNATION_LENGTH, "storeIncarnation");
        final byte[] preparedHash = readFixed(input, HASH_LENGTH, "preparedPublishHash");
        final byte[] admission = readLp32(input, "admissionBytes");
        requireRemaining(input, 1);
        final AttemptLedgerState state = AttemptLedgerState.fromWire(input.get() & 0xff);
        final byte[] outcome = readLp32(input, "outcomeBytes");
        final byte[] evidence = readLp32(input, "evidenceBytes");
        final byte[] source = readLp32(input, "sourcePosition");
        final long sequenceId;
        final boolean mappingDurable;
        final byte[] journalPosition;
        final boolean retirementPending;
        if (version == JOURNAL_VERSION) {
            sequenceId = readU64(input, "journalSequenceId");
            requireRemaining(input, 1);
            final int flags = input.get() & 0xff;
            if ((flags & ~(MAPPING_DURABLE_FLAG | RETIREMENT_PENDING_FLAG)) != 0) {
                throw new IllegalArgumentException("unknown Attempt Journal ledger flags");
            }
            mappingDurable = (flags & MAPPING_DURABLE_FLAG) != 0;
            retirementPending = (flags & RETIREMENT_PENDING_FLAG) != 0;
            journalPosition = readLp32(input, "journalPosition");
        } else {
            sequenceId = ABSENT_SEQUENCE_ID;
            mappingDurable = false;
            journalPosition = new byte[0];
            retirementPending = false;
        }
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing publish attempt ledger bytes");
        }
        final PublishAttemptLedger result = new PublishAttemptLedger(new DelayMessageId(message), generation, attempt,
                claim, ownerEpoch, attemptNo, new DestinationLaneId(lane), laneIncarnation, owner, store, preparedHash,
                admission, state, outcome, evidence, source, firstAttemptAt, retryDeadline,
                sequenceId, mappingDurable, journalPosition, retirementPending);
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical publish attempt ledger");
        }
        return result;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        return fixed(value, HASH_LENGTH, name);
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    private static byte[] optional(final byte[] value) {
        return value == null ? new byte[0] : Bytes.copy(value);
    }

    private static byte[] optionalBounded(final byte[] value, final String name) {
        final byte[] result = optional(value);
        if (result.length > MAX_JOURNAL_POSITION_BYTES) {
            throw new IllegalArgumentException(name + " exceeds local Attempt Journal position bound");
        }
        return result;
    }

    private boolean hasJournalProjection() {
        return hasAllocatedJournalSequence() || mappingDurable || journalPosition.length != 0 || retirementPending;
    }

    private int journalFlags() {
        return (mappingDurable ? MAPPING_DURABLE_FLAG : 0) | (retirementPending ? RETIREMENT_PENDING_FLAG : 0);
    }

    private PublishAttemptLedger copyWithJournal(final long nextSequenceId, final boolean nextMappingDurable,
                                                 final byte[] nextJournalPosition,
                                                 final boolean nextRetirementPending) {
        return new PublishAttemptLedger(delayMessageId, generation, publishAttemptId, claimId, ownerEpoch, attemptNo,
                laneId, laneIncarnation, ownerIdentity, storeIncarnation, preparedPublishHash, admissionBytes,
                state, outcomeBytes, evidenceBytes, sourcePosition, firstAttemptAtEpochMs, retryDeadlineEpochMs,
                nextSequenceId, nextMappingDurable, nextJournalPosition, nextRetirementPending);
    }

    private static void requireRemaining(final ByteBuffer input, final int length) {
        if (length < 0 || input.remaining() < length) {
            throw new IllegalArgumentException("publish attempt ledger is truncated");
        }
    }

    private static int readU32Int(final ByteBuffer input, final String name) {
        requireRemaining(input, Integer.BYTES);
        return input.getInt();
    }

    private static long readU64(final ByteBuffer input, final String name) {
        requireRemaining(input, Long.BYTES);
        return input.getLong();
    }

    private static long readI64(final ByteBuffer input, final String name) {
        requireRemaining(input, Long.BYTES);
        final long value = input.getLong();
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static long readRetryWindowValue(final ByteBuffer input, final String name) {
        requireRemaining(input, Long.BYTES);
        final long value = input.getLong();
        if (value < ABSENT_RETRY_WINDOW) {
            throw new IllegalArgumentException(name + " is below the absent retry-window sentinel");
        }
        return value;
    }

    private static byte[] readFixed(final ByteBuffer input, final int length, final String name) {
        requireRemaining(input, length);
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static byte[] readLp32(final ByteBuffer input, final String name) {
        requireRemaining(input, 4);
        final long length = Integer.toUnsignedLong(input.getInt());
        if (length > input.remaining()) {
            throw new IllegalArgumentException(name + " length outside ledger");
        }
        return readFixed(input, Math.toIntExact(length), name);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PublishAttemptLedger that)) {
            return false;
        }
        return delayMessageId.equals(that.delayMessageId) && generation == that.generation
                && ownerEpoch == that.ownerEpoch && attemptNo == that.attemptNo && laneId.equals(that.laneId)
                && state == that.state && Arrays.equals(publishAttemptId, that.publishAttemptId)
                && Arrays.equals(claimId, that.claimId) && Arrays.equals(laneIncarnation, that.laneIncarnation)
                && Arrays.equals(ownerIdentity, that.ownerIdentity)
                && Arrays.equals(storeIncarnation, that.storeIncarnation)
                && Arrays.equals(preparedPublishHash, that.preparedPublishHash)
                && sequenceId == that.sequenceId && mappingDurable == that.mappingDurable
                && retirementPending == that.retirementPending && Arrays.equals(journalPosition, that.journalPosition)
                && firstAttemptAtEpochMs == that.firstAttemptAtEpochMs
                && retryDeadlineEpochMs == that.retryDeadlineEpochMs
                && Arrays.equals(admissionBytes, that.admissionBytes) && Arrays.equals(outcomeBytes, that.outcomeBytes)
                && Arrays.equals(evidenceBytes, that.evidenceBytes) && Arrays.equals(sourcePosition, that.sourcePosition);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(delayMessageId, generation, ownerEpoch, attemptNo, laneId, state);
        result = 31 * result + Arrays.hashCode(publishAttemptId);
        result = 31 * result + Arrays.hashCode(claimId);
        result = 31 * result + Arrays.hashCode(laneIncarnation);
        result = 31 * result + Arrays.hashCode(ownerIdentity);
        result = 31 * result + Arrays.hashCode(storeIncarnation);
        result = 31 * result + Arrays.hashCode(preparedPublishHash);
        result = 31 * result + Long.hashCode(sequenceId);
        result = 31 * result + Boolean.hashCode(mappingDurable);
        result = 31 * result + Arrays.hashCode(journalPosition);
        result = 31 * result + Boolean.hashCode(retirementPending);
        result = 31 * result + Long.hashCode(firstAttemptAtEpochMs);
        result = 31 * result + Long.hashCode(retryDeadlineEpochMs);
        result = 31 * result + Arrays.hashCode(admissionBytes);
        result = 31 * result + Arrays.hashCode(outcomeBytes);
        result = 31 * result + Arrays.hashCode(evidenceBytes);
        result = 31 * result + Arrays.hashCode(sourcePosition);
        return result;
    }
}
