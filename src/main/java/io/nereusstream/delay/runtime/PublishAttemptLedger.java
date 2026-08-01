package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.store.KeyCodec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Durable open publish-attempt projection.
 *
 * <p>The admission and outcome bytes are retained verbatim so replay can later validate the full Registry body
 * without reconstructing it from mutable runtime state. This embedded V1 subset does not yet interpret all nested
 * Claim/Certificate/Channel fields.</p>
 */
public final class PublishAttemptLedger {
    public static final int VALUE_TYPE = 8;
    public static final int HASH_LENGTH = 32;
    public static final int INCARNATION_LENGTH = 16;

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
    private final AttemptLedgerState state;
    private final byte[] outcomeBytes;
    private final byte[] evidenceBytes;
    private final byte[] sourcePosition;

    public PublishAttemptLedger(final DelayMessageId delayMessageId, final int generation,
                                final byte[] publishAttemptId, final byte[] claimId, final long ownerEpoch,
                                final int attemptNo, final DestinationLaneId laneId, final byte[] laneIncarnation,
                                final byte[] ownerIdentity, final byte[] storeIncarnation,
                                final byte[] preparedPublishHash, final byte[] admissionBytes,
                                final AttemptLedgerState state, final byte[] outcomeBytes,
                                final byte[] evidenceBytes, final byte[] sourcePosition) {
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (generation < 0 || ownerEpoch <= 0 || attemptNo <= 0) {
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
        this.state = Objects.requireNonNull(state, "state");
        this.outcomeBytes = optional(outcomeBytes);
        this.evidenceBytes = optional(evidenceBytes);
        this.sourcePosition = nonEmpty(sourcePosition, "sourcePosition");
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

    public PublishAttemptLedger withUnknownOutcome(final byte[] outcome, final byte[] evidence,
                                                   final byte[] outcomeSourcePosition) {
        return new PublishAttemptLedger(delayMessageId, generation, publishAttemptId, claimId, ownerEpoch, attemptNo,
                laneId, laneIncarnation, ownerIdentity, storeIncarnation, preparedPublishHash, admissionBytes,
                AttemptLedgerState.UNCERTAIN, outcome, evidence, outcomeSourcePosition);
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

    public byte[] encodedKey() {
        return KeyCodec.inflight(state == AttemptLedgerState.PUBLISHING ? (byte) 2 : (byte) 3, ownerEpoch,
                publishAttemptId);
    }

    public AttemptObligationRef obligationRef() {
        return new AttemptObligationRef(publishAttemptId, generation, state, encodedKey());
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(1), delayMessageId.bytes(), Bytes.u32be(generation), publishAttemptId,
                claimId, Bytes.u64be(ownerEpoch), Bytes.u32be(attemptNo), laneId.bytes(), laneIncarnation,
                Bytes.lp32(ownerIdentity), storeIncarnation, preparedPublishHash, Bytes.lp32(admissionBytes),
                new byte[]{(byte) state.wireValue()}, Bytes.lp32(outcomeBytes), Bytes.lp32(evidenceBytes),
                Bytes.lp32(sourcePosition));
    }

    public static PublishAttemptLedger decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        requireRemaining(input, 4 + DelayMessageId.LENGTH + 4 + HASH_LENGTH * 3 + 8 + 4 + 32 + 16 + 1 + 16 + 32);
        if (input.getInt() != 1) {
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
        final byte[] owner = readLp32(input, "ownerIdentity");
        final byte[] store = readFixed(input, INCARNATION_LENGTH, "storeIncarnation");
        final byte[] preparedHash = readFixed(input, HASH_LENGTH, "preparedPublishHash");
        final byte[] admission = readLp32(input, "admissionBytes");
        requireRemaining(input, 1);
        final AttemptLedgerState state = AttemptLedgerState.fromWire(input.get() & 0xff);
        final byte[] outcome = readLp32(input, "outcomeBytes");
        final byte[] evidence = readLp32(input, "evidenceBytes");
        final byte[] source = readLp32(input, "sourcePosition");
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing publish attempt ledger bytes");
        }
        final PublishAttemptLedger result = new PublishAttemptLedger(new DelayMessageId(message), generation, attempt,
                claim, ownerEpoch, attemptNo, new DestinationLaneId(lane), laneIncarnation, owner, store, preparedHash,
                admission, state, outcome, evidence, source);
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

    private static void requireRemaining(final ByteBuffer input, final int length) {
        if (length < 0 || input.remaining() < length) {
            throw new IllegalArgumentException("publish attempt ledger is truncated");
        }
    }

    private static int readU32Int(final ByteBuffer input, final String name) {
        final long value = Integer.toUnsignedLong(input.getInt());
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " exceeds Java int range");
        }
        return (int) value;
    }

    private static long readU64(final ByteBuffer input, final String name) {
        final long value = input.getLong();
        if (value < 0) {
            throw new IllegalArgumentException(name + " exceeds signed range");
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
        result = 31 * result + Arrays.hashCode(admissionBytes);
        result = 31 * result + Arrays.hashCode(outcomeBytes);
        result = 31 * result + Arrays.hashCode(evidenceBytes);
        result = 31 * result + Arrays.hashCode(sourcePosition);
        return result;
    }
}
