package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ClaimMaterializationV1;
import io.nereusstream.delay.protocol.ClaimResultBody;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.store.KeyCodec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Durable local projection of one reversible Claim.
 *
 * <p>This is the embedded runtime subset of Registry {@code inflight_cf/CLAIMED}:
 * it retains the exact canonical ClaimPrecondition, the original timeline key,
 * and a local instance digest so a revoke or source-ordered mutation can never
 * guess which current work was claimed.  The full GenerationRuntimeIndex and
 * adapter materialization ownership are intentionally still separate release
 * work.</p>
 */
public final class ClaimRecord {
    public static final int VALUE_TYPE = 9;
    public static final int HASH_LENGTH = 32;
    public static final int INCARNATION_LENGTH = 16;

    private static final byte INFLIGHT_CLAIMED_KIND = 1;

    private final DelayMessageId delayMessageId;
    private final int generation;
    private final byte[] claimId;
    private final long ownerEpoch;
    private final long claimSequence;
    private final DestinationLaneId laneId;
    private final byte[] laneIncarnation;
    private final long laneControlVersion;
    private final long runtimeLaneVersion;
    private final byte[] ownerIdentity;
    private final byte[] storeIncarnation;
    private final byte[] preconditionBytes;
    private final byte[] timelineKey;
    private final long runtimeRevision;
    private final byte[] instanceDigest;
    private final byte[] sourceTimelineWork;

    public ClaimRecord(final DelayMessageId delayMessageId, final int generation, final byte[] claimId,
                       final long ownerEpoch, final long claimSequence, final DestinationLaneId laneId,
                       final byte[] laneIncarnation, final long laneControlVersion, final long runtimeLaneVersion,
                       final byte[] ownerIdentity, final byte[] storeIncarnation, final byte[] preconditionBytes,
                       final byte[] timelineKey, final long runtimeRevision, final byte[] instanceDigest) {
        this(delayMessageId, generation, claimId, ownerEpoch, claimSequence, laneId, laneIncarnation,
                laneControlVersion, runtimeLaneVersion, ownerIdentity, storeIncarnation, preconditionBytes,
                timelineKey, runtimeRevision, instanceDigest, null);
    }

    public ClaimRecord(final DelayMessageId delayMessageId, final int generation, final byte[] claimId,
                       final long ownerEpoch, final long claimSequence, final DestinationLaneId laneId,
                       final byte[] laneIncarnation, final long laneControlVersion, final long runtimeLaneVersion,
                       final byte[] ownerIdentity, final byte[] storeIncarnation, final byte[] preconditionBytes,
                       final byte[] timelineKey, final long runtimeRevision, final byte[] instanceDigest,
                       final byte[] sourceTimelineWork) {
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        if (generation < 0 || ownerEpoch == 0 || claimSequence == 0 || laneControlVersion <= 0
                || runtimeLaneVersion < 0 || runtimeRevision <= 0) {
            throw new IllegalArgumentException("invalid Claim record numeric fields");
        }
        this.generation = generation;
        this.claimId = fixed(claimId, HASH_LENGTH, "claimId");
        this.ownerEpoch = ownerEpoch;
        this.claimSequence = claimSequence;
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "laneIncarnation");
        this.laneControlVersion = laneControlVersion;
        this.runtimeLaneVersion = runtimeLaneVersion;
        this.ownerIdentity = nonEmpty(ownerIdentity, "ownerIdentity");
        this.storeIncarnation = fixed(storeIncarnation, INCARNATION_LENGTH, "storeIncarnation");
        this.preconditionBytes = nonEmpty(preconditionBytes, "preconditionBytes");
        this.timelineKey = nonEmpty(timelineKey, "timelineKey");
        this.runtimeRevision = runtimeRevision;
        this.instanceDigest = fixed(instanceDigest, HASH_LENGTH, "instanceDigest");
        this.sourceTimelineWork = optional(sourceTimelineWork);

        final ClaimResultBody.ClaimPrecondition precondition = ClaimResultBody.decodePrecondition(this.preconditionBytes);
        if (!Arrays.equals(precondition.claimId(), this.claimId)
                || !Arrays.equals(precondition.messageId(), delayMessageId.bytes())
                || precondition.generation() != generation
                || !Arrays.equals(precondition.destinationLaneId(), laneId.bytes())
                || !Arrays.equals(precondition.laneIncarnation(), this.laneIncarnation)
                || precondition.laneControlVersion() != laneControlVersion
                || precondition.runtimeLaneVersion() != runtimeLaneVersion
                || !Arrays.equals(precondition.ownerIdentity(), this.ownerIdentity)
                || !Arrays.equals(precondition.storeIncarnation(), this.storeIncarnation)
                || !Arrays.equals(precondition.originalTimelineKeySha256(), Bytes.sha256(this.timelineKey))) {
            throw new IllegalArgumentException("Claim record does not match its precondition");
        }
        if (this.sourceTimelineWork.length != 0) {
            final TimelineWorkRef work = TimelineWorkRef.decode(this.sourceTimelineWork);
            if (work.workKind().wireValue() != precondition.sourceWorkKind()
                    || !Arrays.equals(work.encodedTimelineKey(), this.timelineKey)
                    || !Arrays.equals(work.semanticWorkDigest(), precondition.sourceTimelineSemanticDigest())) {
                throw new IllegalArgumentException("Claim source timeline does not match its precondition");
            }
        }
        final AuthorIdentity owner = AuthorIdentity.decode(this.ownerIdentity);
        owner.requireFor(io.nereusstream.delay.protocol.SystemMutationType.CLAIM_RESULT);
        if (owner.generation() != ownerEpoch) {
            throw new IllegalArgumentException("Claim owner epoch does not match OwnerIdentity");
        }
        if (!Arrays.equals(this.instanceDigest, computeInstanceDigest(this.preconditionBytes, this.timelineKey,
                runtimeRevision))) {
            throw new IllegalArgumentException("Claim instance digest mismatch");
        }
    }

    public static ClaimRecord claimed(final DelayMessageId delayMessageId, final int generation,
                                      final byte[] claimId, final long ownerEpoch, final long claimSequence,
                                      final DestinationLaneId laneId, final byte[] laneIncarnation,
                                      final long laneControlVersion, final long runtimeLaneVersion,
                                      final byte[] ownerIdentity, final byte[] storeIncarnation,
                                      final byte[] preconditionBytes, final byte[] timelineKey,
                                      final long runtimeRevision) {
        return claimed(delayMessageId, generation, claimId, ownerEpoch, claimSequence, laneId, laneIncarnation,
                laneControlVersion, runtimeLaneVersion, ownerIdentity, storeIncarnation, preconditionBytes,
                timelineKey, runtimeRevision, null);
    }

    public static ClaimRecord claimed(final DelayMessageId delayMessageId, final int generation,
                                      final byte[] claimId, final long ownerEpoch, final long claimSequence,
                                      final DestinationLaneId laneId, final byte[] laneIncarnation,
                                      final long laneControlVersion, final long runtimeLaneVersion,
                                      final byte[] ownerIdentity, final byte[] storeIncarnation,
                                      final byte[] preconditionBytes, final byte[] timelineKey,
                                      final long runtimeRevision, final byte[] sourceTimelineWork) {
        return new ClaimRecord(delayMessageId, generation, claimId, ownerEpoch, claimSequence, laneId,
                laneIncarnation, laneControlVersion, runtimeLaneVersion, ownerIdentity, storeIncarnation,
                preconditionBytes, timelineKey, runtimeRevision,
                computeInstanceDigest(preconditionBytes, timelineKey, runtimeRevision), sourceTimelineWork);
    }

    public DelayMessageId delayMessageId() {
        return delayMessageId;
    }

    public int generation() {
        return generation;
    }

    public byte[] claimId() {
        return Bytes.copy(claimId);
    }

    public long ownerEpoch() {
        return ownerEpoch;
    }

    public long claimSequence() {
        return claimSequence;
    }

    public DestinationLaneId laneId() {
        return laneId;
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public long laneControlVersion() {
        return laneControlVersion;
    }

    public long runtimeLaneVersion() {
        return runtimeLaneVersion;
    }

    public byte[] ownerIdentity() {
        return Bytes.copy(ownerIdentity);
    }

    public byte[] storeIncarnation() {
        return Bytes.copy(storeIncarnation);
    }

    public byte[] preconditionBytes() {
        return Bytes.copy(preconditionBytes);
    }

    /** Returns whether this Claim retained the complete replay-stable materialization. */
    public boolean hasMaterialization() {
        return ClaimResultBody.decodePrecondition(preconditionBytes).hasMaterialization();
    }

    /** Returns the validated typed materialization retained by this Claim. */
    public ClaimMaterializationV1 materialization() {
        return ClaimResultBody.decodePrecondition(preconditionBytes).materializationValue();
    }

    public byte[] timelineKey() {
        return Bytes.copy(timelineKey);
    }

    /** Returns the optional canonical source timeline retained for exact Claim revoke/recovery. */
    public byte[] sourceTimelineWork() {
        return Bytes.copy(sourceTimelineWork);
    }

    public long runtimeRevision() {
        return runtimeRevision;
    }

    public byte[] instanceDigest() {
        return Bytes.copy(instanceDigest);
    }

    public byte[] encodedKey() {
        return KeyCodec.inflight(INFLIGHT_CLAIMED_KIND, ownerEpoch, claimId);
    }

    public byte[] encode() {
        final int version = sourceTimelineWork.length == 0 ? 1 : 2;
        final byte[] base = Bytes.concat(Bytes.u32be(version), delayMessageId.bytes(), Bytes.u32be(generation), claimId,
                Bytes.u64beBits(ownerEpoch), Bytes.u64beBits(claimSequence), laneId.bytes(), laneIncarnation,
                Bytes.u64be(laneControlVersion), Bytes.u64be(runtimeLaneVersion), Bytes.lp32(ownerIdentity),
                storeIncarnation, Bytes.lp32(preconditionBytes), Bytes.lp32(timelineKey),
                Bytes.u64be(runtimeRevision), instanceDigest);
        return sourceTimelineWork.length == 0 ? base : Bytes.concat(base, Bytes.lp32(sourceTimelineWork));
    }

    public static ClaimRecord decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        requireRemaining(input, 4 + DelayMessageId.LENGTH + 4 + HASH_LENGTH + 8 + 8 + 32 + 16 + 8 + 8
                + 4 + INCARNATION_LENGTH + 4 + 4 + 8 + HASH_LENGTH);
        final int version = input.getInt();
        if (version != 1 && version != 2) {
            throw new IllegalArgumentException("unsupported Claim record version");
        }
        final byte[] message = readFixed(input, DelayMessageId.LENGTH, "delayMessageId");
        final int generation = readU32Int(input, "generation");
        final byte[] claimId = readFixed(input, HASH_LENGTH, "claimId");
        final long ownerEpoch = readU64(input, "ownerEpoch");
        final long claimSequence = readU64(input, "claimSequence");
        final byte[] lane = readFixed(input, HASH_LENGTH, "laneId");
        final byte[] laneIncarnation = readFixed(input, INCARNATION_LENGTH, "laneIncarnation");
        final long laneControlVersion = readU64(input, "laneControlVersion");
        final long runtimeLaneVersion = readU64(input, "runtimeLaneVersion");
        final byte[] owner = readLp32(input, "ownerIdentity");
        final byte[] store = readFixed(input, INCARNATION_LENGTH, "storeIncarnation");
        final byte[] precondition = readLp32(input, "preconditionBytes");
        final byte[] timeline = readLp32(input, "timelineKey");
        final long runtimeRevision = readU64(input, "runtimeRevision");
        final byte[] instanceDigest = readFixed(input, HASH_LENGTH, "instanceDigest");
        final byte[] sourceTimelineWork = version == 2
                ? readLp32(input, "sourceTimelineWork") : new byte[0];
        if (version == 2 && sourceTimelineWork.length == 0) {
            throw new IllegalArgumentException("Claim v2 requires sourceTimelineWork");
        }
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing Claim record bytes");
        }
        final ClaimRecord result = new ClaimRecord(new DelayMessageId(message), generation, claimId, ownerEpoch,
                claimSequence, new DestinationLaneId(lane), laneIncarnation, laneControlVersion, runtimeLaneVersion,
                owner, store, precondition, timeline, runtimeRevision, instanceDigest, sourceTimelineWork);
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical Claim record");
        }
        return result;
    }

    public static byte[] computeInstanceDigest(final byte[] preconditionBytes, final byte[] timelineKey,
                                               final long runtimeRevision) {
        if (runtimeRevision <= 0) {
            throw new IllegalArgumentException("runtimeRevision must be positive");
        }
        return Bytes.sha256(Bytes.utf8("nereus-delay-timeline-work-instance-v1\0"), preconditionBytes,
                Bytes.lp32(timelineKey), Bytes.u64be(runtimeRevision));
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ClaimRecord that)) {
            return false;
        }
        return delayMessageId.equals(that.delayMessageId) && generation == that.generation
                && ownerEpoch == that.ownerEpoch && claimSequence == that.claimSequence
                && laneControlVersion == that.laneControlVersion && runtimeLaneVersion == that.runtimeLaneVersion
                && runtimeRevision == that.runtimeRevision && laneId.equals(that.laneId)
                && Arrays.equals(claimId, that.claimId) && Arrays.equals(laneIncarnation, that.laneIncarnation)
                && Arrays.equals(ownerIdentity, that.ownerIdentity)
                && Arrays.equals(storeIncarnation, that.storeIncarnation)
                && Arrays.equals(preconditionBytes, that.preconditionBytes)
                && Arrays.equals(timelineKey, that.timelineKey) && Arrays.equals(instanceDigest, that.instanceDigest)
                && Arrays.equals(sourceTimelineWork, that.sourceTimelineWork);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(delayMessageId, generation, ownerEpoch, claimSequence, laneId,
                laneControlVersion, runtimeLaneVersion, runtimeRevision);
        result = 31 * result + Arrays.hashCode(claimId);
        result = 31 * result + Arrays.hashCode(laneIncarnation);
        result = 31 * result + Arrays.hashCode(ownerIdentity);
        result = 31 * result + Arrays.hashCode(storeIncarnation);
        result = 31 * result + Arrays.hashCode(preconditionBytes);
        result = 31 * result + Arrays.hashCode(timelineKey);
        result = 31 * result + Arrays.hashCode(instanceDigest);
        result = 31 * result + Arrays.hashCode(sourceTimelineWork);
        return result;
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
            throw new IllegalArgumentException("Claim record is truncated");
        }
    }

    private static byte[] readFixed(final ByteBuffer input, final int length, final String name) {
        requireRemaining(input, length);
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static byte[] readLp32(final ByteBuffer input, final String name) {
        requireRemaining(input, Integer.BYTES);
        final long length = Integer.toUnsignedLong(input.getInt());
        if (length > Integer.MAX_VALUE || length > input.remaining() - 0L) {
            throw new IllegalArgumentException("invalid Claim " + name + " length");
        }
        return readFixed(input, (int) length, name);
    }

    private static int readU32Int(final ByteBuffer input, final String name) {
        requireRemaining(input, Integer.BYTES);
        final long value = Integer.toUnsignedLong(input.getInt());
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " exceeds Java int range");
        }
        return (int) value;
    }

    private static long readU64(final ByteBuffer input, final String name) {
        requireRemaining(input, Long.BYTES);
        return input.getLong();
    }
}
