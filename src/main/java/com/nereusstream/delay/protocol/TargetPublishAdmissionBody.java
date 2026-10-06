package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.AttemptLedgerState;
import com.nereusstream.delay.runtime.AttemptObligationRef;
import com.nereusstream.delay.store.KeyCodec;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Independent Target Publish Admission schema; legacy {@code PUBLISH_ADMISSION} remains closed at v1. */
public final class TargetPublishAdmissionBody {
    /** Target-only operation-body generation, independent from the shared System Mutation envelope body. */
    public static final int BODY_VERSION = 3;
    public static final int MAX_CANONICAL_BYTES = 4096;

    private final ShardId shard;
    private final long retryUntilEpochMs;
    private final OwnerIdentity owner;
    private final byte[] storeIncarnation;
    private final byte[] claimId;
    private final TargetMessageLocator locator;
    private final int attemptNo;
    private final byte[] publishAttemptId;
    private final AttemptObligationRef obligation;
    private final long executionBytes;
    private final CapacityVector commitment;
    private final CapacityVector allocated;
    private final TrustedUtcIntervalEvidence decisionTime;

    public TargetPublishAdmissionBody(
            final ShardId shard,
            final long retryUntilEpochMs,
            final OwnerIdentity owner,
            final byte[] storeIncarnation,
            final byte[] claimId,
            final TargetMessageLocator locator,
            final int attemptNo,
            final byte[] publishAttemptId,
            final AttemptObligationRef obligation,
            final long executionBytes,
            final CapacityVector commitment,
            final CapacityVector allocated,
            final TrustedUtcIntervalEvidence decisionTime) {
        this.shard = Objects.requireNonNull(shard, "shard");
        if (retryUntilEpochMs < 0 || executionBytes < 0) {
            throw new IllegalArgumentException("invalid Target Admission retry/size bound");
        }
        this.retryUntilEpochMs = retryUntilEpochMs;
        this.owner = Objects.requireNonNull(owner, "owner");
        this.storeIncarnation = assigned(storeIncarnation, 16, "storeIncarnation");
        this.claimId = assigned(claimId, 32, "claimId");
        this.locator = Objects.requireNonNull(locator, "locator");
        if (!shard.equals(locator.messageId().routingId().shardId()) || attemptNo <= 0) {
            throw new IllegalArgumentException("Target Admission locator/attempt belongs to another Shard or attempt");
        }
        this.attemptNo = attemptNo;
        this.publishAttemptId = assigned(publishAttemptId, 32, "publishAttemptId");
        this.obligation = Objects.requireNonNull(obligation, "obligation");
        final byte[] expectedAttempt = SystemMutation.computePublishAttemptLogicalIdentity(
                this.claimId,
                locator.messageId(),
                Integer.toUnsignedLong(locator.generation()),
                Integer.toUnsignedLong(attemptNo));
        final byte[] expectedKey = KeyCodec.inflight((byte) 2, owner.ownerEpoch(), expectedAttempt);
        if (!Arrays.equals(this.publishAttemptId, expectedAttempt)
                || !Arrays.equals(obligation.publishAttemptId(), expectedAttempt)
                || obligation.generation() != locator.generation()
                || obligation.ledgerState() != AttemptLedgerState.PUBLISHING
                || !Arrays.equals(obligation.encodedInflightKey(), expectedKey)) {
            throw new IllegalArgumentException("Target Admission attempt does not match its Claim/Owner obligation");
        }
        this.executionBytes = executionBytes;
        this.commitment = Objects.requireNonNull(commitment, "commitment");
        this.allocated = Objects.requireNonNull(allocated, "allocated");
        if (!isOutcomeReserve(commitment)
                || !isOutcomeReserve(allocated)
                || commitment.isZero()
                || !commitment.covers(allocated)) {
            throw new IllegalArgumentException("Target Admission budget is empty or under-committed");
        }
        this.decisionTime = Objects.requireNonNull(decisionTime, "decisionTime");
        if (canonicalBytes().length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target Admission exceeds its canonical body bound");
        }
    }

    public ShardId shard() {
        return shard;
    }

    public long retryUntilEpochMs() {
        return retryUntilEpochMs;
    }

    public OwnerIdentity owner() {
        return owner;
    }

    public byte[] storeIncarnation() {
        return Bytes.copy(storeIncarnation);
    }

    public byte[] claimId() {
        return Bytes.copy(claimId);
    }

    public TargetMessageLocator locator() {
        return locator;
    }

    public int attemptNo() {
        return attemptNo;
    }

    public byte[] publishAttemptId() {
        return Bytes.copy(publishAttemptId);
    }

    public AttemptObligationRef obligation() {
        return obligation;
    }

    public long executionBytes() {
        return executionBytes;
    }

    public CapacityVector commitment() {
        return commitment;
    }

    public CapacityVector allocated() {
        return allocated;
    }

    public TrustedUtcIntervalEvidence decisionTime() {
        return decisionTime;
    }

    public byte[] canonicalBytes() {
        final byte[] subject = new ShardSubject(shard).canonicalBytes();
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.TARGET_PUBLISH_ADMISSION.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntilEpochMs);
            CanonicalProtobuf.uint32(output, 10, BODY_VERSION);
            CanonicalProtobuf.bytes(output, 11, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 12, storeIncarnation);
            CanonicalProtobuf.bytes(output, 13, claimId);
            CanonicalProtobuf.bytes(output, 14, locator.canonicalBytes());
            CanonicalProtobuf.uint32(output, 15, attemptNo);
            CanonicalProtobuf.bytes(output, 16, publishAttemptId);
            CanonicalProtobuf.bytes(output, 17, obligation.canonicalBytes());
            CanonicalProtobuf.uint64(output, 18, executionBytes);
            CanonicalProtobuf.bytes(output, 19, commitment.canonicalBytes());
            CanonicalProtobuf.bytes(output, 20, allocated.canonicalBytes());
            CanonicalProtobuf.bytes(output, 21, decisionTime.canonicalBytes());
        });
    }

    public static TargetPublishAdmissionBody decode(final byte[] canonicalBody) {
        if (canonicalBody == null || canonicalBody.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target Admission exceeds its canonical body bound");
        }
        final List<CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.TARGET_PUBLISH_ADMISSION, canonicalBody);
        final ShardId shard = SystemMutationBodyCodec.subjectShard(fields);
        final long retryUntil = uint(fields.get(2), 3);
        final TargetPublishAdmissionBody result = new TargetPublishAdmissionBody(
                shard,
                retryUntil,
                OwnerIdentity.decode(QueryCodecSupport.nested(fields.get(4), 11)),
                QueryCodecSupport.fixed(fields.get(5), 12, 16),
                QueryCodecSupport.fixed(fields.get(6), 13, 32),
                TargetMessageLocator.decode(QueryCodecSupport.nested(fields.get(7), 14)),
                uint32(fields.get(8), 15),
                QueryCodecSupport.fixed(fields.get(9), 16, 32),
                AttemptObligationRef.decode(QueryCodecSupport.nested(fields.get(10), 17)),
                uint(fields.get(11), 18),
                CapacityVector.decode(QueryCodecSupport.nested(fields.get(12), 19)),
                CapacityVector.decode(QueryCodecSupport.nested(fields.get(13), 20)),
                TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(fields.get(14), 21)));
        QueryCodecSupport.requireCanonical(canonicalBody, result.canonicalBytes(), "TargetPublishAdmissionBody");
        return result;
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid Target Admission scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static int uint32(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = uint(field, number);
        if (value == 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException("Target Admission field must be a positive uint32: " + number);
        }
        return (int) value;
    }

    private static byte[] assigned(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        if (Arrays.equals(value, new byte[length])) {
            throw new IllegalArgumentException(name + " is unassigned");
        }
        return Bytes.copy(value);
    }

    private static boolean isOutcomeReserve(final CapacityVector vector) {
        for (CapacityDimension dimension : CapacityDimension.values()) {
            final int wire = dimension.wireValue();
            if (wire != CapacityDimension.LOGICAL_STATE_BYTES.wireValue()
                    && (wire < CapacityDimension.RESULT_RECORDS.wireValue()
                            || wire > CapacityDimension.EVIDENCE_BYTES.wireValue())
                    && vector.amount(dimension) != 0) {
                return false;
            }
        }
        return true;
    }
}
