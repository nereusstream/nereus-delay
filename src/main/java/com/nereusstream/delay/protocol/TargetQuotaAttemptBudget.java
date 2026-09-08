package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/**
 * Frozen attempt charges and reserve-to-retained transitions.
 * Store and release authority are supplied by the committer.
 */
public final class TargetQuotaAttemptBudget {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 28;
    public static final int MAX_CANONICAL_BYTES = 2
            + 3
            + TargetMessageLocator.MAX_CANONICAL_BYTES
            + 3 * 34
            + 2
            + TargetQuotaAccounting.MAX_CANONICAL_BYTES
            + 11
            + 2 * (3 + TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES)
            + 2
            + 11
            + 2 * (4 + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES)
            + 34
            + 18
            + 35;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-attempt-budget\0");

    public enum Phase {
        ADMITTED(1),
        UNKNOWN(2),
        RESOLVED_AWAITING_FLOOR(3),
        RETAINED(4),
        RELEASED(5);

        private final int wire;

        Phase(final int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }

        private static Phase fromWire(final int wire) {
            for (var phase : values()) {
                if (phase.wire == wire) {
                    return phase;
                }
            }
            throw new IllegalArgumentException("unknown Target attempt budget phase");
        }
    }

    public enum ReleaseKind {
        UNUSED_RESERVATION,
        RETAINED_ALLOCATION
    }

    /**
     * Trusted source-committer seam. It must validate current catalog ancestry/pins, exact ledger ownership,
     * no remaining reserved writer, and (for retained release) actual guarded deletion. Its snapshot must stay
     * valid through the same atomic source commit. A Floor DTO or a callback alone is not this authority.
     */
    @FunctionalInterface
    public interface ReleaseAuthority {
        void requireAuthorized(
                TargetQuotaAttemptBudget prior,
                ReleaseKind kind,
                RecoveryFloorRef floor,
                CapacityVector nextAllocated,
                TargetQuotaMutation mutation);
    }

    private final TargetMessageLocator locator;
    private final byte[] tenantScope;
    private final byte[] publishAttemptId;
    private final byte[] admissionDigest;
    private final TargetQuotaAccounting accounting;
    private final long executionBytes;
    private final CapacityVector commitment;
    private final CapacityVector allocated;
    private final Phase phase;
    private final long revision;
    private final TargetQuotaMutation mutation;
    private final TargetQuotaMutation resolvedAt;
    private final byte[] floorDigest;
    private final byte[] recoveryLineage;
    private final byte[] digest;

    private TargetQuotaAttemptBudget(
            final TargetMessageLocator locator,
            final byte[] tenantScope,
            final byte[] publishAttemptId,
            final byte[] admissionDigest,
            final TargetQuotaAccounting accounting,
            final long executionBytes,
            final CapacityVector commitment,
            final CapacityVector allocated,
            final Phase phase,
            final long revision,
            final TargetQuotaMutation mutation,
            final TargetQuotaMutation resolvedAt,
            final byte[] floorDigest,
            final byte[] recoveryLineage) {
        this.locator = Objects.requireNonNull(locator, "locator");
        this.tenantScope = TargetCompatibilityCodec.assigned(tenantScope, 32, "tenantScope");
        this.publishAttemptId = TargetCompatibilityCodec.assigned(publishAttemptId, 32, "publishAttemptId");
        this.admissionDigest = TargetCompatibilityCodec.assigned(admissionDigest, 32, "admissionDigest");
        this.accounting = Objects.requireNonNull(accounting, "accounting");
        this.recoveryLineage = TargetCompatibilityCodec.assigned(recoveryLineage, 16, "recoveryLineage");
        this.commitment = requireReserve(commitment);
        this.allocated = requireReserve(allocated);
        this.phase = Objects.requireNonNull(phase, "phase");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        mutation.requireSourceApplied();
        if (executionBytes < 0
                || commitment.isZero()
                || !commitment.covers(allocated)
                || revision == 0
                || Long.compareUnsigned(revision, mutation.sequence()) > 0
                || !locator.messageId()
                        .routingId()
                        .shardId()
                        .equals(mutation.source().shardId())) {
            throw new IllegalArgumentException("invalid Target attempt charge/revision/source");
        }
        final boolean resolved = phase.wire >= Phase.RESOLVED_AWAITING_FLOOR.wire;
        final int minimumRevision =
                switch (phase) {
                    case ADMITTED -> 1;
                    case UNKNOWN, RESOLVED_AWAITING_FLOOR -> 2;
                    case RETAINED -> 3;
                    case RELEASED -> 4;
                };
        if (resolved != (resolvedAt != null)
                || (phase.wire >= Phase.RETAINED.wire) != (floorDigest != null)
                || (phase == Phase.RELEASED && !allocated.isZero())
                || (phase == Phase.ADMITTED && revision != 1)
                || Long.compareUnsigned(revision, minimumRevision) < 0) {
            throw new IllegalArgumentException("Target attempt phase/closure/floor fields disagree");
        }
        if (resolvedAt != null) {
            if (phase == Phase.RESOLVED_AWAITING_FLOOR && mutation.sequence() == resolvedAt.sequence()) {
                if (!mutation.equals(resolvedAt)) {
                    throw new IllegalArgumentException("attempt resolution stamp metadata mismatch");
                }
            } else {
                mutation.requireAfter(resolvedAt);
            }
        }
        this.executionBytes = executionBytes;
        this.revision = revision;
        this.resolvedAt = resolvedAt;
        if (resolvedAt != null) {
            resolvedAt.requireSourceApplied();
        }
        this.floorDigest =
                floorDigest == null ? null : TargetCompatibilityCodec.assigned(floorDigest, 32, "floorDigest");
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public static TargetQuotaAttemptBudget admit(
            final TargetMessageLocator locator,
            final byte[] tenantScope,
            final byte[] publishAttemptId,
            final byte[] admissionDigest,
            final TargetQuotaAccounting accounting,
            final long executionBytes,
            final CapacityVector commitment,
            final CapacityVector allocated,
            final TargetQuotaMutation mutation,
            final byte[] recoveryLineage) {
        return new TargetQuotaAttemptBudget(
                locator,
                tenantScope,
                publishAttemptId,
                admissionDigest,
                accounting,
                executionBytes,
                commitment,
                allocated,
                Phase.ADMITTED,
                1,
                mutation,
                null,
                null,
                recoveryLineage);
    }

    private static CapacityVector requireReserve(final CapacityVector value) {
        Objects.requireNonNull(value, "reserve");
        for (CapacityDimension dimension : CapacityDimension.values()) {
            final int n = dimension.wireValue();
            if (!(n == 3 || (n >= 9 && n <= 15)) && value.amount(dimension) != 0) {
                throw new IllegalArgumentException("not an attempt outcome-reserve dimension: " + dimension);
            }
        }
        return value;
    }

    public TargetMessageLocator locator() {
        return locator;
    }

    public byte[] tenantScope() {
        return Bytes.copy(tenantScope);
    }

    public byte[] publishAttemptId() {
        return Bytes.copy(publishAttemptId);
    }

    public byte[] admissionDigest() {
        return Bytes.copy(admissionDigest);
    }

    public TargetQuotaAccounting accounting() {
        return accounting;
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

    public Phase phase() {
        return phase;
    }

    public long revision() {
        return revision;
    }

    public TargetQuotaMutation mutation() {
        return mutation;
    }

    public TargetQuotaMutation resolvedAt() {
        return resolvedAt;
    }

    public byte[] floorDigest() {
        return floorDigest == null ? null : Bytes.copy(floorDigest);
    }

    public byte[] recoveryLineage() {
        return Bytes.copy(recoveryLineage);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public TargetQuotaIdentity primaryIdentity() {
        return new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TARGET,
                mutation.source().shardId(),
                locator.accountingIncarnation(),
                locator.target(),
                null);
    }

    public TargetQuotaIdentity tenantIdentity() {
        return new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TENANT_TARGET,
                mutation.source().shardId(),
                locator.accountingIncarnation(),
                locator.target(),
                tenantScope);
    }

    /** Allocated records consume their reservation; adding both allocated and commitment would double count. */
    public CapacityVector effectiveCharge() {
        if (phase == Phase.RELEASED) {
            return CapacityVector.empty();
        }
        final long[] result = (phase == Phase.RETAINED ? allocated : commitment).amounts();
        if (phase == Phase.ADMITTED || phase == Phase.UNKNOWN) {
            result[CapacityDimension.INFLIGHT_MESSAGES.wireValue() - 1] = 1;
            result[CapacityDimension.INFLIGHT_BYTES.wireValue() - 1] = executionBytes;
        }
        return new CapacityVector(result);
    }

    /** An UNKNOWN result or logical timeout cannot lower the committed reserve or release the attempt. */
    public TargetQuotaAttemptBudget unknown(final CapacityVector nextAllocated, final TargetQuotaMutation stamp) {
        requireOpen();
        return next(nextAllocated, Phase.UNKNOWN, stamp, null, null);
    }

    /** The source committer must have verified the exact attempt's definitive evidence and Outcome before calling. */
    public TargetQuotaAttemptBudget resolve(
            final EvidenceVerificationStatus evidence,
            final CapacityVector nextAllocated,
            final TargetQuotaMutation stamp) {
        requireOpen();
        if (evidence != EvidenceVerificationStatus.VERIFIED_PUBLISHED
                && evidence != EvidenceVerificationStatus.VERIFIED_NOT_PUBLISHED) {
            throw new IllegalArgumentException("unresolved evidence cannot release attempt execution quota");
        }
        return next(nextAllocated, Phase.RESOLVED_AWAITING_FLOOR, stamp, stamp, null);
    }

    public TargetQuotaAttemptBudget retainAfterCheckpoint(
            final RecoveryFloorRef floor, final TargetQuotaMutation stamp, final ReleaseAuthority authority) {
        if (phase != Phase.RESOLVED_AWAITING_FLOOR) {
            throw new IllegalStateException("attempt is not awaiting checkpoint-safe reserve transfer");
        }
        requireFloor(floor, stamp);
        Objects.requireNonNull(authority, "releaseAuthority")
                .requireAuthorized(this, ReleaseKind.UNUSED_RESERVATION, floor, allocated, stamp);
        return next(allocated, Phase.RETAINED, stamp, resolvedAt, floor.floorDigest());
    }

    /** Records a source-validated final writer allocation while its complete reservation is still held. */
    public TargetQuotaAttemptBudget recordResolvedAllocation(
            final CapacityVector nextAllocated, final TargetQuotaMutation stamp) {
        if (phase != Phase.RESOLVED_AWAITING_FLOOR) {
            throw new IllegalStateException("resolved writer allocation requires an unreleased outcome reservation");
        }
        return next(nextAllocated, phase, stamp, resolvedAt, null);
    }

    public TargetQuotaAttemptBudget releaseRetained(
            final CapacityVector nextAllocated,
            final RecoveryFloorRef floor,
            final TargetQuotaMutation stamp,
            final ReleaseAuthority authority) {
        if (phase != Phase.RETAINED
                || !allocated.covers(nextAllocated)
                || (allocated.equals(nextAllocated) && !allocated.isZero())) {
            throw new IllegalStateException("retained release must decrease a retained allocation");
        }
        requireFloor(floor, stamp);
        Objects.requireNonNull(authority, "releaseAuthority")
                .requireAuthorized(this, ReleaseKind.RETAINED_ALLOCATION, floor, nextAllocated, stamp);
        return next(
                nextAllocated,
                nextAllocated.isZero() ? Phase.RELEASED : Phase.RETAINED,
                stamp,
                resolvedAt,
                floor.floorDigest());
    }

    private void requireOpen() {
        if (phase != Phase.ADMITTED && phase != Phase.UNKNOWN) {
            throw new IllegalStateException("attempt quota is already resolved");
        }
    }

    private void requireFloor(final RecoveryFloorRef floor, final TargetQuotaMutation stamp) {
        Objects.requireNonNull(floor, "floor");
        stamp.requireAfter(mutation);
        mutation.requireCoveredByFloor(floor);
        if (!Arrays.equals(recoveryLineage, floor.recoveryLineageId())
                || floor.appliedSourcePosition().compareTo(stamp.source()) >= 0
                || Long.compareUnsigned(floor.includedMutationSequence(), stamp.sequence()) >= 0) {
            throw new IllegalStateException("Floor does not cover the exact charge within the current source/sequence");
        }
    }

    private TargetQuotaAttemptBudget next(
            final CapacityVector nextAllocated,
            final Phase nextPhase,
            final TargetQuotaMutation stamp,
            final TargetQuotaMutation closure,
            final byte[] safeFloor) {
        stamp.requireAfter(mutation);
        return new TargetQuotaAttemptBudget(
                locator,
                tenantScope,
                publishAttemptId,
                admissionDigest,
                accounting,
                executionBytes,
                commitment,
                nextAllocated,
                nextPhase,
                TargetQuotaMutation.increment(revision),
                stamp,
                closure,
                safeFloor,
                recoveryLineage);
    }

    public byte[] key() {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT}, publishAttemptId);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, locator.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, tenantScope);
            CanonicalProtobuf.bytes(out, 4, publishAttemptId);
            CanonicalProtobuf.bytes(out, 5, admissionDigest);
            CanonicalProtobuf.bytes(out, 6, accounting.canonicalBytes());
            CanonicalProtobuf.uint64(out, 7, executionBytes);
            CanonicalProtobuf.bytes(out, 8, commitment.canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, allocated.canonicalBytes());
            CanonicalProtobuf.uint32(out, 10, phase.wireValue());
            CanonicalProtobuf.uint64Bits(out, 11, revision);
            CanonicalProtobuf.bytes(out, 12, mutation.canonicalBytes());
            if (resolvedAt != null) {
                CanonicalProtobuf.bytes(out, 13, resolvedAt.canonicalBytes());
            }
            if (floorDigest != null) {
                CanonicalProtobuf.bytes(out, 14, floorDigest);
            }
            CanonicalProtobuf.bytes(out, 15, recoveryLineage);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 16, digest);
        });
    }

    public static TargetQuotaAttemptBudget decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 16, false, "TargetQuotaAttemptBudget");
        if (fields.size() < 14 || QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target attempt budget version/field set");
        }
        final var phase = Phase.fromWire(QueryCodecSupport.uint32(fields.get(9), 10));
        final int[] numbers = phase.wire >= Phase.RETAINED.wire
                ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
                : phase == Phase.RESOLVED_AWAITING_FLOOR
                        ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16}
                        : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16};
        QueryCodecSupport.requireNumbers(fields, numbers, "TargetQuotaAttemptBudget");
        final var result = new TargetQuotaAttemptBudget(
                TargetMessageLocator.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.fixed(fields.get(2), 3, 32),
                QueryCodecSupport.fixed(fields.get(3), 4, 32),
                QueryCodecSupport.fixed(fields.get(4), 5, 32),
                TargetQuotaAccounting.decode(QueryCodecSupport.bytes(fields.get(5), 6)),
                QueryCodecSupport.uint(fields.get(6), 7),
                CapacityVector.decode(QueryCodecSupport.bytes(fields.get(7), 8)),
                CapacityVector.decode(QueryCodecSupport.bytes(fields.get(8), 9)),
                phase,
                QueryCodecSupport.uint64Bits(fields.get(10), 11),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(11), 12)),
                phase.wire >= Phase.RESOLVED_AWAITING_FLOOR.wire
                        ? TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(12), 13))
                        : null,
                phase.wire >= Phase.RETAINED.wire ? QueryCodecSupport.fixed(fields.get(13), 14, 32) : null,
                QueryCodecSupport.fixed(fields.get(fields.size() - 2), 15, 16));
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.getLast(), 16, 32))) {
            throw new IllegalArgumentException("Target attempt budget digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetQuotaAttemptBudget");
        return result;
    }

    public static TargetQuotaAttemptBudget decodeForStore(final byte[] key, final byte[] encoded, final ShardId shard) {
        final var result = decode(encoded);
        if (!result.mutation.source().shardId().equals(shard) || !Arrays.equals(key, result.key())) {
            throw new IllegalArgumentException("Target attempt budget key/source mismatch");
        }
        return result;
    }
}
