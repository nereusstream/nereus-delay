package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.TargetTimelineWorkRef;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Frozen accounting projection of one actual reversible Claim; never a Claim or SEND authority. */
public final class TargetQuotaClaimCharge {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 34;
    /** Matches the existing durable owner identity envelope. */
    public static final int MAX_OWNER_BYTES = 4096;

    public static final int MAX_CANONICAL_BYTES = 2
            + 34
            + 4
            + TargetTimelineWorkRef.MAX_CANONICAL_BYTES
            + 3
            + TargetQuotaIdentity.MAX_CANONICAL_BYTES
            + 34
            + 2
            + TargetQuotaAccounting.MAX_CANONICAL_BYTES
            + 3
            + MAX_OWNER_BYTES
            + 18
            + 3 * 11
            + 34
            + 4
            + TargetQuotaMutation.MAX_CANONICAL_BYTES
            + 18
            + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-claim-charge\0");

    public enum RemovalKind {
        REVOKE,
        ADMISSION,
        SOURCE_RESULT
    }

    /**
     * Requires the actual complete Claim, work, frozen execution charge, current Message/index before/after,
     * grants and Owner/Store/source guard. A null removal means creation and requires exact key absence.
     * Removal verifies actual Claim consumption/deletion, remaining obligations and exact original charge.
     * The proof and full read set must remain valid through the same atomic business/accounting commit.
     */
    @FunctionalInterface
    public interface Authority {
        void requireAuthorized(TargetQuotaClaimCharge charge, RemovalKind removal, TargetQuotaMutation operation);
    }

    private final byte[] claimId;
    private final TargetTimelineWorkRef work;
    private final TargetQuotaIdentity identity;
    private final byte[] tenant;
    private final TargetQuotaAccounting accounting;
    private final OwnerIdentity owner;
    private final byte[] storeIncarnation;
    private final long claimSequence;
    private final long deadlineEpochMs;
    private final long executionBytes;
    private final byte[] claimRecordDigest;
    private final TargetQuotaMutation creation;
    private final byte[] lineage;
    private final byte[] digest;

    public TargetQuotaClaimCharge(
            final byte[] claimId,
            final TargetTimelineWorkRef work,
            final TargetQuotaIdentity identity,
            final byte[] tenant,
            final TargetQuotaAccounting accounting,
            final OwnerIdentity owner,
            final byte[] storeIncarnation,
            final long claimSequence,
            final long deadlineEpochMs,
            final long executionBytes,
            final byte[] claimRecordDigest,
            final TargetQuotaMutation creation,
            final byte[] lineage) {
        this.claimId = TargetCompatibilityCodec.assigned(claimId, 32, "claimId");
        this.work = Objects.requireNonNull(work, "work");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.tenant = TargetCompatibilityCodec.assigned(tenant, 32, "tenantRoutingScope");
        this.accounting = Objects.requireNonNull(accounting, "accounting");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.storeIncarnation = TargetCompatibilityCodec.assigned(storeIncarnation, 16, "storeIncarnation");
        this.claimRecordDigest = TargetCompatibilityCodec.assigned(claimRecordDigest, 32, "claimRecordDigest");
        this.creation = Objects.requireNonNull(creation, "creation");
        this.lineage = TargetCompatibilityCodec.assigned(lineage, 16, "recoveryLineage");
        final var locator = work.locator();
        if (identity.kind() != TargetQuotaIdentity.Kind.TARGET
                || !identity.shard().equals(locator.messageId().routingId().shardId())
                || !identity.shard().equals(creation.source().shardId())
                || !identity.target().equals(locator.target())
                || !Arrays.equals(identity.accountingIncarnation(), locator.accountingIncarnation())
                || owner.canonicalBytes().length > MAX_OWNER_BYTES
                || claimSequence == 0
                || deadlineEpochMs < 0
                || executionBytes <= 0
                || !creation.isLocalClaim()) {
            throw new IllegalArgumentException("invalid Claim charge owner/work/source or numeric fields");
        }
        this.claimSequence = claimSequence;
        this.deadlineEpochMs = deadlineEpochMs;
        this.executionBytes = executionBytes;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public static TargetQuotaClaimCharge create(
            final TargetQuotaIncarnation descriptor,
            final TargetTimelineWorkRef work,
            final byte[] claimId,
            final OwnerIdentity owner,
            final byte[] storeIncarnation,
            final long claimSequence,
            final long deadlineEpochMs,
            final long executionBytes,
            final byte[] claimRecordDigest,
            final TargetQuotaMutation creation,
            final Authority authority) {
        Objects.requireNonNull(authority, "claimChargeAuthority");
        Objects.requireNonNull(descriptor, "descriptor");
        final var result = new TargetQuotaClaimCharge(
                claimId,
                work,
                descriptor.identity(),
                descriptor.tenantScope(),
                descriptor.accounting(),
                owner,
                storeIncarnation,
                claimSequence,
                deadlineEpochMs,
                executionBytes,
                claimRecordDigest,
                creation,
                descriptor.recoveryLineage());
        result.requireDescriptor(descriptor);
        descriptor.latestMutation().requireAtOrBefore(creation);
        authority.requireAuthorized(result, null, creation);
        return result;
    }

    /** Draining does not invalidate charges for previously accepted work. */
    public void requireDescriptor(final TargetQuotaIncarnation descriptor) {
        if (!identity.equals(descriptor.identity())
                || !Arrays.equals(tenant, descriptor.tenantScope())
                || !accounting.equals(descriptor.accounting())
                || !Arrays.equals(lineage, descriptor.recoveryLineage())) {
            throw new IllegalStateException("Claim charge lost its frozen accounting descriptor");
        }
        descriptor.allocation().requireAtOrBefore(creation);
    }

    /** Checks a local revoke under the exact Claim owner; an Owner takeover must use its separate recovery proof. */
    public void requireLocalRevoke(
            final OwnerIdentity currentOwner,
            final byte[] currentStoreIncarnation,
            final TargetQuotaMutation operation,
            final Authority authority) {
        Objects.requireNonNull(authority, "claimChargeAuthority");
        if (!owner.equals(currentOwner)
                || !Arrays.equals(storeIncarnation, currentStoreIncarnation)
                || !operation.isLocalClaim()) {
            throw new IllegalStateException("local Claim revoke changed Owner/Store or lacks local ordinal");
        }
        operation.requireStoreSuccessorOf(creation);
        authority.requireAuthorized(this, RemovalKind.REVOKE, operation);
    }

    /** Accepted source consumption may outlive the local Owner; it still needs exact business/authority proof. */
    public void requireSourceConsumption(
            final RemovalKind kind, final TargetQuotaMutation operation, final Authority authority) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(authority, "claimChargeAuthority");
        operation.requireAfter(creation);
        authority.requireAuthorized(this, kind, operation);
    }

    public byte[] claimId() {
        return Bytes.copy(claimId);
    }

    public TargetTimelineWorkRef work() {
        return work;
    }

    public TargetQuotaIdentity primaryIdentity() {
        return identity;
    }

    public TargetQuotaIdentity tenantIdentity() {
        return new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TENANT_TARGET,
                identity.shard(),
                identity.accountingIncarnation(),
                identity.target(),
                tenant);
    }

    public byte[] tenantScope() {
        return Bytes.copy(tenant);
    }

    public TargetQuotaAccounting accounting() {
        return accounting;
    }

    public OwnerIdentity owner() {
        return owner;
    }

    public byte[] storeIncarnation() {
        return Bytes.copy(storeIncarnation);
    }

    public long claimSequence() {
        return claimSequence;
    }

    public long deadlineEpochMs() {
        return deadlineEpochMs;
    }

    public long executionBytes() {
        return executionBytes;
    }

    public byte[] claimRecordDigest() {
        return Bytes.copy(claimRecordDigest);
    }

    public TargetQuotaMutation creation() {
        return creation;
    }

    public byte[] recoveryLineage() {
        return Bytes.copy(lineage);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] key() {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_CLAIM_CHARGE_TAG, TargetKeyCodec.KEY_FORMAT},
                TargetQuotaIdentity.shardBytes(identity.shard()),
                Bytes.u64beBits(owner.ownerEpoch()),
                claimId);
    }

    public CapacityVector executionCharge() {
        final long[] values = new long[CapacityDimension.COUNT];
        values[CapacityDimension.INFLIGHT_MESSAGES.wireValue() - 1] = 1;
        values[CapacityDimension.INFLIGHT_BYTES.wireValue() - 1] = executionBytes;
        return new CapacityVector(values);
    }

    public CapacityVector recordCharge() {
        return accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE, key().length, canonicalBytes().length);
    }

    public CapacityVector contribution() {
        return recordCharge().add(executionCharge());
    }

    public void requireStored(final byte[] key, final int valueType, final byte[] typedPayload) {
        if (valueType != VALUE_TYPE || !Arrays.equals(key(), key) || !Arrays.equals(canonicalBytes(), typedPayload)) {
            throw new IllegalStateException("actual Claim charge bytes changed");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, claimId);
            CanonicalProtobuf.bytes(out, 3, work.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, identity.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, tenant);
            CanonicalProtobuf.bytes(out, 6, accounting.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, owner.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, storeIncarnation);
            CanonicalProtobuf.uint64Bits(out, 9, claimSequence);
            CanonicalProtobuf.uint64(out, 10, deadlineEpochMs);
            CanonicalProtobuf.uint64(out, 11, executionBytes);
            CanonicalProtobuf.bytes(out, 12, claimRecordDigest);
            CanonicalProtobuf.bytes(out, 13, creation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 14, lineage);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 15, digest);
        });
    }

    public static TargetQuotaClaimCharge decode(final byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 15, false, "TargetQuotaClaimCharge");
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, "TargetQuotaClaimCharge");
        if (QueryCodecSupport.uint32(f.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Claim charge schema");
        }
        final byte[] ownerBytes = QueryCodecSupport.bytes(f.get(6), 7);
        if (ownerBytes.length > MAX_OWNER_BYTES) {
            throw new IllegalArgumentException("Claim charge owner exceeds its bound");
        }
        final var value = new TargetQuotaClaimCharge(
                QueryCodecSupport.fixed(f.get(1), 2, 32),
                TargetTimelineWorkRef.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                TargetQuotaIdentity.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                QueryCodecSupport.fixed(f.get(4), 5, 32),
                TargetQuotaAccounting.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                OwnerIdentity.decode(ownerBytes),
                QueryCodecSupport.fixed(f.get(7), 8, 16),
                QueryCodecSupport.uint64Bits(f.get(8), 9),
                QueryCodecSupport.uint(f.get(9), 10),
                QueryCodecSupport.uint(f.get(10), 11),
                QueryCodecSupport.fixed(f.get(11), 12, 32),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(12), 13)),
                QueryCodecSupport.fixed(f.get(13), 14, 16));
        if (!Bytes.constantTimeEquals(value.digest, QueryCodecSupport.fixed(f.get(14), 15, 32))) {
            throw new IllegalArgumentException("Claim charge digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "TargetQuotaClaimCharge");
        return value;
    }
}
