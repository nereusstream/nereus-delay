package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.TargetOrderState;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Source-derived accounting origin and one-way ingress drain; retirement requires the actual protected ledger. */
public final class TargetQuotaIncarnation {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 33;
    public static final int MAX_CANONICAL_BYTES = 2
            + 3
            + TargetQuotaIdentity.MAX_CANONICAL_BYTES
            + 34
            + 2
            + TargetQuotaAccounting.MAX_CANONICAL_BYTES
            + 2 * (4 + TargetQuotaMutation.MAX_CANONICAL_BYTES)
            + 18
            + 34;
    private static final byte[] ID_DOMAIN = Bytes.utf8("nereus-delay-target-quota-incarnation-id\0");
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-incarnation\0");

    /** Verifies the complete accepted source body, grant/registry and actual absent/prior Store record. */
    @FunctionalInterface
    public interface StateAuthority {
        void requireAuthorized(TargetQuotaIncarnation prior, TargetQuotaIncarnation next);
    }

    /**
     * Checks actual independent ledgers, latest references/writers, source replay, pins, query/retention and
     * catalog lineage. All records and permissions must remain valid under the Store guard through commit.
     * Structural counter/Floor objects alone are not retirement authority.
     */
    @FunctionalInterface
    public interface RetirementAuthority {
        void requireAuthorized(
                TargetQuotaIncarnation incarnation,
                TargetQuotaCounter primary,
                TargetQuotaCounter mirror,
                TargetQuotaBookkeeping root,
                TargetQuotaAggregate aggregate,
                RecoveryFloorRef floor,
                TargetQuotaMutation deletion);
    }

    private final TargetQuotaIdentity identity;
    private final byte[] tenant;
    private final TargetQuotaAccounting accounting;
    private final TargetQuotaMutation allocation;
    private final byte[] lineage;
    private final TargetQuotaMutation drain;
    private final byte[] digest;
    private final TargetQuotaUsage contribution;

    private TargetQuotaIncarnation(
            final TargetQuotaIdentity identity,
            final byte[] tenant,
            final TargetQuotaAccounting accounting,
            final TargetQuotaMutation allocation,
            final byte[] lineage,
            final TargetQuotaMutation drain) {
        this.identity = Objects.requireNonNull(identity, "identity");
        this.tenant = TargetCompatibilityCodec.assigned(tenant, 32, "tenantRoutingScope");
        this.accounting = Objects.requireNonNull(accounting, "accounting");
        this.allocation = Objects.requireNonNull(allocation, "allocation");
        this.lineage = TargetCompatibilityCodec.assigned(lineage, 16, "recoveryLineage");
        this.drain = drain;
        if (identity.kind().isMirror()
                || !identity.shard().equals(allocation.source().shardId())
                || !Arrays.equals(
                        identity.accountingIncarnation(), allocationId(scope(), accounting, lineage, allocation))) {
            throw new IllegalArgumentException("incarnation identity differs from its complete source origin");
        }
        if (drain != null) {
            drain.requireAfter(allocation);
        }
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
        final long payloadBound = MAX_CANONICAL_BYTES
                - 2L * TargetSourcePosition.MAX_CANONICAL_BYTES
                + 2L * TargetQuotaBookkeeping.maximumSourceBytes(allocation.source());
        contribution = new TargetQuotaUsage(
                accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE, key().length, payloadBound),
                0,
                0,
                0,
                1);
    }

    public static TargetQuotaIncarnation allocate(
            final TargetQuotaScope scope,
            final TargetQuotaAccounting accounting,
            final byte[] lineage,
            final TargetQuotaMutation allocation,
            final StateAuthority authority) {
        final var identity = new TargetQuotaIdentity(
                scope.target() == null ? TargetQuotaIdentity.Kind.SHARD : TargetQuotaIdentity.Kind.TARGET,
                scope.shard(),
                allocationId(scope, accounting, lineage, allocation),
                scope.target(),
                null);
        final var next =
                new TargetQuotaIncarnation(identity, scope.tenantScope(), accounting, allocation, lineage, null);
        Objects.requireNonNull(authority, "allocationAuthority").requireAuthorized(null, next);
        return next;
    }

    private static byte[] allocationId(
            final TargetQuotaScope scope,
            final TargetQuotaAccounting accounting,
            final byte[] lineage,
            final TargetQuotaMutation allocation) {
        final byte[] input = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, accounting.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, TargetCompatibilityCodec.assigned(lineage, 16, "recoveryLineage"));
            CanonicalProtobuf.bytes(out, 5, allocation.canonicalBytes());
        });
        return TargetCompatibilityCodec.assigned(
                Arrays.copyOf(Bytes.sha256(ID_DOMAIN, input), 16), 16, "source-derived accounting incarnation");
    }

    public TargetQuotaIdentity identity() {
        return identity;
    }

    public TargetQuotaIdentity tenantIdentity() {
        return new TargetQuotaIdentity(
                identity.target() == null
                        ? TargetQuotaIdentity.Kind.TENANT_SHARD
                        : TargetQuotaIdentity.Kind.TENANT_TARGET,
                identity.shard(),
                identity.accountingIncarnation(),
                identity.target(),
                tenant);
    }

    public TargetQuotaScope scope() {
        return new TargetQuotaScope(identity.shard(), tenant, identity.target());
    }

    public byte[] tenantScope() {
        return Bytes.copy(tenant);
    }

    public TargetQuotaAccounting accounting() {
        return accounting;
    }

    public TargetQuotaMutation allocation() {
        return allocation;
    }

    public TargetQuotaMutation latestMutation() {
        return drain == null ? allocation : drain;
    }

    public byte[] recoveryLineage() {
        return Bytes.copy(lineage);
    }

    public boolean draining() {
        return drain != null;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public void requireNewIngress(final TargetQuotaAccounting acceptedAccounting) {
        if (draining() || !Arrays.equals(accounting.canonicalBytes(), acceptedAccounting.canonicalBytes())) {
            throw new IllegalStateException("incarnation is draining or uses another accounting artifact");
        }
    }

    public TargetQuotaIncarnation drain(final TargetQuotaMutation mutation, final StateAuthority authority) {
        if (draining()) {
            throw new IllegalStateException("incarnation already entered its irreversible drain phase");
        }
        final var next = new TargetQuotaIncarnation(identity, tenant, accounting, allocation, lineage, mutation);
        Objects.requireNonNull(authority, "drainAuthority").requireAuthorized(this, next);
        return next;
    }

    /** Fixed record envelope reserves the future drain source and never recursively embeds its own usage. */
    public TargetQuotaUsage ownContribution() {
        return contribution;
    }

    public TargetQuotaUsage tenantContribution() {
        return new TargetQuotaUsage(ownContribution().resources(), 0, 0, 0, 0);
    }

    /** Closed/paused queues retain their identity; only actual removal releases its Target/domain contribution. */
    public TargetQuotaUsage queueContribution(
            final byte[] key,
            final byte[] typedPayload,
            final CanonicalTargetPartition physicalIdentity,
            final int maximumDomains) {
        final var queue =
                TargetQueueState.decodeForStore(key, typedPayload, physicalIdentity, identity.shard(), maximumDomains);
        requireTarget(queue.targetId(), queue.accountingIncarnation());
        final long occupied = queue.domains().stream()
                .filter(domain -> domain.lifecycle() != TargetDomainState.Lifecycle.VACANT)
                .count();
        return new TargetQuotaUsage(
                accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, typedPayload.length),
                1,
                occupied,
                0,
                0);
    }

    /** Retained CLOSED state still owns its ordering-domain identity; no live queue selection is implied. */
    public TargetQuotaUsage strictDomainContribution(final byte[] key, final byte[] typedPayload) {
        final var state = TargetOrderState.decode(typedPayload);
        requireTarget(state.target(), state.accountingIncarnation());
        if (!Arrays.equals(key, state.encodedKey()) || !identity.shard().equals(state.sourceShard())) {
            throw new IllegalArgumentException("strict domain key/source differs from its incarnation");
        }
        return new TargetQuotaUsage(
                accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, typedPayload.length),
                0,
                0,
                1,
                0);
    }

    private void requireTarget(final TargetPartitionId target, final byte[] incarnation) {
        if (!Objects.equals(identity.target(), target)
                || !Arrays.equals(identity.accountingIncarnation(), incarnation)) {
            throw new IllegalArgumentException("record does not belong to this frozen Target incarnation");
        }
    }

    public void requirePayloadOwner(final TargetQuotaPayloadOwner owner) {
        requireAttribution(
                owner.primaryIdentity(),
                owner.tenantScope(),
                owner.accounting(),
                owner.recoveryLineage(),
                owner.mutation());
    }

    public void requireAttemptBudget(final TargetQuotaAttemptBudget budget) {
        requireAttribution(
                budget.primaryIdentity(),
                budget.tenantScope(),
                budget.accounting(),
                budget.recoveryLineage(),
                budget.mutation());
    }

    private void requireAttribution(
            final TargetQuotaIdentity owner,
            final byte[] ownerTenant,
            final TargetQuotaAccounting ownerAccounting,
            final byte[] ownerLineage,
            final TargetQuotaMutation stamp) {
        if (!identity.equals(owner)
                || !Arrays.equals(tenant, ownerTenant)
                || !Arrays.equals(accounting.canonicalBytes(), ownerAccounting.canonicalBytes())
                || !Arrays.equals(lineage, ownerLineage)) {
            throw new IllegalArgumentException("record attribution differs from the complete incarnation origin");
        }
        requireAllocatedStamp(stamp);
    }

    private void requireAllocatedStamp(final TargetQuotaMutation stamp) {
        final int sourceOrder = stamp.source().compareTo(allocation.source());
        final int sequenceOrder = Long.compareUnsigned(stamp.sequence(), allocation.sequence());
        if (sourceOrder < 0
                || Integer.signum(sourceOrder) != Integer.signum(sequenceOrder)
                || (sourceOrder == 0 && !stamp.equals(allocation))) {
            throw new IllegalStateException("record predates or contradicts the incarnation allocation");
        }
    }

    public void requireRetirable(
            final TargetQuotaCounter primary,
            final TargetQuotaCounter mirror,
            final TargetQuotaBookkeeping root,
            final TargetQuotaAggregate aggregate,
            final RecoveryFloorRef floor,
            final TargetQuotaMutation deletion,
            final RetirementAuthority authority) {
        if (!draining()
                || !identity.equals(primary.identity())
                || !tenantIdentity().equals(mirror.identity())
                || !ownContribution().equals(primary.usage())
                || !tenantContribution().equals(mirror.usage())) {
            throw new IllegalStateException("incarnation is open or still has other accounted records/resources");
        }
        requireAllocatedStamp(primary.mutation());
        requireAllocatedStamp(mirror.mutation());
        root.requireRoot(aggregate);
        aggregate.requireCounter(primary);
        aggregate.requireCounter(mirror);
        deletion.requireAfter(aggregate.mutation());
        if (!aggregate
                        .usage()
                        .resources()
                        .covers(root.charge().add(ownContribution().resources()))
                || aggregate.usage().accountingIncarnations() < 2) {
            throw new IllegalStateException("aggregate does not cover the root and retiring incarnation");
        }
        if (!identity.shard().equals(root.owner().shard())
                || !Arrays.equals(tenant, root.tenantScope())
                || identity.equals(root.owner())) {
            throw new IllegalStateException("current bookkeeping root cannot retire inside the active Store");
        }
        if (!Arrays.equals(lineage, floor.recoveryLineageId())) {
            throw new IllegalStateException("incarnation retirement has another recovery lineage");
        }
        TargetSourcePosition.requireBounded(floor.appliedSourcePosition());
        requireCovered(floor, latestMutation());
        requireCovered(floor, primary.mutation());
        requireCovered(floor, mirror.mutation());
        if (floor.appliedSourcePosition().compareTo(deletion.source()) >= 0
                || Long.compareUnsigned(floor.includedMutationSequence(), deletion.sequence()) >= 0) {
            throw new IllegalStateException("incarnation deletion must follow the complete retirement Floor");
        }
        Objects.requireNonNull(authority, "retirementAuthority")
                .requireAuthorized(this, primary, mirror, root, aggregate, floor, deletion);
    }

    private static void requireCovered(final RecoveryFloorRef floor, final TargetQuotaMutation mutation) {
        final int sourceOrder = floor.appliedSourcePosition().compareTo(mutation.source());
        final int sequenceOrder = Long.compareUnsigned(floor.includedMutationSequence(), mutation.sequence());
        if (sourceOrder < 0
                || Integer.signum(sourceOrder) != Integer.signum(sequenceOrder)
                || (sourceOrder == 0
                        && !Arrays.equals(
                                floor.appliedSourcePosition().canonicalBytes(),
                                mutation.source().canonicalBytes()))) {
            throw new IllegalStateException("retirement Floor does not cover the latest complete record source");
        }
    }

    public byte[] key() {
        final byte[] key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        return key;
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, identity.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, tenant);
            CanonicalProtobuf.bytes(out, 4, accounting.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, allocation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, lineage);
            if (drain != null) {
                CanonicalProtobuf.bytes(out, 7, drain.canonicalBytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 8, digest);
        });
    }

    public static TargetQuotaIncarnation decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 8, false, "Target quota incarnation");
        final boolean draining = fields.size() == 8;
        QueryCodecSupport.requireNumbers(
                fields,
                draining ? new int[] {1, 2, 3, 4, 5, 6, 7, 8} : new int[] {1, 2, 3, 4, 5, 6, 8},
                "Target quota incarnation");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Target quota incarnation schema");
        }
        final var value = new TargetQuotaIncarnation(
                TargetQuotaIdentity.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.fixed(fields.get(2), 3, 32),
                TargetQuotaAccounting.decode(QueryCodecSupport.bytes(fields.get(3), 4)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(4), 5)),
                QueryCodecSupport.fixed(fields.get(5), 6, 16),
                draining ? TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(6), 7)) : null);
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(fields.getLast(), 8, 32))) {
            throw new IllegalArgumentException("incarnation digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target quota incarnation");
        return value;
    }

    public static TargetQuotaIncarnation decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId shard, final byte[] tenant) {
        final var value = decode(encoded);
        if (!Arrays.equals(key, value.key())
                || !value.identity.shard().equals(shard)
                || !Arrays.equals(tenant, value.tenant)) {
            throw new IllegalArgumentException("incarnation Store key/Shard/tenant mismatch");
        }
        return value;
    }
}
