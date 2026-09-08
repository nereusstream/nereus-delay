package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** Fixed storage commitments for accounting projections; a codec is not an inventory or deletion authority. */
public final class TargetQuotaBookkeeping {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 31;
    public static final int MAX_CANONICAL_BYTES = 2
            + 3
            + TargetQuotaIdentity.MAX_CANONICAL_BYTES
            + 34
            + 2
            + TargetQuotaAccounting.MAX_CANONICAL_BYTES
            + 4 * 11
            + 4
            + TargetQuotaMutation.MAX_CANONICAL_BYTES
            + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-bookkeeping\0");

    public enum Projection {
        COUNTER(103, TargetQuotaCounter.MAX_CANONICAL_BYTES),
        TOTAL(87, TargetQuotaTotal.MAX_CANONICAL_BYTES),
        GRANT_ACTIVATION(87, TargetQuotaGrantActivation.MAX_CANONICAL_BYTES);

        private final int keyBytes;
        private final int payloadBytes;

        Projection(final int keyBytes, final int payloadBytes) {
            this.keyBytes = keyBytes;
            this.payloadBytes = payloadBytes;
        }
    }

    /** Includes both root counters and all retained zero-counter records; summaries are not the inventory oracle. */
    public record Inventory(long counters, long totals, long grantActivations) {
        public Inventory {
            if (counters < 2 || totals < 0 || grantActivations < 0) {
                throw new IllegalArgumentException("bookkeeping inventory must retain both root counters");
            }
        }

        public Inventory change(final Projection projection, final long removed, final long added) {
            if (removed < 0 || added < 0) {
                throw new IllegalArgumentException("inventory changes must be nonnegative");
            }
            Objects.requireNonNull(projection, "projection");
            final long prior =
                    switch (projection) {
                        case COUNTER -> counters;
                        case TOTAL -> totals;
                        case GRANT_ACTIVATION -> grantActivations;
                    };
            if (removed > prior) {
                throw new IllegalStateException("bookkeeping inventory underflow");
            }
            final long next = Math.addExact(prior - removed, added);
            return switch (projection) {
                case COUNTER -> new Inventory(next, totals, grantActivations);
                case TOTAL -> new Inventory(counters, next, grantActivations);
                case GRANT_ACTIVATION -> new Inventory(counters, totals, next);
            };
        }
    }

    private final TargetQuotaIdentity owner;
    private final byte[] tenantScope;
    private final TargetQuotaAccounting accounting;
    private final Inventory inventory;
    private final long revision;
    private final TargetQuotaMutation mutation;
    private final int sourceBytes;
    private final CapacityVector charge;
    private final byte[] digest;

    public TargetQuotaBookkeeping(
            final TargetQuotaIdentity owner,
            final byte[] tenantScope,
            final TargetQuotaAccounting accounting,
            final Inventory inventory,
            final long revision,
            final TargetQuotaMutation mutation) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.tenantScope = TargetCompatibilityCodec.assigned(tenantScope, 32, "tenantRoutingScope");
        this.accounting = Objects.requireNonNull(accounting, "accounting");
        this.inventory = Objects.requireNonNull(inventory, "inventory");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        if (owner.kind() != TargetQuotaIdentity.Kind.SHARD
                || !owner.shard().equals(mutation.source().shardId())
                || revision == 0
                || Long.compareUnsigned(revision, mutation.sequence()) > 0) {
            throw new IllegalArgumentException("bookkeeping root owner/revision/source mismatch");
        }
        this.revision = revision;
        sourceBytes = maximumSourceBytes(mutation.source());
        long bytes = Math.addExact(
                fixedRecordBytes(22, MAX_CANONICAL_BYTES, 1, accounting),
                fixedRecordBytes(22, TargetQuotaAggregate.MAX_CANONICAL_BYTES, 1, accounting));
        bytes = Math.addExact(bytes, Math.multiplyExact(inventory.counters(), projectionBytes(Projection.COUNTER)));
        bytes = Math.addExact(bytes, Math.multiplyExact(inventory.totals(), projectionBytes(Projection.TOTAL)));
        bytes = Math.addExact(
                bytes, Math.multiplyExact(inventory.grantActivations(), projectionBytes(Projection.GRANT_ACTIVATION)));
        charge = metadataCharge(bytes);
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public TargetQuotaIdentity owner() {
        return owner;
    }

    public TargetQuotaIdentity tenantOwner() {
        return new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TENANT_SHARD, owner.shard(), owner.accountingIncarnation(), null, tenantScope);
    }

    public byte[] tenantScope() {
        return Bytes.copy(tenantScope);
    }

    public TargetQuotaAccounting accounting() {
        return accounting;
    }

    public Inventory inventory() {
        return inventory;
    }

    public long revision() {
        return revision;
    }

    public TargetQuotaMutation mutation() {
        return mutation;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    /** Charged once through the root primary counter; its tenant mirror is not added again to the aggregate. */
    public CapacityVector charge() {
        return charge;
    }

    public long projectionBytes(final Projection projection) {
        Objects.requireNonNull(projection, "projection");
        return fixedRecordBytes(
                projection.keyBytes,
                projection.payloadBytes,
                projection == Projection.GRANT_ACTIVATION ? 2 : 1,
                accounting);
    }

    /** Budget record storage is outside its commitment/allocated vectors, even after phase RELEASED. */
    public CapacityVector attemptRecordCharge(final TargetQuotaAttemptBudget budget) {
        requireSource(budget.mutation().source());
        if (!Arrays.equals(tenantScope, budget.tenantScope())) {
            throw new IllegalArgumentException("attempt budget belongs to another Route tenant");
        }
        return metadataCharge(
                fixedRecordBytes(34, TargetQuotaAttemptBudget.MAX_CANONICAL_BYTES, 2, budget.accounting()));
    }

    /** Bound depends only on immutable physical source identity; Kafka reserves the optional leader epoch. */
    public static int maximumSourceBytes(final SourcePosition source) {
        TargetSourcePosition.requireBounded(source);
        final int actual = source.canonicalBytes().length;
        return source instanceof KafkaSourcePosition kafka && kafka.leaderEpoch() == null ? actual + 4 : actual;
    }

    private long fixedRecordBytes(
            final int keyBytes,
            final int payloadBytes,
            final int sourceCopies,
            final TargetQuotaAccounting frozenAccounting) {
        final long boundedPayload = payloadBytes
                - (long) sourceCopies * TargetSourcePosition.MAX_CANONICAL_BYTES
                + (long) sourceCopies * sourceBytes;
        return frozenAccounting.storedRecordBytes(keyBytes, boundedPayload);
    }

    private static CapacityVector metadataCharge(final long bytes) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[CapacityDimension.LOGICAL_STATE_BYTES.wireValue() - 1] = bytes;
        return new CapacityVector(values);
    }

    public TargetQuotaBookkeeping advance(final Inventory next, final TargetQuotaMutation stamp) {
        stamp.requireAfter(mutation);
        if (inventory.equals(next)) {
            throw new IllegalStateException("unchanged inventory does not rewrite bookkeeping");
        }
        return new TargetQuotaBookkeeping(
                owner, tenantScope, accounting, next, TargetQuotaMutation.increment(revision), stamp);
    }

    public byte[] key() {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_BOOKKEEPING_TAG, TargetKeyCodec.KEY_FORMAT},
                TargetQuotaIdentity.shardBytes(owner.shard()));
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, owner.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, tenantScope);
            CanonicalProtobuf.bytes(out, 4, accounting.canonicalBytes());
            CanonicalProtobuf.uint64(out, 5, inventory.counters());
            CanonicalProtobuf.uint64(out, 6, inventory.totals());
            CanonicalProtobuf.uint64(out, 7, inventory.grantActivations());
            CanonicalProtobuf.uint64Bits(out, 8, revision);
            CanonicalProtobuf.bytes(out, 9, mutation.canonicalBytes());
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 10, digest);
        });
    }

    public static TargetQuotaBookkeeping decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 10, false, "Target quota bookkeeping");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, "Target quota bookkeeping");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Target quota bookkeeping version");
        }
        final var value = new TargetQuotaBookkeeping(
                TargetQuotaIdentity.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.fixed(fields.get(2), 3, 32),
                TargetQuotaAccounting.decode(QueryCodecSupport.bytes(fields.get(3), 4)),
                new Inventory(
                        QueryCodecSupport.uint(fields.get(4), 5),
                        QueryCodecSupport.uint(fields.get(5), 6),
                        QueryCodecSupport.uint(fields.get(6), 7)),
                QueryCodecSupport.uint64Bits(fields.get(7), 8),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(8), 9)));
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(fields.getLast(), 10, 32))) {
            throw new IllegalArgumentException("Target quota bookkeeping digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target quota bookkeeping");
        return value;
    }

    public static TargetQuotaBookkeeping decodeForStore(
            final byte[] key,
            final byte[] encoded,
            final TargetQuotaIdentity expectedOwner,
            final byte[] authenticatedTenantRoutingScope) {
        final var value = decode(encoded);
        if (!value.owner.equals(expectedOwner)
                || !Arrays.equals(value.tenantScope, authenticatedTenantRoutingScope)
                || !Arrays.equals(key, value.key())) {
            throw new IllegalArgumentException("Target quota bookkeeping key/root/tenant mismatch");
        }
        return value;
    }

    /** Recovery only: scans actual projection records, including retired counters. Never call on the hot path. */
    public void auditInventory(
            final TargetQuotaAggregate aggregate,
            final Collection<TargetQuotaCounter> counters,
            final Collection<TargetQuotaTotal> totals,
            final Collection<TargetQuotaGrantActivation> grants) {
        requireRoot(aggregate);
        final Set<String> keys = new HashSet<>();
        TargetQuotaCounter primary = null;
        TargetQuotaCounter mirror = null;
        for (var counter : counters) {
            requireSource(counter.mutation().source());
            requireUnique(keys, counter.identity().key());
            if (counter.identity().kind().isMirror()
                    && !Arrays.equals(counter.identity().tenantScope(), tenantScope)) {
                throw new IllegalStateException("bookkeeping inventory has a foreign tenant mirror");
            }
            if (counter.identity().equals(owner)) {
                primary = counter;
            }
            if (counter.identity().equals(tenantOwner())) {
                mirror = counter;
            }
        }
        for (var total : totals) {
            requireSource(total.mutation().source());
            total.scope().requireRoute(owner.shard(), tenantScope);
            requireUnique(keys, total.key());
        }
        for (var grant : grants) {
            requireSource(grant.mutation().source());
            grant.grant().scope().requireRoute(owner.shard(), tenantScope);
            requireUnique(keys, grant.key());
        }
        if (primary == null
                || mirror == null
                || !primary.usage().resources().covers(charge)
                || !mirror.usage().resources().covers(charge)
                || !inventory.equals(new Inventory(counters.size(), totals.size(), grants.size()))) {
            throw new IllegalStateException("bookkeeping inventory or root storage commitment mismatch");
        }
    }

    /** Checks the root incarnation and projection commitment against the actual aggregate. */
    public void requireRoot(final TargetQuotaAggregate aggregate) {
        Objects.requireNonNull(aggregate, "aggregate");
        if (!owner.shard().equals(aggregate.shard())
                || !Arrays.equals(owner.accountingIncarnation(), aggregate.accountingIncarnation())
                || aggregate.mutation() == null
                || !aggregate.usage().resources().covers(charge)) {
            throw new IllegalStateException("bookkeeping root differs from the accounting aggregate");
        }
        requireSource(aggregate.mutation().source());
        final int sequenceOrder =
                Long.compareUnsigned(mutation.sequence(), aggregate.mutation().sequence());
        final int sourceOrder = mutation.source().compareTo(aggregate.mutation().source());
        if (Integer.signum(sequenceOrder) != Integer.signum(sourceOrder)
                || (sequenceOrder == 0 && !mutation.equals(aggregate.mutation()))) {
            throw new IllegalStateException("bookkeeping and aggregate source stamps are inconsistent");
        }
    }

    private void requireSource(final SourcePosition source) {
        if (!owner.shard().equals(source.shardId())
                || !mutation.source().sameSourceIdentity(source)
                || maximumSourceBytes(source) != sourceBytes) {
            throw new IllegalArgumentException("bookkeeping source shape/identity mismatch");
        }
    }

    private static void requireUnique(final Set<String> keys, final byte[] key) {
        if (!keys.add(Bytes.hex(key))) {
            throw new IllegalStateException("duplicate accounting projection key");
        }
    }
}
