package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Per-Target sum of primary counters across incarnations; never added to the shard aggregate again. */
public final class TargetQuotaTotal {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 29;
    public static final int MAX_CANONICAL_BYTES = 2
            + 3
            + TargetQuotaScope.MAX_CANONICAL_BYTES
            + 3
            + TargetQuotaUsage.MAX_CANONICAL_BYTES
            + 11
            + 4
            + TargetQuotaMutation.MAX_CANONICAL_BYTES
            + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-total\0");

    private final TargetQuotaScope scope;
    private final TargetQuotaUsage usage;
    private final long revision;
    private final TargetQuotaMutation mutation;
    private final byte[] digest;

    public TargetQuotaTotal(
            final TargetQuotaScope scope,
            final TargetQuotaUsage usage,
            final long revision,
            final TargetQuotaMutation mutation) {
        this.scope = Objects.requireNonNull(scope, "scope");
        this.usage = Objects.requireNonNull(usage, "usage");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        if (scope.target() == null
                || revision == 0
                || !scope.shard().equals(mutation.source().shardId())) {
            throw new IllegalArgumentException("invalid Target quota total scope/revision/source");
        }
        requireTargetUsage(usage);
        this.revision = revision;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    static void requireTargetUsage(final TargetQuotaUsage usage) {
        if (usage.targets() > 1 || usage.executionDomains() > 64) {
            throw new IllegalArgumentException("Target quota total duplicates a queue or exceeds its domain slots");
        }
        for (CapacityDimension dimension : CapacityDimension.values()) {
            if (dimension.wireValue() >= 51 && usage.resources().amount(dimension) != 0) {
                throw new IllegalArgumentException("unattributed shard records cannot be charged to a Target");
            }
        }
    }

    public TargetQuotaScope scope() {
        return scope;
    }

    public TargetQuotaUsage usage() {
        return usage;
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

    public byte[] key() {
        return Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_TOTAL_TAG, TargetKeyCodec.KEY_FORMAT}, scope.keySuffix());
    }

    /** A net-zero primary transfer still binds the changed leaf revisions to this mutation. */
    public TargetQuotaTotal advance(final TargetQuotaUsage next, final TargetQuotaMutation stamp) {
        stamp.requireStoreSuccessorOf(mutation);
        return new TargetQuotaTotal(scope, next, TargetQuotaMutation.increment(revision), stamp);
    }

    public void requireCounter(final TargetQuotaCounter counter) {
        if (counter.identity().kind() != TargetQuotaIdentity.Kind.TARGET
                || !scope.shard().equals(counter.identity().shard())
                || !scope.target().equals(counter.identity().target())) {
            throw new IllegalStateException("counter is outside the Target total");
        }
        requireNotAfter(counter.revision(), counter.mutation(), revision, mutation);
        requireCovered(usage, counter.usage());
    }

    public void requireAggregate(final TargetQuotaAggregate aggregate) {
        if (!scope.shard().equals(aggregate.shard()) || aggregate.mutation() == null) {
            throw new IllegalStateException("Target total is outside the shard aggregate");
        }
        requireNotAfter(revision, mutation, aggregate.revision(), aggregate.mutation());
        requireCovered(aggregate.usage(), usage);
    }

    private static void requireCovered(final TargetQuotaUsage parent, final TargetQuotaUsage child) {
        if (!parent.resources().covers(child.resources())
                || parent.targets() < child.targets()
                || parent.executionDomains() < child.executionDomains()
                || parent.strictOrderDomains() < child.strictOrderDomains()
                || parent.accountingIncarnations() < child.accountingIncarnations()) {
            throw new IllegalStateException("Target quota parent does not cover its primary child usage");
        }
    }

    private static void requireNotAfter(
            final long childRevision,
            final TargetQuotaMutation child,
            final long parentRevision,
            final TargetQuotaMutation parent) {
        if (Long.compareUnsigned(childRevision, parentRevision) > 0) {
            throw new IllegalStateException("Target quota total revision disagrees");
        }
        child.requireAtOrBefore(parent);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, usage.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 4, revision);
            CanonicalProtobuf.bytes(out, 5, mutation.canonicalBytes());
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 6, digest);
        });
    }

    public static TargetQuotaTotal decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "TargetQuotaTotal");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6}, "TargetQuotaTotal");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Target quota total version");
        }
        final var result = new TargetQuotaTotal(
                TargetQuotaScope.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                TargetQuotaUsage.decode(QueryCodecSupport.bytes(fields.get(2), 3)),
                QueryCodecSupport.uint64Bits(fields.get(3), 4),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(4), 5)));
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.getLast(), 6, 32))) {
            throw new IllegalArgumentException("Target quota total digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetQuotaTotal");
        return result;
    }

    public static TargetQuotaTotal decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId shard, final byte[] authenticatedTenantScope) {
        final var result = decode(encoded);
        result.scope.requireRoute(shard, authenticatedTenantScope);
        if (!Arrays.equals(key, result.key())) {
            throw new IllegalArgumentException("Target quota total key mismatch");
        }
        return result;
    }
}
