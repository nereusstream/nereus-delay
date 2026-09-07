package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Independently encoded usage and local revision. Construction alone does not prove ledger ownership or retirement. */
public final class TargetQuotaCounter {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 26;
    public static final int MAX_CANONICAL_BYTES = 2
            + 3
            + TargetQuotaIdentity.MAX_CANONICAL_BYTES
            + 3
            + TargetQuotaUsage.MAX_CANONICAL_BYTES
            + 11
            + 4
            + TargetQuotaMutation.MAX_CANONICAL_BYTES
            + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-counter\0");

    private final TargetQuotaIdentity identity;
    private final TargetQuotaUsage usage;
    private final long revision;
    private final TargetQuotaMutation mutation;
    private final byte[] digest;

    public TargetQuotaCounter(
            final TargetQuotaIdentity identity,
            final TargetQuotaUsage usage,
            final long revision,
            final TargetQuotaMutation mutation) {
        this.identity = Objects.requireNonNull(identity, "identity");
        this.usage = Objects.requireNonNull(usage, "usage");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        if (revision == 0
                || Long.compareUnsigned(revision, mutation.sequence()) > 0
                || !identity.shard().equals(mutation.source().shardId())) {
            throw new IllegalArgumentException("counter revision/source mismatch");
        }
        requireUsageScope(identity, usage);
        this.revision = revision;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    private static void requireUsageScope(final TargetQuotaIdentity identity, final TargetQuotaUsage usage) {
        if (identity.kind().isMirror() && usage.accountingIncarnations() != 0) {
            throw new IllegalArgumentException("tenant mirror cannot allocate another accounting incarnation");
        }
        if (!identity.kind().isMirror() && usage.accountingIncarnations() != (usage.isZero() ? 0 : 1)) {
            throw new IllegalArgumentException("primary counter must retain exactly one accounting incarnation");
        }
        if (identity.kind().hasTarget()) {
            if (usage.targets() > 1 || usage.executionDomains() > TargetQueueState.MAX_DOMAIN_SLOTS) {
                throw new IllegalArgumentException("Target/domain cardinalities exceed one Target registry");
            }
            for (CapacityDimension dimension : CapacityDimension.values()) {
                if (dimension.wireValue() >= 51
                        && dimension.wireValue() <= 55
                        && usage.resources().amount(dimension) != 0) {
                    throw new IllegalArgumentException("shard control reserve is not target-owned");
                }
            }
        } else {
            if (usage.targets() != 0 || usage.executionDomains() != 0 || usage.strictOrderDomains() != 0) {
                throw new IllegalArgumentException("shard accounting cannot allocate Target/domain slots");
            }
            for (CapacityDimension dimension : CapacityDimension.values()) {
                final int wire = dimension.wireValue();
                if ((wire == 1 || wire == 2 || (wire >= 5 && wire <= 8))
                        && usage.resources().amount(dimension) != 0) {
                    throw new IllegalArgumentException("Target work must retain a Target quota identity");
                }
            }
        }
    }

    public TargetQuotaIdentity identity() {
        return identity;
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

    public TargetQuotaCounter advance(final TargetQuotaUsage next, final TargetQuotaMutation stamp) {
        stamp.requireAfter(mutation);
        if (usage.isZero() || usage.equals(next)) {
            throw new IllegalStateException("cannot revive a retired counter or rewrite unchanged quota usage");
        }
        return new TargetQuotaCounter(identity, next, TargetQuotaMutation.increment(revision), stamp);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, identity.canonicalBytes());
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

    public static TargetQuotaCounter decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "TargetQuotaCounter");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6}, "TargetQuotaCounter");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Target quota counter version");
        }
        final var result = new TargetQuotaCounter(
                TargetQuotaIdentity.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                TargetQuotaUsage.decode(QueryCodecSupport.bytes(fields.get(2), 3)),
                QueryCodecSupport.uint64Bits(fields.get(3), 4),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(4), 5)));
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.get(5), 6, 32))) {
            throw new IllegalArgumentException("Target quota counter digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetQuotaCounter");
        return result;
    }

    public static TargetQuotaCounter decodeForStore(final byte[] key, final byte[] encoded, final ShardId shard) {
        final var result = decode(encoded);
        if (!result.identity.shard().equals(shard) || !Arrays.equals(key, result.identity.key())) {
            throw new IllegalArgumentException("Target quota counter key/source mismatch");
        }
        return result;
    }
}
