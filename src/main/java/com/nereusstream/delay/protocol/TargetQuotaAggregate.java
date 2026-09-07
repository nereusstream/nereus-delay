package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Incremental sum of primary counters with its own revision and last accounting mutation. */
public final class TargetQuotaAggregate {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 27;
    public static final int MAX_CANONICAL_BYTES = 2
            + 22
            + 18
            + 3
            + TargetQuotaUsage.MAX_CANONICAL_BYTES
            + 11
            + 4
            + TargetQuotaMutation.MAX_CANONICAL_BYTES
            + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-aggregate\0");

    private final ShardId shard;
    private final byte[] accountingIncarnation;
    private final TargetQuotaUsage usage;
    private final long revision;
    private final TargetQuotaMutation mutation;
    private final byte[] digest;

    public TargetQuotaAggregate(
            final ShardId shard,
            final byte[] accountingIncarnation,
            final TargetQuotaUsage usage,
            final long revision,
            final TargetQuotaMutation mutation) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.accountingIncarnation =
                TargetCompatibilityCodec.assigned(accountingIncarnation, 16, "shardAccountingIncarnation");
        this.usage = Objects.requireNonNull(usage, "usage");
        if (mutation == null
                ? revision != 0 || !usage.isZero()
                : revision == 0
                        || Long.compareUnsigned(revision, mutation.sequence()) > 0
                        || !shard.equals(mutation.source().shardId())) {
            throw new IllegalArgumentException("invalid aggregate revision/source or nonempty genesis");
        }
        this.revision = revision;
        this.mutation = mutation;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public static TargetQuotaAggregate genesis(final ShardId shard, final byte[] accountingIncarnation) {
        return new TargetQuotaAggregate(shard, accountingIncarnation, TargetQuotaUsage.empty(), 0, null);
    }

    public ShardId shard() {
        return shard;
    }

    public byte[] accountingIncarnation() {
        return Bytes.copy(accountingIncarnation);
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
        return Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_AGGREGATE_TAG, TargetKeyCodec.KEY_FORMAT},
                TargetQuotaIdentity.shardBytes(shard));
    }

    public TargetQuotaAggregate advance(final TargetQuotaUsage next, final TargetQuotaMutation stamp) {
        stamp.requireAfter(mutation);
        return new TargetQuotaAggregate(
                shard, accountingIncarnation, next, TargetQuotaMutation.increment(revision), stamp);
    }

    /** Allows old local revisions; all last-touch stamps must still precede or equal this aggregate's stamp. */
    public void requireCounter(final TargetQuotaCounter counter) {
        final var identity = counter.identity();
        if (mutation == null
                || !shard.equals(identity.shard())
                || Long.compareUnsigned(counter.revision(), revision) > 0
                || Long.compareUnsigned(counter.mutation().sequence(), mutation.sequence()) > 0) {
            throw new IllegalStateException("counter is outside the aggregate source/sequence");
        }
        final int order = counter.mutation().source().compareTo(mutation.source());
        final boolean sameSequence = counter.mutation().sequence() == mutation.sequence();
        if (order > 0
                || (order == 0) != sameSequence
                || (sameSequence && !counter.mutation().equals(mutation))) {
            throw new IllegalStateException("counter stamp disagrees with aggregate mutation");
        }
        // Historical Target and shard accounting incarnations remain separate primary identities.
        // Their eligibility for new work is checked against the active queue/control binding by the caller.
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, TargetQuotaIdentity.shardBytes(shard));
            CanonicalProtobuf.bytes(out, 3, accountingIncarnation);
            CanonicalProtobuf.bytes(out, 4, usage.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 5, revision);
            if (mutation != null) {
                CanonicalProtobuf.bytes(out, 6, mutation.canonicalBytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 7, digest);
        });
    }

    public static TargetQuotaAggregate decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "TargetQuotaAggregate");
        final boolean stamped = fields.size() == 7;
        QueryCodecSupport.requireNumbers(
                fields,
                stamped ? new int[] {1, 2, 3, 4, 5, 6, 7} : new int[] {1, 2, 3, 4, 5, 7},
                "TargetQuotaAggregate");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Target quota aggregate version");
        }
        final var result = new TargetQuotaAggregate(
                TargetQuotaIdentity.shard(QueryCodecSupport.fixed(fields.get(1), 2, 20)),
                QueryCodecSupport.fixed(fields.get(2), 3, 16),
                TargetQuotaUsage.decode(QueryCodecSupport.bytes(fields.get(3), 4)),
                QueryCodecSupport.uint64Bits(fields.get(4), 5),
                stamped ? TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(5), 6)) : null);
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.getLast(), 7, 32))) {
            throw new IllegalArgumentException("Target quota aggregate digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetQuotaAggregate");
        return result;
    }

    public static TargetQuotaAggregate decodeForStore(final byte[] key, final byte[] encoded, final ShardId shard) {
        final var result = decode(encoded);
        if (!result.shard.equals(shard) || !Arrays.equals(key, result.key())) {
            throw new IllegalArgumentException("Target quota aggregate key/source mismatch");
        }
        return result;
    }
}
