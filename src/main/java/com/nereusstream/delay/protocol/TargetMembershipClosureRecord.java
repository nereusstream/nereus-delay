package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Immutable first membership closure; later close operations retain this source position. */
public final class TargetMembershipClosureRecord {
    public static final int VALUE_TYPE = 41;
    public static final int MAX_CANONICAL_BYTES =
            TargetMembershipControlBody.MAX_CANONICAL_BYTES + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 100;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-membership-closure\0");

    private final TargetMembershipControlBody body;
    private final TargetQuotaMutation mutation;
    private final byte[] lineage;
    private final byte[] digest;

    public TargetMembershipClosureRecord(
            TargetMembershipControlBody body, TargetQuotaMutation mutation, byte[] lineage) {
        this.body = Objects.requireNonNull(body, "body");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        mutation.requireSourceApplied();
        this.lineage = TargetCompatibilityCodec.assigned(lineage, 16, "recoveryLineage");
        if (body.request().isIssue() || !body.shard().equals(mutation.source().shardId())) {
            throw new IllegalArgumentException("membership closure body/source mismatch");
        }
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public TargetMembershipControlBody body() {
        return body;
    }

    public TargetQuotaMutation mutation() {
        return mutation;
    }

    public SourcePosition closedAt() {
        return mutation.source();
    }

    public byte[] recoveryLineage() {
        return Bytes.copy(lineage);
    }

    public byte[] key() {
        return TargetKeyCodec.membershipClosure(body.request().value());
    }

    public void requireGrant(final TargetMembershipGrant grant) {
        Objects.requireNonNull(grant, "grant");
        if (!Arrays.equals(grant.digest(), body.request().value())) {
            throw new IllegalStateException("membership closure identifies another grant");
        }
        body.request().policy().requireGrant(grant);
        if (closedAt().compareTo(grant.activationSource()) <= 0) {
            throw new IllegalStateException("membership closure does not follow grant activation");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, body.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, lineage);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 5, digest);
        });
    }

    public static TargetMembershipClosureRecord decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, 5, false, "membership closure");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5}, "membership closure");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown membership closure version");
        }
        final var result = new TargetMembershipClosureRecord(
                TargetMembershipControlBody.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(2), 3)),
                QueryCodecSupport.fixed(fields.get(3), 4, 16));
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.get(4), 5, 32))) {
            throw new IllegalArgumentException("membership closure digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "membership closure");
        return result;
    }

    public static TargetMembershipClosureRecord decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId shard, final byte[] lineage) {
        final var result = decode(encoded);
        if (!Arrays.equals(key, result.key())
                || !shard.equals(result.body.shard())
                || !Arrays.equals(lineage, result.lineage)) {
            throw new IllegalStateException("membership closure Store key/Shard/lineage mismatch");
        }
        return result;
    }
}
