package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Grant scope over every accounting incarnation; the Route registry supplies the immutable tenant binding. */
public final class TargetQuotaScope {
    public static final int VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = 2 + 22 + 34 + 34 + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-scope\0");

    private final ShardId shard;
    private final byte[] tenantScope;
    private final TargetPartitionId target;
    private final byte[] digest;

    public TargetQuotaScope(final ShardId shard, final byte[] tenantScope, final TargetPartitionId target) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.tenantScope = TargetCompatibilityCodec.assigned(tenantScope, 32, "tenantScope");
        this.target = target;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public ShardId shard() {
        return shard;
    }

    public byte[] tenantScope() {
        return Bytes.copy(tenantScope);
    }

    public TargetPartitionId target() {
        return target;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public TargetQuotaScope shardScope() {
        return target == null ? this : new TargetQuotaScope(shard, tenantScope, null);
    }

    public TargetQuotaScope forTarget(final TargetPartitionId value) {
        return new TargetQuotaScope(shard, tenantScope, Objects.requireNonNull(value, "target"));
    }

    public void requireRoute(final ShardId sourceShard, final byte[] authenticatedTenantScope) {
        if (!shard.equals(sourceShard) || !Arrays.equals(tenantScope, authenticatedTenantScope)) {
            throw new IllegalStateException("Target quota scope disagrees with the authenticated Route");
        }
    }

    /** Fixed key suffix; tenant is retained even though one Route can authorize only one tenant. */
    public byte[] keySuffix() {
        return Bytes.concat(
                new byte[] {(byte) (target == null ? 1 : 2)},
                TargetQuotaIdentity.shardBytes(shard),
                tenantScope,
                target == null ? new byte[0] : target.bytes());
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, TargetQuotaIdentity.shardBytes(shard));
            CanonicalProtobuf.bytes(out, 3, tenantScope);
            if (target != null) {
                CanonicalProtobuf.bytes(out, 4, target.bytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 5, digest);
        });
    }

    public static TargetQuotaScope decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 5, false, "TargetQuotaScope");
        final boolean hasTarget = fields.size() == 5;
        QueryCodecSupport.requireNumbers(
                fields, hasTarget ? new int[] {1, 2, 3, 4, 5} : new int[] {1, 2, 3, 5}, "TargetQuotaScope");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Target quota scope version");
        }
        final var result = new TargetQuotaScope(
                TargetQuotaIdentity.shard(QueryCodecSupport.fixed(fields.get(1), 2, 20)),
                QueryCodecSupport.fixed(fields.get(2), 3, 32),
                hasTarget ? new TargetPartitionId(QueryCodecSupport.fixed(fields.get(3), 4, 32)) : null);
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.getLast(), 5, 32))) {
            throw new IllegalArgumentException("Target quota scope digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetQuotaScope");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetQuotaScope that
                && shard.equals(that.shard)
                && Arrays.equals(tenantScope, that.tenantScope)
                && Objects.equals(target, that.target);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
